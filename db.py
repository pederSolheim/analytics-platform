"""
db.py — Resilient database connection layer.

Responsibilities:
- Connection pooling via psycopg2
- Retry logic with exponential backoff
- Structured logging for every failure
- Context manager for safe transactions
"""

import time
import logging
import functools
import psycopg2
import psycopg2.pool
from contextlib import contextmanager
import os 
from dotenv import load_dotenv
load_dotenv()


# ── Structured logger ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "module": "%(module)s", "msg": %(message)s}',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.getenv("DB_HOST", "localhost"),
    "port":            int(os.getenv("DB_PORT", "5432")),
    "dbname":          os.getenv("DB_NAME", "analytics"),
    "user":            os.getenv("DB_USER"),       # no default
    "password":        os.getenv("DB_PASSWORD"),   # no default
    "connect_timeout": 5,
}

# ── Connection pool (created once at import) ──────────────────────────────────
_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            **DB_CONFIG,
        )
        log.info('"Connection pool created"')
    return _pool


# ── Retry decorator ───────────────────────────────────────────────────────────
def with_retry(max_attempts: int = 3, base_delay: float = 1.0, backoff: float = 2.0):
    """
    Retry a function on psycopg2.OperationalError (connection failures).

    Waits: 1s → 2s → 4s (exponential backoff).
    Raises the last exception if all attempts fail.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except psycopg2.OperationalError as exc:
                    log.warning(
                        f'"DB connection error on attempt {attempt}/{max_attempts}: {exc}"'
                    )
                    if attempt == max_attempts:
                        log.error(f'"All {max_attempts} attempts failed. Giving up."')
                        raise
                    log.info(f'"Retrying in {delay:.1f}s..."')
                    time.sleep(delay)
                    delay *= backoff
        return wrapper
    return decorator


# ── Safe transaction context manager ─────────────────────────────────────────
@contextmanager
def transaction():
    """
    Yields a cursor inside a transaction.

    Commits on clean exit.
    Rolls back and re-raises on any exception.

    Usage:
        with transaction() as cur:
            cur.execute("INSERT INTO ...")
    """
    pool = get_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            yield cur
        conn.commit()
        log.info('"Transaction committed"')
    except Exception as exc:
        conn.rollback()
        log.error(f'"Transaction rolled back: {exc}"')
        raise
    finally:
        pool.putconn(conn)


# ── Single-query helper (read-only, no transaction needed) ────────────────────
@with_retry(max_attempts=3)
def fetch_all(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SELECT and return rows as dicts."""
    pool = get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        pool.putconn(conn)


    