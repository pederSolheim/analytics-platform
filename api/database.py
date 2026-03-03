import os
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from api.logger import get_logger

logger = get_logger("database")

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "analytics"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

_pool = None

def get_pool():
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(minconn=2, maxconn=10, **DB_CONFIG)
        logger.info("Connection pool created")
    return _pool

@contextmanager
def get_connection():
    p = get_pool()
    conn = p.getconn()
    try:
        yield conn
    except Exception as e:
        conn.rollback()
        logger.error(f"DB error: {e}")
        raise
    finally:
        p.putconn(conn)