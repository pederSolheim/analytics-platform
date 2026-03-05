"""
pipeline.py — Transformation pipeline with failure recovery.

Each job:
- Runs inside a transaction (atomic)
- Is idempotent (safe to re-run after a crash)
- Logs start, finish, row count, and duration
- Rolls back cleanly on failure

Usage:
    python pipeline.py              # run all jobs
    python pipeline.py daily        # run one job
    python pipeline.py --fail daily # inject a failure mid-job (for testing)
"""

import sys
import time
import logging
from db import transaction, fetch_all, log


# ── Timing decorator ──────────────────────────────────────────────────────────
def timed_job(fn):
    """Log job name, duration, and outcome."""
    import functools
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        job = fn.__name__
        log.info(f'"job_start": "{job}"')
        t0 = time.perf_counter()
        try:
            result = fn(*args, **kwargs)
            elapsed = time.perf_counter() - t0
            log.info(f'"job_complete": "{job}", "duration_ms": {elapsed*1000:.1f}')
            return result
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            log.error(f'"job_failed": "{job}", "duration_ms": {elapsed*1000:.1f}, "error": "{exc}"')
            raise
    return wrapper


# ── Job 1: Daily revenue ──────────────────────────────────────────────────────
@timed_job
def job_daily_revenue(inject_failure: bool = False):
    """
    Aggregate revenue per day into daily_revenue table.
    Idempotent: DELETE then INSERT for affected dates.
    """
    with transaction() as cur:
        # Step 1: find dates to refresh
        cur.execute("""
            SELECT DISTINCT DATE(transaction_date) AS d
            FROM transactions
            ORDER BY d
        """)
        dates = [row[0] for row in cur.fetchall()]
        log.info(f'"daily_revenue: refreshing {len(dates)} dates"')

        # Step 2: delete stale aggregates
        cur.execute("DELETE FROM daily_revenue")

        if inject_failure:
            raise RuntimeError("Injected failure after DELETE, before INSERT")

        # Step 3: recompute and insert
        cur.execute("""
            INSERT INTO daily_revenue (date, total_revenue, transaction_count)
            SELECT
                DATE(transaction_date),
                ROUND(SUM(amount)::numeric, 2),
                COUNT(*)
            FROM transactions
            GROUP BY DATE(transaction_date)
        """)
        cur.execute("SELECT COUNT(*) FROM daily_revenue")
        count = cur.fetchone()[0]
        log.info(f'"daily_revenue: inserted {count} rows"')


# ── Job 2: Category revenue ───────────────────────────────────────────────────
@timed_job
def job_category_revenue(inject_failure: bool = False):
    """
    Aggregate revenue per category per day into category_revenue table.
    """
    with transaction() as cur:
        cur.execute("DELETE FROM category_revenue")

        if inject_failure:
            raise RuntimeError("Injected failure mid category_revenue job")

        cur.execute("""
            INSERT INTO category_revenue (date, category, revenue, transaction_count)
            SELECT
                DATE(t.transaction_date),
                p.category,
                ROUND(SUM(t.amount)::numeric, 2),
                COUNT(*)
            FROM transactions t
            JOIN products p ON p.product_id = t.product_id
            GROUP BY DATE(t.transaction_date), p.category
        """)
        cur.execute("SELECT COUNT(*) FROM category_revenue")
        count = cur.fetchone()[0]
        log.info(f'"category_revenue: inserted {count} rows"')


# ── Job 3: User lifetime value ────────────────────────────────────────────────
@timed_job
def job_user_ltv(inject_failure: bool = False):
    """
    Compute lifetime value per user. Upserts into user_ltv table.
    """
    with transaction() as cur:
        cur.execute("DELETE FROM user_ltv")

        if inject_failure:
            raise RuntimeError("Injected failure mid user_ltv job")

        cur.execute("""
            INSERT INTO user_ltv (user_id, total_revenue, transaction_count, first_transaction, last_transaction)
            SELECT
                user_id,
                ROUND(SUM(amount)::numeric, 2),
                COUNT(*),
                MIN(transaction_date),
                MAX(transaction_date)
            FROM transactions
            GROUP BY user_id
        """)
        cur.execute("SELECT COUNT(*) FROM user_ltv")
        count = cur.fetchone()[0]
        log.info(f'"user_ltv: inserted {count} rows"')


# ── Runner ────────────────────────────────────────────────────────────────────

JOBS = {
    "daily":    job_daily_revenue,
    "category": job_category_revenue,
    "ltv":      job_user_ltv,
}

if __name__ == "__main__":
    args = sys.argv[1:]
    inject = "--fail" in args
    args = [a for a in args if a != "--fail"]

    targets = args if args else list(JOBS.keys())

    for name in targets:
        if name not in JOBS:
            print(f"Unknown job: {name}. Available: {', '.join(JOBS)}")
            sys.exit(1)

    total_start = time.perf_counter()
    errors = []

    for name in targets:
        try:
            JOBS[name](inject_failure=inject)
        except Exception as exc:
            errors.append((name, str(exc)))
            log.error(f'"Pipeline error in job {name}: {exc}"')

    elapsed = time.perf_counter() - total_start

    if errors:
        log.error(f'"Pipeline finished with {len(errors)} error(s) in {elapsed:.2f}s"')
        for name, err in errors:
            log.error(f'"  {name}: {err}"')
        sys.exit(1)
    else:
        log.info(f'"Pipeline complete. All jobs succeeded in {elapsed:.2f}s"')