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
from db import transaction, fetch_all, log
import monitor

# ── Timing decorator ──────────────────────────────────────────────────────────
def timed_job(job_name):
    def decorator(fn):
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            errors = 0
            rows = 0
            try:
                result = fn(*args, **kwargs)
                # Convention: jobs return row count, or None
                rows = result or 0
            except Exception as e:
                errors = 1
                log.error({"job": job_name, "error": str(e)})
                raise
            finally:
                duration = time.perf_counter() - start
                monitor.record_job(job_name, duration, rows, errors)
                log.info({"job": job_name, "duration_s": round(duration, 3), "rows": rows})
            return rows
        return wrapper
    return decorator


# ── Job 1: Daily revenue ──────────────────────────────────────────────────────
@timed_job("daily_revenue")
def job_daily_revenue(inject_failure: bool = False):
    """
    Aggregate revenue per day into daily_revenue table.
    Idempotent: DELETE then INSERT for affected dates.
    """
    with transaction() as cur:
        # Step 1: find dates to refresh
        cur.execute("""
            SELECT DISTINCT DATE(created_at) AS d
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
            INSERT INTO daily_revenue (revenue_date, total_revenue, transaction_count)
            SELECT
                DATE(created_at),
                ROUND(SUM(amount)::numeric, 2),
                COUNT(*)
            FROM transactions
            GROUP BY DATE(created_at)
        """)
        cur.execute("SELECT COUNT(*) FROM daily_revenue")
        count = cur.fetchone()[0]
        log.info(f'"daily_revenue: inserted {count} rows"')
        return cur.rowcount


# ── Job 2: Category revenue ───────────────────────────────────────────────────
@timed_job("category_revenue")
def job_category_revenue(inject_failure: bool = False):
    """
    Aggregate revenue per category per day into category_revenue table.
    """
    with transaction() as cur:
        cur.execute("DELETE FROM analytics_category_revenue")

        if inject_failure:
            raise RuntimeError("Injected failure mid category_revenue job")

        cur.execute("""
            INSERT INTO analytics_category_revenue (category, total_revenue, order_count, avg_order_value, last_updated)
            SELECT
                p.category,
                ROUND(SUM(t.amount)::numeric, 2),
                COUNT(*),
                ROUND(AVG(t.amount)::numeric, 2),
                NOW()
            FROM transactions t
            JOIN products p ON p.product_id = t.product_id
            GROUP BY p.category;
        """)
        cur.execute("SELECT COUNT(*) FROM analytics_category_revenue")
        count = cur.fetchone()[0]
        log.info(f'"analytics_category_revenue: inserted {count} rows"')
        return cur.rowcount


# ── Job 3: User lifetime value ────────────────────────────────────────────────
@timed_job("analytics_user_ltv")
def job_user_ltv(inject_failure: bool = False):
    """
    Compute lifetime value per user. Upserts into analytics_user_ltv table.
    """
    with transaction() as cur:
        cur.execute("DELETE FROM analytics_user_ltv;")

        if inject_failure:
            raise RuntimeError("Injected failure mid user_ltv job")

        cur.execute("""
            INSERT INTO analytics_user_ltv (user_id, total_spent, order_count, avg_order_value, first_purchase, last_purchase, last_updated)
            SELECT
                user_id,
                ROUND(SUM(amount)::numeric, 2),
                COUNT(*),
                ROUND(AVG(amount)::numeric, 2),
                MIN(created_at),
                MAX(created_at),
                NOW()
            FROM transactions
            GROUP BY user_id;
        """)
        cur.execute("SELECT COUNT(*) FROM analytics_user_ltv")
        count = cur.fetchone()[0]
        log.info(f'"analytics_user_ltv: inserted {count} rows"')
        return cur.rowcount


def run_all(dsn: str = None) -> dict:
    results = {}
    for name, fn in [("daily_revenue", job_daily_revenue),
                     ("category_revenue", job_category_revenue),
                     ("user_ltv", job_user_ltv)]:
        try:
            fn()
            results[name] = "ok"
        except Exception as e:
            results[name] = str(e)
    return results

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