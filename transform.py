# transform.py

# Performance measurements (EXPLAIN ANALYZE on daily revenue aggregation query):
# 2M rows  → 250ms  (seq scan)
# 5M rows  → 654ms  (seq scan)
# 10M rows → 1208ms (seq scan)
#
# Index added: CREATE INDEX idx_transactions_created_at ON transactions(created_at)
# Postgres still uses seq scan for full-table aggregations — correct behavior.
# Cache warming effect observed: cold ~700ms, warm ~250ms on same 2M dataset.
# Scaling is roughly linear with row count.


# Performance measurements (EXPLAIN ANALYZE on daily revenue aggregation query):
# 2M rows  → 250ms  (seq scan)
# 5M rows  → 654ms  (seq scan)
# 10M rows → 1208ms (seq scan)
#
# Index added: CREATE INDEX idx_transactions_created_at ON transactions(created_at)
# Postgres still uses seq scan for full-table aggregations — correct behavior.
# Cache warming effect observed: cold ~700ms, warm ~250ms on same 2M dataset.
# Scaling is roughly linear with row count.
#
# Pre-aggregation applied to API layer (Phase 4):
# /revenue/category: 1779ms → 1.4ms  (moved JOIN to transform, reads analytics_category_revenue)
# /users/top:        2541ms → 3.7ms  (moved aggregation to transform, reads analytics_user_ltv)
#
# Index added: CREATE INDEX idx_user_ltv_total_spent ON analytics_user_ltv (total_spent DESC)
# /users/top: 3.7ms → 0.09ms execution time (EXPLAIN ANALYZE)
# Changed from Seq Scan (42,000 rows) to Index Scan (10 rows).


import os
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("transform.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

DB_URL = os.getenv("DATABASE_URL")  # e.g. postgresql://user:pass@localhost/analytics


def get_connection():
    return psycopg2.connect(DB_URL)


def aggregate_daily_revenue(conn):
    """
    Aggregate daily revenue from transactions into daily_revenue table.
    Idempotent: deletes existing rows for each date before re-inserting.
    """
    with conn.cursor() as cur:
        log.info("Fetching daily revenue aggregates...")

        cur.execute("""
            SELECT
                DATE(created_at)  AS revenue_date,
                SUM(amount)       AS total_revenue,
                COUNT(*)          AS transaction_count
            FROM transactions
            GROUP BY DATE(created_at)
        """)

        rows = cur.fetchall()

        if not rows:
            log.warning("No transaction data found. Skipping.")
            return 0

        dates = [r[0] for r in rows]

        log.info(f"Upserting {len(rows)} date rows into daily_revenue...")

        # Idempotent: delete existing rows for those dates first
        cur.execute(
            "DELETE FROM daily_revenue WHERE revenue_date = ANY(%s)",
            (dates,)
        )

        execute_values(cur, """
            INSERT INTO daily_revenue (revenue_date, total_revenue, transaction_count)
            VALUES %s
        """, rows)

        log.info(f"Done. Rows written: {len(rows)}")
        return len(rows)

def aggregate_category_revenue(conn):
    """
    Aggregate revenue by product category.
    Idempotent: uses ON CONFLICT DO UPDATE (upsert by category).
    """
    with conn.cursor() as cur:
        log.info("Aggregating category revenue...")

        cur.execute("""
            INSERT INTO analytics_category_revenue
                (category, total_revenue, order_count, avg_order_value, last_updated)
            SELECT
                p.category,
                SUM(t.amount),
                COUNT(t.transaction_id),
                AVG(t.amount),
                NOW()
            FROM transactions t
            JOIN products p ON t.product_id = p.product_id
            GROUP BY p.category
            ON CONFLICT (category) DO UPDATE SET
                total_revenue   = EXCLUDED.total_revenue,
                order_count     = EXCLUDED.order_count,
                avg_order_value = EXCLUDED.avg_order_value,
                last_updated    = EXCLUDED.last_updated;
        """)

        row_count = cur.rowcount
        log.info(f"Done. Rows affected: {row_count}")
        return row_count

def aggregate_user_ltv(conn):
    """
    Aggregate user lifetime value from transactions.
    Idempotent: uses ON CONFLICT DO UPDATE (upsert by user_id).
    """
    with conn.cursor() as cur:
        log.info("Aggregating user LTV...")

        cur.execute("""
            INSERT INTO analytics_user_ltv
                (user_id, total_spent, order_count, avg_order_value,
                 first_purchase, last_purchase, last_updated)
            SELECT
                user_id,
                SUM(amount),
                COUNT(transaction_id),
                AVG(amount),
                MIN(created_at),
                MAX(created_at),
                NOW()
            FROM transactions
            GROUP BY user_id
            ON CONFLICT (user_id) DO UPDATE SET
                total_spent     = EXCLUDED.total_spent,
                order_count     = EXCLUDED.order_count,
                avg_order_value = EXCLUDED.avg_order_value,
                first_purchase  = EXCLUDED.first_purchase,
                last_purchase   = EXCLUDED.last_purchase,
                last_updated    = EXCLUDED.last_updated;
        """)

        row_count = cur.rowcount
        log.info(f"Done. Rows affected: {row_count}")
        return row_count

def run_transform(label, fn):
    """Run a single transform function with its own connection, commit, and error handling."""
    log.info(f"Starting: {label}")
    start = datetime.now()
    conn = get_connection()
    conn.autocommit = False
    try:
        rows = fn(conn)
        conn.commit()
        elapsed = (datetime.now() - start).total_seconds()
        log.info(f"Done: {label} — {rows} rows in {elapsed:.2f}s")
        return rows
    except Exception as e:
        conn.rollback()
        log.error(f"Failed: {label} — rolled back. Error: {e}")
        raise
    finally:
        conn.close()


def run():
    log.info("=== Transform job started ===")
    start = datetime.now()

    run_transform("daily revenue",    aggregate_daily_revenue)
    run_transform("category revenue", aggregate_category_revenue)
    run_transform("user LTV",         aggregate_user_ltv)

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"=== Transform job finished in {elapsed:.2f}s ===")

if __name__ == "__main__":
    run()