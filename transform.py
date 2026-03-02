# transform.py
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


def run():
    log.info("=== Transform job started ===")
    start = datetime.now()

    try:
        conn = get_connection()
        conn.autocommit = False  # explicit transaction control

        try:
            rows_written = aggregate_daily_revenue(conn)
            conn.commit()
            log.info(f"Transaction committed. {rows_written} rows upserted.")
        except Exception as e:
            conn.rollback()
            log.error(f"Transform failed, rolled back. Error: {e}")
            raise
        finally:
            conn.close()

    except Exception as e:
        log.critical(f"Job failed: {e}")
        raise

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"=== Transform job finished in {elapsed:.2f}s ===")


if __name__ == "__main__":
    run()