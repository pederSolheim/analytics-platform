#queries.py
from database import get_connection
from logger import get_logger

logger = get_logger("queries")


CATEGORY_REVENUE_TOTAL = """
    SELECT category, ROUND(SUM(revenue)::numeric, 2) AS revenue, SUM(transaction_count) AS transaction_count
    FROM category_revenue
    GROUP BY category
    ORDER BY revenue DESC
"""

def get_daily_revenue(start_date=None, end_date=None, limit=30):
    with get_connection() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT
                    revenue_date,
                    total_revenue,
                    transaction_count,
                    ROUND((total_revenue / NULLIF(transaction_count, 0))::numeric, 2) AS avg_transaction_value
                FROM daily_revenue
                WHERE 1=1
            """
            params = []
            if start_date:
                query += " AND revenue_date >= %s"
                params.append(start_date)
            if end_date:
                query += " AND revenue_date <= %s"
                params.append(end_date)
            query += " ORDER BY revenue_date DESC LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            logger.info(f"daily_revenue returned {len(rows)} rows")
            return [dict(zip(columns, row)) for row in rows]


def get_revenue_by_category():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT category, total_revenue, order_count, avg_order_value, last_updated
                FROM analytics_category_revenue
                ORDER BY total_revenue DESC
            """)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            logger.info(f"revenue_by_category returned {len(rows)} rows")
            return [dict(zip(columns, row)) for row in rows]


def get_top_users(limit=10):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT user_id, total_spent, order_count, avg_order_value,
                       first_purchase, last_purchase
                FROM analytics_user_ltv
                ORDER BY total_spent DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            logger.info(f"top_users returned {len(rows)} rows")
            return [dict(zip(columns, row)) for row in rows]


def get_health_metrics():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM transactions")
            tx_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM users")
            user_count = cur.fetchone()[0]

            cur.execute("SELECT MAX(created_at) FROM transactions")
            latest_tx = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM daily_revenue")
            revenue_rows = cur.fetchone()[0]

            return {
                "status": "ok",
                "transaction_count": tx_count,
                "user_count": user_count,
                "latest_transaction": str(latest_tx),
                "daily_revenue_rows": revenue_rows,
            }