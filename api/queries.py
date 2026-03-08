# queries.py — SQL constants only, no functions, no imports

DAILY_REVENUE = """
    SELECT
        revenue_date,
        total_revenue,
        transaction_count,
        ROUND((total_revenue / NULLIF(transaction_count, 0))::numeric, 2) AS avg_transaction_value
    FROM daily_revenue
"""

CATEGORY_REVENUE_TOTAL = """
    SELECT category, ROUND(SUM(total_revenue)::numeric, 2) AS revenue, SUM(order_count) AS order_count
    FROM analytics_category_revenue
    GROUP BY category
    ORDER BY revenue DESC
"""

TOP_USERS = """
    SELECT user_id, total_spent, order_count, avg_order_value, first_purchase, last_purchase
    FROM analytics_user_ltv
    ORDER BY total_spent DESC
"""

HEALTH_CHECK = """
    SELECT
        (SELECT COUNT(*) FROM transactions) AS transaction_count,
        (SELECT COUNT(*) FROM users) AS user_count,
        (SELECT MAX(created_at) FROM transactions) AS latest_transaction,
        (SELECT COUNT(*) FROM daily_revenue) AS daily_revenue_rows
"""