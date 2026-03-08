# benchmarks/benchmark_runner.py
import time
import csv
import os
import psutil
import psycopg2
from datetime import datetime

DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost/analytics")
BENCHMARK_LOG = "benchmark_log.csv" 

QUERIES = {
    "daily_revenue_full_scan": """
        SELECT DATE(created_at), SUM(amount)
        FROM transactions
        GROUP BY 1
        ORDER BY 1;
    """,
    "category_revenue": """
        SELECT p.category, SUM(t.amount)
        FROM transactions t
        JOIN products p ON t.product_id = p.product_id
        GROUP BY p.category;
    """,
    "top_users": """
        SELECT user_id, SUM(amount) as total
        FROM transactions
        GROUP BY user_id
        ORDER BY total DESC
        LIMIT 100;
    """,
    "rolling_7day": """
        SELECT DATE(created_at),
               SUM(SUM(amount)) OVER (
                   ORDER BY DATE(created_at)
                   ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
               ) AS rolling_7d
        FROM transactions
        GROUP BY DATE(created_at)
        ORDER BY 1;
    """
}

def get_row_count(cur):
    cur.execute("SELECT COUNT(*) FROM transactions;")
    return cur.fetchone()[0]

def run_explain_analyze(cur, query):
    cur.execute(f"EXPLAIN ANALYZE {query}")
    rows = cur.fetchall()
    # Extract actual total time from last planning line
    for row in reversed(rows):
        line = row[0]
        if "Execution Time" in line:
            return float(line.split(":")[1].strip().replace(" ms", ""))
    return None

def log_result(label, row_count, elapsed_ms, memory_mb):
    file_exists = os.path.exists(BENCHMARK_LOG)
    with open(BENCHMARK_LOG, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "label", "row_count", "elapsed_ms", "memory_mb"])
        writer.writerow([
            datetime.utcnow().isoformat(),
            label,
            row_count,
            round(elapsed_ms, 2),
            round(memory_mb, 2)
        ])
    print(f"  [{label}] {elapsed_ms:.0f}ms | {memory_mb:.1f}MB RAM | {row_count:,} rows")

def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    process = psutil.Process()
    row_count = get_row_count(cur)
    print(f"\nRunning benchmarks on {row_count:,} rows...\n")

    for label, query in QUERIES.items():
        mem_before = process.memory_info().rss / 1024 / 1024
        elapsed_ms = run_explain_analyze(cur, query)
        mem_after = process.memory_info().rss / 1024 / 1024
        log_result(label, row_count, elapsed_ms, mem_after - mem_before)

    conn.close()
    print(f"\nResults written to {BENCHMARK_LOG}")

if __name__ == "__main__":
    main()