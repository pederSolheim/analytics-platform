# loaders/copy_loader.py
import io
import os
import time
import random
import psycopg2
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost/analytics")
BATCH_SIZE = 50_000

def get_existing_ids(cur):
    cur.execute("SELECT user_id FROM users;")
    user_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id, price FROM products;")
    products = [(r[0], float(r[1])) for r in cur.fetchall()]
    return user_ids, products

def generate_transaction_batch(user_ids, products, batch_size):
    """Yields CSV-formatted lines for COPY."""
    buf = io.StringIO()
    now = datetime.utcnow()
    for _ in range(batch_size):
        user_id = random.choice(user_ids)
        product_id, price = random.choice(products)
        quantity = random.randint(1, 5)
        amount = round(price * quantity, 2)
        created_at = now - timedelta(
            days=random.randint(0, 730),
            seconds=random.randint(0, 86400)
        )
        buf.write(f"{user_id}\t{product_id}\t{quantity}\t{amount}\t{created_at}\n")
    buf.seek(0)
    return buf

def load_transactions(target_rows: int):
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    print("Fetching existing user/product IDs...")
    user_ids, products = get_existing_ids(cur)

    print(f"Loading {target_rows:,} rows in batches of {BATCH_SIZE:,}...")
    total_inserted = 0
    start = time.perf_counter()

    while total_inserted < target_rows:
        batch_size = min(BATCH_SIZE, target_rows - total_inserted)
        buf = generate_transaction_batch(user_ids, products, batch_size)

        cur.copy_from(
            buf,
            "transactions",
            columns=("user_id", "product_id", "quantity", "amount", "created_at")
        )
        conn.commit()

        total_inserted += batch_size
        elapsed = time.perf_counter() - start
        rate = total_inserted / elapsed
        print(f"  {total_inserted:>12,} rows | {rate:>10,.0f} rows/sec | {elapsed:.1f}s elapsed")

    elapsed = time.perf_counter() - start
    print(f"\nDone. {total_inserted:,} rows in {elapsed:.1f}s ({total_inserted/elapsed:,.0f} rows/sec)")
    cur.close()
    conn.close()

if __name__ == "__main__":
    import sys
    target = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000_000
    load_transactions(target)