import psycopg2
from faker import Faker
import random
from dotenv import load_dotenv
import os

load_dotenv()
fake = Faker()


def insert_incremental(n: int) -> int:
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        host=os.getenv("DB_HOST")
    )

    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users ORDER BY random() LIMIT 1000")
    user_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id, price FROM products")
    products = {r[0]: r[1] for r in cur.fetchall()}
    product_ids = list(products.keys())

    for _ in range(n):
        pid = random.choice(product_ids)
        qty = random.randint(1, 5)
        amount = round(float(products[pid]) * qty, 2)
        cur.execute(
            "INSERT INTO transactions (user_id, product_id, quantity, amount, created_at) VALUES (%s, %s, %s, %s, %s)",
            (random.choice(user_ids), pid, qty, amount, fake.date_time_between(start_date="-1y"))
        )
    conn.commit()
    conn.close()
    return n

 
if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        host=os.getenv("DB_HOST")
    )
    cur = conn.cursor()

    print("Inserting users...")
    for _ in range(0):
        email = f"{fake.unique.email().split('@')[0]}_{random.randint(1000,9999)}@{fake.domain_name()}"
        cur.execute(
            "INSERT INTO users (name, email, created_at) VALUES (%s, %s, %s)",
            (fake.name(), email, fake.date_time_between(start_date="-2y"))
        )
    conn.commit()

    print("Inserting products...")
    categories = ["Electronics", "Clothing", "Food", "Books", "Sports"]
    for _ in range(0):
        cur.execute(
            "INSERT INTO products (name, category, price) VALUES (%s, %s, %s)",
            (fake.word().capitalize(), random.choice(categories), round(random.uniform(5, 500), 2))
        )
    conn.commit()

    print("Inserting transactions...")
    cur.execute("SELECT user_id FROM users")
    user_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id, price FROM products")
    products = {r[0]: r[1] for r in cur.fetchall()}
    product_ids = list(products.keys())

    for _ in range(10):
        pid = random.choice(product_ids)
        qty = random.randint(1, 5)
        amount = round(float(products[pid]) * qty, 2)
        cur.execute(
            "INSERT INTO transactions (user_id, product_id, quantity, amount, created_at) VALUES (%s, %s, %s, %s, %s)",
            (random.choice(user_ids), pid, qty, amount, fake.date_time_between(start_date="-1y"))
        )

    conn.commit()
    cur.close()
    conn.close()
    print("Done.")