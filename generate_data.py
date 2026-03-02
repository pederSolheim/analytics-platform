import psycopg2
from faker import Faker
import random
from dotenv import load_dotenv
import os

load_dotenv()
fake = Faker()

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    host=os.getenv("DB_HOST")
)
cur = conn.cursor()

# Generate users
print("Inserting users...")
for _ in range(10000):
    email = f"{fake.unique.email().split('@')[0]}_{random.randint(1000,9999)}@{fake.domain_name()}"
    cur.execute(
        "INSERT INTO users (name, email, created_at) VALUES (%s, %s, %s)",
        (fake.name(), email, fake.date_time_between(start_date="-2y"))
    )

conn.commit()

# Generate products
print("Inserting products...")
categories = ["Electronics", "Clothing", "Food", "Books", "Sports"]
for _ in range(100):
    cur.execute(
        "INSERT INTO products (name, category, price) VALUES (%s, %s, %s)",
        (fake.word().capitalize(), random.choice(categories), round(random.uniform(5, 500), 2))
    )

conn.commit()

# Generate transactions
print("Inserting transactions...")
cur.execute("SELECT user_id FROM users")
user_ids = [r[0] for r in cur.fetchall()]
cur.execute("SELECT product_id FROM products")
product_ids = [r[0] for r in cur.fetchall()]

for _ in range(1000000):
    qty = random.randint(1, 5)
    cur.execute("SELECT price FROM products WHERE product_id = %s", (pid := random.choice(product_ids),))
    price = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO transactions (user_id, product_id, quantity, amount, created_at) VALUES (%s, %s, %s, %s, %s)",
        (random.choice(user_ids), pid, qty, round(float(price) * qty, 2), fake.date_time_between(start_date="-1y"))
    )

conn.commit()
cur.close()
conn.close()
print("Done. 10k transactions inserted.")