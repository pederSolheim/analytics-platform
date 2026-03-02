CREATE TABLE users (
    user_id     SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(150) UNIQUE NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE products (
    product_id  SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    category    VARCHAR(50) NOT NULL,
    price       NUMERIC(10, 2) NOT NULL
);

CREATE TABLE transactions (
    transaction_id  SERIAL PRIMARY KEY,
    user_id         INT REFERENCES users(user_id),
    product_id      INT REFERENCES products(product_id),
    quantity        INT NOT NULL,
    amount          NUMERIC(10, 2) NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE daily_revenue (
    date        DATE PRIMARY KEY,
    total_revenue NUMERIC(12, 2) NOT NULL,
    transaction_count INT NOT NULL
);
