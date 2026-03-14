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
    amount          NUMERIC(10, 2) NOT NULL CHECK (amount > 0),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE daily_revenue (
    revenue_date  DATE PRIMARY KEY,
    total_revenue NUMERIC(12, 2) NOT NULL,
    transaction_count INT NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics_category_revenue (
    category        VARCHAR(100) PRIMARY KEY,
    total_revenue   NUMERIC(15,2) NOT NULL,
    order_count     INTEGER NOT NULL,
    avg_order_value NUMERIC(10,2) NOT NULL,
    last_updated    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics_user_ltv (
    user_id         INTEGER PRIMARY KEY,
    total_spent     NUMERIC(15,2) NOT NULL,
    order_count     INTEGER NOT NULL,
    avg_order_value NUMERIC(10,2) NOT NULL,
    first_purchase  TIMESTAMP,
    last_purchase   TIMESTAMP,
    last_updated    TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_transactions_created_at ON transactions(created_at);
CREATE INDEX idx_transactions_product_id ON transactions(product_id);
CREATE INDEX idx_transactions_product_amount ON transactions(product_id, amount);
CREATE INDEX idx_user_ltv_total_spent ON analytics_user_ltv(total_spent DESC);