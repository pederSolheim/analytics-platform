-- benchmarks/index_experiments.sql
-- Run each block manually. Record EXPLAIN ANALYZE output before and after.

-- ============================================================
-- EXPERIMENT 1: Index on transactions.created_at
-- ============================================================

-- BEFORE: run benchmark_runner.py, note daily_revenue_full_scan time

CREATE INDEX CONCURRENTLY idx_transactions_created_at
    ON transactions(created_at);

-- AFTER: run benchmark_runner.py again, compare

-- ============================================================
-- EXPERIMENT 2: Index on transactions.user_id
-- ============================================================

CREATE INDEX CONCURRENTLY idx_transactions_user_id
    ON transactions(user_id);

-- ============================================================
-- EXPERIMENT 3: Index on transactions.product_id
-- ============================================================

CREATE INDEX CONCURRENTLY idx_transactions_product_id
    ON transactions(product_id);

-- ============================================================
-- EXPERIMENT 4: Composite index for category revenue query
-- ============================================================

CREATE INDEX CONCURRENTLY idx_transactions_product_amount
    ON transactions(product_id, amount);

-- ============================================================
-- EXPERIMENT 5: Partial index — last 90 days only
-- ============================================================

CREATE INDEX CONCURRENTLY idx_transactions_recent
    ON transactions(created_at)
    WHERE created_at > NOW() - INTERVAL '90 days';

-- ============================================================
-- CHECK index sizes
-- ============================================================

SELECT
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size
FROM pg_indexes
WHERE tablename = 'transactions'
ORDER BY pg_relation_size(indexname::regclass) DESC;