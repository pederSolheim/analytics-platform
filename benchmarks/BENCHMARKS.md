# Benchmark Results

## Environment
- Machine: MacBook (Apple Silicon)
- PostgreSQL: local
- Rows: 20,100,400

## Query Performance by Scale

| Query | 10M rows | 20M rows |
|---|---|---|
| daily_revenue_full_scan | 2000ms | 4495ms |
| category_revenue | 585ms | 1190ms |
| top_users | 623ms | 1398ms |
| rolling_7day | 2030ms | 4655ms |

## Index Experiments

### idx_transactions_user_id — DROPPED
- top_users regressed from 1398ms → 16052ms after adding this index
- Cause: query aggregates all rows per user, planner chose Parallel Index Scan over Seq Scan
- Result: random I/O across full table, 12x slower
- Lesson: indexes on high-cardinality columns used in full-table GROUP BY aggregations are counterproductive

### idx_transactions_created_at — KEPT
- Helps date-range queries on transactions
- 499MB index size

### idx_transactions_product_amount — KEPT
- 606MB index size

## Key Findings
- Scaling 10M → 20M is linear (~2x) for full table scans — expected
- Indexes do not always improve performance
- Measure with EXPLAIN ANALYZE before keeping any index
