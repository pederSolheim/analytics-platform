# Analytics Data Platform

A production-style analytics pipeline built from scratch in Python and PostgreSQL.

## What It Does

Generates synthetic transaction data, stores it in PostgreSQL, transforms it into
analytics-ready tables, exposes metrics via a REST API, and runs automatically on a schedule.

## Architecture
```
Data Generator → PostgreSQL → Transformation Layer → Analytics Tables → FastAPI → User
                                      ↓
                                  Scheduler
                                      ↓
                                  Monitoring
```

## Stack

- Python 3.14
- PostgreSQL
- FastAPI
- APScheduler
- psycopg2

## Schema

- `users` — 42,000 synthetic users
- `products` — 100 products across 5 categories
- `transactions` — 20M+ rows
- `daily_revenue` — aggregated daily totals
- `analytics_category_revenue` — revenue by product category
- `analytics_user_ltv` — user lifetime value

## Running the API
```bash
source venv/bin/activate
uvicorn api.main:app --reload
```

Endpoints:
- `GET /revenue/daily` — daily revenue, last 90 days
- `GET /revenue/category` — revenue by product category
- `GET /users/top` — top users by lifetime value
- `GET /metrics/health` — system health check

## Running the Pipeline
```bash
python pipeline.py
```

## Running the Scheduler
```bash
PIPELINE_INTERVAL_MINUTES=60 ROWS_PER_RUN=5000 python scheduler.py
```

## Loading Data at Scale
```bash
python loaders/copy_loader.py 1000000
```

Uses PostgreSQL COPY for ~80,000 rows/sec sustained throughput.

## Benchmarks

See [benchmarks/BENCHMARKS.md](benchmarks/BENCHMARKS.md) for full results at 10M and 20M rows,
including index experiments and query plan analysis.

## Phases Built

| Phase | Description |
|---|---|
| 1 | Data generation — users, products, transactions |
| 2 | Transformation layer — daily revenue, category revenue, user LTV |
| 3 | Performance stress testing at 5M, 10M rows |
| 4 | FastAPI service with 4 endpoints |
| 5 | Failure recovery — retries, rollbacks, structured logging |
| 6 | Monitoring — runtime metrics, health endpoint, metrics.csv |
| 7 | Scheduling — automated pipeline via APScheduler |
| 8 | Scaling — 20M rows, COPY loader, index experiments |