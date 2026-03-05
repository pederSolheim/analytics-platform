"""
main.py — FastAPI service with structured logging and error handling.

New in Phase 5:
- Every request logged: method, path, status, duration
- DB errors return 503 (not 500) — signals "retry later"
- Validation errors return 422 with clean messages
- No raw exceptions leak to the client
"""

import time
import logging
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse
import psycopg2

from db import fetch_all, get_pool, log
from queries import (
    DAILY_REVENUE,
    CATEGORY_REVENUE,
    TOP_USERS,
    HEALTH_CHECK,
)


# ── App setup ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info('"API starting up. Initialising connection pool."')
    get_pool()   # warm the pool at startup
    yield
    log.info('"API shutting down."')

app = FastAPI(title="Analytics API", version="2.0.0", lifespan=lifespan)


# ── Request logging middleware ────────────────────────────────────────────────
@app.middleware("http")
async def log_requests(request: Request, call_next):
    t0 = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    log.info(
        f'"method": "{request.method}", '
        f'"path": "{request.url.path}", '
        f'"status": {response.status_code}, '
        f'"duration_ms": {elapsed_ms:.1f}'
    )
    return response


# ── Error handlers ────────────────────────────────────────────────────────────
@app.exception_handler(psycopg2.OperationalError)
async def db_connection_error(request: Request, exc: psycopg2.OperationalError):
    log.error(f'"DB connection error on {request.url.path}: {exc}"')
    return JSONResponse(
        status_code=503,
        content={"error": "Database unavailable. Try again shortly."},
    )

@app.exception_handler(Exception)
async def unhandled_error(request: Request, exc: Exception):
    log.error(f'"Unhandled error on {request.url.path}: {exc}\n{traceback.format_exc()}"')
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error."},
    )


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/revenue/daily")
def revenue_daily(
    start: str = Query(None, description="Start date YYYY-MM-DD"),
    end:   str = Query(None, description="End date YYYY-MM-DD"),
):
    """
    Daily revenue totals.
    Optional: ?start=2024-01-01&end=2024-03-31
    """
    try:
        if start and end:
            rows = fetch_all(DAILY_REVENUE + " WHERE date BETWEEN %s AND %s ORDER BY date", (start, end))
        elif start:
            rows = fetch_all(DAILY_REVENUE + " WHERE date >= %s ORDER BY date", (start,))
        else:
            rows = fetch_all(DAILY_REVENUE + " ORDER BY date DESC LIMIT 90")
        return {"count": len(rows), "data": rows}
    except psycopg2.Error as exc:
        log.error(f'"DB error in /revenue/daily: {exc}"')
        raise HTTPException(status_code=503, detail="Database error")


@app.get("/revenue/category")
def revenue_category(
    date: str = Query(None, description="Filter by date YYYY-MM-DD"),
):
    """Revenue breakdown by product category."""
    try:
        if date:
            rows = fetch_all(CATEGORY_REVENUE + " WHERE date = %s ORDER BY revenue DESC", (date,))
        else:
            rows = fetch_all(
                """
                SELECT category, ROUND(SUM(revenue)::numeric, 2) AS revenue, SUM(transaction_count) AS transaction_count
                FROM category_revenue
                GROUP BY category
                ORDER BY revenue DESC
                """
            )
        return {"count": len(rows), "data": rows}
    except psycopg2.Error as exc:
        log.error(f'"DB error in /revenue/category: {exc}"')
        raise HTTPException(status_code=503, detail="Database error")


@app.get("/users/top")
def users_top(limit: int = Query(10, ge=1, le=100, description="Number of users to return")):
    """Top users by lifetime value."""
    try:
        rows = fetch_all(TOP_USERS + " LIMIT %s", (limit,))
        return {"count": len(rows), "data": rows}
    except psycopg2.Error as exc:
        log.error(f'"DB error in /users/top: {exc}"')
        raise HTTPException(status_code=503, detail="Database error")


@app.get("/metrics/health")
def metrics_health():
    """
    System health check.
    Returns row counts and latest transaction date.
    Returns 503 if DB is unreachable.
    """
    try:
        rows = fetch_all(HEALTH_CHECK)
        return {"status": "ok", "data": rows[0] if rows else {}}
    except psycopg2.OperationalError as exc:
        log.error(f'"Health check failed: {exc}"')
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "error": "Database unreachable"},
        )