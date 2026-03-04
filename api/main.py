import time
import uuid
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse
import queries
from logger import get_logger


logger = get_logger("api")

app = FastAPI(
    title="Analytics Platform API",
    version="1.0.0",
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = str(uuid.uuid4())[:8]
    start = time.time()
    logger.info(f"[{request_id}] {request.method} {request.url.path} started")
    try:
        response = await call_next(request)
        ms = round((time.time() - start) * 1000, 1)
        logger.info(f"[{request_id}] {request.method} {request.url.path} status={response.status_code} duration={ms}ms")
        return response
    except Exception as e:
        ms = round((time.time() - start) * 1000, 1)
        logger.error(f"[{request_id}] Unhandled error after {ms}ms: {e}")
        return JSONResponse(status_code=500, content={"error": "Internal server error"})


@app.get("/revenue/daily")
def revenue_daily(
    start_date: str = Query(None, description="YYYY-MM-DD"),
    end_date: str   = Query(None, description="YYYY-MM-DD"),
    limit: int      = Query(30, ge=1, le=365),
):
    try:
        data = queries.get_daily_revenue(start_date, end_date, limit)
        return {"count": len(data), "data": data}
    except Exception as e:
        logger.error(f"/revenue/daily failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch daily revenue")


@app.get("/revenue/category")
def revenue_category():
    try:
        data = queries.get_revenue_by_category()
        return {"count": len(data), "data": data}
    except Exception as e:
        logger.error(f"/revenue/category failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch category revenue")


@app.get("/users/top")
def top_users(limit: int = Query(10, ge=1, le=100)):
    try:
        data = queries.get_top_users(limit)
        return {"count": len(data), "data": data}
    except Exception as e:
        logger.error(f"/users/top failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch top users")


@app.get("/metrics/health")
def health():
    try:
        return queries.get_health_metrics()
    except Exception as e:
        logger.error(f"/metrics/health failed: {e}")
        raise HTTPException(status_code=503, detail="Health check failed")