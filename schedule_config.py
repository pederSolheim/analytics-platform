"""
schedule_config.py

Central place for all scheduling configuration.
Override any value via environment variables — no code changes needed
between dev (fast intervals) and production (daily).

Usage:
    PIPELINE_INTERVAL_MINUTES=15 python scheduler.py   # dev/testing
    PIPELINE_INTERVAL_MINUTES=1440 python scheduler.py  # production (daily)
"""

import os

# ---------------------------------------------------------------------------
# Core interval — everything else derives from this
# ---------------------------------------------------------------------------

PIPELINE_INTERVAL_MINUTES: int = int(os.getenv("PIPELINE_INTERVAL_MINUTES", "60"))

# ---------------------------------------------------------------------------
# Individual job toggles — set to "0" to disable a job without removing it
# ---------------------------------------------------------------------------

RUN_DATA_GENERATION:   bool = os.getenv("RUN_DATA_GENERATION",   "1") == "1"
RUN_TRANSFORMATIONS:   bool = os.getenv("RUN_TRANSFORMATIONS",   "1") == "1"
RUN_MONITOR_SNAPSHOT:  bool = os.getenv("RUN_MONITOR_SNAPSHOT",  "1") == "1"
RUN_HEALTH_CHECK:      bool = os.getenv("RUN_HEALTH_CHECK",      "1") == "1"

# ---------------------------------------------------------------------------
# Data generation — how many rows to insert per scheduled run
# (Phase 1 bulk load is separate; this simulates ongoing activity)
# ---------------------------------------------------------------------------

ROWS_PER_RUN: int = int(os.getenv("ROWS_PER_RUN", "5000"))

# ---------------------------------------------------------------------------
# Retry behaviour (mirrors Phase 5 retry config)
# ---------------------------------------------------------------------------

MAX_JOB_RETRIES:    int   = int(os.getenv("MAX_JOB_RETRIES",    "3"))
RETRY_BACKOFF_SECS: float = float(os.getenv("RETRY_BACKOFF_SECS", "5.0"))

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

LOG_FILE:     str = os.getenv("LOG_FILE",     "logs/scheduler.log")
METRICS_CSV:  str = os.getenv("METRICS_CSV",  "logs/metrics.csv")

# ---------------------------------------------------------------------------
# Database — read from env, consistent with the rest of the project
# ---------------------------------------------------------------------------

DB_DSN: str = os.getenv(
    "DATABASE_URL",
    "postgresql://localhost:5432/analytics"
)

# ---------------------------------------------------------------------------
# Quick sanity check — run this file directly to see active config
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    config = {
        "PIPELINE_INTERVAL_MINUTES": PIPELINE_INTERVAL_MINUTES,
        "ROWS_PER_RUN":              ROWS_PER_RUN,
        "RUN_DATA_GENERATION":       RUN_DATA_GENERATION,
        "RUN_TRANSFORMATIONS":       RUN_TRANSFORMATIONS,
        "RUN_MONITOR_SNAPSHOT":      RUN_MONITOR_SNAPSHOT,
        "RUN_HEALTH_CHECK":          RUN_HEALTH_CHECK,
        "MAX_JOB_RETRIES":           MAX_JOB_RETRIES,
        "RETRY_BACKOFF_SECS":        RETRY_BACKOFF_SECS,
        "LOG_FILE":                  LOG_FILE,
        "METRICS_CSV":               METRICS_CSV,
        "DB_DSN":                    DB_DSN,
    }
    print(json.dumps(config, indent=2))
