#!/usr/bin/env bash
# cron_wrapper.sh
#
# Runs the full pipeline once and exits.
# Designed to be called by system cron — the OS manages timing,
# not a long-lived Python process.
#
# WHY THIS EXISTS ALONGSIDE scheduler.py:
#   scheduler.py  → good for local dev (you see it running, easy to kill)
#   cron_wrapper  → better for production (OS restarts it, no zombie processes)
#
# SETUP (add one of these to your crontab via `crontab -e`):
#
#   Run every hour:
#   0 * * * * /path/to/project/cron_wrapper.sh >> /path/to/project/logs/cron.log 2>&1
#
#   Run daily at midnight:
#   0 0 * * * /path/to/project/cron_wrapper.sh >> /path/to/project/logs/cron.log 2>&1
#
#   Run every 15 minutes (testing):
#   */15 * * * * /path/to/project/cron_wrapper.sh >> /path/to/project/logs/cron.log 2>&1
#
# IMPORTANT: cron runs with a minimal environment. This script explicitly
# sets the paths it needs so it works regardless of which user runs it.

set -euo pipefail

# ---------------------------------------------------------------------------
# Config — edit these paths to match your project layout
# ---------------------------------------------------------------------------

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${PROJECT_DIR}/.venv"
LOG_DIR="${PROJECT_DIR}/logs"
LOG_FILE="${LOG_DIR}/cron.log"
PYTHON="${VENV_DIR}/bin/python"

# ---------------------------------------------------------------------------
# Environment — source your .env file if it exists
# ---------------------------------------------------------------------------

ENV_FILE="${PROJECT_DIR}/.env"
if [[ -f "$ENV_FILE" ]]; then
    # Export all KEY=VALUE pairs, skip comments and blank lines
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

# ---------------------------------------------------------------------------
# Sanity checks — fail early with a clear message rather than silently
# ---------------------------------------------------------------------------

if [[ ! -d "$PROJECT_DIR" ]]; then
    echo "[ERROR] PROJECT_DIR not found: $PROJECT_DIR" >&2
    exit 1
fi

if [[ ! -f "$PYTHON" ]]; then
    echo "[ERROR] Python not found at $PYTHON" >&2
    echo "        Run: python -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

mkdir -p "$LOG_DIR"

TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "[$TIMESTAMP] cron_wrapper — starting pipeline"

cd "$PROJECT_DIR"

"$PYTHON" - <<'PYEOF'
"""
Inline pipeline runner — same logic as scheduler.py's run_pipeline(),
but as a one-shot script. No APScheduler dependency needed here.
"""
import sys
import os
import time
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("cron_pipeline")

# Pull config from env (schedule_config.py exports these same vars)
ROWS_PER_RUN          = int(os.getenv("ROWS_PER_RUN",          "5000"))
DB_DSN                = os.getenv("DATABASE_URL",              "postgresql://localhost:5432/analytics")
METRICS_CSV           = os.getenv("METRICS_CSV",               "logs/metrics.csv")
RUN_DATA_GENERATION   = os.getenv("RUN_DATA_GENERATION",       "1") == "1"
RUN_TRANSFORMATIONS   = os.getenv("RUN_TRANSFORMATIONS",       "1") == "1"
RUN_MONITOR_SNAPSHOT  = os.getenv("RUN_MONITOR_SNAPSHOT",      "1") == "1"
RUN_HEALTH_CHECK      = os.getenv("RUN_HEALTH_CHECK",          "1") == "1"

tick = datetime.utcnow().isoformat()
log.info("=" * 60)
log.info("CRON PIPELINE START — %s", tick)
log.info("=" * 60)

errors = []

if RUN_DATA_GENERATION:
    try:
        from generate import insert_incremental  # type: ignore
        t = time.perf_counter()
        rows = insert_incremental(ROWS_PER_RUN, dsn=DB_DSN)
        log.info("data_generation OK | rows=%d | %.2fs", rows, time.perf_counter() - t)
    except ImportError:
        log.warning("data_generation skipped: generate.py / insert_incremental() not found")
    except Exception as e:
        log.error("data_generation FAILED: %s", e, exc_info=True)
        errors.append("data_generation")

if RUN_TRANSFORMATIONS:
    try:
        from pipeline import run_all  # type: ignore
        t = time.perf_counter()
        results = run_all(dsn=DB_DSN)
        log.info("transformations OK | jobs=%d | %.2fs", len(results), time.perf_counter() - t)
    except ImportError:
        log.warning("transformations skipped: pipeline.py / run_all() not found")
    except Exception as e:
        log.error("transformations FAILED: %s", e, exc_info=True)
        errors.append("transformations")

if RUN_MONITOR_SNAPSHOT:
    try:
        from monitor import flush_to_csv  # type: ignore
        t = time.perf_counter()
        path = flush_to_csv(METRICS_CSV)
        log.info("monitor_snapshot OK | path=%s | %.2fs", path, time.perf_counter() - t)
    except ImportError:
        log.warning("monitor_snapshot skipped: monitor.py / flush_to_csv() not found")
    except Exception as e:
        log.error("monitor_snapshot FAILED: %s", e, exc_info=True)
        errors.append("monitor_snapshot")

if RUN_HEALTH_CHECK:
    try:
        import psycopg2  # type: ignore
        t = time.perf_counter()
        conn = psycopg2.connect(DB_DSN)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM transactions;")
        count = cur.fetchone()[0]
        conn.close()
        log.info("health_check OK | transactions=%d | %.2fs", count, time.perf_counter() - t)
    except Exception as e:
        log.warning("health_check WARN: %s", e)

if errors:
    log.error("CRON PIPELINE FINISHED WITH ERRORS: %s", errors)
    sys.exit(1)
else:
    log.info("CRON PIPELINE COMPLETE — %s", datetime.utcnow().isoformat())
    sys.exit(0)
PYEOF

EXIT_CODE=$?
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

if [[ $EXIT_CODE -eq 0 ]]; then
    echo "[$TIMESTAMP] cron_wrapper — pipeline completed successfully"
else
    echo "[$TIMESTAMP] cron_wrapper — pipeline finished with errors (exit $EXIT_CODE)"
fi

exit $EXIT_CODE
