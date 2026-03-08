"""
scheduler.py

Runs the full pipeline on a configurable interval using APScheduler.
This is the Python-native scheduler — runs as a long-lived process.

Start it with:
    python scheduler.py

Stop it with Ctrl+C. Logs to logs/scheduler.log.

For production, prefer cron_wrapper.sh (see that file) so the OS
manages the process lifetime instead of keeping Python alive forever.
Both approaches are valid — use APScheduler locally for visibility,
cron in production for reliability.
"""

import logging
import os
import sys
import time
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

import schedule_config as cfg

# ---------------------------------------------------------------------------
# Logging setup — writes to both console and file
# ---------------------------------------------------------------------------

os.makedirs(os.path.dirname(cfg.LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(cfg.LOG_FILE),
    ],
)
log = logging.getLogger("scheduler")

# ---------------------------------------------------------------------------
# Job functions
# Each job is a thin wrapper that calls into your existing modules.
# They are intentionally separate so APScheduler can track them individually
# and retry or skip them without taking down the whole pipeline.
# ---------------------------------------------------------------------------

def job_generate_data() -> None:
    """Simulate new incoming transactions (ongoing activity, not bulk load)."""
    log.info("JOB START — data_generation (rows=%d)", cfg.ROWS_PER_RUN)
    start = time.perf_counter()
    try:
        # Import here so a bad import doesn't crash the scheduler at startup
        from generate_data import insert_incremental  # type: ignore
        rows = insert_incremental(cfg.ROWS_PER_RUN, dsn=cfg.DB_DSN)
        elapsed = time.perf_counter() - start
        log.info("JOB OK — data_generation | rows=%d | %.2fs", rows, elapsed)
    except ImportError:
        # generate.py may not exist yet — log and skip rather than crash
        log.warning("data_generation skipped: generate.py not found or missing insert_incremental()")
    except Exception as exc:
        elapsed = time.perf_counter() - start
        log.error("JOB FAIL — data_generation | %.2fs | %s", elapsed, exc, exc_info=True)
        raise  # re-raise so APScheduler records the failure


def job_run_transformations() -> None:
    """Run all transformation jobs: daily revenue, category revenue, LTV, rolling avg."""
    log.info("JOB START — transformations")
    start = time.perf_counter()
    try:
        from pipeline import run_all  # type: ignore
        results = run_all(dsn=cfg.DB_DSN)
        elapsed = time.perf_counter() - start
        log.info("JOB OK — transformations | jobs=%d | %.2fs", len(results), elapsed)
        for name, status in results.items():
            log.info("  ↳ %-35s %s", name, status)
    except ImportError:
        log.warning("transformations skipped: pipeline.py not found or missing run_all()")
    except Exception as exc:
        elapsed = time.perf_counter() - start
        log.error("JOB FAIL — transformations | %.2fs | %s", elapsed, exc, exc_info=True)
        raise


def job_snapshot_metrics() -> None:
    """Flush in-memory monitor state to CSV for persistence across restarts."""
    log.info("JOB START — monitor_snapshot")
    start = time.perf_counter()
    try:
        from monitor import flush_to_csv  # type: ignore
        path = flush_to_csv(cfg.METRICS_CSV)
        elapsed = time.perf_counter() - start
        log.info("JOB OK — monitor_snapshot | path=%s | %.2fs", path, elapsed)
    except ImportError:
        log.warning("monitor_snapshot skipped: monitor.py not found or missing flush_to_csv()")
    except Exception as exc:
        elapsed = time.perf_counter() - start
        log.error("JOB FAIL — monitor_snapshot | %.2fs | %s", elapsed, exc, exc_info=True)
        raise


def job_health_check() -> None:
    """
    Lightweight liveness check — verifies DB is reachable and responds.
    Logs a warning (not an error) if the check fails, so one bad DB
    connection doesn't mark the whole scheduler as failed.
    """
    log.info("JOB START — health_check")
    start = time.perf_counter()
    try:
        import psycopg2  # type: ignore
        conn = psycopg2.connect(cfg.DB_DSN)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM transactions;")
        count = cur.fetchone()[0]
        conn.close()
        elapsed = time.perf_counter() - start
        log.info("JOB OK — health_check | transactions=%d | %.2fs", count, elapsed)
    except Exception as exc:
        elapsed = time.perf_counter() - start
        # WARNING not ERROR — a momentary DB hiccup shouldn't page anyone
        log.warning("JOB WARN — health_check | %.2fs | %s", elapsed, exc)


# ---------------------------------------------------------------------------
# APScheduler event listener — central place to react to job outcomes
# ---------------------------------------------------------------------------

def on_job_event(event) -> None:
    if event.exception:
        log.error("SCHEDULER EVENT — job_id=%s FAILED: %s", event.job_id, event.exception)
    else:
        log.debug("SCHEDULER EVENT — job_id=%s succeeded", event.job_id)


# ---------------------------------------------------------------------------
# Pipeline — ordered sequence run each tick
# ---------------------------------------------------------------------------

def run_pipeline() -> None:
    """
    The full pipeline, run in order each interval.

    Order matters:
      1. Generate data first so transformations have fresh rows to work with.
      2. Transform next so analytics tables are up to date.
      3. Snapshot metrics so we capture the work just done.
      4. Health check last — a final confirmation everything is still alive.
    """
    tick = datetime.utcnow().isoformat()
    log.info("=" * 60)
    log.info("PIPELINE TICK — %s", tick)
    log.info("=" * 60)

    if cfg.RUN_DATA_GENERATION:
        job_generate_data()

    if cfg.RUN_TRANSFORMATIONS:
        job_run_transformations()

    if cfg.RUN_MONITOR_SNAPSHOT:
        job_snapshot_metrics()

    if cfg.RUN_HEALTH_CHECK:
        job_health_check()

    log.info("PIPELINE COMPLETE — %s", tick)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("Scheduler starting up")
    log.info("  Interval : every %d minutes", cfg.PIPELINE_INTERVAL_MINUTES)
    log.info("  Rows/run : %d", cfg.ROWS_PER_RUN)
    log.info("  Log file : %s", cfg.LOG_FILE)
    log.info("  DB DSN   : %s", cfg.DB_DSN)

    scheduler = BlockingScheduler(timezone="UTC")

    # Register the pipeline as a single interval job.
    # APScheduler will call run_pipeline() every N minutes.
    scheduler.add_job(
        run_pipeline,
        trigger="interval",
        minutes=cfg.PIPELINE_INTERVAL_MINUTES,
        id="main_pipeline",
        name="Full analytics pipeline",
        max_instances=1,          # never overlap — if a run is late, skip not stack
        misfire_grace_time=60,    # tolerate up to 60s late start before marking missed
        replace_existing=True,
    )

    scheduler.add_listener(on_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    log.info("First run in %d minutes. Press Ctrl+C to stop.", cfg.PIPELINE_INTERVAL_MINUTES)

    try:
        # Run once immediately on startup so you don't wait a full interval
        # to confirm everything is wired up.
        log.info("Running pipeline immediately on startup...")
        run_pipeline()

        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped by user.")
    except Exception as exc:
        log.critical("Scheduler crashed: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
