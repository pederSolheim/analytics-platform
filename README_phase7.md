# Phase 7 — Scheduling

The pipeline now runs automatically. Two modes are provided — use both.

---

## Files

| File | Purpose |
|---|---|
| `schedule_config.py` | Central config, all values overridable via env vars |
| `scheduler.py` | APScheduler-based long-running process (dev/local) |
| `cron_wrapper.sh` | One-shot shell script for system cron (production) |

---

## Quick Start

### Install APScheduler

```bash
pip install apscheduler
```

### Run the scheduler (dev mode)

```bash
# Default: runs every 60 minutes, 5000 rows per run
python scheduler.py

# Fast testing: run every 2 minutes, 500 rows
PIPELINE_INTERVAL_MINUTES=2 ROWS_PER_RUN=500 python scheduler.py
```

The scheduler runs the pipeline **once immediately on startup**, then on the interval. You don't wait a full cycle to confirm it works.

### Set up system cron (production mode)

Make the wrapper executable:
```bash
chmod +x cron_wrapper.sh
```

Add to crontab (`crontab -e`):
```cron
# Run every hour
0 * * * * /full/path/to/project/cron_wrapper.sh >> /full/path/to/project/logs/cron.log 2>&1

# Or daily at midnight
0 0 * * * /full/path/to/project/cron_wrapper.sh >> /full/path/to/project/logs/cron.log 2>&1
```

**Always use full paths in crontab.** Cron runs with a stripped-down environment and won't find relative paths.

---

## Configuration

All settings live in `schedule_config.py` and can be overridden with environment variables. No code changes needed between environments.

| Variable | Default | Description |
|---|---|---|
| `PIPELINE_INTERVAL_MINUTES` | `60` | How often the pipeline runs |
| `ROWS_PER_RUN` | `5000` | Rows generated per scheduled run |
| `RUN_DATA_GENERATION` | `1` | Set to `0` to disable |
| `RUN_TRANSFORMATIONS` | `1` | Set to `0` to disable |
| `RUN_MONITOR_SNAPSHOT` | `1` | Set to `0` to disable |
| `RUN_HEALTH_CHECK` | `1` | Set to `0` to disable |
| `DATABASE_URL` | `postgresql://localhost:5432/analytics` | DB connection string |
| `LOG_FILE` | `logs/scheduler.log` | Scheduler log path |
| `METRICS_CSV` | `logs/metrics.csv` | Metrics persistence path |

Use a `.env` file for local development:

```bash
# .env
DATABASE_URL=postgresql://user:pass@localhost:5432/analytics
PIPELINE_INTERVAL_MINUTES=15
ROWS_PER_RUN=1000
```

`cron_wrapper.sh` sources `.env` automatically if it exists in the project root.

---

## Architecture: APScheduler vs cron

| | APScheduler (`scheduler.py`) | System cron (`cron_wrapper.sh`) |
|---|---|---|
| **Process model** | Long-lived Python process | One-shot: starts, runs, exits |
| **Best for** | Local dev, seeing live logs | Production, servers, VMs |
| **Restart on failure** | Manual or via systemd | OS handles it |
| **Overlap protection** | `max_instances=1` in code | Handled by cron itself |
| **Dependency** | `apscheduler` pip package | Bash + Python only |

For production on a server, prefer cron. If you want APScheduler to survive reboots, wrap it in a systemd service (see below).

---

## Optional: systemd service (APScheduler as a daemon)

Create `/etc/systemd/system/analytics-pipeline.service`:

```ini
[Unit]
Description=Analytics Pipeline Scheduler
After=network.target postgresql.service

[Service]
Type=simple
User=youruser
WorkingDirectory=/full/path/to/project
ExecStart=/full/path/to/project/.venv/bin/python scheduler.py
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable analytics-pipeline
sudo systemctl start analytics-pipeline
sudo journalctl -u analytics-pipeline -f  # tail logs
```

---

## What `run_pipeline()` does each tick

```
1. job_generate_data()      → insert ROWS_PER_RUN new transactions
2. job_run_transformations() → refresh daily_revenue, category_revenue, user_ltv
3. job_snapshot_metrics()   → flush monitor state to CSV
4. job_health_check()       → verify DB is alive, log transaction count
```

Order is intentional: generate first so transformations always have fresh data.

---

## Checking that it's working

```bash
# Watch the scheduler log live
tail -f logs/scheduler.log

# Check last cron run
tail -50 logs/cron.log

# Confirm rows are being added
psql $DATABASE_URL -c "SELECT COUNT(*), MAX(created_at) FROM transactions;"

# Check analytics tables are refreshing
psql $DATABASE_URL -c "SELECT * FROM daily_revenue ORDER BY date DESC LIMIT 5;"
```

---

## What this phase teaches

- **Process model**: understand the difference between a daemon and a one-shot job
- **Environment isolation**: why cron needs explicit paths and sourced env files
- **Overlap protection**: what happens if a job takes longer than its interval
- **Observability**: a scheduled job you can't observe is a liability, not an asset
- **Idempotency matters here**: transformations running on a schedule are only safe because Phase 2 made them idempotent

---

## Next: Phase 8 — Scalability

Scale to 20M+ rows. Switch from INSERT to COPY. Measure memory. Batch everything.

The pipeline you just automated is the thing you're about to stress-test.
