import csv
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

METRICS_FILE = Path("metrics.csv")
_lock = threading.Lock()

# In-memory store — resets on restart
_store = {
    "pipeline_runs": 0,
    "total_rows_processed": 0,
    "total_errors": 0,
    "last_run_at": None,
    "last_run_duration_s": None,
    "job_history": [],   # last 20 job results
}


def record_job(job_name: str, duration_s: float, rows: int, errors: int):
    """Call this after every pipeline job completes."""
    with _lock:
        _store["pipeline_runs"] += 1
        _store["total_rows_processed"] += rows
        _store["total_errors"] += errors
        _store["last_run_at"] = datetime.now(timezone.utc).isoformat()
        _store["last_run_duration_s"] = duration_s

        entry = {
            "job": job_name,
            "ran_at": _store["last_run_at"],
            "duration_s": round(duration_s, 3),
            "rows": rows,
            "errors": errors,
        }
        _store["job_history"].append(entry)
        if len(_store["job_history"]) > 20:
            _store["job_history"].pop(0)

        _append_csv(entry)


def _append_csv(entry: dict):
    file_exists = METRICS_FILE.exists()
    with open(METRICS_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=entry.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(entry)


def get_snapshot() -> dict:
    with _lock:
        return dict(_store)
    

def flush_to_csv(path: str) -> str:
    import csv, os
    snapshot = get_snapshot()
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([snapshot])
    return path