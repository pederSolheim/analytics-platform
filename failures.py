"""
failures.py — Intentional failure simulation.

Run each scenario manually to observe how the system responds.
This is your chaos testing harness.

Usage:
    python failures.py kill_connection
    python failures.py corrupt_data
    python failures.py partial_write
    python failures.py all
"""

import sys
import time
import logging
import psycopg2
from db import transaction, fetch_all, log

# ─────────────────────────────────────────────────────────────────────────────
# Scenario 1: Kill the connection mid-query
# ─────────────────────────────────────────────────────────────────────────────

def scenario_kill_connection():
    """
    What happens: We open a raw connection (bypassing the pool),
    close it manually, then try to use it.

    What you should see:
    - psycopg2.InterfaceError: connection already closed
    - Our retry logic catching and logging OperationalError variants
    - Clean error message, no crash

    Real-world equivalent: DB server restarts mid-request.
    """
    log.info('"[SCENARIO] Kill connection mid-query"')

    import psycopg2
    from db import DB_CONFIG

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    log.info('"Connection opened. Closing it before query..."')
    conn.close()  # ← kill it

    try:
        cur.execute("SELECT COUNT(*) FROM transactions")  # ← will fail
        log.error('"Should not reach here"')
    except psycopg2.InterfaceError as exc:
        log.warning(f'"Caught expected InterfaceError: {exc}"')
        log.info('"System handled dead connection correctly ✓"')

    # Now prove the pool still works after a dead connection elsewhere
    log.info('"Testing pool still healthy after dead connection..."')
    rows = fetch_all("SELECT COUNT(*) AS cnt FROM transactions")
    log.info(f'"Pool query succeeded. Row count: {rows[0]["cnt"]}"')


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 2: Corrupt input data
# ─────────────────────────────────────────────────────────────────────────────

def scenario_corrupt_data():
    log.info('"[SCENARIO] Corrupt input data"')

    corrupt_rows = [
        (9999901, 1, 1, 1,  50.00,  "2024-01-15"),
        (9999902, 2, 2, 1, -99.99,  "2024-01-15"),
        (9999903, 3, 3, 1,  75.00,  "2024-01-15"),
    ]

    log.info(f'"Attempting to insert {len(corrupt_rows)} rows (1 corrupt)..."')

    try:
        with transaction() as cur:
            for txn_id, user_id, product_id, qty, amount, date in corrupt_rows:
                if amount < 0:
                    raise ValueError(
                        f"Invalid amount {amount} for transaction {txn_id}. "
                        "Negative prices not allowed."
                    )
                cur.execute(
                    """
                    INSERT INTO transactions (transaction_id, user_id, product_id, quantity, amount, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (txn_id, user_id, product_id, qty, amount, date),
                )
    except ValueError as exc:
        log.warning(f'"Corrupt data rejected: {exc}"')
        log.info('"Transaction rolled back. No rows inserted. ✓"')

    rows = fetch_all(
        "SELECT COUNT(*) AS cnt FROM transactions WHERE transaction_id >= 9999901"
    )
    assert rows[0]["cnt"] == 0, "Rollback failed — rows leaked!"
    log.info('"Verified: 0 corrupt rows in DB ✓"')

# ─────────────────────────────────────────────────────────────────────────────
# Scenario 3: Partial write simulation
# ─────────────────────────────────────────────────────────────────────────────

def scenario_partial_write():
    log.info('"[SCENARIO] Partial write simulation"')

    SENTINEL_DATE = "1900-01-01"

    try:
        with transaction() as cur:
            log.info('"Writing to daily_revenue..."')
            cur.execute(
                """
                INSERT INTO daily_revenue (revenue_date, total_revenue, transaction_count)
                VALUES (%s, 99999.99, 1)
                ON CONFLICT (revenue_date) DO NOTHING
                """,
                (SENTINEL_DATE,),
            )
            log.info('"daily_revenue write complete. Injecting crash..."')

            raise RuntimeError("Simulated process crash after first write")

    except RuntimeError as exc:
        log.warning(f'"Caught injected failure: {exc}"')
        log.info('"Rollback should have fired. Checking DB state..."')

    rows = fetch_all(
        "SELECT COUNT(*) AS cnt FROM daily_revenue WHERE revenue_date = %s",
        (SENTINEL_DATE,),
    )
    assert rows[0]["cnt"] == 0, "Rollback failed — partial write leaked!"
    log.info('"Verified: partial write rolled back. Both tables consistent. ✓"')

# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

SCENARIOS = {
    "kill_connection": scenario_kill_connection,
    "corrupt_data":    scenario_corrupt_data,
    "partial_write":   scenario_partial_write,
}

if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"

    if arg == "all":
        for name, fn in SCENARIOS.items():
            print(f"\n{'='*60}")
            print(f"  Running: {name}")
            print(f"{'='*60}")
            fn()
    elif arg in SCENARIOS:
        SCENARIOS[arg]()
    else:
        print(f"Unknown scenario: {arg}")
        print(f"Available: {', '.join(SCENARIOS)} all")
        sys.exit(1)