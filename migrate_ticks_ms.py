"""migrate_ticks_ms.py — one-time schema migration to add sub-second
millisecond timestamps to the ticks table.

Before:
    ticks(window_id, elapsed_sec, yes_ask, no_ask)
    UNIQUE(window_id, elapsed_sec)
After:
    ticks(window_id, elapsed_sec, elapsed_ms, yes_ask, no_ask)
    UNIQUE(window_id, elapsed_ms)

Existing rows: elapsed_ms is backfilled as elapsed_sec * 1000.
Old elapsed_sec is kept (so existing analysis scripts still work unchanged).

Run once:  python3 migrate_ticks_ms.py
Idempotent — safe to re-run; bails if elapsed_ms column already exists.
"""
import sqlite3
import sys
import time
from pathlib import Path

DB = Path.home() / ".btc_windows.db"


def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()

    # Check current schema
    cols = {row[1] for row in cur.execute("PRAGMA table_info(ticks)").fetchall()}
    if "elapsed_ms" in cols:
        print("✓ elapsed_ms column already present — nothing to migrate.")
        return

    n_rows = cur.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
    print(f"Migrating {n_rows:,} rows in ticks table...")
    print("This may take ~30s. The DB is locked during this — make sure btc_recorder.py is stopped.")

    t0 = time.time()
    try:
        # Build new table with elapsed_ms
        cur.executescript("""
        BEGIN;

        CREATE TABLE ticks_new (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            window_id   INTEGER NOT NULL REFERENCES windows(id),
            elapsed_sec INTEGER NOT NULL,
            elapsed_ms  INTEGER NOT NULL,
            yes_ask     REAL,
            no_ask      REAL,
            UNIQUE(window_id, elapsed_ms)
        );

        INSERT INTO ticks_new (id, window_id, elapsed_sec, elapsed_ms, yes_ask, no_ask)
        SELECT id, window_id, elapsed_sec, elapsed_sec * 1000, yes_ask, no_ask
        FROM ticks;

        DROP TABLE ticks;
        ALTER TABLE ticks_new RENAME TO ticks;

        CREATE INDEX idx_ticks_wid ON ticks(window_id);
        CREATE INDEX idx_ticks_wid_sec ON ticks(window_id, elapsed_sec);

        COMMIT;
        """)

        new_n = cur.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
        elapsed = time.time() - t0
        print(f"✓ migrated {new_n:,} rows in {elapsed:.1f}s")
        print(f"  schema: {cur.execute('PRAGMA table_info(ticks)').fetchall()}")
    except Exception as e:
        print(f"✗ migration failed: {e}")
        cur.execute("ROLLBACK")
        sys.exit(1)


if __name__ == "__main__":
    main()
