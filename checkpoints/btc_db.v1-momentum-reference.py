"""
btc_db.py — SQLite persistence for Kalshi 15-min window tick data.

Schema:
  windows — one row per window (ticker, floor_strike, winner)
  ticks   — one row per second (elapsed_sec, yes_ask, no_ask)
"""
import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

DB_PATH   = Path.home() / ".btc_windows.db"
_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="btc_db")
_id_cache: dict[str, int] = {}        # Kalshi ticker → window_id
_poly_id_cache: dict[str, int] = {}   # Polymarket slug → poly_window_id

SCHEMA = """
CREATE TABLE IF NOT EXISTS windows (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL UNIQUE,
    window_start_ts INTEGER NOT NULL,
    floor_strike    REAL,
    winner          TEXT,
    created_at      INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS ticks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    window_id   INTEGER NOT NULL REFERENCES windows(id),
    elapsed_sec INTEGER NOT NULL,
    yes_ask     REAL,
    no_ask      REAL,
    UNIQUE(window_id, elapsed_sec)
);

CREATE INDEX IF NOT EXISTS idx_ticks_wid ON ticks(window_id);
CREATE INDEX IF NOT EXISTS idx_win_start ON windows(window_start_ts);

CREATE TABLE IF NOT EXISTS poly_windows (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    slug            TEXT    NOT NULL UNIQUE,
    coin            TEXT    NOT NULL,
    window_start_ts INTEGER NOT NULL,
    winner          TEXT,
    created_at      INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS poly_ticks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    poly_window_id  INTEGER NOT NULL REFERENCES poly_windows(id),
    elapsed_sec     INTEGER NOT NULL,
    ask_up          REAL,
    ask_dn          REAL,
    UNIQUE(poly_window_id, elapsed_sec)
);

CREATE INDEX IF NOT EXISTS idx_poly_ticks_wid ON poly_ticks(poly_window_id);
CREATE INDEX IF NOT EXISTS idx_poly_win_start ON poly_windows(window_start_ts);
"""


SCHEMA_VERSION = 2   # bump when schema changes

def _sync_init() -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    # Schema version check — drop and recreate if stale
    con.execute("CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT)")
    row = con.execute("SELECT value FROM _meta WHERE key='schema_version'").fetchone()
    if row is None or int(row[0]) < SCHEMA_VERSION:
        con.executescript(
            "DROP TABLE IF EXISTS ticks;"
            "DROP TABLE IF EXISTS windows;"
        )
        con.execute("INSERT OR REPLACE INTO _meta VALUES ('schema_version', ?)",
                    (str(SCHEMA_VERSION),))
    con.executescript(SCHEMA)

    # Additive column migration: `coin_open_price` captures the true coin
    # spot at window start (separate from Kalshi's floor_strike). Safe to
    # re-run; ignored if the column already exists.
    try:
        con.execute("ALTER TABLE windows ADD COLUMN coin_open_price REAL")
    except sqlite3.OperationalError:
        pass

    con.commit()
    con.close()


def _sync_ensure_window(ticker: str, window_start_ts: int, floor_strike: float) -> int:
    if ticker in _id_cache:
        return _id_cache[ticker]
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT OR IGNORE INTO windows (ticker, window_start_ts, floor_strike) VALUES (?,?,?)",
        (ticker, window_start_ts, floor_strike or None),
    )
    con.commit()
    row = con.execute("SELECT id FROM windows WHERE ticker=?", (ticker,)).fetchone()
    con.close()
    wid = row[0]
    _id_cache[ticker] = wid
    return wid


def _sync_record_tick(window_id: int, elapsed_sec: int, yes_ask, no_ask) -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT OR REPLACE INTO ticks (window_id, elapsed_sec, yes_ask, no_ask) VALUES (?,?,?,?)",
        (window_id, elapsed_sec, yes_ask, no_ask),
    )
    con.commit()
    con.close()


def _sync_set_winner(ticker: str, winner: str) -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("UPDATE windows SET winner=? WHERE ticker=?", (winner, ticker))
    con.commit()
    con.close()
    _id_cache.pop(ticker, None)


def _sync_set_floor(ticker: str, floor: float) -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("UPDATE windows SET floor_strike=? WHERE ticker=?", (floor, ticker))
    con.commit()
    con.close()


def _sync_set_coin_open_price(ticker: str, price: float) -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("UPDATE windows SET coin_open_price=? WHERE ticker=?", (price, ticker))
    con.commit()
    con.close()


# ── Polymarket sync helpers ──────────────────────────────────────────────────

def _sync_ensure_poly_window(slug: str, coin: str, window_start_ts: int) -> int:
    if slug in _poly_id_cache:
        return _poly_id_cache[slug]
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT OR IGNORE INTO poly_windows (slug, coin, window_start_ts) VALUES (?,?,?)",
        (slug, coin, window_start_ts),
    )
    con.commit()
    row = con.execute("SELECT id FROM poly_windows WHERE slug=?", (slug,)).fetchone()
    con.close()
    wid = row[0]
    _poly_id_cache[slug] = wid
    return wid


def _sync_record_poly_tick(poly_window_id: int, elapsed_sec: int, ask_up, ask_dn) -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT OR REPLACE INTO poly_ticks (poly_window_id, elapsed_sec, ask_up, ask_dn) VALUES (?,?,?,?)",
        (poly_window_id, elapsed_sec, ask_up, ask_dn),
    )
    con.commit()
    con.close()


def _sync_set_poly_winner(slug: str, winner: str) -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("UPDATE poly_windows SET winner=? WHERE slug=?", (winner, slug))
    con.commit()
    con.close()
    _poly_id_cache.pop(slug, None)


# ── Public API ────────────────────────────────────────────────────────────────

def init_db() -> None:
    _sync_init()


async def ensure_window(ticker: str, window_start_ts: int, floor_strike: float = 0) -> int:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _executor, _sync_ensure_window, ticker, window_start_ts, floor_strike
    )


async def record_tick(window_id: int, elapsed_sec: int, yes_ask, no_ask) -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        _executor, _sync_record_tick, window_id, elapsed_sec, yes_ask, no_ask
    )


async def set_winner(ticker: str, winner: str) -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, _sync_set_winner, ticker, winner)


async def set_floor(ticker: str, floor: float) -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, _sync_set_floor, ticker, floor)


async def set_coin_open_price(ticker: str, price: float) -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, _sync_set_coin_open_price, ticker, price)


async def ensure_poly_window(slug: str, coin: str, window_start_ts: int) -> int:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _executor, _sync_ensure_poly_window, slug, coin, window_start_ts
    )


async def record_poly_tick(poly_window_id: int, elapsed_sec: int, ask_up, ask_dn) -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        _executor, _sync_record_poly_tick, poly_window_id, elapsed_sec, ask_up, ask_dn
    )


async def set_poly_winner(slug: str, winner: str) -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, _sync_set_poly_winner, slug, winner)
