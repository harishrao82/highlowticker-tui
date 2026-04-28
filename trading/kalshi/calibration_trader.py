"""calibration_trader.py — independent strategy that buys when Kalshi's ask
falls into a historically-underpriced (coin, side, T-zone, ask-bucket) cell.

Reads ~/.calibration_table.json (built by build_calibration_table.py).
Polls Kalshi 15m markets every CALIB_POLL_SEC, looks up each side's current
bucket, fires a small BUY when implied prob is below realized WR by ≥ MIN_EDGE.

Holds to settlement — no exit logic. Calibration edge realizes only at settle.

Logs trades to ~/.calibration_trades.jsonl.

Run:   python calibration_trader.py
Stop:  Ctrl-C
"""
import asyncio
import base64
import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv

# Force line-buffered stdout so prints flush to log file in real time
# (otherwise they're buffered until process exits or 4KB+ accumulates).
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────────
KALSHI_BASE      = "https://api.elections.kalshi.com/trade-api/v2"
ET_OFFSET        = timedelta(hours=-4)

CALIB_TABLE_PATH = Path.home() / ".calibration_table.json"
TRADE_LOG        = Path.home() / ".calibration_trades.jsonl"

CALIB_POLL_SEC   = 5      # how often to check market state
RELOAD_SEC       = 600    # reload calibration table every N sec
SHARES_PER_FIRE  = 3      # conservative — calibration edge is small (~5pp)
MIN_EDGE         = 0.05   # only fire when underpricing ≥ 5pp
MIN_ASK          = 0.05   # don't enter at <5¢ (illiquid)
MAX_ASK          = 0.95   # don't enter above 95¢ (no upside)

# Dead hours (ET): skip firing during these hours. Same set used by
# build_calibration_table.py to filter polluted windows.
DEAD_HOURS_ET    = {0, 5, 19, 20}

COINS = {
    "BTC": "KXBTC15M",
    "ETH": "KXETH15M",
    "SOL": "KXSOL15M",
    "XRP": "KXXRP15M",
}

# (coin, ws, side) → True after we've fired
_acted: set[tuple[str, int, str]] = set()
_calibration: dict = {"cells": {}}
_calibration_loaded_at: float = 0

# ── Kalshi auth ─────────────────────────────────────────────────────────
_api_key     = os.environ.get("KALSHI_API_KEY")
_secret_pem  = os.environ.get("KALSHI_API_SECRET")
if not _api_key or not _secret_pem:
    print("ERROR: KALSHI_API_KEY / KALSHI_API_SECRET not set")
    sys.exit(1)
_private_key = serialization.load_pem_private_key(_secret_pem.encode(), password=None)


def _sign(method: str, path: str) -> dict:
    ts  = str(round(time.time() * 1000))
    msg = ts + method.upper() + path
    sig = _private_key.sign(
        msg.encode(),
        asym_padding.PSS(mgf=asym_padding.MGF1(hashes.SHA256()),
                         salt_length=asym_padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       _api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }


def _headers(method: str, path: str) -> dict:
    return {**_sign(method, path), "Content-Type": "application/json"}


# ── Calibration lookups ─────────────────────────────────────────────────
def time_zone(elapsed: int) -> str:
    if elapsed < 60:    return "T<60"
    if elapsed < 180:   return "T60-180"
    if elapsed < 420:   return "T180-420"
    if elapsed < 720:   return "T420-720"
    return "T720+"


def bucket_label(ask: float) -> str | None:
    if ask is None or ask <= 0 or ask >= 1: return None
    if ask >= 0.85 or ask <= 0.15:
        return f"{round(ask * 100) / 100:.2f}"
    lo = int(ask * 20) / 20.0
    return f"{lo:.2f}-{lo+0.05:.2f}"


def lookup_edge(coin: str, side: str, elapsed: int, ask: float) -> dict | None:
    """Return the calibration cell dict, or None if no qualifying edge."""
    tz = time_zone(elapsed)
    b = bucket_label(ask)
    if not b: return None
    key = f"{coin}|{side}|{tz}|{b}"
    cell = _calibration.get("cells", {}).get(key)
    if cell is None: return None
    if cell["edge"] < MIN_EDGE: return None
    return cell


def load_calibration() -> bool:
    global _calibration, _calibration_loaded_at
    if not CALIB_TABLE_PATH.exists():
        print(f"  [warn] calibration table not found at {CALIB_TABLE_PATH}")
        return False
    try:
        _calibration = json.loads(CALIB_TABLE_PATH.read_text())
        _calibration_loaded_at = time.time()
        n = len(_calibration.get("cells", {}))
        pos = sum(1 for c in _calibration["cells"].values() if c["edge"] > 0)
        print(f"  loaded calibration: {n} cells ({pos} underpriced)")
        return True
    except Exception as e:
        print(f"  [err] calibration load: {e}")
        return False


# ── Kalshi market data ──────────────────────────────────────────────────
def current_ticker(coin_series: str) -> str:
    """Current 15m ticker, e.g. KXBTC15M-26APR271415-15."""
    now = datetime.now(timezone.utc)
    mins = (now.minute // 15) * 15
    cur_utc = now.replace(minute=mins, second=0, microsecond=0)
    close_utc = cur_utc + timedelta(minutes=15)
    close_et = close_utc + ET_OFFSET
    return (f"{coin_series}-{close_et.strftime('%y%b%d%H%M').upper()}-"
            f"{close_et.strftime('%M')}")


def current_window_start_ts() -> int:
    now_ts = int(time.time())
    return (now_ts // 900) * 900


async def get_market(client: httpx.AsyncClient, ticker: str) -> dict | None:
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        r = await client.get(KALSHI_BASE + f"/markets/{ticker}",
                             headers=_headers("GET", path), timeout=10)
        if r.status_code != 200: return None
        m = r.json().get("market", {})
        def _f(v):
            try: return float(v) if v is not None else None
            except: return None
        return {
            "yes_bid": _f(m.get("yes_bid_dollars")),
            "yes_ask": _f(m.get("yes_ask_dollars")),
            "no_bid":  _f(m.get("no_bid_dollars")),
            "no_ask":  _f(m.get("no_ask_dollars")),
        }
    except Exception:
        return None


async def place_buy(client: httpx.AsyncClient, ticker: str, side: str,
                     price: float, shares: int) -> dict | None:
    """Place a limit BUY at `price`. Returns order dict or None."""
    yes_price = f"{price:.2f}" if side == "yes" else f"{1.0 - price:.2f}"
    coid = str(uuid.uuid4())
    body = {
        "ticker": ticker,
        "action": "buy",
        "side":   side,
        "count":  shares,
        "type":   "limit",
        "yes_price_dollars": yes_price,
        "client_order_id": coid,
    }
    path = "/trade-api/v2/portfolio/orders"
    try:
        r = await client.post(KALSHI_BASE + "/portfolio/orders",
                              headers=_headers("POST", path),
                              content=json.dumps(body), timeout=10)
        if r.status_code in (200, 201):
            return r.json().get("order", {})
        else:
            print(f"  [err] place_buy {ticker} {side}@{price}: "
                  f"{r.status_code} {r.text[:150]}")
    except Exception as e:
        print(f"  [err] place_buy {ticker}: {e}")
    return None


def log_trade(entry: dict) -> None:
    try:
        with open(TRADE_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        print(f"  [err] log: {e}")


# ── Main loop ───────────────────────────────────────────────────────────
async def trader_loop():
    """Per-coin per-tick: check both sides for calibration edge, fire if found."""
    last_heartbeat = 0.0
    HEARTBEAT_SEC = 60   # status line every minute
    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            try:
                # Reload calibration periodically (picks up nightly rebuilds)
                if time.time() - _calibration_loaded_at > RELOAD_SEC:
                    load_calibration()

                ws = current_window_start_ts()
                now = time.time()
                elapsed = int(now - ws)
                if elapsed < 5 or elapsed > 870:
                    await asyncio.sleep(CALIB_POLL_SEC); continue

                # Dead-hour block: skip firing during high-loss hours
                et_hour = (datetime.now(timezone.utc) + ET_OFFSET).hour
                if et_hour in DEAD_HOURS_ET:
                    await asyncio.sleep(CALIB_POLL_SEC); continue

                # Snapshot all 4 coin asks for heartbeat
                ask_snapshot = {}
                for coin, series in COINS.items():
                    ticker = current_ticker(series)
                    md = await get_market(client, ticker)
                    if not md: continue
                    ask_snapshot[coin] = md

                    for side, ask in (("yes", md["yes_ask"]), ("no", md["no_ask"])):
                        if ask is None or ask < MIN_ASK or ask > MAX_ASK: continue
                        key = (coin, ws, side)
                        if key in _acted: continue

                        cell = lookup_edge(coin, side, elapsed, ask)
                        if not cell: continue

                        _acted.add(key)
                        order = await place_buy(client, ticker, side, ask,
                                                 SHARES_PER_FIRE)
                        et_h = (datetime.now(timezone.utc) + ET_OFFSET).strftime("%H:%M:%S")
                        if order:
                            print(f"  [{et_h}] ★ CALIB {coin} {side.upper()} "
                                  f"@${ask:.3f} sh={SHARES_PER_FIRE} "
                                  f"edge=+{cell['edge']*100:.1f}pp "
                                  f"(WR {cell['wr']*100:.1f}% vs imp {cell['implied']*100:.1f}%) "
                                  f"order={order.get('order_id','?')[:8]}")
                            log_trade({
                                "ts":          time.time(),
                                "ticker":      ticker, "coin": coin, "side": side,
                                "ws":          ws, "elapsed": elapsed,
                                "ask":         ask, "shares": SHARES_PER_FIRE,
                                "cell_edge":   cell["edge"],
                                "cell_wr":     cell["wr"],
                                "cell_implied":cell["implied"],
                                "order_id":    order.get("order_id"),
                                "client_order_id": order.get("client_order_id"),
                            })
                        else:
                            print(f"  [{et_h}] ✗ CALIB place failed {coin} {side} @{ask}")

                # Heartbeat: every HEARTBEAT_SEC, print current ask snapshot + active cells for this T-zone.
                if time.time() - last_heartbeat > HEARTBEAT_SEC:
                    last_heartbeat = time.time()
                    et_h = (datetime.now(timezone.utc) + ET_OFFSET).strftime("%H:%M:%S")
                    tz = time_zone(elapsed)
                    asks_str = " | ".join(
                        f"{c} Y{md['yes_ask']:.2f}/N{md['no_ask']:.2f}"
                        for c, md in ask_snapshot.items() if md.get("yes_ask") is not None
                    )
                    n_active = sum(
                        1 for k in _calibration.get("cells", {})
                        if tz in k and _calibration["cells"][k]["edge"] >= MIN_EDGE
                    )
                    print(f"  [{et_h}] T+{elapsed:>3}s ({tz})  {asks_str}  "
                          f"|  active cells: {n_active}  acted: {len(_acted)}")

                await asyncio.sleep(CALIB_POLL_SEC)
            except Exception as e:
                print(f"  [loop err] {e}")
                await asyncio.sleep(5)


async def main():
    print(f"calibration_trader starting...")
    print(f"  Calibration table: {CALIB_TABLE_PATH}")
    print(f"  Trade log:         {TRADE_LOG}")
    print(f"  Poll interval:     {CALIB_POLL_SEC}s")
    print(f"  Shares per fire:   {SHARES_PER_FIRE}")
    print(f"  Min edge:          {MIN_EDGE*100:.1f}pp")
    print(f"  Ask range:         [{MIN_ASK}, {MAX_ASK}]")
    print()
    if not load_calibration():
        print("Cannot start without calibration table. Run build_calibration_table.py first.")
        return
    print()
    await trader_loop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
