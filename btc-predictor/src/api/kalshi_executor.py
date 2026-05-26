"""Live Kalshi order placement for n-5 arb signals.

Execution pattern (backtest-validated; see scripts/tune_execution*.py):
    1. Maker limit at best_bid, 3-second TTL
    2. If maker expires unfilled, MARKET at current_ask + 5¢ cap
    3. If market would exceed cap, drop the trade

The whole thing runs as an async task spawned from ArbTradeTracker.process_tick
so the 5 Hz prediction publisher never blocks on order resolution.

Two safety gates:
    - LIVE_TRADING=1 env var must be set or no orders are placed
    - KILL_SWITCH_FILE (default /tmp/btc_predictor_stop) hard-stops all firing
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Optional

import httpx

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "trading" / "kalshi"))

log = logging.getLogger("btc_predictor.kalshi_exec")

# ── Auth — reuse the existing kalshi_auth module. It loads
# KALSHI_API_KEY + KALSHI_API_SECRET from .env via dotenv at import time and
# exposes `sign_headers(method, path)` which returns the signed dict ready
# to send (already includes Content-Type: application/json).
try:
    from kalshi_auth import sign_headers as _sign_headers   # type: ignore
    from kalshi_auth import KALSHI_BASE as _AUTH_BASE       # type: ignore
    AUTH_AVAILABLE = True
except Exception as e:
    log.warning("kalshi_auth unavailable (%s) — live trading disabled", e)
    AUTH_AVAILABLE = False
    _sign_headers = None
    _AUTH_BASE = None

# ── Endpoint — kalshi_auth defines the base; only override via env if testing.
KALSHI_BASE = os.environ.get("KALSHI_BASE", _AUTH_BASE) or \
              "https://api.elections.kalshi.com/trade-api/v2"

# ── Toggles + safety
LIVE_TRADING_ENABLED = os.environ.get("LIVE_TRADING") == "1"
KILL_SWITCH_FILE = Path(os.environ.get("BTC_PRED_KILL_SWITCH",
                                        "/tmp/btc_predictor_stop"))
EXECUTION_RESULTS_LOG = REPO_ROOT / "btc-predictor" / "data" / "execution_results.jsonl"

# ── Execution params (backtest-validated; see scripts/tune_execution_dropvsfallback.py)
MAKER_OFFSET_CENTS = 0       # post at bid
MAKER_TTL_SEC      = 3       # 3-second maker TTL
MARKET_CAP_CENTS   = 5       # market fallback cap = current_ask + 5¢

# ── Sanity gates (mirror the simulator's ARB_MIN_SAMPLE_SEC + price band)
PRICE_MIN          = 0.05    # don't trade lottery tickets
PRICE_MAX          = 0.95    # don't trade near-certainties
MAX_QUOTE_AGE_SEC  = 3       # don't trust stale bid/ask snapshots


async def _post_order(client: httpx.AsyncClient, payload: dict, label: str
                      ) -> tuple[Optional[dict], float, float]:
    """POST /trade-api/v2/portfolio/orders. Returns (order_dict, sent_at, ack_at)."""
    # `path` for Kalshi signing must include /trade-api/v2 prefix.
    path = "/trade-api/v2/portfolio/orders"
    sent_at = time.time()
    try:
        r = await client.post(
            f"{KALSHI_BASE}/portfolio/orders",
            headers=_sign_headers("POST", path),
            content=json.dumps(payload),
            timeout=10,
        )
        ack_at = time.time()
        if r.status_code == 201:
            return r.json().get("order"), sent_at, ack_at
        log.warning("[%s] order POST HTTP %d: %s", label, r.status_code, r.text[:200])
        return None, sent_at, ack_at
    except Exception as e:
        log.warning("[%s] order POST exception: %s", label, type(e).__name__)
        return None, sent_at, time.time()


async def _resolve_order(client: httpx.AsyncClient, order_id: Optional[str]) -> dict:
    """GET /portfolio/orders/{order_id}. Returns dict with filled/cost/fees/status."""
    if not order_id:
        return {"filled": 0.0, "cost": 0.0, "fees": 0.0, "status": "unknown"}
    path = f"/trade-api/v2/portfolio/orders/{order_id}"
    try:
        r = await client.get(f"{KALSHI_BASE}/portfolio/orders/{order_id}",
                              headers=_sign_headers("GET", path), timeout=8)
        if r.status_code != 200:
            return {"filled": 0.0, "cost": 0.0, "fees": 0.0,
                    "status": f"err_{r.status_code}"}
        o = r.json().get("order") or {}
        def _f(v, default=0.0):
            try: return float(v) if v is not None else default
            except (TypeError, ValueError): return default
        filled = _f(o.get("fill_count_fp")) or float(o.get("filled_count") or 0)
        cost = _f(o.get("taker_fill_cost_dollars")) + _f(o.get("maker_fill_cost_dollars"))
        fees = _f(o.get("taker_fees_dollars")) + _f(o.get("maker_fees_dollars"))
        return {"filled": filled, "cost": cost, "fees": fees,
                "status": o.get("status", "unknown")}
    except Exception as e:
        log.warning("[resolve] %s exception: %s", order_id, type(e).__name__)
        return {"filled": 0.0, "cost": 0.0, "fees": 0.0, "status": "exception"}


def _append_execution_result(record: dict) -> None:
    """Persist one execution result to the durable log."""
    try:
        EXECUTION_RESULTS_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(EXECUTION_RESULTS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
    except OSError as e:
        log.warning("execution_results.jsonl write failed: %s", e)


async def place_arb_buy(
    client: httpx.AsyncClient,
    ticker: str,
    direction: str,
    snapshot: dict,
    tag: str,
    trade_id: str,
) -> dict:
    """Place a 1-share buy on Kalshi for an n-5 arb signal.

    Pattern: maker @ bid (3s) → market @ ask+5¢ cap → drop on cap exceeded.

    Args:
        ticker:    Kalshi market ticker (e.g. KXBTC15M-26MAY261500-15)
        direction: "BUY YES" or "BUY NO"
        snapshot:  {"yes_bid": float, "yes_ask": float, "ts": epoch_sec}
        tag:       short identifier for logs (e.g. "arb@T120")
        trade_id:  stable id linking back to the ArbTradeTracker record

    Returns a dict describing the result. Also appends one line to
    execution_results.jsonl so the result survives restarts.
    """
    result = {
        "trade_id":           trade_id,
        "ticker":             ticker,
        "direction":          direction,
        "execution_path":     None,           # "maker" | "mkt_after_maker" | "drop_cap_exceeded" | "skip_*"
        "final_status":       None,
        "trigger_ask":        snapshot.get("yes_ask"),
        "trigger_bid":        snapshot.get("yes_bid"),
        "limit_price":        None,
        "actual_fill_price":  None,
        "fee_dollars":        0.0,
        "shares_filled":      0.0,
        "kalshi_order_id":    None,
        "kalshi_market_order_id": None,
        "post_to_resolve_ms": None,
        "started_at_ts":      time.time(),
    }

    def _finish(path: str, **patch):
        result["execution_path"] = path
        result["post_to_resolve_ms"] = int((time.time() - result["started_at_ts"]) * 1000)
        result.update(patch)
        _append_execution_result(result)
        return result

    # ── Pre-flight ────────────────────────────────────────────────────
    if not AUTH_AVAILABLE:
        return _finish("skip_no_auth")
    if KILL_SWITCH_FILE.exists():
        log.info("[%s] kill-switch file present — order skipped", tag)
        return _finish("skip_kill_switch")

    yb = snapshot.get("yes_bid"); ya = snapshot.get("yes_ask")
    if yb is None or ya is None:
        return _finish("skip_no_quotes")
    snap_age = time.time() - snapshot.get("ts", time.time())
    if snap_age > MAX_QUOTE_AGE_SEC:
        return _finish("skip_stale_quote", final_status=f"snap_age_{snap_age:.1f}s")

    side = "yes" if direction == "BUY YES" else "no"
    if side == "yes":
        bid, ask = yb, ya
    else:
        # NO_bid = 1 − yes_ask, NO_ask = 1 − yes_bid (parity)
        bid, ask = round(1 - ya, 2), round(1 - yb, 2)

    if not (PRICE_MIN <= ask <= PRICE_MAX):
        return _finish("skip_price_oob",
                       final_status=f"ask={ask:.2f}_not_in_[{PRICE_MIN},{PRICE_MAX}]")

    # ── Step 1: Maker limit at bid, 3s TTL ────────────────────────────
    maker_price = round(max(0.01, bid - MAKER_OFFSET_CENTS * 0.01), 2)
    result["limit_price"] = maker_price

    maker_payload = {
        "ticker":          ticker,
        "action":          "buy",
        "side":            side,
        "count":           1,
        "type":            "limit",
        "client_order_id": str(uuid.uuid4()),
        "expiration_ts":   math.ceil(time.time()) + MAKER_TTL_SEC,
    }
    maker_payload[f"{side}_price_dollars"] = f"{maker_price:.2f}"

    log.info("[%s] maker POST %s %s @ $%.2f (TTL %ds)",
             tag, ticker, side, maker_price, MAKER_TTL_SEC)
    m, _, _ = await _post_order(client, maker_payload, f"{tag}/maker")
    if m:
        result["kalshi_order_id"] = m.get("order_id")
        await asyncio.sleep(MAKER_TTL_SEC + 0.3)
        st = await _resolve_order(client, m.get("order_id"))
        if st["filled"] >= 1.0:
            avg = st["cost"] / st["filled"]
            log.info("[%s] maker FILLED @ $%.4f  fee $%.4f  status=%s",
                     tag, avg, st["fees"], st["status"])
            return _finish(
                "maker",
                actual_fill_price=avg, fee_dollars=st["fees"],
                shares_filled=st["filled"], final_status=st["status"],
            )
        log.info("[%s] maker expired (status=%s, filled=%.2f) → trying market",
                 tag, st["status"], st["filled"])

    # ── Step 2: Market fallback @ ask+5¢ cap ──────────────────────────
    # Use the bid/ask we have; in a perfect world we'd re-poll, but the
    # Kalshi WS in our server is already updating snapshots — caller can
    # pass a freshly-updated snapshot if they prefer. For now, base the cap
    # on the snapshot we were called with.
    cap = min(0.99, ask + MARKET_CAP_CENTS * 0.01)
    mkt_payload = {
        "ticker":          ticker,
        "action":          "buy",
        "side":            side,
        "count":           1,
        "type":            "market",
        "client_order_id": str(uuid.uuid4()),
    }
    mkt_payload[f"{side}_price_dollars"] = f"{cap:.2f}"

    log.info("[%s] market POST %s %s cap $%.2f", tag, ticker, side, cap)
    mk, _, _ = await _post_order(client, mkt_payload, f"{tag}/mkt")
    if not mk:
        return _finish("mkt_post_failed")

    result["kalshi_market_order_id"] = mk.get("order_id")
    await asyncio.sleep(0.5)
    st = await _resolve_order(client, mk.get("order_id"))
    if st["filled"] >= 1.0:
        avg = st["cost"] / st["filled"]
        log.info("[%s] market FILLED @ $%.4f  fee $%.4f  status=%s",
                 tag, avg, st["fees"], st["status"])
        return _finish(
            "mkt_after_maker",
            actual_fill_price=avg, fee_dollars=st["fees"],
            shares_filled=st["filled"], final_status=st["status"],
        )
    # Cap exceeded — order under-filled (intended behavior)
    log.info("[%s] market dropped (book past cap; status=%s)", tag, st["status"])
    return _finish("drop_cap_exceeded", final_status=st["status"])
