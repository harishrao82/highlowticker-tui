#!/usr/bin/env python3
"""
Kalshi BTC 15-min Up/Down live odds monitor.

Polls Kalshi REST every 2s for yes_ask / no_ask prices on the current
KXBTC15M window, compares against the historical model, and alerts on
mispricing edges.

Uses the same probability surface as btc_odds_research.py.

Run:
    python3 kalshi_odds_monitor.py

Build/refresh surface:
    python3 btc_model_builder.py --days 365
"""
import asyncio
import json
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx

from btc_odds_research import get_odds, load_surface, MISMATCH_THRESHOLD, WINDOW_SEC

# ── Config ────────────────────────────────────────────────────────────────────
KALSHI_API   = "https://api.elections.kalshi.com/trade-api/v2"
CB_CANDLES   = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
CB_WS        = "wss://advanced-trade-ws.coinbase.com"

POLL_INTERVAL  = 2      # seconds between Kalshi REST polls
ARB_TARGET     = 0.90   # direct arb: yes + no < this
STAGED_TARGET  = 0.82   # staged arb leg2 limit

ET_OFFSET = timedelta(hours=-4)   # EDT


# ── Window ticker helpers ─────────────────────────────────────────────────────

def _current_window_et() -> datetime:
    """Current 15-min window start in ET."""
    now_et = datetime.now(timezone.utc) + ET_OFFSET
    mins   = (now_et.minute // 15) * 15
    return now_et.replace(minute=mins, second=0, microsecond=0)


def _ticker(window_et: datetime) -> str:
    """KXBTC15M-26APR051830-30  (close time; suffix = close minutes)"""
    close = window_et + timedelta(minutes=15)
    return "KXBTC15M-" + close.strftime("%y%b%d%H%M").upper() + "-" + close.strftime("%M")


def _elapsed_sec(window_et: datetime) -> int:
    now_et = datetime.now(timezone.utc) + ET_OFFSET
    return int((now_et - window_et).total_seconds())


# ── Shared state ──────────────────────────────────────────────────────────────
_btc_price:      float = 0.0
_btc_prev_price: float = 0.0
_btc_open:       float = 0.0   # floor_strike from Kalshi

_yes_ask: float = 0.0   # Yes = price goes UP
_no_ask:  float = 0.0   # No  = price goes DOWN

_staged: dict = {}   # window_ticker → list of {side, entry_price, limit_target, ...}

_pnl_profit: float = 0.0
_pnl_loss:   float = 0.0
_pnl_wins:   int   = 0
_pnl_losses: int   = 0

_alerts: list[str] = []
MAX_ALERTS = 12

_last_ticker: str = ""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    _alerts.append(f"  {ts}  {msg}")
    if len(_alerts) > MAX_ALERTS:
        _alerts.pop(0)


# ── Kalshi REST poll ──────────────────────────────────────────────────────────

async def _poll_kalshi(client: httpx.AsyncClient) -> None:
    """Poll current window yes_ask / no_ask every POLL_INTERVAL seconds."""
    global _yes_ask, _no_ask, _btc_open, _staged, _last_ticker
    global _pnl_loss, _pnl_losses

    last_window_et = None

    while True:
        window_et = _current_window_et()
        ticker    = _ticker(window_et)

        # Window rolled — settle expired positions
        if last_window_et and window_et != last_window_et:
            old_ticker = _ticker(last_window_et)
            for pos in _staged.get(old_ticker, []):
                if not pos.get("arb_fired") and not pos.get("expired"):
                    _pnl_loss   += pos["entry_price"]
                    _pnl_losses += 1
                    pos["expired"] = True
                    _log(f"\033[91m✗ EXPIRED — {pos['side']} @ {pos['entry_price']:.3f}  "
                         f"loss={pos['entry_price']:.3f} ({pos['entry_price']*100:.1f}¢)\033[0m")
            # Purge old windows
            for k in [k for k in _staged if k != ticker]:
                del _staged[k]

        last_window_et = window_et
        _last_ticker   = ticker

        try:
            r = await client.get(f"{KALSHI_API}/markets/{ticker}", timeout=5)
            if r.status_code == 200:
                m = r.json()["market"]
                ya = float(m.get("yes_ask_dollars") or 0)
                na = float(m.get("no_ask_dollars") or 0)
                fs = m.get("floor_strike")
                if ya > 0: _yes_ask = ya
                if na > 0: _no_ask  = na
                if fs:     _btc_open = float(fs)
        except Exception:
            pass

        await asyncio.sleep(POLL_INTERVAL)


# ── Coinbase price feed ───────────────────────────────────────────────────────

async def _coinbase_feed() -> None:
    global _btc_price, _btc_prev_price
    import websockets
    sub = {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "ticker"}
    while True:
        try:
            async with websockets.connect(CB_WS, ping_interval=20) as ws:
                await ws.send(json.dumps(sub))
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("channel") != "ticker":
                        continue
                    for evt in msg.get("events", []):
                        for ticker in evt.get("tickers", []):
                            price = float(ticker.get("price", 0) or 0)
                            if price > 0:
                                _btc_prev_price = _btc_price if _btc_price > 0 else price
                                _btc_price = price
        except Exception:
            await asyncio.sleep(3)


# ── Signal check ──────────────────────────────────────────────────────────────

def _check_signal(elapsed_sec: int, ticker: str) -> None:
    global _staged, _pnl_profit, _pnl_wins

    if elapsed_sec <= 30 or elapsed_sec >= WINDOW_SEC:
        return
    if _yes_ask < 0.05 or _yes_ask > 0.95 or _no_ask < 0.05 or _no_ask > 0.95:
        return
    if _btc_open == 0 or _btc_price == 0:
        return

    delta = (_btc_price - _btc_open) / _btc_open * 100
    emp   = get_odds(elapsed_sec, delta)

    if emp["confidence"] not in ("high", "med"):
        return

    yes_ask = _yes_ask
    no_ask  = _no_ask
    edge_up = emp["p_up"]   - yes_ask   # Yes = Up
    edge_dn = emp["p_down"] - no_ask    # No  = Down

    open_positions = _staged.get(ticker, [])
    open_sides     = {p["side"] for p in open_positions if not p.get("arb_fired")}

    # ── Edge entries ──────────────────────────────────────────────────────────
    if edge_up > MISMATCH_THRESHOLD and emp["p_up"] > 0.50 and "Yes" not in open_sides:
        pos = {
            "side": "Yes", "other": "No",
            "entry_price": yes_ask, "entry_sec": elapsed_sec,
            "entry_model": emp["p_up"], "entry_neff": emp["n_eff"],
            "limit_target": round(STAGED_TARGET - yes_ask, 3),
        }
        _staged.setdefault(ticker, []).append(pos)
        _log(f"\033[92m▲ BUY YES @ {yes_ask:.3f}  model={emp['p_up']:.1%}  "
             f"edge=+{edge_up:.2f}  n={emp['n_eff']}\033[0m  "
             f"| limit NO target ≤ {STAGED_TARGET - yes_ask:.3f}")

    elif edge_dn > MISMATCH_THRESHOLD and emp["p_down"] > 0.50 and "No" not in open_sides:
        pos = {
            "side": "No", "other": "Yes",
            "entry_price": no_ask, "entry_sec": elapsed_sec,
            "entry_model": emp["p_down"], "entry_neff": emp["n_eff"],
            "limit_target": round(STAGED_TARGET - no_ask, 3),
        }
        _staged.setdefault(ticker, []).append(pos)
        _log(f"\033[92m▼ BUY NO  @ {no_ask:.3f}  model={emp['p_down']:.1%}  "
             f"edge=+{edge_dn:.2f}  n={emp['n_eff']}\033[0m  "
             f"| limit YES target ≤ {STAGED_TARGET - no_ask:.3f}")

    # ── Limit tracker ─────────────────────────────────────────────────────────
    for pos in _staged.get(ticker, []):
        if pos.get("arb_fired"):
            continue
        other_price = _no_ask if pos["other"] == "No" else _yes_ask
        total  = pos["entry_price"] + other_price
        if other_price >= 0.05 and other_price <= pos["limit_target"]:
            profit = 1.0 - total
            _pnl_profit += profit
            _pnl_wins   += 1
            pos["arb_fired"]     = True
            pos["locked_price"]  = round(other_price, 4)
            pos["locked_profit"] = round(profit, 4)
            _log(f"\033[1;92m*** ARB LOCKED — buy {pos['other']} @ {other_price:.3f}  "
                 f"total={total:.3f}  profit={profit:.3f} ({profit*100:.1f}¢) ***\033[0m")

    # ── Direct arb ────────────────────────────────────────────────────────────
    if yes_ask + no_ask < ARB_TARGET and yes_ask > 0.05 and no_ask > 0.05:
        profit = 1.0 - (yes_ask + no_ask)
        _log(f"\033[93m◆ DIRECT ARB  yes={yes_ask:.3f} + no={no_ask:.3f} = "
             f"{yes_ask+no_ask:.3f}  profit={profit:.3f} ({profit*100:.1f}¢)\033[0m")


# ── Render ────────────────────────────────────────────────────────────────────

def _render(elapsed_sec: int, window_et: datetime, ticker: str) -> None:
    os.system("clear")
    now       = datetime.now()
    time_left = max(0, WINDOW_SEC - elapsed_sec)
    minute    = elapsed_sec // 60

    print(f"━━━  KALSHI BTC 15M MONITOR  ━━━  {now.strftime('%H:%M:%S')}  "
          f"━━━  window +{elapsed_sec}s / {time_left}s left  (min {minute}/14)  ━━━")
    print(f"  Ticker: {ticker}   Window: {window_et.strftime('%I:%M %p ET')}")
    print()

    if _btc_price > 0 and _btc_open > 0:
        delta   = (_btc_price - _btc_open) / _btc_open * 100
        emp     = get_odds(elapsed_sec, delta)
        cf      = emp["confidence"][0].upper()

        edge_up = emp["p_up"]   - _yes_ask
        edge_dn = emp["p_down"] - _no_ask

        def _clob_col(price, edge):
            if edge >  MISMATCH_THRESHOLD: return f"\033[92m{price:.3f}\033[0m"
            if edge < -MISMATCH_THRESHOLD: return f"\033[91m{price:.3f}\033[0m"
            return f"{price:.3f}"

        def _edge_col(e):
            if e >  MISMATCH_THRESHOLD: return f"\033[92m{e:>+.3f}\033[0m"
            if e < -MISMATCH_THRESHOLD: return f"\033[91m{e:>+.3f}\033[0m"
            return f"{e:>+.3f}"

        signal = ""
        if edge_up > MISMATCH_THRESHOLD and emp["p_up"] > 0.50:
            signal = f"\033[92m▲ BUY YES\033[0m"
        elif edge_dn > MISMATCH_THRESHOLD and emp["p_down"] > 0.50:
            signal = f"\033[92m▼ BUY NO\033[0m"
        elif _yes_ask + _no_ask < ARB_TARGET:
            signal = f"\033[93m◆ DIRECT ARB\033[0m"

        _pc = "\033[42m\033[30m" if _btc_price > _btc_prev_price else \
              ("\033[41m\033[97m" if _btc_price < _btc_prev_price else "")
        _rc = "\033[0m" if _pc else ""
        delta_col = "\033[92m" if delta >= 0 else "\033[91m"

        print(f"  BTC  {_pc}${_btc_price:>10,.2f}{_rc}   "
              f"delta {delta_col}{delta:>+.3f}%\033[0m   "
              f"open (strike) ${_btc_open:>10,.2f}   n={emp['n_eff']:,}  [{cf}]")
        print()
        print(f"  {'':4}  {'Model':>6}  {'Kalshi':>6}  {'Edge':>6}    "
              f"{'':4}  {'Model':>6}  {'Kalshi':>6}  {'Edge':>6}   {'Sum':>5}  Signal")
        print(f"  {'─'*4}  {'─'*6}  {'─'*6}  {'─'*6}    "
              f"{'─'*4}  {'─'*6}  {'─'*6}  {'─'*6}   {'─'*5}  {'─'*12}")
        print(f"  {'YES':>4}  {emp['p_up']:>6.3f}  {_clob_col(_yes_ask, edge_up)}  {_edge_col(edge_up)}    "
              f"{'NO':>4}  {emp['p_down']:>6.3f}  {_clob_col(_no_ask, edge_dn)}  {_edge_col(edge_dn)}   "
              f"{_yes_ask+_no_ask:>5.3f}  {signal}")
    else:
        print("  Waiting for price data…")

    # ── Staged positions ──────────────────────────────────────────────────────
    print()
    positions = _staged.get(ticker, [])
    if positions:
        for pos in positions:
            other_price = _no_ask if pos["other"] == "No" else _yes_ask
            total  = pos["entry_price"] + other_price
            target = pos["limit_target"]
            gap    = other_price - target
            if pos.get("arb_fired"):
                status = f"\033[1;92m*** LOCKED +{pos.get('locked_profit', 0):.3f} ***\033[0m"
            else:
                status = f"gap {gap:+.3f} to target"
            print(f"  STAGED: {pos['side']} @ {pos['entry_price']:.3f}  "
                  f"(t={pos['entry_sec']}s  model {pos['entry_model']:.1%}  n={pos['entry_neff']})")
            print(f"  {pos['other']:>3} side now: {other_price:.3f}   "
                  f"combined: {total:.3f}   limit ≤ {target:.3f}   {status}")
    else:
        print("  No staged positions this window.")

    # ── P&L ───────────────────────────────────────────────────────────────────
    net     = _pnl_profit - _pnl_loss
    net_col = "\033[92m" if net >= 0 else "\033[91m"
    total_t = _pnl_wins + _pnl_losses
    print()
    print(f"  ── P&L ────────────────────────────────────────────────")
    print(f"  Profit: \033[92m+{_pnl_profit:.3f}\033[0m  "
          f"Loss: \033[91m-{_pnl_loss:.3f}\033[0m  "
          f"Net: {net_col}{net:+.3f}\033[0m  ({net*100:+.1f}¢)   "
          f"Trades: {total_t}  (W:{_pnl_wins} / L:{_pnl_losses})")

    # ── Alerts ────────────────────────────────────────────────────────────────
    if _alerts:
        print()
        print(f"  ── Alerts ──────────────────────────────────────────────")
        for a in _alerts:
            print(a)

    print()
    print(f"  Threshold: {MISMATCH_THRESHOLD:.0%}  "
          f"Direct ARB: {ARB_TARGET:.2f}  Staged ARB: {STAGED_TARGET:.2f}  "
          f"Poll: {POLL_INTERVAL}s  Ctrl+C to stop.")
    print(f"━━━  KALSHI BTC 15M  ━━━  {now.strftime('%H:%M:%S')}  ━━━")


# ── Render loop ───────────────────────────────────────────────────────────────

async def _render_loop() -> None:
    while True:
        window_et   = _current_window_et()
        ticker      = _ticker(window_et)
        elapsed_sec = _elapsed_sec(window_et)
        _check_signal(elapsed_sec, ticker)
        _render(elapsed_sec, window_et, ticker)
        await asyncio.sleep(1)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    import websockets  # noqa: F401 — ensure installed
    load_surface()
    print("Starting Kalshi BTC 15M monitor…")

    async with httpx.AsyncClient() as client:
        await asyncio.gather(
            _poll_kalshi(client),
            _coinbase_feed(),
            _render_loop(),
        )


if __name__ == "__main__":
    asyncio.run(main())
