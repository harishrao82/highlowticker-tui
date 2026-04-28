#!/usr/bin/env python3
"""
BTC 15-min live odds monitor — streaming mode.

Streams real-time BTC price (Coinbase WS) and CLOB prices (Polymarket WS).
On every tick, compares CLOB asks to historical model odds and prints alerts
when the gap exceeds the threshold.

Staged arb: when leg 1 is bought (cheap side flagged), monitors the other side
for a limit-order target so both sides together cost < ARB_TARGET.

Build/refresh the surface:
    python3 btc_model_builder.py --days 365

Run:
    python3 btc_odds_research.py --live
    python3 btc_odds_research.py          # just print summary table
"""
import asyncio
import json
import os
import time
import argparse
from datetime import datetime
from pathlib import Path

import httpx
import websockets

# ── Config ─────────────────────────────────────────────────────────────────────
SURFACE_FILE = Path.home() / ".btc_model_surface.json"
GAMMA_API    = "https://gamma-api.polymarket.com/markets"
CB_CANDLES   = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
CB_WS        = "wss://advanced-trade-ws.coinbase.com"
CLOB_WS      = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WINDOW_SEC   = 900

MISMATCH_THRESHOLD = 0.07   # flag when CLOB underprices vs model by more than this
ARB_TARGET         = 0.90   # direct arb: both sides together must cost < this (10¢ min profit)
STAGED_TARGET      = 0.82   # staged arb: leg1 + leg2 limit target (18¢ min profit)


# ── Surface lookup ─────────────────────────────────────────────────────────────

_surface: dict = {}


def load_surface() -> None:
    global _surface
    if not SURFACE_FILE.exists():
        raise FileNotFoundError(
            f"Surface not found: {SURFACE_FILE}\n"
            f"Run: python3 btc_model_builder.py"
        )
    _surface = json.loads(SURFACE_FILE.read_text())
    meta = _surface["meta"]
    print(f"Loaded surface: {meta['n_windows']:,} windows · "
          f"{meta['n_obs']:,} obs · built {meta['built_at'][:19]} UTC")


def get_odds(time_seconds: float, delta_pct: float) -> dict:
    """O(1) lookup into precomputed surface grid."""
    t_vals = _surface["t_vals"]
    d_vals = _surface["d_vals"]
    t_step = _surface["meta"]["t_step"]
    d_step = _surface["meta"]["d_step"]
    d_min  = _surface["meta"]["d_min"]

    ti = max(0, min(int(round(time_seconds / t_step)), len(t_vals) - 1))
    di = max(0, min(int(round((delta_pct - d_min) / d_step)), len(d_vals) - 1))

    cell  = _surface["surface"][ti][di]
    p_up  = cell["p_up"]
    n_eff = cell["n_eff"]
    return {
        "p_up":       p_up,
        "p_down":     round(1 - p_up, 4),
        "n_eff":      n_eff,
        "confidence": "high" if n_eff >= 30 else "med" if n_eff >= 10 else "low",
    }


def print_summary() -> None:
    meta = _surface["meta"]
    print(f"\n{'='*65}")
    print(f"SURFACE: {meta['n_windows']:,} 15-min windows  ·  {meta['n_obs']:,} training obs")
    print(f"{'='*65}\n")
    print("P(Up) — time (s) × delta from open:")
    times  = [60, 90, 120, 240, 420, 600, 780]
    deltas = [-0.30, -0.20, -0.10, -0.05, 0.00, +0.05, +0.10, +0.20, +0.30]
    hdr = f"{'t→':>5}" + "".join(f"  {d:>+6.2f}%" for d in deltas)
    print(hdr)
    print("─" * len(hdr))
    for t in times:
        row = f"{t:>4}s"
        for d in deltas:
            r = get_odds(t, d)
            tag = "" if r["confidence"] == "high" else "~" if r["confidence"] == "med" else "?"
            row += f"  {r['p_up']:>6.1%}{tag}"
        print(row)
    print("\n  ~ med confidence   ? low confidence\n")


# ── Shared live state ──────────────────────────────────────────────────────────

# BTC price state
_btc_price:      float = 0.0
_btc_prev_price: float = 0.0   # previous tick — used for up/down color
_btc_open:       float = 0.0   # window open price (fetched once per window)
_window_ts:      int   = 0     # current window start timestamp

# CLOB state for current BTC window
_clob_up:        float = 0.0
_clob_dn:        float = 0.0
_token_up:       str   = ""
_token_dn:       str   = ""
_clob_ready:     bool  = False

# Staged arb
_staged: dict = {}   # window_ts → list of {side, other, entry_price, entry_sec, entry_model, entry_neff}

# Running P&L (per share, paper)
_pnl_profit:  float = 0.0   # sum of locked arb profits
_pnl_loss:    float = 0.0   # sum of unhedged leg-1 costs written off at window close
_pnl_wins:    int   = 0     # arb locks completed
_pnl_losses:  int   = 0     # positions closed without arb

# Alert log (printed below the live line)
_alerts: list[str] = []
MAX_ALERTS = 12


def _log_alert(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    _alerts.append(f"  {ts}  {msg}")
    if len(_alerts) > MAX_ALERTS:
        _alerts.pop(0)


# ── Window open price fetch ────────────────────────────────────────────────────

async def _refresh_window_open(ts: int) -> None:
    """Fetch the open price for the current window from Coinbase REST."""
    global _btc_open, _window_ts
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(CB_CANDLES,
                params={"granularity": 60, "start": ts, "end": ts + 180})
            candles = r.json()
            if isinstance(candles, list) and candles:
                _btc_open  = candles[-1][3]
                _window_ts = ts
            elif _btc_price > 0:
                _btc_open  = _btc_price   # fallback: use current price
                _window_ts = ts
    except Exception:
        if _btc_price > 0:
            _btc_open  = _btc_price
            _window_ts = ts


# ── CLOB market metadata fetch ────────────────────────────────────────────────

async def _fetch_clob_tokens(ts: int) -> None:
    """Fetch token IDs for the current BTC window from Gamma API."""
    global _token_up, _token_dn, _clob_ready
    slug = f"btc-updown-15m-{ts}"
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get(GAMMA_API, params={"slug": slug})
            market = r.json()
            if not market:
                return
            m        = market[0]
            tokens   = json.loads(m.get("clobTokenIds", "[]"))
            outcomes = json.loads(m.get("outcomes", "[]"))
            up_idx   = next((i for i, o in enumerate(outcomes)
                             if "up" in str(o).lower()), 0)
            _token_up  = tokens[up_idx]
            _token_dn  = tokens[1 - up_idx]
            _clob_ready = True
    except Exception:
        pass


# ── Limit order tracker (runs every second from render loop) ──────────────────

def _check_limits(ts: int) -> None:
    """Check all staged positions against current CLOB prices and lock arb if hit."""
    global _pnl_profit, _pnl_wins
    if _clob_up < 0.05 or _clob_dn < 0.05:
        return
    for pos in _staged.get(ts, []):
        if pos.get("arb_fired"):
            continue
        other  = _clob_dn if pos["other"] == "Down" else _clob_up
        total  = pos["entry_price"] + other
        target = pos["limit_target"]
        if other <= target:
            profit = 1.0 - total
            _pnl_profit += profit
            _pnl_wins   += 1
            _log_alert(
                f"\033[1;92m*** ARB LOCKED — buy {pos['other']} @ {other:.3f}  "
                f"total={total:.3f}  profit={profit:.3f}/share "
                f"({profit*100:.1f}¢) ***\033[0m"
            )
            pos["arb_fired"] = True


# ── Core signal check ─────────────────────────────────────────────────────────

def _check_signal(elapsed_sec: int) -> None:
    """Run on every CLOB or price tick. Flags edges and arb opportunities."""
    global _staged

    if not _clob_ready or _btc_open == 0 or _btc_price == 0:
        return
    if elapsed_sec <= 30 or elapsed_sec >= WINDOW_SEC:
        return

    ts    = _window_ts
    delta = (_btc_price - _btc_open) / _btc_open * 100
    emp   = get_odds(elapsed_sec, delta)

    clob_up  = _clob_up
    clob_dn  = _clob_dn
    clob_sum = clob_up + clob_dn

    # Reject stale/garbage CLOB ticks — valid market price is always 0.05–0.95
    if clob_up < 0.05 or clob_up > 0.95 or clob_dn < 0.05 or clob_dn > 0.95:
        return

    # ── Edge detection ─────────────────────────────────────────────────────────
    if emp["confidence"] in ("high", "med"):
        edge_up = emp["p_up"] - clob_up
        edge_dn = emp["p_down"] - clob_dn

        # Check if there's already an open (unfired) position on this side
        open_positions = _staged.get(ts, [])
        open_sides = {p["side"] for p in open_positions if not p.get("arb_fired")}

        if edge_up > MISMATCH_THRESHOLD and emp["p_up"] > 0.50 and "Up" not in open_sides:
            pos = {
                "side": "Up", "other": "Down",
                "entry_price": clob_up, "entry_sec": elapsed_sec,
                "entry_model": emp["p_up"], "entry_neff": emp["n_eff"],
                "limit_target": round(STAGED_TARGET - clob_up, 3),
            }
            _staged.setdefault(ts, []).append(pos)
            _log_alert(
                f"\033[92m▲ BUY UP  @ {clob_up:.3f}  "
                f"model={emp['p_up']:.1%}  edge=+{edge_up:.2f}  n={emp['n_eff']}\033[0m  "
                f"| limit DOWN target ≤ {STAGED_TARGET - clob_up:.3f}"
            )
        elif edge_dn > MISMATCH_THRESHOLD and emp["p_down"] > 0.50 and "Down" not in open_sides:
            pos = {
                "side": "Down", "other": "Up",
                "entry_price": clob_dn, "entry_sec": elapsed_sec,
                "entry_model": emp["p_down"], "entry_neff": emp["n_eff"],
                "limit_target": round(STAGED_TARGET - clob_dn, 3),
            }
            _staged.setdefault(ts, []).append(pos)
            _log_alert(
                f"\033[92m▼ BUY DOWN @ {clob_dn:.3f}  "
                f"model={emp['p_down']:.1%}  edge=+{edge_dn:.2f}  n={emp['n_eff']}\033[0m  "
                f"| limit UP target ≤ {STAGED_TARGET - clob_dn:.3f}"
            )

    # ── Direct arb ────────────────────────────────────────────────────────────
    if clob_sum < ARB_TARGET and clob_up > 0 and clob_dn > 0:
        profit = 1.0 - clob_sum
        _log_alert(
            f"\033[93m◆ DIRECT ARB  up={clob_up:.3f} + dn={clob_dn:.3f} = {clob_sum:.3f}  "
            f"profit={profit:.3f}/share ({profit*100:.1f}¢)\033[0m"
        )


# ── Render ─────────────────────────────────────────────────────────────────────

def _render(elapsed_sec: int) -> None:
    os.system("clear")
    now       = datetime.now()
    ts        = _window_ts
    time_left = max(0, WINDOW_SEC - elapsed_sec)
    minute    = elapsed_sec // 60

    print(f"━━━  BTC ODDS MONITOR  ━━━  {now.strftime('%H:%M:%S')}  "
          f"━━━  window +{elapsed_sec}s  /  {time_left}s left  (min {minute}/14)  ━━━")
    print()

    if _btc_open > 0 and _btc_price > 0:
        delta = (_btc_price - _btc_open) / _btc_open * 100
        emp   = get_odds(elapsed_sec, delta)
        cf    = emp["confidence"][0].upper()

        edge_up = emp["p_up"] - _clob_up   # positive = CLOB underpriced (edge)
        edge_dn = emp["p_down"] - _clob_dn

        def _clob_col(clob, edge):
            """Color CLOB price: green if underpriced vs model, red if overpriced."""
            if edge >  MISMATCH_THRESHOLD: return f"\033[92m{clob:.3f}\033[0m"
            if edge < -MISMATCH_THRESHOLD: return f"\033[91m{clob:.3f}\033[0m"
            return f"{clob:.3f}"

        def _edge_col(e):
            if e >  MISMATCH_THRESHOLD: return f"\033[92m{e:>+.3f}\033[0m"
            if e < -MISMATCH_THRESHOLD: return f"\033[91m{e:>+.3f}\033[0m"
            return f"{e:>+.3f}"

        signal = ""
        if edge_up > MISMATCH_THRESHOLD and emp["p_up"] > 0.50:
            signal = f"\033[92m▲ BUY UP\033[0m"
        elif edge_dn > MISMATCH_THRESHOLD and emp["p_down"] > 0.50:
            signal = f"\033[92m▼ BUY DOWN\033[0m"
        elif (_clob_up + _clob_dn) < ARB_TARGET:
            signal = f"\033[93m◆ DIRECT ARB\033[0m"

        _pc = "\033[42m\033[30m" if _btc_price > _btc_prev_price else ("\033[41m\033[97m" if _btc_price < _btc_prev_price else "")
        _rc = "\033[0m" if _pc else ""
        delta_col = "\033[92m" if delta >= 0 else "\033[91m"
        print(f"  BTC  {_pc}${_btc_price:>10,.2f}{_rc}   "
              f"delta {delta_col}{delta:>+.3f}%\033[0m   "
              f"open ${_btc_open:>10,.2f}   "
              f"n={emp['n_eff']:,}  [{cf}]")
        print()
        #  Paired layout: UP block | DN block
        #  Model  CLOB  Edge  |  Model  CLOB  Edge  Sum  Signal
        print(f"  {'':4}  {'Model':>6}  {'CLOB':>6}  {'Edge':>6}    "
              f"{'':4}  {'Model':>6}  {'CLOB':>6}  {'Edge':>6}   {'Sum':>5}  Signal")
        print(f"  {'─'*4}  {'─'*6}  {'─'*6}  {'─'*6}    "
              f"{'─'*4}  {'─'*6}  {'─'*6}  {'─'*6}   {'─'*5}  {'─'*12}")
        print(f"  {'UP':>4}  {emp['p_up']:>6.3f}  {_clob_col(_clob_up, edge_up):>6}  {_edge_col(edge_up):>6}    "
              f"{'DN':>4}  {emp['p_down']:>6.3f}  {_clob_col(_clob_dn, edge_dn):>6}  {_edge_col(edge_dn):>6}   "
              f"{_clob_up+_clob_dn:>5.3f}  {signal}")
    else:
        print("  Waiting for price data…")

    # ── Staged arb status ──────────────────────────────────────────────────────
    print()
    positions = _staged.get(ts, [])
    if positions:
        for pos in positions:
            leg1   = pos["entry_price"]
            other  = _clob_dn if pos["other"] == "Down" else _clob_up
            total  = leg1 + other
            target = pos["limit_target"]
            gap    = other - target
            status = "\033[1;92m*** LOCKED ***\033[0m" if pos.get("arb_fired") else \
                     f"gap {gap:+.3f} to target"
            print(f"  STAGED: BTC {pos['side']} @ {leg1:.3f}  "
                  f"(t={pos['entry_sec']}s  model {pos['entry_model']:.1%}  n={pos['entry_neff']})")
            print(f"  {pos['other']:>5} side now: {other:.3f}   "
                  f"combined: {total:.3f}   limit target ≤ {target:.3f}   {status}")
    else:
        print("  No staged position this window.")

    # ── P&L counter ───────────────────────────────────────────────────────────
    net     = _pnl_profit - _pnl_loss
    total_t = _pnl_wins + _pnl_losses
    net_col = "\033[92m" if net >= 0 else "\033[91m"
    print()
    print(f"  ── P&L ─────────────────────────────────────────────────")
    print(f"  Profit: \033[92m+{_pnl_profit:.3f}\033[0m  "
          f"Loss: \033[91m-{_pnl_loss:.3f}\033[0m  "
          f"Net: {net_col}{net:+.3f}\033[0m  "
          f"({net*100:+.1f}¢/share)   "
          f"Trades: {total_t}  (W:{_pnl_wins} / L:{_pnl_losses})")

    # ── Alert log ──────────────────────────────────────────────────────────────
    if _alerts:
        print()
        print("  ── Alerts ──────────────────────────────────────────────")
        for a in _alerts:
            print(a)

    print()
    print(f"  Surface: {_surface['meta']['n_windows']:,} windows  "
          f"Threshold: {MISMATCH_THRESHOLD:.0%}  "
          f"Direct ARB: {ARB_TARGET:.2f}  Staged ARB: {STAGED_TARGET:.2f}  "
          f"Streaming live  Ctrl+C to stop.")


# ── WebSocket feeds ────────────────────────────────────────────────────────────

async def _coinbase_feed() -> None:
    """Stream BTC price from Coinbase Advanced Trade WS."""
    global _btc_price, _btc_prev_price, _btc_open, _window_ts
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
                                # If open not set yet, use current price as fallback
                                if _btc_open == 0 and _window_ts > 0:
                                    _btc_open = price
        except Exception as e:
            await asyncio.sleep(3)


async def _clob_feed() -> None:
    """Stream CLOB prices from Polymarket WS."""
    global _clob_up, _clob_dn
    subscribed: set[str] = set()

    while True:
        try:
            async with websockets.connect(CLOB_WS, ping_interval=20) as ws:

                async def _subscribe() -> None:
                    tokens = [t for t in [_token_up, _token_dn] if t]
                    new    = set(tokens) - subscribed
                    if new:
                        await ws.send(json.dumps({"assets_ids": list(new), "type": "market"}))
                        subscribed.update(new)

                await _subscribe()

                async for raw in ws:
                    await _subscribe()   # pick up new tokens when window rolls

                    msg = json.loads(raw)

                    # Snapshot
                    if isinstance(msg, list):
                        for book in msg:
                            tid  = book.get("asset_id", "")
                            asks = book.get("asks", [])
                            if asks and tid:
                                price = float(asks[0]["price"])
                                if tid == _token_up:   _clob_up = price
                                elif tid == _token_dn: _clob_dn = price

                    # Incremental update
                    elif isinstance(msg, dict) and "price_changes" in msg:
                        for ch in msg["price_changes"]:
                            tid       = ch.get("asset_id", "")
                            best_ask  = float(ch.get("best_ask", 0) or 0)
                            if best_ask <= 0:
                                continue
                            if tid == _token_up:   _clob_up = best_ask
                            elif tid == _token_dn: _clob_dn = best_ask

                        # Signal check on every CLOB tick
                        now         = int(time.time())
                        ts          = (now // WINDOW_SEC) * WINDOW_SEC
                        elapsed_sec = now - ts
                        _check_signal(elapsed_sec)

        except Exception as e:
            subscribed.clear()
            await asyncio.sleep(3)


# ── Window manager ────────────────────────────────────────────────────────────

async def _window_manager() -> None:
    """Detects window rolls, fetches open price and CLOB tokens for each new window."""
    global _staged, _window_ts
    last_ts = 0

    while True:
        now = int(time.time())
        ts  = (now // WINDOW_SEC) * WINDOW_SEC

        if ts != last_ts:
            # New window — set ts immediately so price fallback can use it
            _window_ts = ts
            # NOTE: do NOT clear _staged here — render loop settles expired positions
            # first, then clears them. Clearing here causes a race where positions
            # disappear before they can be marked as losses.
            await asyncio.gather(
                _refresh_window_open(ts),
                _fetch_clob_tokens(ts),
            )
            last_ts = ts

        await asyncio.sleep(5)


# ── Render loop ───────────────────────────────────────────────────────────────

async def _render_loop() -> None:
    """Redraws the screen every second. Also runs limit tracker and expiry settlement."""
    global _pnl_loss, _pnl_losses
    last_settled_ts = 0
    while True:
        now         = int(time.time())
        ts          = (now // WINDOW_SEC) * WINDOW_SEC
        elapsed_sec = now - ts

        # Settle expired positions exactly at window boundary, then purge them
        if ts != last_settled_ts:
            for old_ts, positions in list(_staged.items()):
                if old_ts < ts:
                    for pos in positions:
                        if not pos.get("arb_fired") and not pos.get("expired"):
                            _pnl_loss   += pos["entry_price"]
                            _pnl_losses += 1
                            pos["expired"] = True
                            _log_alert(
                                f"\033[91m✗ EXPIRED — {pos['side']} @ {pos['entry_price']:.3f}  "
                                f"no arb  loss={pos['entry_price']:.3f}/share "
                                f"({pos['entry_price']*100:.1f}¢)\033[0m"
                            )
            # Now safe to purge — all old positions have been settled
            for old_ts in [k for k in _staged if k < ts]:
                del _staged[old_ts]
            last_settled_ts = ts

        _check_limits(ts)
        _render(elapsed_sec)
        await asyncio.sleep(1)


# ── Main ───────────────────────────────────────────────────────────────────────

async def run_live() -> None:
    global _window_ts
    # Set window ts immediately so feeds have context from the first tick
    _window_ts = (int(time.time()) // WINDOW_SEC) * WINDOW_SEC
    # Fetch open price and CLOB tokens before starting streams
    now = int(time.time())
    ts  = (now // WINDOW_SEC) * WINDOW_SEC
    await asyncio.gather(
        _refresh_window_open(ts),
        _fetch_clob_tokens(ts),
    )
    await asyncio.gather(
        _window_manager(),
        _coinbase_feed(),
        _clob_feed(),
        _render_loop(),
    )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true",
                        help="Run streaming live monitor")
    args = parser.parse_args()

    load_surface()
    print_summary()

    if args.live:
        print("Starting streams… (Ctrl+C to stop)\n")
        await run_live()
    else:
        print("Run with --live to start the streaming monitor.")
        print("Run  python3 btc_model_builder.py  to rebuild the surface.")


if __name__ == "__main__":
    asyncio.run(main())
