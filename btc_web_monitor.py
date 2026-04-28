#!/usr/bin/env python3
"""
BTC Odds Monitor — real-time web dashboard.

Run:
    python3 btc_web_monitor.py [--port 7334]
    open http://localhost:7334
"""
import argparse
import asyncio
import json
import time
from datetime import datetime
from typing import Set

import httpx
import uvicorn
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from btc_odds_research import (
    get_odds, load_surface,
    MISMATCH_THRESHOLD, ARB_TARGET, STAGED_TARGET, WINDOW_SEC,
)

CB_CANDLES = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
CB_WS      = "wss://advanced-trade-ws.coinbase.com"
CLOB_WS    = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_API  = "https://gamma-api.polymarket.com/markets"

# ── Shared state ──────────────────────────────────────────────────────────────
_btc_price:      float = 0.0
_btc_prev_price: float = 0.0
_btc_open:       float = 0.0
_window_ts:      int   = 0

_clob_up:    float = 0.0
_clob_dn:    float = 0.0
_token_up:   str   = ""
_token_dn:   str   = ""
_clob_ready: bool  = False

_staged:     dict  = {}

_pnl_profit: float = 0.0
_pnl_loss:   float = 0.0
_pnl_wins:   int   = 0
_pnl_losses: int   = 0

# ── Hold strategy (buy edge, let expire, no arb hedge) ────────────────────────
_hold:        dict  = {}   # window_ts → list of {side, entry_price, entry_sec, entry_model}
_hold_profit: float = 0.0
_hold_loss:   float = 0.0
_hold_wins:   int   = 0
_hold_losses: int   = 0

_alerts: list = []          # [{ts, msg, kind}]  kind: buy|lock|expire|arb
_window_history: list = []  # [{t, model_up, model_dn, clob_up, clob_dn}] current window
_prev_histories: list = []  # last 2 completed windows (each is a list of points)

_clients: Set[WebSocket] = set()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _log_alert(msg: str, kind: str = "info") -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    _alerts.append({"ts": ts, "msg": msg, "kind": kind})
    if len(_alerts) > 60:
        _alerts.pop(0)


# ── Window open + CLOB token fetch ────────────────────────────────────────────

async def _refresh_window_open(ts: int) -> None:
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
                _btc_open  = _btc_price
                _window_ts = ts
    except Exception:
        if _btc_price > 0:
            _btc_open  = _btc_price
            _window_ts = ts


async def _fetch_clob_tokens(ts: int) -> None:
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
            _token_up   = tokens[up_idx]
            _token_dn   = tokens[1 - up_idx]
            _clob_ready = True
    except Exception:
        pass


# ── Limit tracker ─────────────────────────────────────────────────────────────

def _check_limits(ts: int) -> None:
    global _pnl_profit, _pnl_wins
    if _clob_up < 0.05 or _clob_dn < 0.05:
        return
    for pos in _staged.get(ts, []):
        if pos.get("arb_fired"):
            continue
        other  = _clob_dn if pos["other"] == "Down" else _clob_up
        total  = pos["entry_price"] + other
        if other <= pos["limit_target"]:
            profit = 1.0 - total
            _pnl_profit += profit
            _pnl_wins   += 1
            pos["arb_fired"]      = True
            pos["locked_price"]   = round(other, 4)
            pos["locked_total"]   = round(total, 4)
            pos["locked_profit"]  = round(profit, 4)
            _log_alert(
                f"ARB LOCKED — buy {pos['other']} @ {other:.3f} | "
                f"total={total:.3f} | profit={profit:.3f} ({profit*100:.1f}¢)",
                kind="lock"
            )


# ── Signal check ──────────────────────────────────────────────────────────────

def _check_signal(elapsed_sec: int) -> None:
    global _staged, _hold
    if not _clob_ready or _btc_open == 0 or _btc_price == 0:
        return
    if elapsed_sec <= 30 or elapsed_sec >= WINDOW_SEC:
        return
    if _clob_up < 0.05 or _clob_up > 0.95 or _clob_dn < 0.05 or _clob_dn > 0.95:
        return

    ts    = _window_ts
    delta = (_btc_price - _btc_open) / _btc_open * 100
    emp   = get_odds(elapsed_sec, delta)

    if emp["confidence"] not in ("high", "med"):
        return

    clob_up  = _clob_up
    clob_dn  = _clob_dn
    edge_up  = emp["p_up"]  - clob_up
    edge_dn  = emp["p_down"] - clob_dn

    arb_open_sides  = {p["side"] for p in _staged.get(ts, []) if not p.get("arb_fired")}
    hold_open_sides = {p["side"] for p in _hold.get(ts, [])}

    if edge_up > MISMATCH_THRESHOLD and emp["p_up"] > 0.50:
        if "Up" not in arb_open_sides:
            _staged.setdefault(ts, []).append({
                "side": "Up", "other": "Down",
                "entry_price": clob_up, "entry_sec": elapsed_sec,
                "entry_model": emp["p_up"], "entry_neff": emp["n_eff"],
                "limit_target": round(STAGED_TARGET - clob_up, 3),
            })
        if "Up" not in hold_open_sides:
            _hold.setdefault(ts, []).append({
                "side": "Up", "entry_price": clob_up,
                "entry_sec": elapsed_sec, "entry_model": emp["p_up"],
            })
        _log_alert(
            f"BUY UP @ {clob_up:.3f} | model={emp['p_up']:.1%} edge=+{edge_up:.2f} "
            f"n={emp['n_eff']} | arb limit DOWN ≤ {STAGED_TARGET - clob_up:.3f}",
            kind="buy"
        )

    elif edge_dn > MISMATCH_THRESHOLD and emp["p_down"] > 0.50:
        if "Down" not in arb_open_sides:
            _staged.setdefault(ts, []).append({
                "side": "Down", "other": "Up",
                "entry_price": clob_dn, "entry_sec": elapsed_sec,
                "entry_model": emp["p_down"], "entry_neff": emp["n_eff"],
                "limit_target": round(STAGED_TARGET - clob_dn, 3),
            })
        if "Down" not in hold_open_sides:
            _hold.setdefault(ts, []).append({
                "side": "Down", "entry_price": clob_dn,
                "entry_sec": elapsed_sec, "entry_model": emp["p_down"],
            })
        _log_alert(
            f"BUY DOWN @ {clob_dn:.3f} | model={emp['p_down']:.1%} edge=+{edge_dn:.2f} "
            f"n={emp['n_eff']} | arb limit UP ≤ {STAGED_TARGET - clob_dn:.3f}",
            kind="buy"
        )

    if clob_up + clob_dn < ARB_TARGET and clob_up > 0 and clob_dn > 0:
        profit = 1.0 - (clob_up + clob_dn)
        _log_alert(
            f"DIRECT ARB | up={clob_up:.3f} + dn={clob_dn:.3f} = {clob_up+clob_dn:.3f} | "
            f"profit={profit:.3f} ({profit*100:.1f}¢)",
            kind="arb"
        )


# ── Broadcast ─────────────────────────────────────────────────────────────────

async def _broadcast(payload: dict) -> None:
    global _clients
    dead = set()
    msg  = json.dumps(payload)
    for ws in _clients:
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    _clients -= dead


# ── WebSocket feeds ───────────────────────────────────────────────────────────

async def _coinbase_feed() -> None:
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
                                if _btc_open == 0 and _window_ts > 0:
                                    _btc_open = price
        except Exception:
            await asyncio.sleep(3)


async def _clob_feed() -> None:
    global _clob_up, _clob_dn
    subscribed: set = set()
    while True:
        try:
            async with websockets.connect(CLOB_WS, ping_interval=20) as ws:
                async def _subscribe():
                    tokens = [t for t in [_token_up, _token_dn] if t]
                    new    = set(tokens) - subscribed
                    if new:
                        await ws.send(json.dumps({"assets_ids": list(new), "type": "market"}))
                        subscribed.update(new)

                await _subscribe()
                async for raw in ws:
                    await _subscribe()
                    msg = json.loads(raw)
                    if isinstance(msg, list):
                        for book in msg:
                            tid  = book.get("asset_id", "")
                            asks = book.get("asks", [])
                            if asks and tid:
                                price = float(asks[0]["price"])
                                if tid == _token_up:   _clob_up = price
                                elif tid == _token_dn: _clob_dn = price
                    elif isinstance(msg, dict) and "price_changes" in msg:
                        for ch in msg["price_changes"]:
                            tid      = ch.get("asset_id", "")
                            best_ask = float(ch.get("best_ask", 0) or 0)
                            if best_ask <= 0:
                                continue
                            if tid == _token_up:   _clob_up = best_ask
                            elif tid == _token_dn: _clob_dn = best_ask
                        now         = int(time.time())
                        ts          = (now // WINDOW_SEC) * WINDOW_SEC
                        elapsed_sec = now - ts
                        _check_signal(elapsed_sec)
        except Exception:
            subscribed.clear()
            await asyncio.sleep(3)


# ── Window manager ────────────────────────────────────────────────────────────

async def _window_manager() -> None:
    global _staged, _window_ts, _window_history, _prev_histories
    last_ts = 0
    while True:
        now = int(time.time())
        ts  = (now // WINDOW_SEC) * WINDOW_SEC
        if ts != last_ts:
            # Archive current window history
            if _window_history:
                _prev_histories.append(list(_window_history))
                if len(_prev_histories) > 2:
                    _prev_histories.pop(0)
            _window_history = []
            _window_ts = ts
            # NOTE: do NOT clear _staged here — broadcast loop settles expired
            # positions first, then clears them to avoid the race condition.
            await asyncio.gather(
                _refresh_window_open(ts),
                _fetch_clob_tokens(ts),
            )
            last_ts = ts
        await asyncio.sleep(5)


# ── Broadcast loop ────────────────────────────────────────────────────────────

async def _broadcast_loop() -> None:
    global _window_history, _pnl_loss, _pnl_losses, _hold
    global _hold_profit, _hold_loss, _hold_wins, _hold_losses
    last_settled_ts = 0
    while True:
        now         = int(time.time())
        ts          = (now // WINDOW_SEC) * WINDOW_SEC
        elapsed_sec = now - ts
        time_left   = WINDOW_SEC - elapsed_sec

        # Settle expired positions exactly at window boundary, then purge
        if ts != last_settled_ts:
            for old_ts, positions in list(_staged.items()):
                if old_ts < ts:
                    for pos in positions:
                        if not pos.get("arb_fired") and not pos.get("expired"):
                            _pnl_loss   += pos["entry_price"]
                            _pnl_losses += 1
                            pos["expired"] = True
                            _log_alert(
                                f"EXPIRED — {pos['side']} @ {pos['entry_price']:.3f} | "
                                f"no arb | loss={pos['entry_price']:.3f} ({pos['entry_price']*100:.1f}¢)",
                                kind="expire"
                            )
            # Settle hold positions — winner determined by BTC price vs open
            actual_winner = "Up" if _btc_price >= _btc_open else "Down"
            for old_ts, positions in list(_hold.items()):
                if old_ts < ts:
                    for pos in positions:
                        if pos.get("settled"):
                            continue
                        pos["settled"] = True
                        if pos["side"] == actual_winner:
                            profit = round(1.0 - pos["entry_price"], 4)
                            _hold_profit += profit
                            _hold_wins   += 1
                            _log_alert(
                                f"[HOLD] WON {pos['side']} @ {pos['entry_price']:.3f} | "
                                f"profit={profit:.3f} ({profit*100:.1f}¢)",
                                kind="hold_win"
                            )
                        else:
                            loss = pos["entry_price"]
                            _hold_loss   += loss
                            _hold_losses += 1
                            _log_alert(
                                f"[HOLD] LOST {pos['side']} @ {pos['entry_price']:.3f} | "
                                f"loss={loss:.3f} ({loss*100:.1f}¢)",
                                kind="hold_loss"
                            )
            for old_ts in [k for k in _hold if k < ts]:
                del _hold[old_ts]

            # Now safe to purge arb positions — all settled
            for old_ts in [k for k in _staged if k < ts]:
                del _staged[old_ts]
            last_settled_ts = ts

        _check_limits(ts)

        # Record history point every second
        if _btc_open > 0 and _btc_price > 0 and _clob_ready:
            delta = (_btc_price - _btc_open) / _btc_open * 100
            emp   = get_odds(elapsed_sec, delta)
            point = {
                "t":        elapsed_sec,
                "model_up": emp["p_up"],
                "model_dn": emp["p_down"],
                "clob_up":  _clob_up,
                "clob_dn":  _clob_dn,
            }
            # Replace last point if same second, else append
            if _window_history and _window_history[-1]["t"] == elapsed_sec:
                _window_history[-1] = point
            else:
                _window_history.append(point)
        else:
            emp = None

        # Build payload
        if emp:
            delta = (_btc_price - _btc_open) / _btc_open * 100
            signal = ""
            if _clob_up < emp["p_up"] - MISMATCH_THRESHOLD and emp["p_up"] > 0.50:
                signal = "BUY_UP"
            elif _clob_dn < emp["p_down"] - MISMATCH_THRESHOLD and emp["p_down"] > 0.50:
                signal = "BUY_DOWN"
            elif (_clob_up + _clob_dn) < ARB_TARGET:
                signal = "DIRECT_ARB"

            odds = {
                "model_up":   emp["p_up"],
                "model_dn":   emp["p_down"],
                "clob_up":    _clob_up,
                "clob_dn":    _clob_dn,
                "clob_sum":   round(_clob_up + _clob_dn, 4),
                "edge_up":    round(emp["p_up"]  - _clob_up, 4),
                "edge_dn":    round(emp["p_down"] - _clob_dn, 4),
                "n_eff":      emp["n_eff"],
                "confidence": emp["confidence"][0].upper(),
                "signal":     signal,
                "delta":      round(delta, 4),
            }
        else:
            odds = None

        staged_out = []
        for pos in _staged.get(ts, []):
            other = _clob_dn if pos["other"] == "Down" else _clob_up
            total = pos["entry_price"] + other
            staged_out.append({
                "side":         pos["side"],
                "other":        pos["other"],
                "entry_price":  pos["entry_price"],
                "entry_sec":    pos["entry_sec"],
                "entry_model":  pos["entry_model"],
                "limit_target": pos["limit_target"],
                "other_now":      round(other, 4),
                "total_now":      round(total, 4),
                "gap":            round(other - pos["limit_target"], 4),
                "locked":         pos.get("arb_fired", False),
                "locked_price":   pos.get("locked_price"),
                "locked_total":   pos.get("locked_total"),
                "locked_profit":  pos.get("locked_profit"),
            })

        hold_out = []
        for pos in _hold.get(ts, []):
            hold_out.append({
                "side":        pos["side"],
                "entry_price": pos["entry_price"],
                "entry_sec":   pos["entry_sec"],
                "entry_model": pos["entry_model"],
                "clob_now":    _clob_up if pos["side"] == "Up" else _clob_dn,
            })

        net      = _pnl_profit  - _pnl_loss
        hold_net = _hold_profit - _hold_loss
        payload = {
            "ts":           datetime.now().strftime("%H:%M:%S"),
            "elapsed":      elapsed_sec,
            "time_left":    time_left,
            "minute":       elapsed_sec // 60,
            "btc_price":    _btc_price,
            "btc_open":     _btc_open,
            "btc_up":       _btc_price >= _btc_prev_price,
            "clob_ready":   _clob_ready,
            "odds":         odds,
            "staged":       staged_out,
            "hold":         hold_out,
            "pnl": {
                "profit":  round(_pnl_profit, 4),
                "loss":    round(_pnl_loss, 4),
                "net":     round(net, 4),
                "wins":    _pnl_wins,
                "losses":  _pnl_losses,
            },
            "hold_pnl": {
                "profit":  round(_hold_profit, 4),
                "loss":    round(_hold_loss, 4),
                "net":     round(hold_net, 4),
                "wins":    _hold_wins,
                "losses":  _hold_losses,
            },
            "alerts":       list(reversed(_alerts))[:30],
            "history":      _window_history,
            "prev_histories": _prev_histories,
        }

        await _broadcast(payload)
        await asyncio.sleep(1)


# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI()

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>BTC Odds Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #0d1117; color: #e6edf3;
    font-family: 'Courier New', monospace; font-size: 14px;
    padding: 16px; min-height: 100vh;
  }
  h2 { font-size: 13px; color: #8b949e; text-transform: uppercase;
       letter-spacing: 2px; margin-bottom: 8px; }
  .header {
    display: flex; align-items: center; gap: 24px;
    background: #161b22; border: 1px solid #30363d;
    border-radius: 8px; padding: 12px 20px; margin-bottom: 16px;
  }
  .price { font-size: 28px; font-weight: bold; padding: 4px 10px; border-radius: 4px; }
  .price.up   { background: #1a4731; color: #3fb950; }
  .price.down { background: #4a1c1c; color: #f85149; }
  .price.flat { color: #e6edf3; }
  .hstat { display: flex; flex-direction: column; }
  .hstat span:first-child { font-size: 11px; color: #8b949e; }
  .hstat span:last-child  { font-size: 16px; }
  .timer { margin-left: auto; text-align: right; }
  .timer .big { font-size: 20px; }
  .timer .sub { font-size: 12px; color: #8b949e; }

  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  @media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }

  .card {
    background: #161b22; border: 1px solid #30363d;
    border-radius: 8px; padding: 16px;
  }
  .card.full { grid-column: 1 / -1; }

  /* Odds table */
  .odds-row {
    display: grid; grid-template-columns: 60px 1fr 1fr 1fr;
    gap: 8px; align-items: center; padding: 6px 0;
    border-bottom: 1px solid #21262d;
  }
  .odds-row:last-child { border-bottom: none; }
  .odds-label { font-size: 12px; color: #8b949e; }
  .val { text-align: right; font-size: 15px; }
  .val.edge-buy  { color: #3fb950; font-weight: bold; }
  .val.edge-over { color: #f85149; }
  .col-hdr { text-align: right; font-size: 11px; color: #8b949e; padding: 0 0 4px; }

  /* Signal badge */
  .signal { display: inline-block; padding: 4px 12px; border-radius: 4px;
            font-size: 13px; font-weight: bold; margin-top: 8px; }
  .signal.buy_up    { background: #1a4731; color: #3fb950; }
  .signal.buy_down  { background: #4a1c1c; color: #f85149; }
  .signal.direct_arb { background: #3d2b00; color: #e3b341; }
  .signal.none      { display: none; }

  /* Staged */
  .staged-item {
    border: 1px solid #30363d; border-radius: 6px; padding: 10px 12px;
    margin-bottom: 8px; position: relative;
  }
  .staged-item.locked { border-color: #3fb950; }
  .staged-top { display: flex; justify-content: space-between; align-items: center; }
  .staged-side { font-weight: bold; font-size: 15px; }
  .staged-side.Up   { color: #3fb950; }
  .staged-side.Down { color: #f85149; }
  .locked-badge { background: #1a4731; color: #3fb950; padding: 2px 8px;
                  border-radius: 4px; font-size: 11px; font-weight: bold; }
  .progress-bar { height: 6px; background: #21262d; border-radius: 3px; margin-top: 8px; }
  .progress-fill { height: 100%; border-radius: 3px; background: #e3b341;
                   transition: width 0.3s; }
  .progress-fill.locked { background: #3fb950; }
  .staged-detail { font-size: 12px; color: #8b949e; margin-top: 4px; }

  /* P&L */
  .pnl-row { display: flex; justify-content: space-between; padding: 4px 0;
              border-bottom: 1px solid #21262d; }
  .pnl-row:last-child { border-bottom: none; }
  .green { color: #3fb950; }
  .red   { color: #f85149; }
  .gold  { color: #e3b341; }

  /* Alerts */
  .alerts-list { max-height: 280px; overflow-y: auto; }
  .alert-item { padding: 5px 0; border-bottom: 1px solid #21262d;
                font-size: 12px; line-height: 1.4; }
  .alert-item:last-child { border-bottom: none; }
  .alert-ts { color: #8b949e; margin-right: 8px; }
  .alert-item.buy       { color: #3fb950; }
  .alert-item.lock      { color: #3fb950; font-weight: bold; }
  .alert-item.expire    { color: #f85149; }
  .alert-item.arb       { color: #e3b341; }
  .alert-item.hold_win  { color: #58a6ff; font-weight: bold; }
  .alert-item.hold_loss { color: #f85149; font-style: italic; }

  /* Chart */
  .chart-wrap { position: relative; height: 260px; }

  /* Status bar */
  .statusbar { text-align: center; color: #8b949e; font-size: 11px; margin-top: 16px; }

  #no-data { color: #8b949e; text-align: center; padding: 40px; }
</style>
</head>
<body>

<div class="header">
  <div>
    <div style="font-size:11px;color:#8b949e;margin-bottom:2px;">BTC-USD</div>
    <div class="price flat" id="price">—</div>
  </div>
  <div class="hstat">
    <span>Open</span>
    <span id="open">—</span>
  </div>
  <div class="hstat">
    <span>Delta</span>
    <span id="delta">—</span>
  </div>
  <div class="hstat">
    <span>n_eff</span>
    <span id="neff">—</span>
  </div>
  <div class="timer">
    <div class="big" id="timer">—</div>
    <div class="sub" id="timer-sub">—</div>
  </div>
</div>

<div class="grid">

  <!-- Chart: full width -->
  <div class="card full">
    <h2>Window Progression — Model vs CLOB</h2>
    <div class="chart-wrap">
      <canvas id="chart"></canvas>
    </div>
  </div>

  <!-- Odds -->
  <div class="card">
    <h2>Live Odds</h2>
    <div id="odds-body">
      <div id="no-data">Waiting for data…</div>
    </div>
    <div id="signal-wrap"></div>
  </div>

  <!-- Staged ARB -->
  <div class="card">
    <h2>Strategy A — Staged ARB</h2>
    <div style="font-size:11px;color:#8b949e;margin-bottom:8px">Buy edge, wait for other side to hit limit target (18¢ min profit)</div>
    <div id="staged-body"><div style="color:#8b949e">No staged positions.</div></div>
  </div>

  <!-- Hold -->
  <div class="card">
    <h2>Strategy B — Hold to Expiry</h2>
    <div style="font-size:11px;color:#8b949e;margin-bottom:8px">Buy edge, hold to window close, collect $1 if correct</div>
    <div id="hold-body"><div style="color:#8b949e">No hold positions.</div></div>
  </div>

  <!-- P&L -->
  <div class="card">
    <h2>P&amp;L — Strategy A (ARB)</h2>
    <div id="pnl-body"></div>
  </div>

  <!-- Hold P&L -->
  <div class="card">
    <h2>P&amp;L — Strategy B (Hold)</h2>
    <div id="hold-pnl-body"></div>
  </div>

  <!-- Alerts -->
  <div class="card">
    <h2>Alerts</h2>
    <div class="alerts-list" id="alerts-body"></div>
  </div>

</div>

<div class="statusbar" id="statusbar">Connecting…</div>

<script>
const ws = new WebSocket(`ws://${location.host}/ws`);

// ── Chart ──────────────────────────────────────────────────────────────────
const ctx   = document.getElementById('chart').getContext('2d');
const chart = new Chart(ctx, {
  type: 'line',
  data: {
    datasets: [
      { label: 'Model Up',   borderColor: '#3fb950', backgroundColor: 'transparent',
        borderWidth: 2, pointRadius: 0, tension: 0.2, data: [] },
      { label: 'CLOB Up',    borderColor: '#3fb950', backgroundColor: 'transparent',
        borderWidth: 1.5, borderDash: [5,4], pointRadius: 0, tension: 0.2, data: [] },
      { label: 'Model Down', borderColor: '#f85149', backgroundColor: 'transparent',
        borderWidth: 2, pointRadius: 0, tension: 0.2, data: [] },
      { label: 'CLOB Down',  borderColor: '#f85149', backgroundColor: 'transparent',
        borderWidth: 1.5, borderDash: [5,4], pointRadius: 0, tension: 0.2, data: [] },
      // Prev window 1 (faded)
      { label: 'Prev Model Up',   borderColor: 'rgba(63,185,80,0.2)',  backgroundColor:'transparent',
        borderWidth: 1, pointRadius: 0, tension: 0.2, data: [] },
      { label: 'Prev Model Down', borderColor: 'rgba(248,81,73,0.2)',  backgroundColor:'transparent',
        borderWidth: 1, pointRadius: 0, tension: 0.2, data: [] },
    ]
  },
  options: {
    animation: false,
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { labels: { color: '#8b949e', font: { family: 'Courier New', size: 11 },
                          filter: (i) => i.datasetIndex < 4 } },
      tooltip: { callbacks: {
        label: (c) => ` ${c.dataset.label}: ${c.parsed.y.toFixed(3)}`
      }}
    },
    scales: {
      x: {
        type: 'linear', min: 0, max: 900,
        grid: { color: '#21262d' },
        ticks: { color: '#8b949e', callback: v => v + 's' },
        title: { display: true, text: 'Elapsed (sec)', color: '#8b949e' }
      },
      y: {
        min: 0, max: 1,
        grid: { color: '#21262d' },
        ticks: { color: '#8b949e', callback: v => (v*100).toFixed(0) + '%' },
      }
    }
  }
});

// Threshold reference lines via annotation-free approach (draw after)
function addThresholdPlugin() {
  Chart.register({
    id: 'threshold',
    afterDraw(chart) {
      const { ctx, chartArea, scales } = chart;
      const y50 = scales.y.getPixelForValue(0.5);
      ctx.save();
      ctx.setLineDash([4, 4]);
      ctx.strokeStyle = 'rgba(139,148,158,0.3)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(chartArea.left, y50);
      ctx.lineTo(chartArea.right, y50);
      ctx.stroke();
      ctx.restore();
    }
  });
}
addThresholdPlugin();

// ── Update helpers ────────────────────────────────────────────────────────
function fmt(v, dec=3) { return v != null ? v.toFixed(dec) : '—'; }

function edgeCls(edge) {
  if (edge >  0.07) return 'val edge-buy';
  if (edge < -0.07) return 'val edge-over';
  return 'val';
}

function updateHeader(d) {
  const priceEl = document.getElementById('price');
  priceEl.textContent = d.btc_price > 0
    ? '$' + d.btc_price.toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2})
    : '—';
  priceEl.className = 'price ' + (d.btc_price === d.btc_prev ? 'flat' : d.btc_up ? 'up' : 'down');

  document.getElementById('open').textContent = d.btc_open > 0
    ? '$' + d.btc_open.toLocaleString('en-US', {minimumFractionDigits:2})
    : '—';

  if (d.odds) {
    const delta = d.odds.delta;
    const dEl = document.getElementById('delta');
    dEl.textContent = (delta >= 0 ? '+' : '') + delta.toFixed(3) + '%';
    dEl.style.color = delta >= 0 ? '#3fb950' : '#f85149';
    document.getElementById('neff').textContent =
      d.odds.n_eff.toLocaleString() + ' [' + d.odds.confidence + ']';
  }

  const mm = Math.floor(d.time_left / 60);
  const ss = d.time_left % 60;
  document.getElementById('timer').textContent =
    mm + ':' + String(ss).padStart(2,'0') + ' left';
  document.getElementById('timer-sub').textContent =
    '+' + d.elapsed + 's  (min ' + d.minute + '/14)  ' + d.ts;
}

function updateOdds(d) {
  const o = d.odds;
  if (!o) return;
  const wrap = document.getElementById('odds-body');
  wrap.innerHTML = `
    <div class="odds-row">
      <div class="odds-label"></div>
      <div class="col-hdr">Model</div>
      <div class="col-hdr">CLOB</div>
      <div class="col-hdr">Edge</div>
    </div>
    <div class="odds-row">
      <div class="odds-label">▲ UP</div>
      <div class="val">${fmt(o.model_up)}</div>
      <div class="${edgeCls(o.edge_up)}">${fmt(o.clob_up)}</div>
      <div class="${edgeCls(o.edge_up)}">${o.edge_up >= 0 ? '+' : ''}${fmt(o.edge_up)}</div>
    </div>
    <div class="odds-row">
      <div class="odds-label">▼ DN</div>
      <div class="val">${fmt(o.model_dn)}</div>
      <div class="${edgeCls(o.edge_dn)}">${fmt(o.clob_dn)}</div>
      <div class="${edgeCls(o.edge_dn)}">${o.edge_dn >= 0 ? '+' : ''}${fmt(o.edge_dn)}</div>
    </div>
    <div class="odds-row">
      <div class="odds-label">Sum</div>
      <div class="val">—</div>
      <div class="val">${fmt(o.clob_sum)}</div>
      <div class="val">—</div>
    </div>
  `;

  const sw = document.getElementById('signal-wrap');
  if (o.signal) {
    const labels = { BUY_UP: '▲ BUY UP', BUY_DOWN: '▼ BUY DOWN', DIRECT_ARB: '◆ DIRECT ARB' };
    sw.innerHTML = `<span class="signal ${o.signal.toLowerCase()}">${labels[o.signal]}</span>`;
  } else {
    sw.innerHTML = '';
  }
}

function updateStaged(d) {
  const el = document.getElementById('staged-body');
  if (!d.staged || d.staged.length === 0) {
    el.innerHTML = '<div style="color:#8b949e">No staged positions.</div>';
    return;
  }
  el.innerHTML = d.staged.map(pos => {
    const pct = pos.locked ? 100
      : Math.min(100, Math.max(0, (1 - pos.gap / pos.limit_target) * 100));
    const profit = pos.locked && pos.locked_profit != null ? pos.locked_profit.toFixed(3) : null;
    return `
      <div class="staged-item ${pos.locked ? 'locked' : ''}">
        <div class="staged-top">
          <span>
            <span class="staged-side ${pos.side}">${pos.side === 'Up' ? '▲' : '▼'} ${pos.side}</span>
            <span style="color:#8b949e;font-size:12px"> @ ${fmt(pos.entry_price)}  t=${pos.entry_sec}s</span>
          </span>
          ${pos.locked
            ? `<span class="locked-badge">✓ LOCKED ${profit ? '+'+profit : ''}</span>`
            : `<span style="color:#8b949e;font-size:12px">gap ${pos.gap >= 0 ? '+' : ''}${fmt(pos.gap)} to target</span>`}
        </div>
        <div class="staged-detail">
          Waiting ${pos.other} ≤ ${fmt(pos.limit_target)} &nbsp;|&nbsp;
          ${pos.other} now: ${fmt(pos.other_now)} &nbsp;|&nbsp;
          combined: ${fmt(pos.total_now)} &nbsp;|&nbsp;
          model: ${(pos.entry_model*100).toFixed(1)}%
        </div>
        <div class="progress-bar">
          <div class="progress-fill ${pos.locked ? 'locked' : ''}" style="width:${pct}%"></div>
        </div>
      </div>`;
  }).join('');
}

function updatePnl(d) {
  const p = d.pnl;
  const netCls = p.net >= 0 ? 'green' : 'red';
  document.getElementById('pnl-body').innerHTML = `
    <div class="pnl-row"><span>Profit</span><span class="green">+${fmt(p.profit)} (${(p.profit*100).toFixed(1)}¢)</span></div>
    <div class="pnl-row"><span>Loss</span><span class="red">-${fmt(p.loss)} (${(p.loss*100).toFixed(1)}¢)</span></div>
    <div class="pnl-row"><span>Net</span><span class="${netCls}">${p.net >= 0 ? '+' : ''}${fmt(p.net)} (${(p.net*100).toFixed(1)}¢)</span></div>
    <div class="pnl-row"><span>Trades</span><span>${p.wins + p.losses} &nbsp; <span class="green">W:${p.wins}</span> / <span class="red">L:${p.losses}</span></span></div>
  `;
}

function updateHold(d) {
  const el = document.getElementById('hold-body');
  if (!d.hold || d.hold.length === 0) {
    el.innerHTML = '<div style="color:#8b949e">No hold positions.</div>';
    return;
  }
  el.innerHTML = d.hold.map(pos => {
    const clr = pos.side === 'Up' ? '#3fb950' : '#f85149';
    const arr = pos.side === 'Up' ? '▲' : '▼';
    const move = pos.clob_now - pos.entry_price;
    const moveCls = move >= 0 ? 'green' : 'red';
    return `
      <div class="staged-item">
        <div class="staged-top">
          <span>
            <span class="staged-side ${pos.side}">${arr} ${pos.side}</span>
            <span style="color:#8b949e;font-size:12px"> @ ${fmt(pos.entry_price)}  t=${pos.entry_sec}s</span>
          </span>
          <span style="color:#8b949e;font-size:12px">model ${(pos.entry_model*100).toFixed(1)}%</span>
        </div>
        <div class="staged-detail">
          CLOB now: <span style="color:${clr}">${fmt(pos.clob_now)}</span>
          &nbsp;|&nbsp;
          move: <span class="${moveCls}">${move >= 0 ? '+' : ''}${fmt(move)}</span>
          &nbsp;|&nbsp; holding to expiry
        </div>
      </div>`;
  }).join('');
}

function updateHoldPnl(d) {
  const p = d.hold_pnl;
  if (!p) return;
  const netCls = p.net >= 0 ? 'green' : 'red';
  document.getElementById('hold-pnl-body').innerHTML = `
    <div class="pnl-row"><span>Profit</span><span class="green">+${fmt(p.profit)} (${(p.profit*100).toFixed(1)}¢)</span></div>
    <div class="pnl-row"><span>Loss</span><span class="red">-${fmt(p.loss)} (${(p.loss*100).toFixed(1)}¢)</span></div>
    <div class="pnl-row"><span>Net</span><span class="${netCls}">${p.net >= 0 ? '+' : ''}${fmt(p.net)} (${(p.net*100).toFixed(1)}¢)</span></div>
    <div class="pnl-row"><span>Trades</span><span>${p.wins + p.losses} &nbsp; <span class="green">W:${p.wins}</span> / <span class="red">L:${p.losses}</span></span></div>
  `;
}

function updateAlerts(d) {
  const el = document.getElementById('alerts-body');
  if (!d.alerts || d.alerts.length === 0) {
    el.innerHTML = '<div style="color:#8b949e">No alerts yet.</div>';
    return;
  }
  el.innerHTML = d.alerts.map(a =>
    `<div class="alert-item ${a.kind}">
       <span class="alert-ts">${a.ts}</span>${a.msg}
     </div>`
  ).join('');
}

function updateChart(d) {
  const toXY = (arr, key) => arr.map(p => ({ x: p.t, y: p[key] }));

  chart.data.datasets[0].data = toXY(d.history, 'model_up');
  chart.data.datasets[1].data = toXY(d.history, 'clob_up');
  chart.data.datasets[2].data = toXY(d.history, 'model_dn');
  chart.data.datasets[3].data = toXY(d.history, 'clob_dn');

  const prev = d.prev_histories && d.prev_histories.length > 0
    ? d.prev_histories[d.prev_histories.length - 1] : [];
  chart.data.datasets[4].data = toXY(prev, 'model_up');
  chart.data.datasets[5].data = toXY(prev, 'model_dn');

  chart.update('none');
}

// ── Main update ───────────────────────────────────────────────────────────
ws.onmessage = (e) => {
  const d = JSON.parse(e.data);
  updateHeader(d);
  updateOdds(d);
  updateChart(d);
  updateStaged(d);
  updateHold(d);
  updatePnl(d);
  updateHoldPnl(d);
  updateAlerts(d);
  document.getElementById('statusbar').textContent =
    `Surface: LOADED  |  Threshold: 7%  |  Direct ARB: 0.90  |  Staged ARB: 0.82  |  Live`;
};

ws.onclose = () => {
  document.getElementById('statusbar').textContent = '⚠ Disconnected — reload to reconnect';
};
</script>
</body>
</html>
"""


@app.get("/")
async def index():
    return HTMLResponse(HTML)


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    _clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        _clients.discard(websocket)
    except Exception:
        _clients.discard(websocket)


@app.on_event("startup")
async def startup():
    load_surface()
    global _window_ts
    _window_ts = (int(time.time()) // WINDOW_SEC) * WINDOW_SEC
    now = int(time.time())
    ts  = (now // WINDOW_SEC) * WINDOW_SEC
    asyncio.create_task(_window_manager())
    asyncio.create_task(_coinbase_feed())
    asyncio.create_task(_clob_feed())
    asyncio.create_task(_broadcast_loop())
    asyncio.create_task(_refresh_window_open(ts))
    asyncio.create_task(_fetch_clob_tokens(ts))


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=7334)
    args = parser.parse_args()
    print(f"Starting BTC Odds Monitor at http://localhost:{args.port}")
    uvicorn.run("btc_web_monitor:app", host="0.0.0.0", port=args.port, log_level="warning")
