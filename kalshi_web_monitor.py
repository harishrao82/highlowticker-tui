#!/usr/bin/env python3
"""
Kalshi BTC 15-min Up/Down — real-time web dashboard.

Run:
    python3 kalshi_web_monitor.py [--port 7335]
    open http://localhost:7335
"""
import argparse
import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Set

import httpx
import uvicorn
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from btc_odds_research import (
    get_odds, load_surface,
    MISMATCH_THRESHOLD, WINDOW_SEC,
)

KALSHI_API    = "https://api.elections.kalshi.com/trade-api/v2"
CB_WS         = "wss://advanced-trade-ws.coinbase.com"
POLL_INTERVAL = 2
ARB_TARGET    = 0.90
STAGED_TARGET = 0.82
ET_OFFSET     = timedelta(hours=-4)

# ── Shared state ──────────────────────────────────────────────────────────────
_btc_price:      float = 0.0
_btc_prev_price: float = 0.0
_btc_open:       float = 0.0   # floor_strike from Kalshi

_yes_ask: float = 0.0
_no_ask:  float = 0.0
_ready:   bool  = False

_staged:     dict  = {}
_pnl_profit: float = 0.0
_pnl_loss:   float = 0.0
_pnl_wins:   int   = 0
_pnl_losses: int   = 0

_hold:        dict  = {}
_hold_profit: float = 0.0
_hold_loss:   float = 0.0
_hold_wins:   int   = 0
_hold_losses: int   = 0

_alerts: list = []
_window_history: list = []
_prev_histories: list = []
_clients: Set[WebSocket] = set()


# ── Window helpers ────────────────────────────────────────────────────────────

def _window_et() -> datetime:
    now_et = datetime.now(timezone.utc) + ET_OFFSET
    mins   = (now_et.minute // 15) * 15
    return now_et.replace(minute=mins, second=0, microsecond=0)

def _ticker(w: datetime) -> str:
    """KXBTC15M-26APR051830-30  (close time; suffix = close minutes)."""
    close = w + timedelta(minutes=15)
    return "KXBTC15M-" + close.strftime("%y%b%d%H%M").upper() + "-" + close.strftime("%M")

def _elapsed(w: datetime) -> int:
    now_et = datetime.now(timezone.utc) + ET_OFFSET
    return int((now_et - w).total_seconds())


# ── Helpers ───────────────────────────────────────────────────────────────────

def _log(msg: str, kind: str = "info") -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    _alerts.append({"ts": ts, "msg": msg, "kind": kind})
    if len(_alerts) > 60:
        _alerts.pop(0)


# ── Kalshi REST poll ──────────────────────────────────────────────────────────

async def _poll_kalshi(client: httpx.AsyncClient) -> None:
    global _yes_ask, _no_ask, _btc_open, _ready, _staged, _hold
    global _pnl_loss, _pnl_losses, _window_history, _prev_histories
    global _hold_profit, _hold_loss, _hold_wins, _hold_losses

    last_w = None

    while True:
        w      = _window_et()
        ticker = _ticker(w)

        if last_w and w != last_w:
            old_ticker = _ticker(last_w)
            # Settle staged losses
            for pos in _staged.get(old_ticker, []):
                if not pos.get("arb_fired") and not pos.get("expired"):
                    _pnl_loss   += pos["entry_price"]
                    _pnl_losses += 1
                    pos["expired"] = True
                    _log(f"EXPIRED — {pos['side']} @ {pos['entry_price']:.3f} | "
                         f"loss={pos['entry_price']:.3f} ({pos['entry_price']*100:.1f}¢)", kind="expire")
            # Settle hold positions
            actual_winner = "Yes" if _btc_price >= _btc_open else "No"
            for pos in _hold.get(old_ticker, []):
                if pos.get("settled"):
                    continue
                pos["settled"] = True
                if pos["side"] == actual_winner:
                    profit = round(1.0 - pos["entry_price"], 4)
                    _hold_profit += profit; _hold_wins += 1
                    _log(f"[HOLD] WON {pos['side']} @ {pos['entry_price']:.3f} | profit={profit:.3f} ({profit*100:.1f}¢)", kind="hold_win")
                else:
                    loss = pos["entry_price"]
                    _hold_loss += loss; _hold_losses += 1
                    _log(f"[HOLD] LOST {pos['side']} @ {pos['entry_price']:.3f} | loss={loss:.3f} ({loss*100:.1f}¢)", kind="hold_loss")
            # Archive history
            if _window_history:
                _prev_histories.append(list(_window_history))
                if len(_prev_histories) > 2:
                    _prev_histories.pop(0)
            _window_history = []
            # Purge old windows
            for k in [k for k in _staged if k != ticker]: del _staged[k]
            for k in [k for k in _hold   if k != ticker]: del _hold[k]
            # Reset stale state for new window
            _yes_ask = 0.0
            _no_ask  = 0.0
            _btc_open = 0.0
            _ready   = False

        last_w = w

        try:
            r = await client.get(f"{KALSHI_API}/markets/{ticker}", timeout=5)
            if r.status_code == 200:
                m  = r.json()["market"]
                ya = float(m.get("yes_ask_dollars") or 0)
                na = float(m.get("no_ask_dollars")  or 0)
                fs = m.get("floor_strike")
                if ya > 0: _yes_ask = ya
                if na > 0: _no_ask  = na
                if fs:     _btc_open = float(fs)
                _ready = ya > 0 and na > 0
            else:
                _log(f"Kalshi API {r.status_code} for {ticker}", kind="info")
        except Exception as e:
            _log(f"Kalshi poll error: {e}", kind="info")

        await asyncio.sleep(POLL_INTERVAL)


# ── Coinbase WS ───────────────────────────────────────────────────────────────

async def _coinbase_feed() -> None:
    global _btc_price, _btc_prev_price
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
                        for t in evt.get("tickers", []):
                            price = float(t.get("price", 0) or 0)
                            if price > 0:
                                _btc_prev_price = _btc_price if _btc_price > 0 else price
                                _btc_price = price
        except Exception:
            await asyncio.sleep(3)


# ── Signal + limit check ──────────────────────────────────────────────────────

def _check_signals(elapsed_sec: int, ticker: str) -> None:
    global _staged, _hold, _pnl_profit, _pnl_wins

    if not _ready or elapsed_sec <= 30 or elapsed_sec >= WINDOW_SEC:
        return
    if _yes_ask < 0.05 or _yes_ask > 0.95 or _no_ask < 0.05 or _no_ask > 0.95:
        return
    if _btc_open == 0 or _btc_price == 0:
        return

    delta   = (_btc_price - _btc_open) / _btc_open * 100
    emp     = get_odds(elapsed_sec, delta)
    if emp["confidence"] not in ("high", "med"):
        return

    yes_ask = _yes_ask
    no_ask  = _no_ask
    edge_up = emp["p_up"]   - yes_ask
    edge_dn = emp["p_down"] - no_ask

    arb_open_sides  = {p["side"] for p in _staged.get(ticker, []) if not p.get("arb_fired")}
    hold_open_sides = {p["side"] for p in _hold.get(ticker, [])}

    if edge_up > MISMATCH_THRESHOLD and emp["p_up"] > 0.50:
        if "Yes" not in arb_open_sides:
            _staged.setdefault(ticker, []).append({
                "side": "Yes", "other": "No",
                "entry_price": yes_ask, "entry_sec": elapsed_sec,
                "entry_model": emp["p_up"], "entry_neff": emp["n_eff"],
                "limit_target": round(STAGED_TARGET - yes_ask, 3),
            })
        if "Yes" not in hold_open_sides:
            _hold.setdefault(ticker, []).append({
                "side": "Yes", "entry_price": yes_ask,
                "entry_sec": elapsed_sec, "entry_model": emp["p_up"],
            })
        _log(f"BUY YES @ {yes_ask:.3f} | model={emp['p_up']:.1%} edge=+{edge_up:.2f} "
             f"n={emp['n_eff']} | arb limit NO ≤ {STAGED_TARGET - yes_ask:.3f}", kind="buy")

    elif edge_dn > MISMATCH_THRESHOLD and emp["p_down"] > 0.50:
        if "No" not in arb_open_sides:
            _staged.setdefault(ticker, []).append({
                "side": "No", "other": "Yes",
                "entry_price": no_ask, "entry_sec": elapsed_sec,
                "entry_model": emp["p_down"], "entry_neff": emp["n_eff"],
                "limit_target": round(STAGED_TARGET - no_ask, 3),
            })
        if "No" not in hold_open_sides:
            _hold.setdefault(ticker, []).append({
                "side": "No", "entry_price": no_ask,
                "entry_sec": elapsed_sec, "entry_model": emp["p_down"],
            })
        _log(f"BUY NO  @ {no_ask:.3f} | model={emp['p_down']:.1%} edge=+{edge_dn:.2f} "
             f"n={emp['n_eff']} | arb limit YES ≤ {STAGED_TARGET - no_ask:.3f}", kind="buy")

    if yes_ask + no_ask < ARB_TARGET and yes_ask > 0.05 and no_ask > 0.05:
        profit = 1.0 - (yes_ask + no_ask)
        _log(f"DIRECT ARB | yes={yes_ask:.3f} + no={no_ask:.3f} = {yes_ask+no_ask:.3f} | "
             f"profit={profit:.3f} ({profit*100:.1f}¢)", kind="arb")

    # Limit tracker
    for pos in _staged.get(ticker, []):
        if pos.get("arb_fired"):
            continue
        other = _no_ask if pos["other"] == "No" else _yes_ask
        if other < 0.05:
            continue
        total = pos["entry_price"] + other
        if other <= pos["limit_target"]:
            profit = 1.0 - total
            _pnl_profit += profit; _pnl_wins += 1
            pos["arb_fired"]     = True
            pos["locked_price"]  = round(other, 4)
            pos["locked_profit"] = round(profit, 4)
            _log(f"ARB LOCKED — buy {pos['other']} @ {other:.3f} | "
                 f"total={total:.3f} | profit={profit:.3f} ({profit*100:.1f}¢)", kind="lock")


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


async def _broadcast_loop() -> None:
    global _window_history
    while True:
        w           = _window_et()
        ticker      = _ticker(w)
        elapsed_sec = _elapsed(w)
        time_left   = max(0, WINDOW_SEC - elapsed_sec)

        _check_signals(elapsed_sec, ticker)

        if _ready and _btc_open > 0 and _btc_price > 0:
            delta = (_btc_price - _btc_open) / _btc_open * 100
            emp   = get_odds(elapsed_sec, delta)
            point = {"t": elapsed_sec, "model_up": emp["p_up"], "model_dn": emp["p_down"],
                     "yes_ask": _yes_ask, "no_ask": _no_ask}
            if _window_history and _window_history[-1]["t"] == elapsed_sec:
                _window_history[-1] = point
            else:
                _window_history.append(point)

            signal = ""
            edge_up = emp["p_up"]   - _yes_ask
            edge_dn = emp["p_down"] - _no_ask
            if edge_up > MISMATCH_THRESHOLD and emp["p_up"] > 0.50:   signal = "BUY_YES"
            elif edge_dn > MISMATCH_THRESHOLD and emp["p_down"] > 0.50: signal = "BUY_NO"
            elif _yes_ask + _no_ask < ARB_TARGET:                        signal = "DIRECT_ARB"

            odds = {
                "model_up":   emp["p_up"],   "model_dn":   emp["p_down"],
                "yes_ask":    _yes_ask,       "no_ask":     _no_ask,
                "yes_no_sum": round(_yes_ask + _no_ask, 4),
                "edge_up":    round(edge_up, 4), "edge_dn": round(edge_dn, 4),
                "n_eff":      emp["n_eff"],   "confidence": emp["confidence"][0].upper(),
                "signal":     signal,          "delta":     round(delta, 4),
            }
        else:
            odds = None

        staged_out = []
        for pos in _staged.get(ticker, []):
            other = _no_ask if pos["other"] == "No" else _yes_ask
            total = pos["entry_price"] + other
            staged_out.append({
                "side": pos["side"], "other": pos["other"],
                "entry_price": pos["entry_price"], "entry_sec": pos["entry_sec"],
                "entry_model": pos["entry_model"], "limit_target": pos["limit_target"],
                "other_now": round(other, 4), "total_now": round(total, 4),
                "gap": round(other - pos["limit_target"], 4),
                "locked": pos.get("arb_fired", False),
                "locked_price": pos.get("locked_price"),
                "locked_profit": pos.get("locked_profit"),
            })

        hold_out = []
        for pos in _hold.get(ticker, []):
            hold_out.append({
                "side": pos["side"], "entry_price": pos["entry_price"],
                "entry_sec": pos["entry_sec"], "entry_model": pos["entry_model"],
                "clob_now": _yes_ask if pos["side"] == "Yes" else _no_ask,
            })

        net      = _pnl_profit  - _pnl_loss
        hold_net = _hold_profit - _hold_loss
        payload  = {
            "ts": datetime.now().strftime("%H:%M:%S"),
            "ticker": ticker, "window_label": w.strftime("%I:%M %p ET"),
            "elapsed": elapsed_sec, "time_left": time_left, "minute": elapsed_sec // 60,
            "btc_price": _btc_price, "btc_open": _btc_open,
            "btc_up": _btc_price >= _btc_prev_price,
            "ready": _ready, "odds": odds,
            "staged": staged_out, "hold": hold_out,
            "pnl": {"profit": round(_pnl_profit,4), "loss": round(_pnl_loss,4),
                    "net": round(net,4), "wins": _pnl_wins, "losses": _pnl_losses},
            "hold_pnl": {"profit": round(_hold_profit,4), "loss": round(_hold_loss,4),
                         "net": round(hold_net,4), "wins": _hold_wins, "losses": _hold_losses},
            "alerts": list(reversed(_alerts))[:30],
            "history": _window_history, "prev_histories": _prev_histories,
        }
        await _broadcast(payload)
        await asyncio.sleep(1)


# ── FastAPI ───────────────────────────────────────────────────────────────────

app = FastAPI()

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Kalshi BTC 15M Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0d1117; color: #e6edf3; font-family: 'Courier New', monospace;
         font-size: 14px; padding: 16px; min-height: 100vh; }
  h2 { font-size: 13px; color: #8b949e; text-transform: uppercase;
       letter-spacing: 2px; margin-bottom: 8px; }
  .header { display: flex; align-items: center; gap: 24px; flex-wrap: wrap;
            background: #161b22; border: 1px solid #30363d;
            border-radius: 8px; padding: 12px 20px; margin-bottom: 16px; }
  .price { font-size: 28px; font-weight: bold; padding: 4px 10px; border-radius: 4px; }
  .price.up   { background: #1a4731; color: #3fb950; }
  .price.down { background: #4a1c1c; color: #f85149; }
  .price.flat { color: #e6edf3; }
  .hstat { display: flex; flex-direction: column; }
  .hstat span:first-child { font-size: 11px; color: #8b949e; }
  .hstat span:last-child  { font-size: 16px; }
  .ticker-badge { background: #1f2937; border: 1px solid #374151; border-radius: 4px;
                  padding: 3px 10px; font-size: 12px; color: #60a5fa; }
  .timer { margin-left: auto; text-align: right; }
  .timer .big { font-size: 20px; }
  .timer .sub { font-size: 12px; color: #8b949e; }

  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  @media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
  .card.full { grid-column: 1 / -1; }

  .odds-row { display: grid; grid-template-columns: 60px 1fr 1fr 1fr;
              gap: 8px; align-items: center; padding: 6px 0;
              border-bottom: 1px solid #21262d; }
  .odds-row:last-child { border-bottom: none; }
  .odds-label { font-size: 12px; color: #8b949e; }
  .val { text-align: right; font-size: 15px; }
  .val.edge-buy  { color: #3fb950; font-weight: bold; }
  .val.edge-over { color: #f85149; }
  .col-hdr { text-align: right; font-size: 11px; color: #8b949e; padding: 0 0 4px; }

  .signal { display: inline-block; padding: 4px 12px; border-radius: 4px;
            font-size: 13px; font-weight: bold; margin-top: 8px; }
  .signal.buy_yes    { background: #1a4731; color: #3fb950; }
  .signal.buy_no     { background: #4a1c1c; color: #f85149; }
  .signal.direct_arb { background: #3d2b00; color: #e3b341; }

  .staged-item { border: 1px solid #30363d; border-radius: 6px; padding: 10px 12px;
                 margin-bottom: 8px; }
  .staged-item.locked { border-color: #3fb950; }
  .staged-top { display: flex; justify-content: space-between; align-items: center; }
  .staged-side { font-weight: bold; font-size: 15px; }
  .staged-side.Yes { color: #3fb950; }
  .staged-side.No  { color: #f85149; }
  .locked-badge { background: #1a4731; color: #3fb950; padding: 2px 8px;
                  border-radius: 4px; font-size: 11px; font-weight: bold; }
  .progress-bar { height: 6px; background: #21262d; border-radius: 3px; margin-top: 8px; }
  .progress-fill { height: 100%; border-radius: 3px; background: #e3b341; transition: width 0.3s; }
  .progress-fill.locked { background: #3fb950; }
  .staged-detail { font-size: 12px; color: #8b949e; margin-top: 4px; }

  .pnl-row { display: flex; justify-content: space-between; padding: 4px 0;
             border-bottom: 1px solid #21262d; }
  .pnl-row:last-child { border-bottom: none; }
  .green { color: #3fb950; } .red { color: #f85149; }

  .alerts-list { max-height: 280px; overflow-y: auto; }
  .alert-item { padding: 5px 0; border-bottom: 1px solid #21262d; font-size: 12px; line-height: 1.4; }
  .alert-item:last-child { border-bottom: none; }
  .alert-ts { color: #8b949e; margin-right: 8px; }
  .alert-item.buy       { color: #3fb950; }
  .alert-item.lock      { color: #3fb950; font-weight: bold; }
  .alert-item.expire    { color: #f85149; }
  .alert-item.arb       { color: #e3b341; }
  .alert-item.hold_win  { color: #58a6ff; font-weight: bold; }
  .alert-item.hold_loss { color: #f85149; font-style: italic; }

  .chart-wrap { position: relative; height: 260px; }
  .statusbar { text-align: center; color: #8b949e; font-size: 11px; margin-top: 16px; }
</style>
</head>
<body>

<div class="header">
  <div>
    <div style="font-size:11px;color:#8b949e;margin-bottom:2px">BTC-USD · Kalshi 15M</div>
    <div class="price flat" id="price">—</div>
  </div>
  <div class="hstat"><span>Strike (Open)</span><span id="open">—</span></div>
  <div class="hstat"><span>Delta</span><span id="delta">—</span></div>
  <div class="hstat"><span>n_eff</span><span id="neff">—</span></div>
  <div class="ticker-badge" id="ticker">—</div>
  <div class="timer">
    <div class="big" id="timer">—</div>
    <div class="sub" id="timer-sub">—</div>
  </div>
</div>

<div class="grid">

  <div class="card full">
    <h2>Window Progression — Model vs Kalshi</h2>
    <div class="chart-wrap"><canvas id="chart"></canvas></div>
  </div>

  <div class="card">
    <h2>Live Odds</h2>
    <div id="odds-body"><div style="color:#8b949e">Waiting for data…</div></div>
    <div id="signal-wrap"></div>
  </div>

  <div class="card">
    <h2>Strategy A — Staged ARB</h2>
    <div style="font-size:11px;color:#8b949e;margin-bottom:8px">Buy edge, wait for other side to hit limit target (18¢ min)</div>
    <div id="staged-body"><div style="color:#8b949e">No staged positions.</div></div>
  </div>

  <div class="card">
    <h2>Strategy B — Hold to Expiry</h2>
    <div style="font-size:11px;color:#8b949e;margin-bottom:8px">Buy edge, hold to window close</div>
    <div id="hold-body"><div style="color:#8b949e">No hold positions.</div></div>
  </div>

  <div class="card">
    <h2>P&amp;L — Strategy A (ARB)</h2>
    <div id="pnl-body"></div>
  </div>

  <div class="card">
    <h2>P&amp;L — Strategy B (Hold)</h2>
    <div id="hold-pnl-body"></div>
  </div>

  <div class="card full">
    <h2>Alerts</h2>
    <div class="alerts-list" id="alerts-body"></div>
  </div>

</div>
<div class="statusbar" id="statusbar">Connecting…</div>

<script>
const ws = new WebSocket(`ws://${location.host}/ws`);

const ctx   = document.getElementById('chart').getContext('2d');
const chart = new Chart(ctx, {
  type: 'line',
  data: {
    datasets: [
      { label: 'Model YES', borderColor: '#3fb950', backgroundColor: 'transparent',
        borderWidth: 2, pointRadius: 0, tension: 0.2, data: [] },
      { label: 'Kalshi YES', borderColor: '#3fb950', backgroundColor: 'transparent',
        borderWidth: 1.5, borderDash: [5,4], pointRadius: 0, tension: 0.2, data: [] },
      { label: 'Model NO',  borderColor: '#f85149', backgroundColor: 'transparent',
        borderWidth: 2, pointRadius: 0, tension: 0.2, data: [] },
      { label: 'Kalshi NO', borderColor: '#f85149', backgroundColor: 'transparent',
        borderWidth: 1.5, borderDash: [5,4], pointRadius: 0, tension: 0.2, data: [] },
      { label: 'Prev YES', borderColor: 'rgba(63,185,80,0.2)', backgroundColor:'transparent',
        borderWidth: 1, pointRadius: 0, tension: 0.2, data: [] },
      { label: 'Prev NO',  borderColor: 'rgba(248,81,73,0.2)', backgroundColor:'transparent',
        borderWidth: 1, pointRadius: 0, tension: 0.2, data: [] },
    ]
  },
  options: {
    animation: false, responsive: true, maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { labels: { color: '#8b949e', font: { family: 'Courier New', size: 11 },
                          filter: i => i.datasetIndex < 4 } },
      tooltip: { callbacks: { label: c => ` ${c.dataset.label}: ${c.parsed.y.toFixed(3)}` } }
    },
    scales: {
      x: { type: 'linear', min: 0, max: 900, grid: { color: '#21262d' },
           ticks: { color: '#8b949e', callback: v => v + 's' },
           title: { display: true, text: 'Elapsed (sec)', color: '#8b949e' } },
      y: { min: 0, max: 1, grid: { color: '#21262d' },
           ticks: { color: '#8b949e', callback: v => (v*100).toFixed(0) + '%' } }
    }
  }
});

function fmt(v, d=3) { return v != null ? v.toFixed(d) : '—'; }

function edgeCls(e) {
  if (e >  0.07) return 'val edge-buy';
  if (e < -0.07) return 'val edge-over';
  return 'val';
}

ws.onmessage = (e) => {
  const d = JSON.parse(e.data);

  // Header
  const priceEl = document.getElementById('price');
  priceEl.textContent = d.btc_price > 0
    ? '$' + d.btc_price.toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2}) : '—';
  priceEl.className = 'price ' + (d.btc_price === 0 ? 'flat' : d.btc_up ? 'up' : 'down');
  document.getElementById('open').textContent =
    d.btc_open > 0 ? '$' + d.btc_open.toLocaleString('en-US', {minimumFractionDigits:2}) : '—';
  document.getElementById('ticker').textContent = d.ticker || '—';

  if (d.odds) {
    const delta = d.odds.delta;
    const dEl = document.getElementById('delta');
    dEl.textContent = (delta >= 0 ? '+' : '') + delta.toFixed(3) + '%';
    dEl.style.color = delta >= 0 ? '#3fb950' : '#f85149';
    document.getElementById('neff').textContent =
      d.odds.n_eff.toLocaleString() + ' [' + d.odds.confidence + ']';
  }

  const mm = Math.floor(d.time_left / 60), ss = d.time_left % 60;
  document.getElementById('timer').textContent = mm + ':' + String(ss).padStart(2,'0') + ' left';
  document.getElementById('timer-sub').textContent =
    '+' + d.elapsed + 's  (min ' + d.minute + '/14)  ' + d.ts;

  // Odds table
  const o = d.odds;
  const wrap = document.getElementById('odds-body');
  if (o) {
    wrap.innerHTML = `
      <div class="odds-row">
        <div class="odds-label"></div>
        <div class="col-hdr">Model</div><div class="col-hdr">Kalshi</div><div class="col-hdr">Edge</div>
      </div>
      <div class="odds-row">
        <div class="odds-label">▲ YES</div>
        <div class="val">${fmt(o.model_up)}</div>
        <div class="${edgeCls(o.edge_up)}">${fmt(o.yes_ask)}</div>
        <div class="${edgeCls(o.edge_up)}">${o.edge_up>=0?'+':''}${fmt(o.edge_up)}</div>
      </div>
      <div class="odds-row">
        <div class="odds-label">▼ NO</div>
        <div class="val">${fmt(o.model_dn)}</div>
        <div class="${edgeCls(o.edge_dn)}">${fmt(o.no_ask)}</div>
        <div class="${edgeCls(o.edge_dn)}">${o.edge_dn>=0?'+':''}${fmt(o.edge_dn)}</div>
      </div>
      <div class="odds-row">
        <div class="odds-label">Sum</div>
        <div class="val">—</div>
        <div class="val">${fmt(o.yes_no_sum)}</div>
        <div class="val">—</div>
      </div>`;
    const sw = document.getElementById('signal-wrap');
    const labels = { BUY_YES: '▲ BUY YES', BUY_NO: '▼ BUY NO', DIRECT_ARB: '◆ DIRECT ARB' };
    sw.innerHTML = o.signal
      ? `<span class="signal ${o.signal.toLowerCase()}">${labels[o.signal]}</span>` : '';
  }

  // Chart
  const toXY = (arr, key) => arr.map(p => ({ x: p.t, y: p[key] }));
  chart.data.datasets[0].data = toXY(d.history, 'model_up');
  chart.data.datasets[1].data = toXY(d.history, 'yes_ask');
  chart.data.datasets[2].data = toXY(d.history, 'model_dn');
  chart.data.datasets[3].data = toXY(d.history, 'no_ask');
  const prev = d.prev_histories && d.prev_histories.length > 0
    ? d.prev_histories[d.prev_histories.length - 1] : [];
  chart.data.datasets[4].data = toXY(prev, 'model_up');
  chart.data.datasets[5].data = toXY(prev, 'model_dn');
  chart.update('none');

  // Staged
  const sel = document.getElementById('staged-body');
  if (!d.staged || d.staged.length === 0) {
    sel.innerHTML = '<div style="color:#8b949e">No staged positions.</div>';
  } else {
    sel.innerHTML = d.staged.map(pos => {
      const pct = pos.locked ? 100 : Math.min(100, Math.max(0, (1 - pos.gap / pos.limit_target) * 100));
      const profit = pos.locked && pos.locked_profit != null ? pos.locked_profit.toFixed(3) : null;
      return `<div class="staged-item ${pos.locked ? 'locked' : ''}">
        <div class="staged-top">
          <span>
            <span class="staged-side ${pos.side}">${pos.side === 'Yes' ? '▲' : '▼'} ${pos.side}</span>
            <span style="color:#8b949e;font-size:12px"> @ ${fmt(pos.entry_price)}  t=${pos.entry_sec}s</span>
          </span>
          ${pos.locked
            ? `<span class="locked-badge">✓ LOCKED ${profit ? '+'+profit : ''}</span>`
            : `<span style="color:#8b949e;font-size:12px">gap ${pos.gap>=0?'+':''}${fmt(pos.gap)} to target</span>`}
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

  // Hold
  const hel = document.getElementById('hold-body');
  if (!d.hold || d.hold.length === 0) {
    hel.innerHTML = '<div style="color:#8b949e">No hold positions.</div>';
  } else {
    hel.innerHTML = d.hold.map(pos => {
      const clr  = pos.side === 'Yes' ? '#3fb950' : '#f85149';
      const arr  = pos.side === 'Yes' ? '▲' : '▼';
      const move = pos.clob_now - pos.entry_price;
      return `<div class="staged-item">
        <div class="staged-top">
          <span><span class="staged-side ${pos.side}">${arr} ${pos.side}</span>
          <span style="color:#8b949e;font-size:12px"> @ ${fmt(pos.entry_price)}  t=${pos.entry_sec}s</span></span>
          <span style="color:#8b949e;font-size:12px">model ${(pos.entry_model*100).toFixed(1)}%</span>
        </div>
        <div class="staged-detail">
          Kalshi now: <span style="color:${clr}">${fmt(pos.clob_now)}</span>
          &nbsp;|&nbsp; move: <span class="${move>=0?'green':'red'}">${move>=0?'+':''}${fmt(move)}</span>
          &nbsp;|&nbsp; holding to expiry
        </div>
      </div>`;
    }).join('');
  }

  // P&L
  function renderPnl(p, elId) {
    const netCls = p.net >= 0 ? 'green' : 'red';
    document.getElementById(elId).innerHTML = `
      <div class="pnl-row"><span>Profit</span><span class="green">+${fmt(p.profit)} (${(p.profit*100).toFixed(1)}¢)</span></div>
      <div class="pnl-row"><span>Loss</span><span class="red">-${fmt(p.loss)} (${(p.loss*100).toFixed(1)}¢)</span></div>
      <div class="pnl-row"><span>Net</span><span class="${netCls}">${p.net>=0?'+':''}${fmt(p.net)} (${(p.net*100).toFixed(1)}¢)</span></div>
      <div class="pnl-row"><span>Trades</span><span>${p.wins+p.losses} &nbsp;
        <span class="green">W:${p.wins}</span> / <span class="red">L:${p.losses}</span></span></div>`;
  }
  renderPnl(d.pnl, 'pnl-body');
  renderPnl(d.hold_pnl, 'hold-pnl-body');

  // Alerts
  const ael = document.getElementById('alerts-body');
  if (!d.alerts || d.alerts.length === 0) {
    ael.innerHTML = '<div style="color:#8b949e">No alerts yet.</div>';
  } else {
    ael.innerHTML = d.alerts.map(a =>
      `<div class="alert-item ${a.kind}"><span class="alert-ts">${a.ts}</span>${a.msg}</div>`
    ).join('');
  }

  document.getElementById('statusbar').textContent =
    `Kalshi KXBTC15M  |  Model threshold: 7%  |  Direct ARB: 0.90  |  Staged ARB: 0.82  |  Poll: 2s  |  Live`;
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
    client = httpx.AsyncClient()
    asyncio.create_task(_poll_kalshi(client))
    asyncio.create_task(_coinbase_feed())
    asyncio.create_task(_broadcast_loop())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=7335)
    args = parser.parse_args()
    print(f"Starting Kalshi BTC 15M Monitor at http://localhost:{args.port}")
    uvicorn.run("kalshi_web_monitor:app", host="0.0.0.0", port=args.port, log_level="warning")
