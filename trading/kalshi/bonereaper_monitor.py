#!/usr/bin/env python3
"""lively_monitor.py — fancy live dashboard for the Lively follower.

Runs forever. Background async tasks pull AWS data and Kalshi winners; the
foreground task redraws a Rich layout 2× per second so only the changing
numbers visibly update. Same data as `lively_compare`, in place.

  • scp data    every 5s (AWS → ~/.kalshi_lively_trades.jsonl etc.)
  • winners     every 30s (Kalshi REST, only for unresolved windows)
  • render      every 0.5s (in-place via rich.Live)

Keys:  Ctrl-C to quit.
"""
from __future__ import annotations
import asyncio
import base64
import json
import os
import subprocess
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")

ET    = ZoneInfo("America/New_York")
COINS = ["BTC", "ETH", "SOL", "XRP"]
SIZE_RATIO = 80   # Bonereaper shadow trader uses 80:1

LIVELY_SIGNALS = Path.home() / ".bonereaper_fast_signals.jsonl"
LIVE_TRADES    = Path.home() / ".kalshi_bonereaper_trades.jsonl"
SHADOW_TRADES  = Path.home() / ".bonereaper_shadow_trades_legacy.jsonl"   # unused
STATUS_FILE    = Path.home() / ".kalshi_bonereaper_status.json"

AWS              = "ec2-user@23.20.34.12"
SCP_INTERVAL     = 5
WINNER_INTERVAL  = 30
RENDER_INTERVAL  = 0.5

KALSHI_BASE  = "https://api.elections.kalshi.com/trade-api/v2"
KS_WS        = "wss://api.elections.kalshi.com/trade-api/ws/v2"
_api_key     = os.environ.get("KALSHI_API_KEY")
_private_pem = os.environ.get("KALSHI_API_SECRET")
_private_key = (serialization.load_pem_private_key(_private_pem.encode(), password=None)
                if _private_pem else None)

KS_SERIES = {"BTC": "KXBTC15M", "ETH": "KXETH15M",
              "SOL": "KXSOL15M", "XRP": "KXXRP15M"}


# ── Auth + winner fetch ──────────────────────────────────────────────
def _sign(method: str, path: str) -> dict:
    ts = str(round(time.time() * 1000))
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
        "Content-Type":            "application/json",
    }


def kalshi_ticker(coin: str, ws_ts: int) -> str:
    close_dt = datetime.fromtimestamp(ws_ts + 900, tz=timezone.utc).astimezone(ET)
    return (f"{KS_SERIES[coin]}-{close_dt.strftime('%y%b%d%H%M').upper()}-"
            f"{close_dt.strftime('%M')}")


async def fetch_winner(client: httpx.AsyncClient, coin: str, ws_ts: int) -> str | None:
    if not _private_key:
        return None
    ticker = kalshi_ticker(coin, ws_ts)
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        r = await client.get(f"{KALSHI_BASE}/markets/{ticker}",
                              headers=_sign("GET", path), timeout=10)
        if r.status_code != 200:
            return None
        m = r.json().get("market", {})
        result = (m.get("result") or "").lower()
        if result in ("yes", "no") and m.get("status") in ("finalized", "settled"):
            return result
    except Exception:
        return None
    return None


# ── P&L helpers ──────────────────────────────────────────────────────
def lively_signed(side: str, outcome: str, size: float) -> float:
    if side == "BUY"  and outcome == "Up":   return  size
    if side == "BUY"  and outcome == "Down": return -size
    if side == "SELL" and outcome == "Up":   return -size
    if side == "SELL" and outcome == "Down": return  size
    return 0.0


def lively_pnl(side: str, outcome: str, size: float, price: float,
                fee: float, winner: str | None) -> float | None:
    if winner is None:
        return None
    if (side == "BUY"  and outcome == "Up")   or (side == "SELL" and outcome == "Down"):
        bet = "Up"
    elif (side == "BUY" and outcome == "Down") or (side == "SELL" and outcome == "Up"):
        bet = "Down"
    else:
        return None
    won = (bet == "Up" and winner == "yes") or (bet == "Down" and winner == "no")
    payout = size if won else 0.0
    if side == "BUY": return payout - price * size - fee
    else:             return price * size - payout - fee


# ── Shared state (mutated by background tasks, read by render) ───────
state = {
    "signals":        [],
    "trades":         [],
    "winners":        {},
    "last_sync_at":   0.0,
    "last_sync_dur":  0.0,
    "syncing":        False,
    "sync_status":    "—",
    "started_at":     time.time(),
    # Live KS book (populated by kalshi_ws_loop). For each coin:
    #   {"yes_ask":, "no_ask":, "yes_bid":, "no_bid":, "ts":, "ts_clean":, "sum":}
    # ts:        last delta received (any kind)
    # ts_clean:  last delta where Σ was in [0.95, 1.10]  (real freshness)
    "ks":             {},
    "ks_books":       {},  # ticker → {"yes": {price: size}, "no": {...}}
    "ks_status":      "connecting",
    "ks_resub":       set(),   # tickers that need a force-resubscribe
}


def load_files() -> tuple[list, list]:
    sigs = []
    if LIVELY_SIGNALS.exists():
        with open(LIVELY_SIGNALS) as f:
            for line in f:
                if not line.strip(): continue
                try: sigs.append(json.loads(line))
                except: continue
    trades = []
    src = LIVE_TRADES if LIVE_TRADES.exists() else SHADOW_TRADES
    if src.exists():
        with open(src) as f:
            for line in f:
                if not line.strip(): continue
                try: t = json.loads(line)
                except: continue
                if "fill_price" not in t:
                    t["fill_price"] = (t.get("avg_fill_price")
                                        or t.get("ask_at_trigger") or 0)
                if (t.get("shares_filled") or 0) > 0:
                    trades.append(t)
    return sigs, trades


# ── Background loops ─────────────────────────────────────────────────
async def _scp_one(remote: str, local: Path) -> bool:
    def _do():
        try:
            r = subprocess.run(
                ["scp", "-q", "-o", "ConnectTimeout=8",
                  f"{AWS}:{remote}", str(local)],
                timeout=20, capture_output=True)
            return r.returncode == 0
        except Exception:
            return False
    return await asyncio.to_thread(_do)


async def fetch_loop() -> None:
    # Initial load from local cache (fast paint before first scp returns)
    state["signals"], state["trades"] = load_files()
    while True:
        state["syncing"] = True
        t0 = time.time()
        ok1 = await _scp_one("~/.kalshi_bonereaper_trades.jsonl", LIVE_TRADES)
        ok2 = await _scp_one("~/.bonereaper_fast_signals.jsonl",  LIVELY_SIGNALS)
        await _scp_one("~/.kalshi_bonereaper_status.json",        STATUS_FILE)
        dur = time.time() - t0
        state["syncing"] = False
        state["last_sync_at"]  = time.time()
        state["last_sync_dur"] = dur
        state["sync_status"]   = "ok" if (ok1 and ok2) else "FAIL"
        if ok1 or ok2:
            state["signals"], state["trades"] = load_files()
        await asyncio.sleep(SCP_INTERVAL)


def _ws_auth_headers() -> dict:
    return {k: v for k, v in _sign("GET", "/trade-api/ws/v2").items()
            if k != "Content-Type"}


def _current_window_start_utc() -> datetime:
    now = datetime.now(timezone.utc)
    mins = (now.minute // 15) * 15
    return now.replace(minute=mins, second=0, microsecond=0)


async def kalshi_ws_loop() -> None:
    """Live KS orderbook → state['ks'][coin] = {yes_ask, no_ask, ts}."""
    if not _private_key:
        state["ks_status"] = "no-auth"
        return
    msg_id = 0
    def next_id():
        nonlocal msg_id; msg_id += 1; return msg_id

    while True:
        subscribed: set[str] = set()
        ticker_to_coin: dict[str, str] = {}
        try:
            async with websockets.connect(
                KS_WS, additional_headers=_ws_auth_headers(),
                ping_interval=20, open_timeout=10,
            ) as ws:
                state["ks_status"] = "connected"

                async def resub():
                    ws_dt = _current_window_start_utc()
                    cur = {kalshi_ticker(c, int(ws_dt.timestamp())): c for c in COINS}
                    new = set(cur.keys()) - subscribed
                    if new:
                        await ws.send(json.dumps({
                            "id": next_id(), "cmd": "subscribe",
                            "params": {"channels": ["orderbook_delta"],
                                        "market_tickers": list(new)},
                        }))
                        subscribed.update(new)
                        ticker_to_coin.update(cur)
                        for tk in new:
                            state["ks_books"][tk] = {"yes": {}, "no": {}}

                async def force_resub_anomalies():
                    """Unsubscribe + resubscribe any ticker flagged anomalous,
                    so Kalshi sends a fresh snapshot and we clear phantoms."""
                    flagged = state["ks_resub"] & subscribed
                    if not flagged: return
                    state["ks_resub"].clear()
                    tks = list(flagged)
                    try:
                        await ws.send(json.dumps({
                            "id": next_id(), "cmd": "unsubscribe",
                            "params": {"channels": ["orderbook_delta"],
                                        "market_tickers": tks},
                        }))
                        subscribed.difference_update(tks)
                        for tk in tks:
                            state["ks_books"][tk] = {"yes": {}, "no": {}}
                        # next resub() call will resubscribe + fetch fresh snapshot
                    except Exception:
                        pass

                async def resub_loop():
                    while True:
                        try:
                            await force_resub_anomalies()
                            await resub()
                        except Exception: return
                        await asyncio.sleep(2)

                await resub()
                resub_task = asyncio.create_task(resub_loop())

                def publish(ticker, coin):
                    book = state["ks_books"].get(ticker)
                    if not book: return
                    yb_list = [p for p, sz in book["yes"].items() if sz > 0]
                    nb_list = [p for p, sz in book["no"].items()  if sz > 0]
                    if not yb_list or not nb_list: return
                    yb = round(max(yb_list), 4)
                    nb = round(max(nb_list), 4)
                    ya = round(1.0 - nb, 4)
                    na = round(1.0 - yb, 4)
                    if not (0.001 <= ya <= 0.999): return
                    if not (0.001 <= na <= 0.999): return
                    bid_sum = yb + nb
                    # Healthy market: bid_sum < 1 (spread). Crossed/corrupted
                    # when bid_sum > 1.05 — the same condition that produces
                    # ask_sum < 0.95 we saw in the trader. Track it explicitly.
                    book_clean = (bid_sum <= 1.05)
                    now_ts = time.time()
                    prev = state["ks"].get(coin, {})
                    state["ks"][coin] = {
                        "yes_ask": ya, "no_ask": na,
                        "yes_bid": yb, "no_bid": nb,
                        "bid_sum": bid_sum,
                        "ask_sum": ya + na,
                        "ticker": ticker, "ts": now_ts,
                        "ts_clean": now_ts if book_clean else prev.get("ts_clean", 0),
                        "clean":   book_clean,
                    }
                    # If book has been anomalous for >5s, mark for resub —
                    # forces Kalshi to send a fresh snapshot, clearing phantoms
                    if not book_clean:
                        last_ok = prev.get("ts_clean", 0)
                        if last_ok and (now_ts - last_ok) > 5:
                            state["ks_resub"].add(ticker)

                try:
                    async for raw in ws:
                        try: msg = json.loads(raw)
                        except: continue
                        mtype = msg.get("type")
                        d = msg.get("msg", msg)
                        ticker = d.get("market_ticker", "")
                        coin = ticker_to_coin.get(ticker)
                        if not coin: continue
                        if mtype == "orderbook_snapshot":
                            book = {"yes": {}, "no": {}}
                            for entry in (d.get("yes_dollars_fp") or []):
                                try:
                                    p, sz = round(float(entry[0]), 4), float(entry[1])
                                    if sz > 0: book["yes"][p] = sz
                                except: continue
                            for entry in (d.get("no_dollars_fp") or []):
                                try:
                                    p, sz = round(float(entry[0]), 4), float(entry[1])
                                    if sz > 0: book["no"][p] = sz
                                except: continue
                            state["ks_books"][ticker] = book
                            publish(ticker, coin)
                        elif mtype == "orderbook_delta":
                            side = d.get("side")
                            if side not in ("yes", "no"): continue
                            try:
                                price = round(float(d.get("price_dollars", 0)), 4)
                                delta = float(d.get("delta_fp", 0))
                            except: continue
                            book = state["ks_books"].setdefault(ticker, {"yes":{},"no":{}})
                            new_sz = book[side].get(price, 0) + delta
                            if new_sz <= 0: book[side].pop(price, None)
                            else:           book[side][price] = new_sz
                            publish(ticker, coin)
                finally:
                    if not resub_task.done(): resub_task.cancel()
        except Exception as e:
            state["ks_status"] = f"reconnect: {type(e).__name__}"
            await asyncio.sleep(3)


async def winner_loop() -> None:
    async with httpx.AsyncClient() as client:
        while True:
            now = int(time.time())
            cur_ws  = (now // 900) * 900
            prev_ws = cur_ws - 900
            for ws in (prev_ws, cur_ws):
                for c in COINS:
                    key = (c, ws)
                    if state["winners"].get(key) is None:
                        w = await fetch_winner(client, c, ws)
                        if w:
                            state["winners"][key] = w
            await asyncio.sleep(WINNER_INTERVAL)


# ── Rendering ────────────────────────────────────────────────────────
def fmt_pair(sh: float, avg: float) -> str:
    if sh:
        # Format share count: integer for ≥10, 1 decimal for ≥1, 2 for <1.
        if sh >= 10:
            sh_str = f"{sh:>5.0f}"
        elif sh >= 1:
            sh_str = f"{sh:>5.1f}"
        else:
            sh_str = f"{sh:>5.2f}"
        return f"{sh_str}×${avg:.3f}"
    return "[dim]      ─     [/]"


def color_pnl(v: float, width: int = 7) -> str:
    color = "bright_green" if v >= 0 else "red"
    return f"[{color}]${v:>+{width}.2f}[/]"


def color_diff(diff: float | None) -> str:
    if diff is None:
        return "[dim]   ─  [/]"
    if diff <= 0:
        return f"[bright_green]{diff:+.3f}[/]"
    if diff < 0.02:
        return f"[yellow]{diff:+.3f}[/]"
    return f"[red]{diff:+.3f}[/]"


def color_pos(v: float, width: int = 8) -> str:
    if v == 0:
        return f"[dim]{v:>+{width}.1f}[/]"
    color = "bright_green" if v > 0 else "red"
    return f"[{color}]{v:>+{width}.1f}[/]"


def build_window_panel(ws: int, now: int) -> Panel:
    sigs   = state["signals"]
    trades = state["trades"]
    cur_ws = (now // 900) * 900
    is_open = (ws == cur_ws)
    elapsed = now - ws
    et  = datetime.fromtimestamp(ws, tz=timezone.utc).astimezone(ET).strftime("%H:%M ET")
    utc = datetime.fromtimestamp(ws, tz=timezone.utc).strftime("%H:%M UTC")
    status = (f"[yellow]OPEN  T+{elapsed//60:>2}m{elapsed%60:02d}s[/]"
              if is_open else "[dim]CLOSED[/]")
    title = f"[bold]{utc}[/]  [dim]({et})[/]  ·  {status}"

    tbl = Table(box=None, expand=False, show_edge=False, padding=(0, 1),
                 header_style="bold cyan")
    tbl.add_column("coin",     justify="left",  width=4)
    tbl.add_column("side",     justify="left",  width=4)
    tbl.add_column("his BUY",  justify="right", width=12)
    tbl.add_column("ours+fee", justify="right", width=12)
    tbl.add_column("Δ avg",    justify="right", width=8)
    tbl.add_column("his_pos",  justify="right", width=10)
    tbl.add_column("our_pos",  justify="right", width=8)
    tbl.add_column("his P&L",  justify="right", width=10)
    tbl.add_column("our P&L",  justify="right", width=10)
    tbl.add_column("win",      justify="center",width=4)

    win_lively_total = 0.0
    win_ours_total   = 0.0
    win_resolved     = False
    any_row          = False

    # Read trader's canonical his_pos for the OPEN window — keeps display
    # consistent with the values driving the trader's fire decisions.
    trader_lively_now = {}
    trader_status_ws  = None
    if STATUS_FILE.exists():
        try:
            with open(STATUS_FILE) as f:
                _s = json.load(f)
                trader_lively_now = _s.get("lively_now", {}) or {}
                trader_status_ws  = _s.get("ws_ts")
        except Exception:
            pass

    for c in COINS:
        his = [t for t in sigs
               if t.get("coin", "").upper() == c
               and t.get("market_type") == "15m"
               and t.get("window_start_ts") == ws
               and 0.0 < float(t.get("price") or 0) < 1.0]
        # For the OPEN window, use the trader's own lively_now (canonical).
        # For closed windows the trader's value isn't kept, so compute from
        # the signal file (BUY-only diff matches the trader's methodology).
        if ws == trader_status_ws and c in trader_lively_now:
            his_pos = float(trader_lively_now[c])
        else:
            his_pos = (sum(float(t["size"]) for t in his
                           if t["side"] == "BUY" and t["outcome"] == "Up")
                       - sum(float(t["size"]) for t in his
                             if t["side"] == "BUY" and t["outcome"] == "Down"))

        ours = [s for s in trades
                if s.get("coin") == c and s.get("window_start_ts") == ws]
        our_buys  = [s for s in ours if s.get("direction") != "sell"]
        our_sells = [s for s in ours if s.get("direction") == "sell"]
        our_pos = (
            sum((s["shares_filled"] if s["side"] == "yes" else -s["shares_filled"])
                for s in our_buys)
            - sum((s["shares_filled"] if s["side"] == "yes" else -s["shares_filled"])
                  for s in our_sells)
        )

        yes_buys  = [s for s in our_buys  if s["side"] == "yes"]
        no_buys   = [s for s in our_buys  if s["side"] == "no"]
        yes_sells = [s for s in our_sells if s["side"] == "yes"]
        no_sells  = [s for s in our_sells if s["side"] == "no"]
        def _buy_cost(s):
            fc = s.get("fill_cost_dollars")
            if fc is None:
                fc = s["shares_filled"] * s["fill_price"]
            return fc + (s.get("fees_dollars") or 0)
        def _sell_proceeds(s):
            p = s.get("fill_proceeds_dollars")
            if p is None:
                p = s["shares_filled"] * s.get("avg_fill_price",
                    s.get("limit_price", 0))
            return p - (s.get("fees_dollars") or 0)
        def _stats(rows):
            sh   = sum(s["shares_filled"] for s in rows)
            cost = sum(_buy_cost(s) for s in rows)
            return sh, (cost / sh if sh else 0)
        yes_sh, yes_avg = _stats(yes_buys)
        no_sh,  no_avg  = _stats(no_buys)
        yes_sold_sh   = sum(s["shares_filled"] for s in yes_sells)
        no_sold_sh    = sum(s["shares_filled"] for s in no_sells)
        yes_proceeds  = sum(_sell_proceeds(s) for s in yes_sells)
        no_proceeds   = sum(_sell_proceeds(s) for s in no_sells)

        his_buy_up   = [t for t in his if t["side"] == "BUY" and t["outcome"] == "Up"]
        his_buy_down = [t for t in his if t["side"] == "BUY" and t["outcome"] == "Down"]
        def _hstats(rows):
            sh   = sum(float(t["size"]) for t in rows)
            cost = sum(float(t["size"]) * float(t["price"]) for t in rows)
            return sh, (cost / sh if sh else 0)
        hbu_sh, hbu_avg = _hstats(his_buy_up)
        hbd_sh, hbd_avg = _hstats(his_buy_down)

        winner = state["winners"].get((c, ws))
        if winner is not None:
            win_resolved = True
            his_pnl = sum(lively_pnl(t["side"], t["outcome"], t["size"],
                                       float(t.get("price", 0)),
                                       float(t.get("fee", 0)),
                                       winner) or 0 for t in his)
            # P&L = held × $1 (if their side won) + realized sell proceeds − total buy cost
            held_yes = yes_sh - yes_sold_sh
            held_no  = no_sh  - no_sold_sh
            yes_won  = (winner == "yes")
            no_won   = (winner == "no")
            payout   = (held_yes * (1.0 if yes_won else 0.0)
                        + held_no  * (1.0 if no_won  else 0.0))
            realized = yes_proceeds + no_proceeds
            total_buy_cost = sum(_buy_cost(s) for s in our_buys)
            our_pnl  = payout + realized - total_buy_cost
            win_lively_total += his_pnl
            win_ours_total   += our_pnl
            his_pnl_str = color_pnl(his_pnl)
            our_pnl_str = color_pnl(our_pnl)
            wcolor = "bright_green" if winner == "yes" else "red"
            winner_str = f"[bold {wcolor}]{winner.upper()}[/]"
        else:
            his_pnl_str = "[dim] (open)[/]"
            our_pnl_str = "[dim] (open)[/]"
            winner_str  = "[dim]—[/]"

        if not (his or ours):
            continue
        any_row = True

        up_diff = (yes_avg - hbu_avg) if (yes_sh and hbu_sh) else None
        dn_diff = (no_avg  - hbd_avg) if (no_sh  and hbd_sh) else None

        tbl.add_row(
            f"[bold cyan]{c}[/]", "[bright_green]Up[/]",
            fmt_pair(hbu_sh, hbu_avg),
            fmt_pair(yes_sh, yes_avg),
            color_diff(up_diff),
            color_pos(his_pos),
            color_pos(our_pos, 6),
            his_pnl_str, our_pnl_str, winner_str,
        )
        tbl.add_row(
            "", "[red]Down[/]",
            fmt_pair(hbd_sh, hbd_avg),
            fmt_pair(no_sh, no_avg),
            color_diff(dn_diff),
            "", "", "", "", "",
        )

    if not any_row:
        tbl.add_row("[dim](no Lively activity yet)[/]",
                     "", "", "", "", "", "", "", "", "")

    if win_resolved:
        ours_at_scale = win_ours_total * SIZE_RATIO    # what we'd earn at his volume
        edge = ours_at_scale - win_lively_total
        subtitle = (f"Lively {color_pnl(win_lively_total, 6)}   "
                    f"Ours {color_pnl(win_ours_total, 6)} "
                    f"(×{SIZE_RATIO} = {color_pnl(ours_at_scale, 7)})   "
                    f"Edge {color_pnl(edge, 7)}")
    else:
        subtitle = None

    return Panel(tbl, title=title, title_align="left",
                  border_style=("bright_yellow" if is_open else "dim cyan"),
                  subtitle=subtitle, subtitle_align="right",
                  padding=(0, 1))


def build_header(now: int) -> Panel:
    cur_ws = (now // 900) * 900
    cur_dt = datetime.fromtimestamp(cur_ws, tz=timezone.utc)
    elapsed = now - cur_ws
    secs_left = 900 - elapsed
    sync_age = int(now - state["last_sync_at"]) if state["last_sync_at"] else None
    if state["syncing"]:
        sync_str = "[yellow]⟳ syncing…[/]"
    elif sync_age is None:
        sync_str = "[dim]waiting for first sync…[/]"
    elif state["sync_status"] != "ok":
        sync_str = f"[red]✗ sync FAILED {sync_age}s ago[/]"
    else:
        col = "bright_green" if sync_age < 10 else "yellow"
        sync_str = (f"[{col}]✓ synced {sync_age}s ago "
                    f"({state['last_sync_dur']:.1f}s)[/]")

    win_str  = (f"{cur_dt.strftime('%H:%M')}–"
                 f"{(cur_dt + timedelta(minutes=15)).strftime('%H:%M')} UTC")
    secs_col = "bright_green" if secs_left > 60 else "yellow" if secs_left > 15 else "red"
    line1 = (f"[bold red]✦ Bonereaper LIVE (real $)[/]   "
             f"[bold]{datetime.fromtimestamp(now, tz=timezone.utc).strftime('%H:%M:%S')} UTC[/]   "
             f"·   window [bold]{win_str}[/]   "
             f"T+[bold]{elapsed//60}m{elapsed%60:02d}s[/] "
             f"([{secs_col}]{secs_left//60}m{secs_left%60:02d}s left[/])   "
             f"·   {sync_str}")

    # Live KS prices — bids (max yes_bid / max no_bid). Bid sum should
    # be < 1; > 1.05 = crossed/phantom book. Age uses ts_clean (last
    # update where the book was sane), so phantom books visibly age.
    ks_parts = []
    for c in COINS:
        v = state["ks"].get(c)
        if not v:
            ks_parts.append(f"[dim]{c} —[/]")
            continue
        yb = v["yes_bid"]; nb = v["no_bid"]
        bid_sum = v.get("bid_sum", yb + nb)
        clean = v.get("clean", True)
        if not clean:
            age_label = "[red]STALE[/]"
        else:
            age = int(now - v.get("ts_clean", v["ts"]))
            age_col = "bright_green" if age < 5 else "yellow" if age < 30 else "red"
            age_label = f"[{age_col}]{age}s[/]"
        flag = "[red]✗[/]" if not clean else " "
        ks_parts.append(
            f"[bold cyan]{c}[/] [bright_green]Y${yb:.2f}[/]/[red]N${nb:.2f}[/]"
            f" Σ{bid_sum:.2f}{flag} {age_label}"
        )
    ks_line = "  ".join(ks_parts)
    ks_status = state.get("ks_status", "?")
    if ks_status != "connected":
        ks_line = f"[yellow]KS WS: {ks_status}[/]   " + ks_line
    return Panel(Text.from_markup(line1 + "\n" + ks_line),
                  border_style="magenta", padding=(0, 1))


def build_footer(now: int) -> Panel:
    n_sigs   = len(state["signals"])
    n_trades = len(state["trades"])
    uptime   = int(now - state["started_at"])
    return Panel(
        Text.from_markup(
            f"[dim]signals loaded: {n_sigs:>5}   "
            f"our trades: {n_trades:>4}   "
            f"uptime: {uptime//60}m{uptime%60:02d}s   "
            f"·   scp every {SCP_INTERVAL}s · render {1/RENDER_INTERVAL:.0f} Hz "
            f"·   Ctrl-C to quit[/]"
        ),
        border_style="dim", padding=(0, 1),
    )


def build_layout(now: int):
    cur_ws  = (now // 900) * 900
    prev_ws = cur_ws - 900
    return Group(
        build_header(now),
        build_window_panel(prev_ws, now),
        build_window_panel(cur_ws, now),
        build_footer(now),
    )


async def render_loop(live: Live) -> None:
    while True:
        try:
            now = int(time.time())
            live.update(build_layout(now))
        except Exception:
            pass
        await asyncio.sleep(RENDER_INTERVAL)


async def main() -> None:
    console = Console()
    with Live(Text("starting…"), console=console, screen=True,
              refresh_per_second=4, transient=False) as live:
        await asyncio.gather(
            fetch_loop(),
            winner_loop(),
            kalshi_ws_loop(),
            render_loop(live),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
