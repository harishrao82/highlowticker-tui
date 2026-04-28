#!/usr/bin/env python3
"""
btc_recorder.py — Kalshi 15-min window yes/no tick recorder.

Records yes_ask / no_ask every second for BTC, ETH, SOL, XRP windows.
Floor strike fetched from Kalshi REST once per window.
Winner inferred from final odds at close.

Run:   python btc_recorder.py
Stop:  Ctrl-C
"""
import asyncio
import base64
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import ccxt
import httpx
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv
from rich.console import Console

import btc_db

load_dotenv()
console = Console()

# ── Config ────────────────────────────────────────────────────────────────────
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS   = "wss://api.elections.kalshi.com/trade-api/ws/v2"
COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
ET_OFFSET   = timedelta(hours=-4)

COINS: dict[str, str] = {
    "BTC": "KXBTC15M",
    "ETH": "KXETH15M",
    "SOL": "KXSOL15M",
    "XRP": "KXXRP15M",
}

# Coinbase product IDs for live spot price capture (used for coin_open_price)
COINBASE_PRODUCTS: dict[str, str] = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "SOL": "SOL-USD",
    "XRP": "XRP-USD",
}

# Polymarket 15-min Up/Down markets — parallel recording via CLOB WebSocket.
POLY_SLUG_PREFIX: dict[str, str] = {
    "BTC": "btc-updown-15m",
    "ETH": "eth-updown-15m",
    "SOL": "sol-updown-15m",
    "XRP": "xrp-updown-15m",
}
GAMMA_API = "https://gamma-api.polymarket.com/markets"
CLOB_WS   = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ── Kalshi auth ───────────────────────────────────────────────────────────────
_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)

def _kalshi_headers(method: str, path: str) -> dict:
    ts  = str(round(time.time() * 1000))
    msg = ts + method.upper() + path
    sig = _private_key.sign(msg.encode(),
        asym_padding.PSS(mgf=asym_padding.MGF1(hashes.SHA256()),
                         salt_length=asym_padding.PSS.MAX_LENGTH),
        hashes.SHA256())
    return {"KALSHI-ACCESS-KEY": _api_key, "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "Content-Type": "application/json"}

def _ws_auth_headers() -> dict:
    ts  = str(round(time.time() * 1000))
    msg = ts + "GET" + "/trade-api/ws/v2"
    sig = _private_key.sign(msg.encode(),
        asym_padding.PSS(mgf=asym_padding.MGF1(hashes.SHA256()),
                         salt_length=asym_padding.PSS.MAX_LENGTH),
        hashes.SHA256())
    return {"KALSHI-ACCESS-KEY": _api_key, "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode()}

# ── Model surface ────────────────────────────────────────────────────────────
SURFACE_FILE = Path.home() / ".btc_model_surface.json"
_surface: dict = {}

def _load_surface() -> None:
    global _surface
    if SURFACE_FILE.exists():
        _surface = json.loads(SURFACE_FILE.read_text())
        m = _surface["meta"]
        console.print(f"[dim]Model: {m['n_windows']:,} windows  {m['n_obs']:,} obs[/dim]")
    else:
        console.print("[yellow]No model surface found — model odds disabled[/yellow]")

def _get_odds(time_seconds: float, delta_pct: float) -> tuple[float, float, int]:
    """Returns (p_up, p_down, n_eff). Falls back to (0.5, 0.5, 0) if no surface."""
    if not _surface:
        return 0.5, 0.5, 0
    t_step = _surface["meta"]["t_step"]
    d_step = _surface["meta"]["d_step"]
    d_min  = _surface["meta"]["d_min"]
    ti = max(0, min(int(round(time_seconds / t_step)), len(_surface["t_vals"]) - 1))
    di = max(0, min(int(round((delta_pct - d_min) / d_step)), len(_surface["d_vals"]) - 1))
    cell = _surface["surface"][ti][di]
    return cell["p_up"], round(1 - cell["p_up"], 4), cell["n_eff"]

# ── Live coin prices ─────────────────────────────────────────────────────────
# `live_prices[coin]` is the latest spot from Coinbase WebSocket (sub-second).
# We snap it at each window boundary into the DB as `coin_open_price`. This is
# the price-from-our-source, distinct from Kalshi's `floor_strike`.
live_prices: dict[str, float] = {}
_btc_price: float = 0.0       # legacy alias kept for existing model-odds code
_btc_open:  float = 0.0       # legacy alias kept for existing model-odds code
_btc_open_slot: int = -1

async def _coinbase_ws_feed() -> None:
    """Subscribe to Coinbase WS ticker channel for all 4 coins and update
    `live_prices` on every event. Also maintains legacy `_btc_price` alias."""
    global _btc_price
    import websockets
    products        = list(COINBASE_PRODUCTS.values())
    product_to_coin = {v: k for k, v in COINBASE_PRODUCTS.items()}
    while True:
        try:
            async with websockets.connect(
                COINBASE_WS, ping_interval=20, open_timeout=10
            ) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": products,
                    "channel": "ticker",
                }))
                console.print(f"[dim]Coinbase WS connected — {', '.join(products)}[/dim]")
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("channel") != "ticker":
                        continue
                    for ev in msg.get("events", []):
                        for t in ev.get("tickers", []):
                            p   = t.get("product_id", "")
                            px  = float(t.get("price", 0) or 0)
                            sym = product_to_coin.get(p)
                            if sym and px > 0:
                                live_prices[sym] = px
                                if sym == "BTC":
                                    _btc_price = px
        except Exception as e:
            console.print(f"[yellow]Coinbase WS: {e} — reconnect 3s[/yellow]")
            await asyncio.sleep(3)


async def _btc_open_slot_tracker() -> None:
    """Legacy helper — keeps `_btc_open` fresh at each 15-min boundary for the
    model-odds calculation. Uses `live_prices["BTC"]` populated by the WS."""
    global _btc_open, _btc_open_slot
    while True:
        try:
            px = live_prices.get("BTC", 0.0)
            now = datetime.now()
            slot = (now.hour * 60 + now.minute) // 15
            if slot != _btc_open_slot and px > 0:
                _btc_open = px
                _btc_open_slot = slot
        except Exception:
            pass
        await asyncio.sleep(1)


# ── Ticker helpers ────────────────────────────────────────────────────────────

def _window_start_ts() -> int:
    return (int(time.time()) // 900) * 900

def _window_start_et(offset: int = 0) -> datetime:
    now_et = datetime.now(timezone.utc) + ET_OFFSET
    mins   = (now_et.minute // 15) * 15
    w = now_et.replace(minute=mins, second=0, microsecond=0)
    return w + timedelta(minutes=15 * offset)

def _ticker_for(series: str, w: datetime) -> str:
    close = w + timedelta(minutes=15)
    return series + "-" + close.strftime("%y%b%d%H%M").upper() + "-" + close.strftime("%M")

def _all_tickers(offset: int = 0) -> dict[str, str]:
    """Returns {ticker: sym} for all coins at the given window offset."""
    w = _window_start_et(offset)
    return {_ticker_for(series, w): sym for sym, series in COINS.items()}

# ── Per-coin state ────────────────────────────────────────────────────────────
# state[sym] = {ticker, window_ts, window_id, floor, yes_ask, no_ask, winner_set}
state: dict[str, dict] = {
    sym: {"ticker": "", "window_ts": 0, "window_id": 0,
          "floor": 0.0, "yes_ask": 0.0, "no_ask": 0.0, "winner_set": False}
    for sym in COINS
}

# Kalshi WS routes incoming ticks: ticker → sym
_ticker_to_sym: dict[str, str] = {}

# ── Polymarket state ─────────────────────────────────────────────────────────
# poly_state[sym] = {slug, window_ts, poly_window_id, ask_up, ask_dn, winner_set}
poly_state: dict[str, dict] = {
    sym: {"slug": "", "window_ts": 0, "poly_window_id": 0,
          "ask_up": 0.0, "ask_dn": 0.0, "winner_set": False}
    for sym in COINS
}
# CLOB WS routes by token id: token_id → (sym, "Up"|"Down")
_poly_token_to_info: dict[str, tuple[str, str]] = {}

# ── Kalshi REST helpers ───────────────────────────────────────────────────────

async def _fetch_floor(client: httpx.AsyncClient, ticker: str) -> float:
    """Fetch floor_strike from Kalshi market REST."""
    try:
        path = f"/trade-api/v2/markets/{ticker}"
        r = await client.get(f"{KALSHI_BASE}/markets/{ticker}",
                             headers=_kalshi_headers("GET", path), timeout=8)
        if r.status_code == 200:
            fs = r.json().get("market", {}).get("floor_strike")
            if fs:
                return float(fs)
    except Exception as e:
        console.print(f"[dim yellow]floor fetch {ticker}: {e}[/dim yellow]")
    return 0.0


async def _init_windows(client: httpx.AsyncClient) -> None:
    """Called at every window roll — sets up state and DB rows for all coins."""
    ts = _window_start_ts()
    w  = _window_start_et()
    console.print(f"\n[bold cyan]━━ {w.strftime('%H:%M')} ET ━━[/bold cyan]")

    async def _init_one(sym: str, series: str) -> None:
        ticker = _ticker_for(series, w)
        state[sym].update({"ticker": ticker, "window_ts": ts,
                           "winner_set": False, "yes_ask": 0.0, "no_ask": 0.0,
                           "coin_open_price": 0.0})
        _ticker_to_sym[ticker] = sym
        _ticker_to_sym[_ticker_for(series, _window_start_et(1))] = sym  # next window pre-map

        wid = await btc_db.ensure_window(ticker, ts)
        state[sym]["window_id"] = wid

        floor = await _fetch_floor(client, ticker)
        state[sym]["floor"] = floor
        if floor > 0:
            asyncio.ensure_future(btc_db.set_floor(ticker, floor))
            console.print(f"  [dim]{sym}[/dim]  {ticker}  floor={floor:,.4f}")
        else:
            console.print(f"  [dim]{sym}[/dim]  {ticker}  [yellow]floor N/A[/yellow]")

    await asyncio.gather(*[_init_one(sym, series) for sym, series in COINS.items()])

    # Snap coin open prices from Coinbase WS. Wait up to 3s for live_prices
    # to populate for each coin (usually already there from prior ticks).
    async def _snap_open(sym: str) -> None:
        for _ in range(30):
            px = live_prices.get(sym, 0.0)
            if px > 0:
                state[sym]["coin_open_price"] = px
                asyncio.ensure_future(btc_db.set_coin_open_price(state[sym]["ticker"], px))
                console.print(f"  [dim]{sym} coin_open = ${px:,.2f}[/dim]")
                return
            await asyncio.sleep(0.1)
        console.print(f"  [yellow]{sym} coin_open snap failed (no live price)[/yellow]")
    await asyncio.gather(*[_snap_open(sym) for sym in COINS])


# ── Kalshi WebSocket ──────────────────────────────────────────────────────────

async def _kalshi_ws_feed() -> None:
    """Single Kalshi WS — receives yes/no ticks for all coins."""
    _msg_id   = 0
    subscribed: set[str] = set()

    def _next_id() -> int:
        nonlocal _msg_id
        _msg_id += 1
        return _msg_id

    while True:
        try:
            async with websockets.connect(
                KALSHI_WS, additional_headers=_ws_auth_headers(),
                ping_interval=20, open_timeout=10,
            ) as ws:
                console.print("[dim]Kalshi WS connected[/dim]")
                subscribed.clear()

                async def _subscribe() -> None:
                    tickers = []
                    for offset in [0, 1]:
                        for ticker, sym in _all_tickers(offset).items():
                            if ticker not in subscribed:
                                tickers.append(ticker)
                                subscribed.add(ticker)
                                _ticker_to_sym[ticker] = sym
                    if tickers:
                        await ws.send(json.dumps({
                            "id": _next_id(), "cmd": "subscribe",
                            "params": {"channels": ["ticker"], "market_tickers": tickers},
                        }))

                await _subscribe()
                last_resub = time.time()

                async for raw in ws:
                    msg   = json.loads(raw)
                    mtype = msg.get("type", "")
                    if mtype == "ticker":
                        data = msg.get("msg", msg)
                        t    = data.get("market_ticker", "")
                        ya   = float(data.get("yes_ask_dollars", 0) or 0)
                        yb   = float(data.get("yes_bid_dollars", 0) or 0)
                        na   = round(1.0 - yb, 4) if yb > 0 else 0.0
                        sym  = _ticker_to_sym.get(t)
                        if sym and ya > 0:
                            state[sym]["yes_ask"] = ya
                            state[sym]["no_ask"]  = na

                    if time.time() - last_resub > 30:
                        await _subscribe()
                        last_resub = time.time()

        except Exception as e:
            console.print(f"[yellow]Kalshi WS: {e} — reconnecting[/yellow]")
            await asyncio.sleep(3)


# ── Polymarket discovery + CLOB WebSocket ───────────────────────────────────

def _poly_slug(sym: str, window_ts: int) -> str:
    return f"{POLY_SLUG_PREFIX[sym]}-{window_ts}"


async def _fetch_poly_market(client: httpx.AsyncClient, slug: str) -> dict | None:
    try:
        r = await client.get(GAMMA_API, params={"slug": slug}, timeout=8)
        if r.status_code == 200:
            arr = r.json()
            if isinstance(arr, list) and arr:
                return arr[0]
    except Exception:
        pass
    return None


def _register_poly_tokens(sym: str, market: dict) -> None:
    """Parse clobTokenIds + outcomes, register token→(sym, side) mapping."""
    try:
        tokens   = json.loads(market.get("clobTokenIds", "[]"))
        outcomes = json.loads(market.get("outcomes", "[]"))
        if len(tokens) < 2 or len(outcomes) < 2:
            return
        up_idx = next((i for i, o in enumerate(outcomes) if str(o).lower() == "up"), 0)
        dn_idx = 1 - up_idx
        _poly_token_to_info[tokens[up_idx]] = (sym, "Up")
        _poly_token_to_info[tokens[dn_idx]] = (sym, "Down")
    except Exception as e:
        console.print(f"  [dim yellow]poly token parse {sym}: {e}[/dim yellow]")


async def _init_poly_windows(client: httpx.AsyncClient) -> None:
    """Called on every window roll — discover Polymarket markets for each coin."""
    ts = _window_start_ts()

    async def _init_one(sym: str) -> None:
        slug = _poly_slug(sym, ts)
        poly_state[sym].update({
            "slug": slug, "window_ts": ts,
            "ask_up": 0.0, "ask_dn": 0.0, "winner_set": False,
        })
        market = await _fetch_poly_market(client, slug)
        if not market:
            console.print(f"  [dim]{sym}[/dim]  poly {slug}  [yellow]not found[/yellow]")
            poly_state[sym]["poly_window_id"] = 0
            return
        pwid = await btc_db.ensure_poly_window(slug, sym, ts)
        poly_state[sym]["poly_window_id"] = pwid
        _register_poly_tokens(sym, market)
        console.print(f"  [dim]{sym}[/dim]  poly {slug}")

    await asyncio.gather(*[_init_one(sym) for sym in COINS])


async def _poly_clob_ws_feed() -> None:
    """Single Polymarket CLOB WS — updates poly_state[sym].ask_up/ask_dn in real time."""
    while True:
        try:
            async with websockets.connect(CLOB_WS, ping_interval=20, open_timeout=10) as ws:
                console.print("[dim]Polymarket CLOB WS connected[/dim]")
                subscribed: set[str] = set()

                async def _subscribe_new() -> None:
                    new = set(_poly_token_to_info.keys()) - subscribed
                    if new:
                        await ws.send(json.dumps({
                            "assets_ids": list(new), "type": "market",
                        }))
                        subscribed.update(new)

                await _subscribe_new()
                last_resub = time.time()

                async for raw in ws:
                    # Periodically subscribe to any new tokens (new windows rolled in)
                    if time.time() - last_resub > 30:
                        await _subscribe_new()
                        last_resub = time.time()

                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    # Initial snapshot: list of orderbook dicts
                    if isinstance(msg, list):
                        for book in msg:
                            token_id = book.get("asset_id", "")
                            info = _poly_token_to_info.get(token_id)
                            if not info:
                                continue
                            sym, side = info
                            asks = book.get("asks", [])
                            if not asks:
                                continue
                            best_ask = float(asks[0].get("price") or 0)
                            if best_ask <= 0:
                                continue
                            if side == "Up":
                                poly_state[sym]["ask_up"] = best_ask
                            else:
                                poly_state[sym]["ask_dn"] = best_ask

                    # price_changes event
                    elif isinstance(msg, dict) and "price_changes" in msg:
                        for change in msg["price_changes"]:
                            token_id = change.get("asset_id", "")
                            info = _poly_token_to_info.get(token_id)
                            if not info:
                                continue
                            sym, side = info
                            best_ask = float(change.get("best_ask") or 0)
                            if best_ask <= 0:
                                continue
                            if side == "Up":
                                poly_state[sym]["ask_up"] = best_ask
                            else:
                                poly_state[sym]["ask_dn"] = best_ask

        except Exception as e:
            console.print(f"[yellow]Poly CLOB WS: {e} — reconnecting[/yellow]")
            await asyncio.sleep(3)


# ── Recording loop ────────────────────────────────────────────────────────────

async def _record_loop(client: httpx.AsyncClient) -> None:
    await _init_windows(client)
    await _init_poly_windows(client)
    current_ts = _window_start_ts()

    while True:
        await asyncio.sleep(1)

        now     = time.time()
        elapsed = now - current_ts
        e_int   = int(elapsed)

        if elapsed >= 900:
            current_ts = _window_start_ts()
            await _init_windows(client)
            await _init_poly_windows(client)
            continue

        parts = []
        for sym in COINS:
            st  = state[sym]
            ya  = st["yes_ask"]
            na  = st["no_ask"]
            wid = st["window_id"]

            if wid == 0:
                continue

            asyncio.ensure_future(btc_db.record_tick(
                wid, e_int,
                ya if ya > 0 else None,
                na if na > 0 else None,
            ))

            # Record Polymarket tick in parallel (if we have a poly window for this sym)
            pst  = poly_state[sym]
            pwid = pst["poly_window_id"]
            if pwid > 0:
                pau = pst["ask_up"]
                pad = pst["ask_dn"]
                asyncio.ensure_future(btc_db.record_poly_tick(
                    pwid, e_int,
                    pau if pau > 0 else None,
                    pad if pad > 0 else None,
                ))

            # Fetch actual result from Kalshi REST at close (more reliable than odds heuristic)
            if not st["winner_set"] and elapsed >= 895:
                async def _fetch_and_set_winner(ticker: str, s: str) -> None:
                    try:
                        path = f"/trade-api/v2/markets/{ticker}"
                        r = await client.get(f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}",
                                             headers=_kalshi_headers("GET", path), timeout=8)
                        if r.status_code == 200:
                            result = r.json().get("market", {}).get("result", "")
                            if result in ("yes", "no"):
                                await btc_db.set_winner(ticker, result)
                                label = "[green]YES[/green]" if result == "yes" else "[red]NO[/red]"
                                console.print(f"  {s} → {label}  (REST confirmed)")
                    except Exception as e:
                        console.print(f"  [yellow]winner fetch {ticker}: {e}[/yellow]")
                asyncio.ensure_future(_fetch_and_set_winner(st["ticker"], sym))
                st["winner_set"] = True

            if e_int % 5 == 0 and ya > 0:
                parts.append(f"[dim]{sym}[/dim] Y={ya:.2f} N={na:.2f}")

        if e_int % 5 == 0 and parts:
            line = f"t={e_int:>3}s  " + "   ".join(parts)
            # Append model odds — use Kalshi floor_strike, fallback to ccxt open
            btc_floor = state["BTC"]["floor"]
            ref_price = btc_floor if btc_floor > 0 else _btc_open
            if _btc_price > 0 and ref_price > 0:
                delta_pct = 100 * (_btc_price - ref_price) / ref_price
                p_up, p_down, n_eff = _get_odds(e_int, delta_pct)
                dc = "+" if delta_pct >= 0 else ""
                line += (f"   [bold]Δ{dc}{delta_pct:.3f}%[/bold]"
                         f"  model Y={p_up:.0%} N={p_down:.0%}")
            console.print(line)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    console.print("[bold cyan]Kalshi Recorder — BTC / ETH / SOL / XRP 15-min yes/no ticks[/bold cyan]")
    console.print(f"DB: [dim]{btc_db.DB_PATH}[/dim]\n")

    btc_db.init_db()
    _load_surface()

    async with httpx.AsyncClient() as client:
        await asyncio.gather(
            _kalshi_ws_feed(),
            _poly_clob_ws_feed(),
            _record_loop(client),
            _coinbase_ws_feed(),
            _btc_open_slot_tracker(),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Recorder stopped.[/dim]")
