#!/usr/bin/env python3
"""
BTC 15-min Up/Down — LIVE Kalshi trader.

Same model + trigger logic as btc_strategy_paper.py but places real orders on Kalshi.
Credentials read from .env (KALSHI_API_KEY, KALSHI_API_SECRET).

Run:  python3 btc_kalshi_trader.py
Stop: Ctrl-C  (open orders are NOT cancelled — they expire with the window)
"""
import asyncio
import base64
import json
import os
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
import websockets
from dotenv import load_dotenv
from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding

# ── Probability surface (same as paper trader) ────────────────────────────────
_SURFACE_FILE = Path.home() / ".btc_model_surface.json"
_surface: dict = {}

def _load_surface() -> None:
    global _surface
    if _SURFACE_FILE.exists():
        _surface = json.loads(_SURFACE_FILE.read_text())
        console.print(f"[dim]Surface loaded: {_surface['meta']['n_windows']:,} windows[/dim]")

def _get_odds(elapsed: float, delta_pct: float) -> dict | None:
    if not _surface:
        return None
    meta  = _surface["meta"]
    ti = max(0, min(int(round(elapsed / meta["t_step"])), len(_surface["t_vals"]) - 1))
    di = max(0, min(int(round((delta_pct - meta["d_min"]) / meta["d_step"])), len(_surface["d_vals"]) - 1))
    cell  = _surface["surface"][ti][di]
    p_up  = cell["p_up"]
    n_eff = cell["n_eff"]
    return {
        "p_up":       p_up,
        "p_down":     round(1 - p_up, 4),
        "n_eff":      n_eff,
        "confidence": "high" if n_eff >= 30 else "med" if n_eff >= 10 else "low",
    }


def _implied_btc(elapsed: float, yes_ask: float, floor: float) -> float | None:
    """Back-calculate implied BTC price from Kalshi's yes_ask using the model surface.
    Scans delta values at given elapsed time to find which delta produces p_up ≈ yes_ask."""
    if not _surface or floor <= 0 or yes_ask <= 0:
        return None
    meta   = _surface["meta"]
    ti     = max(0, min(int(round(elapsed / meta["t_step"])), len(_surface["t_vals"]) - 1))
    row    = _surface["surface"][ti]
    d_vals = _surface["d_vals"]
    # Find delta where model p_up is closest to yes_ask
    best_di   = min(range(len(row)), key=lambda i: abs(row[i]["p_up"] - yes_ask))
    best_delta = d_vals[best_di]
    return floor * (1 + best_delta / 100)

# ── Config ─────────────────────────────────────────────────────────────────────
load_dotenv()
KALSHI_BASE   = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS     = "wss://api.elections.kalshi.com/trade-api/ws/v2"
CB_WS         = "wss://advanced-trade-ws.coinbase.com"
KALSHI_POLL   = 30    # REST fallback poll interval (WS is primary)

# ── Risk limits (conservative real-money defaults) ────────────────────────────
MAX_PER_WINDOW  = 150   # max total $ per 15-min window
MAX_PER_SIDE    = 100   # max $ on any single side (YES or NO)
ODDS_THRESHOLD  = 0.07  # model must beat Kalshi by ≥7%
ODDS_STAKE      = 20.0  # base stake per maker order; time-scaled up to 4× late in window
ODDS_COOLDOWN   = 20    # seconds between buys on same side
PROBE_ELAPSED   = 90    # first 90s: probe only
PROBE_STAKE     = 10.0  # max per trade in probe phase
CONVICTION_MODEL   = 0.70
CONVICTION_EDGE    = 0.15
CONVICTION_ELAPSED = 300
INSURANCE_PRICE    = 0.25
INSURANCE_STAKE    = 10.0
ARB_THRESHOLD   = 0.94
ARB_STAKE       = 40.0
ARB_COOLDOWN    = 20

ET_OFFSET = timedelta(hours=-4)

STATE_FILE = Path.home() / ".btc_kalshi_state.json"

console = Console()

# ── Kalshi auth ────────────────────────────────────────────────────────────────
_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)

def _kalshi_headers(method: str, path: str) -> dict:
    """Generate signed auth headers. Path = path only, no query string."""
    ts  = str(round(time.time() * 1000))
    msg = ts + method.upper() + path
    sig = _private_key.sign(
        msg.encode(),
        asym_padding.PSS(
            mgf=asym_padding.MGF1(hashes.SHA256()),
            salt_length=asym_padding.PSS.MAX_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       _api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "Content-Type":            "application/json",
    }

# ── Shared state ───────────────────────────────────────────────────────────────
btc_price:   float = 0.0   # composite (BRTI approx) — primary price used everywhere
btc_prev:    float = 0.0
_src_prices: dict  = {}    # exchange → latest price
_src_times:  dict  = {}    # exchange → last update timestamp


def _update_composite() -> None:
    """Recompute composite BTC price from all live exchange feeds."""
    global btc_prev, btc_price
    vals = [v for k, v in _src_prices.items() if v > 0
            and (time.time() - _src_times.get(k, 0)) < 10]  # only fresh prices
    if not vals:
        return
    composite = sum(vals) / len(vals)
    if composite > 0:
        btc_prev  = btc_price if btc_price > 0 else composite
        btc_price = composite
positions:   dict  = {}   # kalshi_ticker → position dict
session_pnl: float = 0.0
trade_count: int   = 0
kalshi_prices: dict = {}  # kalshi_ticker → (yes_ask, no_ask)
kalshi_floor:  dict = {}  # kalshi_ticker → floor_strike
candle_open:   dict = {}  # kalshi_ticker → coinbase 15min candle open price
_price_updated: asyncio.Event = asyncio.Event()  # set on each WS price tick
_ws_tick_count: int = 0   # increments on every WS price message received

# Running limit order — at most one open resting order per window at a time
_running_order: dict = {}  # {ticker, order_id, side, limit_price, count}


# ── Ticker helpers ─────────────────────────────────────────────────────────────

def _window_ts(offset: int = 0) -> int:
    """Current 15-min window start as real UTC unix timestamp."""
    now = int(time.time())
    return (now // 900) * 900 + offset * 900

def _window_start(offset: int = 0) -> datetime:
    """Window start as ET datetime (for ticker formatting only)."""
    now_et = datetime.now(timezone.utc) + ET_OFFSET
    mins   = (now_et.minute // 15) * 15
    w = now_et.replace(minute=mins, second=0, microsecond=0)
    if offset:
        w = w + timedelta(minutes=15 * offset)
    return w

def _ticker_for(w: datetime) -> str:
    close = w + timedelta(minutes=15)
    return "KXBTC15M-" + close.strftime("%y%b%d%H%M").upper() + "-" + close.strftime("%M")

def _elapsed(pos: dict) -> float:
    return time.time() - pos["window_start"]


# ── Persistence ────────────────────────────────────────────────────────────────

def _save() -> None:
    try:
        STATE_FILE.write_text(json.dumps({
            "session_pnl": session_pnl,
            "trade_count": trade_count,
            "positions":   positions,
            "saved_at":    datetime.now().isoformat(),
        }, indent=2))
    except Exception as e:
        console.print(f"[yellow]State save error: {e}[/yellow]")

def _load() -> None:
    global trade_count
    if not STATE_FILE.exists():
        return
    try:
        s = json.loads(STATE_FILE.read_text())
        # session_pnl intentionally NOT restored — past values were wrong (cost bug)
        trade_count = s.get("trade_count", 0)
        for kt, p in s.get("positions", {}).items():
            if not p.get("resolved") and time.time() < p.get("window_end", 0) + 3600:
                positions[kt] = p
        console.print(f"[dim]Restored: {len(positions)} active positions  (P&L resets each run)[/dim]")
    except Exception as e:
        console.print(f"[yellow]State load error: {e}[/yellow]")


# ── Kalshi order helpers ───────────────────────────────────────────────────────

async def _cancel_order_id(client: httpx.AsyncClient, order_id: str) -> None:
    """Cancel a resting order by ID. Silently ignores errors (already filled/expired)."""
    try:
        opath = f"/portfolio/orders/{order_id}"
        await client.delete(
            f"{KALSHI_BASE}{opath}",
            headers=_kalshi_headers("DELETE", f"/trade-api/v2{opath}"),
            timeout=5,
        )
    except Exception:
        pass


async def _place_limit_maker(
    client: httpx.AsyncClient,
    ticker: str,
    side: str,
    limit_price: float,
    stake_dollars: float,
    reason: str,
) -> str | None:
    """Place a non-blocking limit order at limit_price. Returns order_id or None."""
    global trade_count
    limit_str = f"{limit_price:.2f}"
    count     = max(1, int(stake_dollars / limit_price))
    price_field = "yes_price_dollars" if side == "yes" else "no_price_dollars"
    order = {
        "ticker":          ticker,
        "action":          "buy",
        "side":            side,
        "count":           count,
        "type":            "limit",
        price_field:       limit_str,
        "client_order_id": str(uuid.uuid4()),
    }
    try:
        r = await client.post(
            f"{KALSHI_BASE}/portfolio/orders",
            headers=_kalshi_headers("POST", "/trade-api/v2/portfolio/orders"),
            content=json.dumps(order),
            timeout=8,
        )
        if r.status_code not in (200, 201):
            console.print(f"  [yellow]Limit order failed {r.status_code}: {r.text[:120]}[/yellow]")
            return None
        o = r.json().get("order", {})
        trade_count += 1
        order_id = o.get("order_id", "")
        ts = datetime.now().strftime("%H:%M:%S")
        console.print(
            f"  [dim][{ts}] LIMIT {side.upper()} {count}ct @ {limit_str}  "
            f"model-based maker order  [{reason}][/dim]"
        )
        return order_id
    except Exception as e:
        console.print(f"  [yellow]Limit order error: {e}[/yellow]")
        return None


async def _check_limit_fill(
    client: httpx.AsyncClient,
    order_id: str,
    side: str,
    limit_price: float,
    ticker: str,
) -> int:
    """Poll an order and return number of contracts filled (0 if still resting)."""
    try:
        opath = f"/portfolio/orders/{order_id}"
        r = await client.get(
            f"{KALSHI_BASE}{opath}",
            headers=_kalshi_headers("GET", f"/trade-api/v2{opath}"),
            timeout=5,
        )
        if r.status_code == 200:
            o = r.json().get("order", {})
            return int(float(o.get("fill_count_fp", 0) or 0))
    except Exception:
        pass
    return 0


# ── Kalshi order placement ─────────────────────────────────────────────────────

async def _place_order(
    client: httpx.AsyncClient,
    ticker: str,
    side: str,          # "yes" or "no"
    stake_dollars: float,
    ask_price: float,   # current best ask in dollars
    reason: str,
) -> tuple[float, int]:
    """Place a limit order. Returns (dollars_filled, contracts_filled). Both 0 if failed."""
    global trade_count
    if ask_price <= 0 or ask_price >= 1:
        return 0.0, 0
    # 10% above ask to cross the spread and fill immediately as taker
    # 2¢ above ask to cross the spread — keeps edge intact vs 10% which erodes it at high prices
    limit_price = min(round(ask_price + 0.02, 2), 0.99)
    limit_str   = f"{limit_price:.2f}"
    count = max(1, int(stake_dollars / limit_price))

    price_field = "yes_price_dollars" if side == "yes" else "no_price_dollars"
    order = {
        "ticker":          ticker,
        "action":          "buy",
        "side":            side,
        "count":           count,
        "type":            "limit",
        price_field:       limit_str,
        "client_order_id": str(uuid.uuid4()),
    }
    body = json.dumps(order)
    path = "/portfolio/orders"

    try:
        r = await client.post(
            f"{KALSHI_BASE}{path}",
            headers=_kalshi_headers("POST", f"/trade-api/v2{path}"),
            content=body,
            timeout=8,
        )
        if r.status_code not in (200, 201):
            console.print(f"  [yellow]Order failed {r.status_code}: {r.text[:200]}[/yellow]")
            return 0.0, 0

        o      = r.json().get("order", {})
        status = o.get("status", "?")
        trade_count += 1

        filled_ct = 0
        if status == "executed":
            # taker_fill_cost_dollars is often empty — use count * limit_price as truth
            filled_ct = int(float(o.get("fill_count_fp", count) or count))
            filled    = filled_ct * limit_price
            fees      = float(o.get("taker_fees_dollars", 0) or 0)
        elif status == "resting":
            ts_placed = datetime.now().strftime("%H:%M:%S")
            console.print(
                f"  [dim][{ts_placed}] RESTING {side.upper()} {count}ct @ {limit_price:.3f}  "
                f"waiting 2s...  [{reason}][/dim]"
            )
            # Order is in the book — poll once after 2s to get actual fill
            await asyncio.sleep(2)
            order_id = o.get("order_id", "")
            opath = f"/portfolio/orders/{order_id}"
            r2 = await client.get(
                f"{KALSHI_BASE}{opath}",
                headers=_kalshi_headers("GET", f"/trade-api/v2{opath}"),
                timeout=8,
            )
            if r2.status_code == 200:
                o2        = r2.json().get("order", {})
                filled_ct = int(float(o2.get("fill_count_fp", 0) or 0))
                filled    = filled_ct * limit_price
                fees      = float(o2.get("taker_fees_dollars", 0) or 0)
                status    = o2.get("status", status)
                if filled_ct == 0:
                    # Still not filled — cancel it, don't block capital
                    await client.delete(
                        f"{KALSHI_BASE}{opath}",
                        headers=_kalshi_headers("DELETE", f"/trade-api/v2{opath}"),
                        timeout=5,
                    )
                    console.print(
                        f"  [yellow]CANCELLED {side.upper()} {count}ct @ {limit_price:.3f} "
                        f"(no takers)  [{reason}][/yellow]"
                    )
                    return 0.0, 0
            else:
                filled    = 0.0
                fees      = 0.0
        else:
            filled    = 0.0
            fees      = 0.0

        now = datetime.now().strftime("%H:%M:%S")
        console.rule(f"[bold]ORDER FILLED[/bold]")
        t = Text()
        t.append(f"  [{now}] ", "dim")
        t.append("LIVE BUY ", "bold")
        t.append(f"{'YES' if side=='yes' else 'NO':<3} ", "bold green" if side == "yes" else "bold red")
        t.append(f"{filled_ct} contracts @ {limit_price:.3f}  ")
        t.append(f"filled=${filled:.2f} fees=${fees:.3f} [{status}]  ", "dim")
        t.append(f"[{reason}]", "dim")
        console.print(t)
        return filled, filled_ct

    except Exception as e:
        console.print(f"  [yellow]Order error: {e}[/yellow]")
        return 0.0, 0


# ── Trigger logic (mirrors paper trader _check_triggers for BTC) ───────────────

async def _check_triggers(client: httpx.AsyncClient, ticker: str) -> None:
    if ticker not in positions:
        return
    pos = positions[ticker]
    if pos["resolved"]:
        return

    yes_ask, no_ask = kalshi_prices.get(ticker, (0.0, 0.0))
    if yes_ask <= 0 or no_ask <= 0:
        return
    if yes_ask < 0.05 or yes_ask > 0.95 or no_ask < 0.05 or no_ask > 0.95:
        return

    now      = time.time()
    cd       = pos.setdefault("cooldowns", {})
    elapsed  = _elapsed(pos)
    spent    = pos["spent_yes"] + pos["spent_no"]
    remaining = MAX_PER_WINDOW - spent
    if remaining < 5:
        return

    # ── ARB ───────────────────────────────────────────────────────────────────
    if yes_ask + no_ask < ARB_THRESHOLD:
        if now - cd.get("arb", 0) > ARB_COOLDOWN:
            stake = min(ARB_STAKE, remaining / 2)
            if stake >= 5:
                filled_y, ct_y = await _place_order(client, ticker, "yes", stake, yes_ask,
                                                   f"arb-yes@{yes_ask:.3f}+no@{no_ask:.3f}={yes_ask+no_ask:.3f}")
                filled_n, ct_n = await _place_order(client, ticker, "no",  stake, no_ask,
                                                   f"arb-no@{yes_ask:.3f}+no@{no_ask:.3f}={yes_ask+no_ask:.3f}")
                pos["spent_yes"]  += filled_y
                pos["spent_no"]   += filled_n
                pos["shares_yes"] += ct_y
                pos["shares_no"]  += ct_n
                cd["arb"] = now
                _save()

    # ── ODDS + CONVICTION + INSURANCE ─────────────────────────────────────────
    open_p  = pos.get("floor_strike", 0)
    cur_btc = btc_price
    if elapsed <= 30 or open_p <= 0 or cur_btc <= 0:
        return

    delta = (cur_btc - open_p) / open_p * 100
    emp   = _get_odds(elapsed, delta)
    if not emp or emp["confidence"] not in ("high", "med"):
        return

    in_probe = elapsed < PROBE_ELAPSED
    edge_yes = emp["p_up"]   - yes_ask
    edge_no  = emp["p_down"] - no_ask

    # ── Running limit order (maker strategy) ──────────────────────────────────
    # Determine dominant side (50–70% range; conviction handles >70%)
    global _running_order
    p_yes, p_no = emp["p_up"], emp["p_down"]
    dominant_side  = "yes" if p_yes >= p_no else "no"
    dominant_prob  = p_yes if dominant_side == "yes" else p_no
    dominant_spent = pos["spent_yes"] if dominant_side == "yes" else pos["spent_no"]
    dominant_cap   = MAX_PER_SIDE - dominant_spent

    # Only run maker orders in 50–70% range; don't open opposite side if already committed
    already_opposite = (
        (dominant_side == "yes" and pos["spent_no"]  > 0 and pos["spent_yes"] == 0) or
        (dominant_side == "no"  and pos["spent_yes"] > 0 and pos["spent_no"]  == 0)
    )
    # Sanity check: model and Kalshi must agree on direction.
    # If model says NO but Kalshi prices YES > 0.50 (or vice versa), signals are flipped —
    # something is wrong (likely BRTI vs Coinbase divergence) so don't trade.
    kalshi_favors_yes = yes_ask > no_ask
    model_favors_yes  = p_yes > p_no
    signals_agree     = kalshi_favors_yes == model_favors_yes
    if not signals_agree:
        # Log once per minute so it doesn't spam
        if now - cd.get("flip_warn", 0) > 60:
            console.print(
                f"  [yellow]⚠ signals flipped: model={'YES' if model_favors_yes else 'NO'} "
                f"but kalshi={'YES' if kalshi_favors_yes else 'NO'} — skipping orders[/yellow]"
            )
            cd["flip_warn"] = now

    maker_ok = (
        0.50 < dominant_prob < CONVICTION_MODEL
        and not already_opposite
        and remaining >= 5
        and dominant_cap >= 5
        and signals_agree
    )

    if maker_ok:
        # Limit price = model probability - ODDS_THRESHOLD (post below fair value)
        target_price = round(dominant_prob - ODDS_THRESHOLD, 2)
        target_price = max(0.02, min(target_price, 0.97))
        # Scale stake by elapsed time: 0–90s=probe, 90–300s=25%, 300–600s=50%, 600s+=100%
        if in_probe:
            time_stake = PROBE_STAKE
        elif elapsed < 300:
            time_stake = ODDS_STAKE * 0.5   # $10 — cautious, lots of window left
        elif elapsed < 600:
            time_stake = ODDS_STAKE * 1.0   # $20 — mid window
        else:
            time_stake = ODDS_STAKE * 2.0   # $40 — late window, commit more
        stake = min(time_stake, remaining, dominant_cap)
        ro           = _running_order

        need_replace = False
        if ro.get("ticker") == ticker and ro.get("order_id"):
            if ro["side"] != dominant_side:
                # Direction flipped — cancel and repost
                console.print(
                    f"  [dim]LIMIT flip {ro['side'].upper()}→{dominant_side.upper()} "
                    f"cancelling {ro['order_id'][:8]}[/dim]"
                )
                await _cancel_order_id(client, ro["order_id"])
                _running_order = {}
                need_replace = True
            elif abs(ro["limit_price"] - target_price) >= 0.02:
                # Price moved >2¢ — reprice
                console.print(
                    f"  [dim]LIMIT reprice {ro['side'].upper()} "
                    f"{ro['limit_price']:.2f}→{target_price:.2f}[/dim]"
                )
                # Check fills before cancelling
                filled_ct = await _check_limit_fill(client, ro["order_id"], ro["side"], ro["limit_price"], ticker)
                if filled_ct > 0:
                    cost = filled_ct * ro["limit_price"]
                    if ro["side"] == "yes":
                        pos["spent_yes"]  += cost
                        pos["shares_yes"] += filled_ct
                    else:
                        pos["spent_no"]   += cost
                        pos["shares_no"]  += filled_ct
                    ts = datetime.now().strftime("%H:%M:%S")
                    console.rule("[bold]ORDER FILLED[/bold]")
                    console.print(
                        f"  [{ts}] LIMIT FILL {ro['side'].upper()} {filled_ct}ct "
                        f"@ {ro['limit_price']:.3f}  filled=${cost:.2f}"
                    )
                    _save()
                await _cancel_order_id(client, ro["order_id"])
                _running_order = {}
                need_replace = True
            # else: order still good, leave it
        else:
            # No running order for this ticker — place one
            if ro.get("order_id") and ro.get("ticker") != ticker:
                await _cancel_order_id(client, ro["order_id"])
                _running_order = {}
            need_replace = True

        if need_replace and remaining >= 5:
            order_id = await _place_limit_maker(
                client, ticker, dominant_side, target_price, stake,
                f"limit-{dominant_side}@{target_price:.2f} model={dominant_prob:.1%} n={emp['n_eff']}"
            )
            if order_id:
                _running_order = {
                    "ticker":      ticker,
                    "order_id":    order_id,
                    "side":        dominant_side,
                    "limit_price": target_price,
                    "count":       max(1, int(stake / target_price)),
                }
    else:
        # Conditions not met — cancel any outstanding maker order for this ticker
        ro = _running_order
        if ro.get("ticker") == ticker and ro.get("order_id"):
            # Check fills first
            filled_ct = await _check_limit_fill(client, ro["order_id"], ro["side"], ro["limit_price"], ticker)
            if filled_ct > 0:
                cost = filled_ct * ro["limit_price"]
                if ro["side"] == "yes":
                    pos["spent_yes"]  += cost
                    pos["shares_yes"] += filled_ct
                else:
                    pos["spent_no"]   += cost
                    pos["shares_no"]  += filled_ct
                ts = datetime.now().strftime("%H:%M:%S")
                console.rule("[bold]ORDER FILLED[/bold]")
                console.print(
                    f"  [{ts}] LIMIT FILL {ro['side'].upper()} {filled_ct}ct "
                    f"@ {ro['limit_price']:.3f}  filled=${cost:.2f}"
                )
                _save()
            await _cancel_order_id(client, ro["order_id"])
            _running_order = {}

    # Conviction burst — also gated on signals agreeing
    if elapsed > CONVICTION_ELAPSED and signals_agree:
        if emp["p_up"] > CONVICTION_MODEL and edge_yes > CONVICTION_EDGE:
            if now - cd.get("conv_yes", 0) > 5:
                stake = min(remaining, MAX_PER_SIDE - pos["spent_yes"])
                if stake >= 5:
                    filled, ct = await _place_order(client, ticker, "yes", stake, yes_ask,
                                                   f"conviction@{yes_ask:.3f} model={emp['p_up']:.1%} "
                                                   f"edge=+{edge_yes:.2f} t={int(elapsed)}s")
                    if filled > 0:
                        pos["spent_yes"]       += filled
                        pos["shares_yes"]      += ct
                        pos["conviction_fired"] = True
                        cd["conv_yes"] = now
                        _save()
        if emp["p_down"] > CONVICTION_MODEL and edge_no > CONVICTION_EDGE:
            if now - cd.get("conv_no", 0) > 5:
                stake = min(remaining, MAX_PER_SIDE - pos["spent_no"])
                if stake >= 5:
                    filled, ct = await _place_order(client, ticker, "no", stake, no_ask,
                                                    f"conviction@{no_ask:.3f} model={emp['p_down']:.1%} "
                                                    f"edge=+{edge_no:.2f} t={int(elapsed)}s")
                    if filled > 0:
                        pos["spent_no"]        += filled
                        pos["shares_no"]       += ct
                        pos["conviction_fired"] = True
                        cd["conv_no"] = now
                        _save()

    # Insurance disabled — not worth the EV cost given model accuracy


# ── Kalshi price feed (WebSocket primary + REST fallback) ─────────────────────

def _ws_auth_headers() -> dict:
    """Signed headers for the Kalshi WebSocket handshake (same scheme as REST)."""
    ts  = str(round(time.time() * 1000))
    msg = ts + "GET" + "/trade-api/ws/v2"
    sig = _private_key.sign(
        msg.encode(),
        asym_padding.PSS(
            mgf=asym_padding.MGF1(hashes.SHA256()),
            salt_length=asym_padding.PSS.MAX_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       _api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }


def _update_price(ticker: str, ya: float, na: float, source: str = "ws") -> None:
    """Central price update used by both WS and REST paths."""
    global _ws_tick_count
    if ya > 0 and na > 0:
        kalshi_prices[ticker] = (ya, na)
        if ticker in positions:
            positions[ticker]["yes_ask"] = ya
            positions[ticker]["no_ask"]  = na
        if source == "ws":
            _ws_tick_count += 1
        _price_updated.set()


async def _kalshi_ws_feed() -> None:
    """Real-time Kalshi prices via WebSocket. Reconnects automatically."""
    _msg_id  = 0
    subscribed: set[str] = set()

    def _next_id() -> int:
        nonlocal _msg_id
        _msg_id += 1
        return _msg_id

    while True:
        try:
            async with websockets.connect(
                KALSHI_WS,
                additional_headers=_ws_auth_headers(),
                ping_interval=20,
                open_timeout=10,
            ) as ws:
                console.print("[dim]Kalshi WS connected — real-time prices active[/dim]")
                subscribed.clear()
                _first_tick = True

                # Subscribe to current + next window
                async def _subscribe_new() -> None:
                    tickers = []
                    for offset in [0, 1]:
                        t = _ticker_for(_window_start(offset))
                        if t not in subscribed:
                            tickers.append(t)
                            subscribed.add(t)
                    if tickers:
                        await ws.send(json.dumps({
                            "id": _next_id(),
                            "cmd": "subscribe",
                            "params": {"channels": ["ticker"], "market_tickers": tickers},
                        }))

                await _subscribe_new()
                last_resub = time.time()

                async for raw in ws:
                    msg = json.loads(raw)
                    mtype = msg.get("type", "")

                    # Ticker snapshot or update
                    if mtype == "ticker":
                        data   = msg.get("msg", msg)
                        ticker = data.get("market_ticker", "")
                        ya = float(data.get("yes_ask_dollars", 0) or 0)
                        # no_ask derived from yes_bid: in a binary market no_ask = 1 - yes_bid
                        yes_bid = float(data.get("yes_bid_dollars", 0) or 0)
                        na = round(1.0 - yes_bid, 4) if yes_bid > 0 else 0.0
                        _update_price(ticker, ya, na)
                        if _first_tick and ya > 0:
                            console.print(f"[dim]WS first tick: {ticker}  yes_ask={ya:.3f}  no_ask={na:.3f}[/dim]")
                            _first_tick = False

                    # Resubscribe when window rolls (every ~30s check)
                    if time.time() - last_resub > 30:
                        await _subscribe_new()
                        last_resub = time.time()

        except Exception as e:
            console.print(f"[yellow]Kalshi WS error: {e} — reconnecting in 3s[/yellow]")
            await asyncio.sleep(3)


_market_fields_logged = False

async def _fetch_candle_open(client: httpx.AsyncClient, ticker: str, window_ts: int) -> None:
    """Fetch the Coinbase 15-min candle open at window start and store it."""
    if ticker in candle_open:
        return
    try:
        r = await client.get(
            "https://api.coinbase.com/api/v3/brokerage/products/BTC-USD/candles",
            params={"granularity": "FIFTEEN_MINUTE", "start": window_ts, "end": window_ts + 900},
            timeout=5,
        )
        if r.status_code == 200:
            candles = r.json().get("candles", [])
            if candles:
                # candle fields: start, open, high, low, close, volume
                open_price = float(candles[0].get("open", 0))
                if open_price > 0:
                    candle_open[ticker] = open_price
                    floor = kalshi_floor.get(ticker, 0)
                    diff  = open_price - floor if floor else 0
                    console.print(
                        f"[dim]Candle open: {ticker}  CB=${open_price:,.2f}  "
                        f"KS=${floor:,.2f}  diff={diff:+.2f}[/dim]"
                    )
    except Exception as e:
        console.print(f"[dim]Candle fetch error: {e}[/dim]")


async def _fetch_market(client: httpx.AsyncClient, ticker: str) -> None:
    """Fetch prices + floor_strike for a single ticker immediately."""
    global _market_fields_logged
    try:
        r = await client.get(f"{KALSHI_BASE}/markets/{ticker}", timeout=5)
        if r.status_code == 200:
            m  = r.json()["market"]
            # One-time debug: log all fields so we can see if BRTI price is present
            if not _market_fields_logged:
                _market_fields_logged = True
                # Also probe the event endpoint for underlying BRTI price
                event_ticker = m.get("event_ticker", "")
                if event_ticker:
                    try:
                        re = await client.get(f"{KALSHI_BASE}/events/{event_ticker}", timeout=5)
                        if re.status_code == 200:
                            ev = re.json().get("event", {})
                            console.print(f"[dim]Event API fields: {list(ev.keys())}[/dim]")
                            console.print(f"[dim]Event API sample: {json.dumps({k: ev[k] for k in list(ev.keys())}, default=str)[:600]}[/dim]")
                    except Exception as ep:
                        console.print(f"[dim]Event probe error: {ep}[/dim]")
            ya = float(m.get("yes_ask_dollars") or 0)
            na = float(m.get("no_ask_dollars")  or 0)
            fs = m.get("floor_strike")
            _update_price(ticker, ya, na, source="rest")
            if fs:
                kalshi_floor[ticker] = float(fs)
                console.print(f"[dim]Floor fetched: {ticker}  ${float(fs):,.2f}[/dim]")
    except Exception:
        pass


async def _poll_kalshi(client: httpx.AsyncClient) -> None:
    """REST fallback: fetches prices and floor_strike every 30s.
    Runs alongside WS — fills in floor_strike and catches any WS gaps."""
    fetched: set[str] = set()
    while True:
        for offset in [0, 1]:
            w      = _window_start(offset)
            ticker = _ticker_for(w)
            # Fetch immediately on first sight of a new ticker
            if ticker not in fetched:
                await _fetch_market(client, ticker)
                fetched.add(ticker)
            else:
                await _fetch_market(client, ticker)
            # Clean up old tickers
            fetched = {t for t in fetched if t in kalshi_floor or t in kalshi_prices}
        await asyncio.sleep(KALSHI_POLL)


# ── Multi-source BTC price feeds (composite ≈ BRTI) ───────────────────────────

async def _feed_coinbase() -> None:
    sub = {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "ticker"}
    while True:
        try:
            async with websockets.connect(CB_WS, ping_interval=20) as ws:
                await ws.send(json.dumps(sub))
                console.print("[dim]Coinbase feed connected[/dim]")
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("channel") != "ticker":
                        continue
                    for evt in msg.get("events", []):
                        for t in evt.get("tickers", []):
                            p = float(t.get("price", 0) or 0)
                            if p > 0:
                                _src_prices["coinbase"] = p
                                _src_times["coinbase"]  = time.time()
                                _update_composite()
        except Exception as e:
            console.print(f"[yellow]Coinbase feed error: {e}[/yellow]")
            await asyncio.sleep(3)


async def _feed_bitstamp() -> None:
    sub = {"event": "bts:subscribe", "data": {"channel": "live_trades_btcusd"}}
    while True:
        try:
            async with websockets.connect("wss://ws.bitstamp.net", ping_interval=20) as ws:
                await ws.send(json.dumps(sub))
                console.print("[dim]Bitstamp feed connected[/dim]")
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("event") == "trade":
                        p = float(msg.get("data", {}).get("price", 0) or 0)
                        if p > 0:
                            _src_prices["bitstamp"] = p
                            _src_times["bitstamp"]  = time.time()
                            _update_composite()
        except Exception as e:
            console.print(f"[yellow]Bitstamp feed error: {e}[/yellow]")
            await asyncio.sleep(3)


async def _feed_kraken() -> None:
    sub = {"method": "subscribe", "params": {"channel": "ticker", "symbol": ["BTC/USD"]}}
    while True:
        try:
            async with websockets.connect("wss://ws.kraken.com/v2", ping_interval=20) as ws:
                await ws.send(json.dumps(sub))
                console.print("[dim]Kraken feed connected[/dim]")
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("channel") == "ticker":
                        for d in msg.get("data", []):
                            p = float(d.get("last", 0) or 0)
                            if p > 0:
                                _src_prices["kraken"] = p
                                _src_times["kraken"]  = time.time()
                                _update_composite()
        except Exception as e:
            console.print(f"[yellow]Kraken feed error: {e}[/yellow]")
            await asyncio.sleep(3)


async def _feed_gemini() -> None:
    while True:
        try:
            async with websockets.connect(
                "wss://api.gemini.com/v1/marketdata/BTCUSD?trades=true",
                ping_interval=20
            ) as ws:
                console.print("[dim]Gemini feed connected[/dim]")
                async for raw in ws:
                    msg = json.loads(raw)
                    for evt in msg.get("events", []):
                        if evt.get("type") == "trade":
                            p = float(evt.get("price", 0) or 0)
                            if p > 0:
                                _src_prices["gemini"] = p
                                _src_times["gemini"]  = time.time()
                                _update_composite()
        except Exception as e:
            console.print(f"[yellow]Gemini feed error: {e}[/yellow]")
            await asyncio.sleep(3)


def _price_feed():
    """Placeholder — replaced by individual feed coroutines."""
    pass


# ── Window lifecycle ───────────────────────────────────────────────────────────

async def _open_window(client: httpx.AsyncClient, w: datetime, ts: int) -> None:
    ticker = _ticker_for(w)
    if ticker in positions:
        return
    # Eagerly fetch floor + prices + candle open if not already known
    if ticker not in kalshi_floor or ticker not in kalshi_prices:
        await _fetch_market(client, ticker)
    await _fetch_candle_open(client, ticker, ts)
    ya, na = kalshi_prices.get(ticker, (0.0, 0.0))
    if ya <= 0 or na <= 0:
        return
    floor = kalshi_floor.get(ticker, btc_price)

    positions[ticker] = {
        "ticker":       ticker,
        "window_start": ts,
        "window_end":   ts + 900,
        "floor_strike": floor,
        "yes_ask":      ya,
        "no_ask":       na,
        "spent_yes":    0.0,
        "spent_no":     0.0,
        "shares_yes":   0.0,
        "shares_no":    0.0,
        "cooldowns":    {},
        "trades":       [],
        "resolved":     False,
        "winner":       "",
        "pnl":          0.0,
        "conviction_fired": False,
    }
    wlabel = w.strftime("%I:%M %p ET")
    console.rule(f"[bold cyan]NEW WINDOW  BTC  {wlabel}  ({ticker})[/bold cyan]")
    console.print(f"  Floor strike : ${floor:,.2f}")
    console.print(f"  Kalshi asks  : YES={ya:.3f}  NO={na:.3f}  Sum={ya+na:.3f}")
    console.print()


async def _settle_window(client: httpx.AsyncClient, ticker: str) -> None:
    global session_pnl, _running_order
    pos = positions[ticker]
    # Cancel any outstanding maker order for this window
    ro = _running_order
    if ro.get("ticker") == ticker and ro.get("order_id"):
        await _cancel_order_id(client, ro["order_id"])
        _running_order = {}
    if pos["resolved"]:
        return

    # Determine winner from final BTC price vs floor
    floor   = pos.get("floor_strike", 0)
    end_btc = btc_price

    # If we have no BTC price yet, wait up to 30s past expiry then force settle
    overdue = time.time() - pos["window_end"]
    if (end_btc == 0 or floor == 0) and overdue < 30:
        return

    winner = "yes" if end_btc >= floor else "no"

    # Fetch fills for this specific ticker to compute real P&L
    # Use pos["spent_yes"/"spent_no"] as fallback if API fails
    path = "/portfolio/fills"
    fills = []
    try:
        r = await client.get(
            f"{KALSHI_BASE}{path}",
            headers=_kalshi_headers("GET", f"/trade-api/v2{path}"),
            params={"ticker": ticker, "limit": 100},
            timeout=8,
        )
        if r.status_code == 200:
            fills = [f for f in r.json().get("fills", [])
                     if f.get("market_ticker") == ticker]
    except Exception as e:
        console.print(f"  [yellow]Fills fetch error: {e} — using tracked spend[/yellow]")

    if fills:
        # taker_fill_cost_dollars is unreliable — compute cost as count * price
        yes_count = sum(float(f.get("count_fp") or 0)
                        for f in fills if f.get("side") == "yes" and f.get("action") == "buy")
        no_count  = sum(float(f.get("count_fp") or 0)
                        for f in fills if f.get("side") == "no"  and f.get("action") == "buy")
        yes_cost  = sum(float(f.get("count_fp") or 0) * float(f.get("yes_price_dollars") or 0)
                        for f in fills if f.get("side") == "yes" and f.get("action") == "buy")
        no_cost   = sum(float(f.get("count_fp") or 0) * float(f.get("no_price_dollars") or 0)
                        for f in fills if f.get("side") == "no"  and f.get("action") == "buy")
        fees      = sum(float(f.get("taker_fees_dollars") or 0) for f in fills)
    else:
        # Fallback: use tracked spend from pos dict (no fee info)
        yes_cost  = pos.get("spent_yes", 0)
        no_cost   = pos.get("spent_no",  0)
        yes_count = yes_cost / pos.get("yes_ask", 0.5) if yes_cost else 0
        no_count  = no_cost  / pos.get("no_ask",  0.5) if no_cost  else 0
        fees      = 0.0

    gross      = yes_count if winner == "yes" else no_count
    total_cost = yes_cost + no_cost + fees
    pnl        = gross - total_cost

    pos["resolved"]   = True
    pos["winner"]     = winner
    pos["pnl"]        = pnl
    pos["yes_cost"]   = yes_cost
    pos["no_cost"]    = no_cost
    pos["yes_count"]  = yes_count
    pos["no_count"]   = no_count
    pos["fees"]       = fees
    session_pnl      += pnl

    w_label = datetime.fromtimestamp(pos["window_start"]).strftime("%I:%M %p")
    pnl_col = "green" if pnl >= 0 else "red"
    sign    = "+" if pnl >= 0 else ""
    console.rule(f"[bold yellow]SETTLED  BTC  {w_label}  →  {'YES (Up)' if winner=='yes' else 'NO (Down)'} won[/bold yellow]")
    console.print(f"  BTC open ${floor:,.2f}  →  close ${end_btc:,.2f}  "
                  f"{'↑' if end_btc >= floor else '↓'} {abs((end_btc-floor)/floor*100):.3f}%")
    console.print(f"  YES: ${yes_cost:.2f} cost  {yes_count:.1f} contracts")
    console.print(f"  NO:  ${no_cost:.2f} cost  {no_count:.1f} contracts")
    console.print(f"  Fees: ${fees:.3f}")
    console.print(f"  Gross payout: ${gross:.2f}  Net P&L: [{pnl_col}]{sign}${pnl:.2f}[/{pnl_col}]")
    console.print(f"  Session P&L: [{pnl_col}]{'+' if session_pnl>=0 else ''}${session_pnl:.2f}[/{pnl_col}]")
    console.print()
    _save()


# ── Main trading loop ──────────────────────────────────────────────────────────

async def _trading_loop(client: httpx.AsyncClient) -> None:
    # Wait for first BTC price
    for _ in range(30):
        if btc_price > 0:
            break
        await asyncio.sleep(0.5)

    while True:
        now = time.time()

        # Settle any expired positions
        for ticker, pos in list(positions.items()):
            if not pos["resolved"] and now >= pos["window_end"]:
                await _settle_window(client, ticker)

        # Purge resolved positions older than 2 windows (keep last 2 for reference)
        resolved = sorted(
            [t for t, p in positions.items() if p["resolved"]],
            key=lambda t: positions[t]["window_end"],
        )
        for old in resolved[:-2]:
            del positions[old]

        for offset in [0, 1]:
            ts     = _window_ts(offset)
            w      = _window_start(offset)
            ticker = _ticker_for(w)
            if ticker not in positions:
                await _open_window(client, w, ts)
            elif not positions[ticker]["resolved"]:
                await _check_triggers(client, ticker)

        # Wake immediately on next WS price tick (max 5s wait as safety)
        try:
            await asyncio.wait_for(_price_updated.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            pass
        _price_updated.clear()


# ── Status ─────────────────────────────────────────────────────────────────────

def _side_label(price: float) -> str:
    if price >= 0.90: return "[bold green]WINNING[/bold green]"
    if price >= 0.55: return "[green]leading[/green]"
    if price >= 0.45: return "[dim]even[/dim]"
    return "[red]trailing[/red]"


async def _status_loop() -> None:
    while True:
        await asyncio.sleep(1)
        if not btc_price:
            continue
        now  = datetime.now().strftime("%H:%M:%S")
        pdir = "↑" if btc_price >= btc_prev else "↓"
        col  = "green" if session_pnl >= 0 else "red"
        sign = "+" if session_pnl >= 0 else ""
        ws_indicator = f"[green]WS✓{_ws_tick_count}[/green]" if _ws_tick_count > 0 else "[yellow]WS?[/yellow]"
        # Per-source prices for spread visibility
        src_parts = []
        for src in ["coinbase", "bitstamp", "kraken", "gemini"]:
            p = _src_prices.get(src, 0)
            age = time.time() - _src_times.get(src, 0)
            if p > 0 and age < 15:
                src_parts.append(f"{src[:2].upper()}=${p:,.0f}")
        src_str = "  ".join(src_parts) if src_parts else ""

        # Show only the current active window (not expired ones)
        active = [p for p in positions.values()
                  if not p["resolved"] and time.time() < p["window_end"]]
        active.sort(key=lambda p: p["window_start"])

        for pos in active:
            ticker    = pos["ticker"]
            ya, na    = kalshi_prices.get(ticker, (0.0, 0.0))
            elapsed   = int(_elapsed(pos))
            secs_left = max(0, int(pos["window_end"] - time.time()))
            floor     = pos.get("floor_strike", 0)
            cb_open   = candle_open.get(ticker, 0)
            # delta_ks: (CB_now - CB_open) / KS_floor  — matches Kalshi's display formula
            # delta_cb: (CB_now - CB_open) / CB_open   — pure Coinbase-relative
            delta_ks  = (btc_price - cb_open) / floor   * 100 if (floor and cb_open) else \
                        (btc_price - floor)   / floor   * 100 if floor else 0
            delta_cb  = (btc_price - cb_open) / cb_open * 100 if cb_open else delta_ks
            delta     = delta_ks
            emp       = _get_odds(elapsed, delta_ks) if floor else None

            yes_spent  = pos["spent_yes"]
            no_spent   = pos["spent_no"]
            yes_shares = pos.get("shares_yes", 0.0)
            no_shares  = pos.get("shares_no",  0.0)
            yes_avg    = yes_spent / yes_shares if yes_shares > 0 else 0.0
            no_avg     = no_spent  / no_shares  if no_shares  > 0 else 0.0
            yes_payout = yes_shares * 1.00
            no_payout  = no_shares  * 1.00

            if ya >= 0.90:
                lead = "[green]YES winning[/green]"
            elif na >= 0.90:
                lead = "[red]NO winning[/red]"
            else:
                lead = "[dim]open[/dim]"

            mod_str    = f"model={emp['p_up']:.0%}↑/{emp['p_down']:.0%}↓" if emp else ""
            kalshi_str = f"kalshi={ya:.2f}YES/{na:.2f}NO" if ya > 0 else ""

            # Composite price + per-source spread
            implied = _implied_btc(elapsed, ya, floor) if ya > 0 and floor > 0 else None
            if implied:
                gap      = implied - btc_price
                gap_str  = f"[yellow]gap={gap:+.0f}[/yellow]" if abs(gap) > 50 else f"[dim]gap={gap:+.0f}[/dim]"
                price_str = f"comp=${btc_price:,.0f}  KL≈${implied:,.0f}  {gap_str}  [{src_str}]"
            else:
                price_str = f"comp=${btc_price:,.0f}  [{src_str}]"

            if cb_open:
                floor_str  = f"KS=${floor:,.0f}  CB_open=${cb_open:,.0f}"
                delta_str  = f"dKS={delta_ks:+.3f}%  dCB={delta_cb:+.3f}%"
                if abs(delta_ks - delta_cb) > 0.05:
                    delta_str = f"[yellow]{delta_str}[/yellow]"
            else:
                floor_str = f"KS=${floor:,.0f}"
                delta_str = f"dKS={delta_ks:+.3f}%"

            # Header line
            console.print(
                f"[dim]{now}[/dim]  {price_str}  "
                f"{floor_str}  {delta_str}  "
                f"t={elapsed}s left={secs_left}s  {lead}  "
                f"{mod_str}  {kalshi_str}  "
                f"P&L[{col}]{sign}${session_pnl:.2f}[/{col}]  {ws_indicator}"
            )

            # Position lines only if we have shares
            if yes_shares > 0:
                console.print(
                    f"  YES {yes_shares:.0f}sh avg${yes_avg:.3f} cost=${yes_spent:.2f} "
                    f"payout=${yes_payout:.2f} now={ya:.3f} {_side_label(ya)}"
                )
            if no_shares > 0:
                console.print(
                    f"  NO  {no_shares:.0f}sh avg${no_avg:.3f} cost=${no_spent:.2f} "
                    f"payout=${no_payout:.2f} now={na:.3f} {_side_label(na)}"
                )

        if not active:
            console.print(
                f"[dim]{now}[/dim]  composite=${btc_price:,.0f}{pdir}  [{src_str}]  no open position  "
                f"P&L[{col}]{sign}${session_pnl:.2f}[/{col}]  {ws_indicator}"
            )


# ── Entry point ────────────────────────────────────────────────────────────────

async def main() -> None:
    console.print("[bold cyan]BTC 15-min LIVE Kalshi Trader[/bold cyan]")
    console.print(f"  Limits   : ${MAX_PER_WINDOW}/window  ${MAX_PER_SIDE}/side")
    console.print(f"  Probe    : ${PROBE_STAKE}/trade for first {PROBE_ELAPSED}s")
    console.print(f"  Edge     : ≥{ODDS_THRESHOLD:.0%} model vs Kalshi  (base ${ODDS_STAKE}, up to 4×)")
    console.print(f"  Conviction: model≥{CONVICTION_MODEL:.0%} edge≥{CONVICTION_EDGE:.0%} at {CONVICTION_ELAPSED}s+")
    console.print(f"  State    : {STATE_FILE}")
    console.print()

    _load_surface()
    _load()

    async with httpx.AsyncClient() as client:
        # Check balance
        r = await client.get(
            f"{KALSHI_BASE}/portfolio/balance",
            headers=_kalshi_headers("GET", "/trade-api/v2/portfolio/balance"),
            timeout=8,
        )
        if r.status_code == 200:
            bal = r.json().get("balance", 0) / 100
            console.print(f"  Kalshi balance: [bold green]${bal:.2f}[/bold green]")
        console.print()

        await asyncio.gather(
            _feed_coinbase(),
            _feed_bitstamp(),
            _feed_kraken(),
            _feed_gemini(),
            _kalshi_ws_feed(),
            _poll_kalshi(client),
            _trading_loop(client),
            _status_loop(),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        col  = "green" if session_pnl >= 0 else "red"
        sign = "+" if session_pnl >= 0 else ""
        console.print(f"\n[dim]Stopped.  Session P&L: [{col}]{sign}${session_pnl:.2f}[/{col}][/dim]")
