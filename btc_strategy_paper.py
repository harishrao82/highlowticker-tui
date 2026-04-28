#!/usr/bin/env python3
"""
BTC/ETH/SOL/XRP 15-min Up-or-Down — paper strategy trader.

Reverse-engineered from Idolized-Scallops bot behaviour:

  Phase 1 (0–90s)  : Open a position in either direction (configurable or random).
  Phase 2 (90–600s): Watch real coin price vs window open price.
                     If losing → buy the winning side cheap while it's still depressed.
                     If winning → add conviction to the winning side.
  Phase 3 (600–870s): Final conviction load on the confirmed winner.
  Close              : Window expires, one side pays $1/share.

Real price feed: Coinbase Advanced Trade WebSocket (no API key needed).
Polymarket prices: CLOB best-ask via REST (polled every 10s per window).

Run: python3 btc_strategy_paper.py
"""
import asyncio
import json
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import httpx
import websockets
from rich.console import Console
from rich.text import Text
from rich.table import Table

# ── BTC probability surface (built by btc_model_builder.py) ───────────────────
_SURFACE_FILE = Path.home() / ".btc_model_surface.json"
_surface: dict = {}

def _load_surface() -> None:
    global _surface
    if _SURFACE_FILE.exists():
        _surface = json.loads(_SURFACE_FILE.read_text())
        meta = _surface["meta"]
        console_early = Console()
        console_early.print(f"[dim]Surface loaded: {meta['n_windows']:,} windows[/dim]")

def _get_odds(time_seconds: float, delta_pct: float) -> dict | None:
    """O(1) lookup — returns {p_up, p_down, n_eff, confidence} or None."""
    if not _surface:
        return None
    t_step = _surface["meta"]["t_step"]
    d_step = _surface["meta"]["d_step"]
    d_min  = _surface["meta"]["d_min"]
    t_vals = _surface["t_vals"]
    d_vals = _surface["d_vals"]
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
from rich import box

# ── Config ─────────────────────────────────────────────────────────────────────
COINS = {
    "BTC": {"poly_slug": "btc-updown-15m", "cb_id": "BTC-USD"},
    "ETH": {"poly_slug": "eth-updown-15m", "cb_id": "ETH-USD"},
    "SOL": {"poly_slug": "sol-updown-15m", "cb_id": "SOL-USD"},
    "XRP": {"poly_slug": "xrp-updown-15m", "cb_id": "XRP-USD"},
}

GAMMA_API     = "https://gamma-api.polymarket.com/markets"
TRADES_API    = "https://data-api.polymarket.com/trades"
CLOB_API      = "https://clob.polymarket.com"
CB_WS         = "wss://advanced-trade-ws.coinbase.com"
CLOB_WS       = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
KALSHI_API    = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_POLL   = 2   # seconds between Kalshi REST polls

SHADOW_WALLET = "0xe1d6b51521bd4365769199f392f9818661bd907c"  # Idolized-Scallops
# Pad to 32-byte topic format for Polygon log subscription
_SHADOW_TOPIC  = "0x000000000000000000000000" + SHADOW_WALLET[2:]
POLYGON_WS     = "wss://rpc-mainnet.matic.quiknode.pro"   # free public Polygon RPC

STATE_FILE    = Path.home() / ".btc_strategy_state.json"   # persists across restarts
IMPROVE_FILE  = Path.home() / ".btc_strategy_improvements.json"  # improvement log

# ── Trigger thresholds (reverse-engineered from Idolized-Scallops) ────────────
ARB_THRESHOLD   = 0.94   # fire both sides when ask_up + ask_dn < this
MAX_PER_WINDOW  = 700    # max total $ spent per window
MAX_PER_SIDE    = 450    # max $ on any single side

# Stakes per trigger
ARB_STAKE      = 100.0   # per side on arb trigger (buy both = $200 total)
ODDS_THRESHOLD = 0.07    # CLOB must underprice vs model by ≥7% to fire
ODDS_STAKE     = 75.0    # base stake — scales with edge strength
ODDS_COOLDOWN  = 60      # seconds between odds-based buys on same side

# Conviction burst — go big when model is very confident late in window
CONVICTION_MODEL   = 0.70   # model must say ≥70%
CONVICTION_EDGE    = 0.15   # CLOB must be ≥15% underpriced
CONVICTION_ELAPSED = 300    # at least 5 min into window

# Cheap insurance — buy near-free loser side after conviction
INSURANCE_PRICE  = 0.20   # buy other side if priced below this
INSURANCE_STAKE  = 15.0   # small fixed amount

# Probe phase — small stakes for first 90s while direction is uncertain
PROBE_ELAPSED = 90     # seconds
PROBE_STAKE   = 25.0   # max stake per trade during probe phase

# Cooldowns (seconds)
ARB_COOLDOWN   = 20

console = Console()

# ── Shared state ───────────────────────────────────────────────────────────────
live_prices: dict[str, float] = {}   # coin → latest price
window_open: dict[str, float] = {}   # slug  → price at window start
positions:   dict[str, dict]  = {}   # slug  → position dict
session_pnl  = 0.0
trade_count  = 0

# Shadow tracker state
shadow: dict[str, dict] = {}         # slug → {trades, spent_up, spent_down, shares_up, shares_down, pnl, resolved}
shadow_pnl   = 0.0

# CLOB WebSocket state — updated in real-time, sub-second (ETH/SOL/XRP only)
clob_prices: dict[str, tuple[float, float]] = {}  # slug → (ask_up, ask_dn)
_token_to_info: dict[str, tuple[str, str]]  = {}  # token_id → (slug, "Up"/"Down")
_subscribed_tokens: set[str] = set()

# Kalshi prices for BTC — polled every 2s
kalshi_prices: dict[str, tuple[float, float]] = {}  # kalshi_ticker → (yes_ask, no_ask)
kalshi_floor:  dict[str, float] = {}                # kalshi_ticker → floor_strike

# Fields NOT safe to serialize (httpx objects, market dicts with non-JSON types)
_SKIP_FIELDS = {"market"}


# ── Persistence ────────────────────────────────────────────────────────────────

def _save_state() -> None:
    """Persist positions + shadow to disk."""
    try:
        def _clean(d: dict) -> dict:
            return {k: v for k, v in d.items() if k not in _SKIP_FIELDS}

        state = {
            "session_pnl": session_pnl,
            "shadow_pnl":  shadow_pnl,
            "trade_count": trade_count,
            "positions":   {slug: _clean(p) for slug, p in positions.items()},
            "shadow":      {slug: {k: v for k, v in s.items() if k != "seen_txs"}
                            for slug, s in shadow.items()},
            "saved_at":    datetime.now().isoformat(),
        }
        STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        console.print(f"[yellow]State save error: {e}[/yellow]")


def _load_state() -> None:
    """Restore positions + shadow from disk on startup."""
    global session_pnl, shadow_pnl, trade_count
    if not STATE_FILE.exists():
        return
    try:
        state = json.loads(STATE_FILE.read_text())
        session_pnl = state.get("session_pnl", 0.0)
        shadow_pnl  = state.get("shadow_pnl",  0.0)
        trade_count = state.get("trade_count",  0)

        for slug, p in state.get("positions", {}).items():
            if p.get("resolved"):
                positions[slug] = p
            elif time.time() < p.get("window_end", 0) + 7200:
                p["market"] = None   # will be re-fetched in _tick_window
                p.setdefault("trades", [])
                positions[slug] = p

        for slug, s in state.get("shadow", {}).items():
            s["seen_txs"] = set()   # re-dedupe on next poll
            shadow[slug]  = s

        saved = state.get("saved_at", "?")
        console.print(f"[dim]Restored state from {saved}  "
                      f"({len(positions)} positions  realized P&L ${session_pnl:.2f})[/dim]")
    except Exception as e:
        console.print(f"[yellow]State load error: {e}[/yellow]")


# ── Improvement log ───────────────────────────────────────────────────────────

def _log_improvement(slug: str, our_pnl: float, bot_pnl: float,
                     our_pos: dict, bot_pos: dict | None) -> None:
    """Record each resolved window for strategy analysis."""
    try:
        log = json.loads(IMPROVE_FILE.read_text()) if IMPROVE_FILE.exists() else []
        our_up_avg = our_pos["spent_up"]  / our_pos["shares_up"]   if our_pos["shares_up"]   else 0
        our_dn_avg = our_pos["spent_down"]/ our_pos["shares_down"]  if our_pos["shares_down"] else 0
        entry = {
            "slug":         slug,
            "coin":         our_pos["coin"],
            "title":        our_pos["title"],
            "winner":       our_pos["winner"],
            "open_price":   our_pos.get("open_price", 0),
            "end_price":    our_pos.get("end_price", 0),
            "our_pnl":      round(our_pnl, 2),
            "bot_pnl":      round(bot_pnl, 2) if bot_pos else None,
            "our_up_avg":   round(our_up_avg, 3),
            "our_dn_avg":   round(our_dn_avg, 3),
            "our_shares_up":   our_pos["shares_up"],
            "our_shares_dn":   our_pos["shares_down"],
            "bot_shares_up":   bot_pos["shares_up"]   if bot_pos else None,
            "bot_shares_dn":   bot_pos["shares_down"]  if bot_pos else None,
            "bot_up_avg":   round(bot_pos["spent_up"]  / bot_pos["shares_up"],  3) if bot_pos and bot_pos["shares_up"]  else None,
            "bot_dn_avg":   round(bot_pos["spent_down"]/ bot_pos["shares_down"], 3) if bot_pos and bot_pos["shares_down"] else None,
            "phase1_side":  our_pos.get("phase1_side", "?"),
            "our_trades":   len(our_pos.get("trades", [])),
            "bot_trades":   len(bot_pos.get("trades", [])) if bot_pos else None,
            "timestamp":    datetime.now().isoformat(),
        }
        log.append(entry)
        IMPROVE_FILE.write_text(json.dumps(log, indent=2))
    except Exception as e:
        console.print(f"[yellow]Improvement log error: {e}[/yellow]")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _window_ts(coin: str, offset: int = 0) -> int:
    now  = int(time.time())
    base = (now // 900) * 900
    return base + offset * 900


def _slug(coin: str, ts: int) -> str:
    return f"{COINS[coin]['poly_slug']}-{ts}"


async def _fetch_market(client: httpx.AsyncClient, slug: str) -> dict | None:
    try:
        r = await client.get(GAMMA_API, params={"slug": slug}, timeout=8)
        r.raise_for_status()
        d = r.json()
        return d[0] if d else None
    except Exception:
        return None


async def _clob_asks_rest(client: httpx.AsyncClient, market: dict) -> tuple[float, float] | None:
    """(ask_up, ask_down) from CLOB REST — used only for initial window fetch."""
    try:
        tokens   = json.loads(market.get("clobTokenIds", "[]"))
        outcomes = json.loads(market.get("outcomes", "[]"))
        up_idx   = next((i for i, o in enumerate(outcomes) if str(o).lower() == "up"), 0)
        dn_idx   = 1 - up_idx

        async def _ask(token):
            r = await client.get(f"{CLOB_API}/price",
                                 params={"token_id": token, "side": "buy"}, timeout=6)
            return float(r.json()["price"])

        up_ask, dn_ask = await asyncio.gather(_ask(tokens[up_idx]), _ask(tokens[dn_idx]))
        return up_ask, dn_ask
    except Exception:
        return None


def _register_market_tokens(slug: str, market: dict) -> list[str]:
    """Register token IDs for a market so the CLOB WS feed can subscribe to them."""
    try:
        tokens   = json.loads(market.get("clobTokenIds", "[]"))
        outcomes = json.loads(market.get("outcomes", "[]"))
        up_idx   = next((i for i, o in enumerate(outcomes) if str(o).lower() == "up"), 0)
        dn_idx   = 1 - up_idx
        _token_to_info[tokens[up_idx]] = (slug, "Up")
        _token_to_info[tokens[dn_idx]] = (slug, "Down")
        return tokens
    except Exception:
        return []


def _parse_end(market: dict) -> float:
    try:
        end = market.get("endDate", "")
        dt  = datetime.fromisoformat(end.replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        return time.time() + 900


def _elapsed(pos: dict) -> float:
    return time.time() - pos["window_start"]


def _direction(pos: dict, coin: str) -> str:
    """Current BTC direction vs window open price."""
    current = live_prices.get(coin, 0)
    open_p  = pos.get("open_price", current)
    if current == 0 or open_p == 0:
        return "FLAT"
    return "Up" if current >= open_p else "Down"


def _check_triggers(slug: str) -> None:
    """Fire ARB or CHEAP buys whenever CLOB prices meet thresholds."""
    if slug not in positions:
        return
    pos = positions[slug]
    if pos["resolved"] or _elapsed(pos) < 0:
        return

    ask_up = pos.get("ask_up", 0.5)
    ask_dn = pos.get("ask_dn", 0.5)
    if ask_up <= 0 or ask_dn <= 0:
        return

    now      = time.time()
    cd       = pos.setdefault("cooldowns", {})
    spent    = pos["spent_up"] + pos["spent_down"]
    remaining = MAX_PER_WINDOW - spent
    if remaining < 10:
        return

    # ── ARB: both sides sum < threshold → guaranteed edge ─────────────────────
    if ask_up + ask_dn < ARB_THRESHOLD:
        if now - cd.get("arb", 0) > ARB_COOLDOWN:
            stake = min(ARB_STAKE, remaining / 2)
            if stake >= 10:
                _paper_buy(pos, "Up",   ask_up, stake, f"arb-up@{ask_up:.2f}+dn@{ask_dn:.2f}={ask_up+ask_dn:.2f}")
                _paper_buy(pos, "Down", ask_dn, stake, f"arb-dn@{ask_up:.2f}+dn@{ask_dn:.2f}={ask_up+ask_dn:.2f}")
                cd["arb"] = now

    # ── ODDS: model says CLOB is significantly underpricing a side (BTC only) ──
    coin = pos.get("coin", "")
    if coin == "BTC" and remaining >= 10:
        elapsed   = _elapsed(pos)
        open_p    = pos.get("open_price", 0)
        current_p = live_prices.get("BTC", 0)
        if elapsed > 30 and open_p > 0 and current_p > 0:
            delta = (current_p - open_p) / open_p * 100
            emp   = _get_odds(elapsed, delta)
            if emp and emp["confidence"] in ("high", "med"):
                edge_up = emp["p_up"]  - ask_up
                edge_dn = emp["p_down"] - ask_dn

                # ── Regular ODDS buys (probe-capped early, scaled by edge later) ─
                in_probe = elapsed < PROBE_ELAPSED
                if edge_up > ODDS_THRESHOLD and emp["p_up"] > 0.50:
                    if now - cd.get("odds_up", 0) > ODDS_COOLDOWN:
                        stake = (min(PROBE_STAKE, remaining) if in_probe
                                 else _edge_stake(edge_up, remaining, pos["spent_up"]))
                        if stake >= 10:
                            tag = "probe" if in_probe else "odds"
                            _paper_buy(pos, "Up", ask_up, stake,
                                       f"{tag}-up@{ask_up:.2f} model={emp['p_up']:.1%} "
                                       f"edge=+{edge_up:.2f} n={emp['n_eff']}")
                            cd["odds_up"] = now
                if edge_dn > ODDS_THRESHOLD and emp["p_down"] > 0.50:
                    if now - cd.get("odds_dn", 0) > ODDS_COOLDOWN:
                        stake = (min(PROBE_STAKE, remaining) if in_probe
                                 else _edge_stake(edge_dn, remaining, pos["spent_down"]))
                        if stake >= 10:
                            tag = "probe" if in_probe else "odds"
                            _paper_buy(pos, "Down", ask_dn, stake,
                                       f"{tag}-dn@{ask_dn:.2f} model={emp['p_down']:.1%} "
                                       f"edge=+{edge_dn:.2f} n={emp['n_eff']}")
                            cd["odds_dn"] = now

                # ── Conviction burst — dump remaining capital when very confident ──
                if elapsed > CONVICTION_ELAPSED:
                    if emp["p_up"] > CONVICTION_MODEL and edge_up > CONVICTION_EDGE:
                        if now - cd.get("conviction_up", 0) > 5:
                            stake = min(remaining, MAX_PER_SIDE - pos["spent_up"])
                            if stake >= 10:
                                _paper_buy(pos, "Up", ask_up, stake,
                                           f"conviction@{ask_up:.2f} model={emp['p_up']:.1%} "
                                           f"edge=+{edge_up:.2f} elapsed={int(elapsed)}s")
                                cd["conviction_up"] = now
                                pos["conviction_fired"] = True
                    elif emp["p_down"] > CONVICTION_MODEL and edge_dn > CONVICTION_EDGE:
                        if now - cd.get("conviction_dn", 0) > 5:
                            stake = min(remaining, MAX_PER_SIDE - pos["spent_down"])
                            if stake >= 10:
                                _paper_buy(pos, "Down", ask_dn, stake,
                                           f"conviction@{ask_dn:.2f} model={emp['p_down']:.1%} "
                                           f"edge=+{edge_dn:.2f} elapsed={int(elapsed)}s")
                                cd["conviction_dn"] = now
                                pos["conviction_fired"] = True

                # ── Cheap insurance — near-free other side after conviction ────
                if pos.get("conviction_fired") and remaining >= INSURANCE_STAKE:
                    loser_side  = "Down" if pos["spent_up"] >= pos["spent_down"] else "Up"
                    loser_price = ask_dn  if loser_side == "Down" else ask_up
                    if 0.05 < loser_price < INSURANCE_PRICE:
                        if now - cd.get("insurance", 0) > 30:
                            stake = min(INSURANCE_STAKE, remaining)
                            if stake >= 5:
                                _paper_buy(pos, loser_side, loser_price, stake,
                                           f"insurance@{loser_price:.2f}")
                                cd["insurance"] = now


# ── Stake sizing ──────────────────────────────────────────────────────────────

def _edge_stake(edge: float, remaining: float, side_spent: float) -> float:
    """Scale stake by edge strength. 7% edge → base $75. 28%+ edge → 4× = $300."""
    scale = min(edge / ODDS_THRESHOLD, 4.0)
    return min(ODDS_STAKE * scale, remaining, MAX_PER_SIDE - side_spent)


# ── Paper trade execution ──────────────────────────────────────────────────────

def _paper_buy(pos: dict, side: str, price: float, stake: float, reason: str) -> None:
    global trade_count
    if price <= 0 or price >= 1:
        return
    shares = stake / price
    pos["trades"].append({
        "side": side, "price": price, "shares": shares, "cost": stake,
        "reason": reason, "time": datetime.now().strftime("%H:%M:%S")
    })
    pos["spent_up"]   += stake if side == "Up"   else 0
    pos["spent_down"] += stake if side == "Down"  else 0
    pos["shares_up"]  += shares if side == "Up"  else 0
    pos["shares_down"]+= shares if side == "Down" else 0
    trade_count += 1
    _save_state()

    now = datetime.now().strftime("%H:%M:%S")
    t = Text()
    t.append(f"  [{now}] ", "dim")
    t.append(f"PAPER BUY ", "bold")
    t.append(f"{side:<4} ", "bold green" if side == "Up" else "bold red")
    t.append(f"{shares:.1f} shares @ {price:.3f}  ")
    t.append(f"${stake:.0f}  ", "bold")
    t.append(f"[{reason}]", "dim")
    console.print(t)


# ── Polygon WebSocket — instant bot-trade detection ───────────────────────────

_shadow_trigger = asyncio.Event()   # set when a bot tx lands on-chain


async def _polygon_ws_feed() -> None:
    """Subscribe to Polygon logs involving SHADOW_WALLET.
    Fires _shadow_trigger immediately when any on-chain activity is detected,
    so _shadow_poll runs within ~2s of a bot trade confirming."""
    seen_txs: set[str] = set()
    while True:
        try:
            async with websockets.connect(POLYGON_WS, ping_interval=20,
                                          open_timeout=10) as ws:
                sub = json.dumps({
                    "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
                    "params": ["logs", {"topics": [None, _SHADOW_TOPIC]}],
                })
                await ws.send(sub)
                resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
                sub_id = resp.get("result")
                console.print(f"[dim]Polygon WS connected — watching bot wallet (sub {sub_id})[/dim]")

                async for raw in ws:
                    msg  = json.loads(raw)
                    log  = msg.get("params", {}).get("result")
                    if not log:
                        continue
                    tx = log.get("transactionHash", "")
                    if tx and tx not in seen_txs:
                        seen_txs.add(tx)
                        if len(seen_txs) > 500:
                            seen_txs.clear()   # prevent unbounded growth
                        console.print(f"[cyan dim]  ◆ Bot tx on-chain: {tx[:20]}…[/cyan dim]")
                        _shadow_trigger.set()

        except Exception as e:
            console.print(f"[yellow]Polygon WS error: {e} — reconnecting in 5s[/yellow]")
            await asyncio.sleep(5)


async def _shadow_watch(client: httpx.AsyncClient) -> None:
    """On Polygon WS trigger, retry polling until the data API indexes the trade."""
    while True:
        await _shadow_trigger.wait()
        _shadow_trigger.clear()
        console.print("  [cyan dim]◆ Polygon trigger — polling data API (retry up to 2 min)[/cyan dim]")
        # Retry every 10s for up to 2 min — data API indexing lag is variable
        before = sum(len(s.get("trades", [])) for s in shadow.values())
        for attempt in range(12):
            await asyncio.sleep(10)
            await _shadow_poll(client)
            after = sum(len(s.get("trades", [])) for s in shadow.values())
            if after > before:
                console.print(f"  [cyan]◆ Bot trade indexed after {(attempt+1)*10}s[/cyan]")
                break


# ── Shadow tracker ────────────────────────────────────────────────────────────

async def _shadow_fetch(client: httpx.AsyncClient) -> list[dict]:
    """Fetch recent trades for the shadow wallet."""
    try:
        r = await client.get(TRADES_API,
                             params={"user": SHADOW_WALLET, "limit": 200}, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []


async def _shadow_poll(client: httpx.AsyncClient) -> None:
    """Update shadow positions with any new trades from the real bot."""
    global shadow_pnl
    raw = await _shadow_fetch(client)
    console.print(f"  [dim]shadow poll: {len(raw)} trades from API  "
                  f"tracking {len([p for p in positions if not positions[p].get('resolved')])} open windows[/dim]")

    for t in raw:
        slug = t.get("slug", "")
        if not slug or t.get("side") != "BUY":
            continue
        # Only track windows we're also trading
        if slug not in positions:
            continue

        if slug not in shadow:
            shadow[slug] = {
                "trades": [], "seen_txs": set(),
                "spent_up": 0.0, "spent_down": 0.0,
                "shares_up": 0.0, "shares_down": 0.0,
                "resolved": False, "winner": "", "pnl": 0.0,
            }

        tx = t.get("transactionHash", "")
        if tx in shadow[slug]["seen_txs"]:
            continue
        shadow[slug]["seen_txs"].add(tx)

        price      = float(t.get("price") or 0)
        size       = float(t.get("size")  or 0)
        spent      = price * size
        side       = t.get("outcome", "").strip()
        ts_str     = datetime.fromtimestamp(float(t.get("timestamp", 0))).strftime("%H:%M:%S")
        fetched_at = datetime.now().strftime("%H:%M:%S")

        shadow[slug]["trades"].append({
            "side": side, "price": price, "shares": size, "cost": spent,
            "time": ts_str, "fetched_at": fetched_at,
        })
        if side == "Up":
            shadow[slug]["spent_up"]  += spent
            shadow[slug]["shares_up"] += size
        elif side == "Down":
            shadow[slug]["spent_down"]  += spent
            shadow[slug]["shares_down"] += size

        lag = (datetime.now() - datetime.strptime(ts_str, "%H:%M:%S").replace(
            year=datetime.now().year, month=datetime.now().month, day=datetime.now().day
        )).total_seconds()
        console.print(
            f"  [cyan]◆ BOT[/cyan] [{slug[-6:]}]  "
            f"[{'green' if side=='Up' else 'red'}]{side:<4}[/{'green' if side=='Up' else 'red'}]"
            f"  @{price:.3f}  ${spent:.0f}  "
            f"[dim]trade={ts_str}  fetched={fetched_at}  lag={lag:.0f}s[/dim]"
        )


async def _shadow_resolve(slug: str, winner: str) -> None:
    """Compute shadow P&L when a window closes."""
    global shadow_pnl
    if slug not in shadow or shadow[slug]["resolved"]:
        return
    s = shadow[slug]
    gross = s["shares_up"] if winner == "Up" else s["shares_down"] if winner == "Down" else \
            (s["shares_up"] + s["shares_down"]) * 0.5
    spent = s["spent_up"] + s["spent_down"]
    pnl   = gross - spent
    s["resolved"] = True
    s["winner"]   = winner
    s["pnl"]      = pnl
    shadow_pnl   += pnl


def _print_comparison(slug: str, our_pos: dict) -> None:
    """Print side-by-side comparison of our paper trade vs shadow bot."""
    s     = shadow.get(slug)
    coin  = our_pos["coin"]
    title = our_pos["title"][our_pos["title"].find("April")+6:].strip()
    winner = our_pos["winner"]

    our_spent  = our_pos["spent_up"]  + our_pos["spent_down"]
    our_pnl    = our_pos["pnl"]
    shad_spent = (s["spent_up"] + s["spent_down"]) if s else 0
    shad_pnl   = s["pnl"] if s else 0

    our_color  = "green" if our_pnl  >= 0 else "red"
    shad_color = "green" if shad_pnl >= 0 else "red"

    open_p = our_pos.get("open_price", 0)
    end_p  = our_pos.get("end_price", live_prices.get(coin, 0))
    moved  = "↑" if end_p >= open_p else "↓"
    pct    = ((end_p - open_p) / open_p * 100) if open_p else 0

    console.rule(f"[bold yellow]★ COMPARISON  {coin}  {title}  →  {winner} won[/bold yellow]")
    console.print(
        f"  {coin} open ${open_p:,.2f}  →  close ${end_p:,.2f}  "
        f"{moved} {abs(pct):.3f}%  ({'matched' if (winner=='Up')==(end_p>=open_p) else 'mismatch — Chainlink differs from Coinbase'})"
    )
    console.print()

    # Per-side avg prices
    our_up_avg  = our_pos["spent_up"]  / our_pos["shares_up"]   if our_pos["shares_up"]   else 0
    our_dn_avg  = our_pos["spent_down"]/ our_pos["shares_down"]  if our_pos["shares_down"] else 0

    tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold cyan", padding=(0,1))
    tbl.add_column("",          width=14)
    tbl.add_column("UpSh",      width=7)
    tbl.add_column("AvgUp",     width=7)
    tbl.add_column("DnSh",      width=7)
    tbl.add_column("AvgDn",     width=7)
    tbl.add_column("Spent",     width=7)
    tbl.add_column("Payout",    width=8)
    tbl.add_column("Net P&L",   width=10)

    def _row(label, pos_d, color):
        if pos_d is None:
            return
        su = pos_d["spent_up"]; sd = pos_d["spent_down"]
        shu = pos_d["shares_up"]; shd = pos_d["shares_down"]
        avg_u = su / shu if shu else 0
        avg_d = sd / shd if shd else 0
        gross = shu if winner=="Up" else shd if winner=="Down" else (shu+shd)*0.5
        pnl   = pos_d["pnl"]
        sign  = "+" if pnl >= 0 else ""
        tbl.add_row(
            label,
            f"{shu:.0f}", f"{avg_u:.3f}" if avg_u else "-",
            f"{shd:.0f}", f"{avg_d:.3f}" if avg_d else "-",
            f"${su+sd:.0f}",
            f"${gross:.0f}",
            Text(f"{sign}${pnl:.2f}", style=f"bold {color}"),
        )

    _row("★ Our Paper", our_pos, our_color)
    if s:
        _row("◆ Idolized-S", s, shad_color)

    console.print(tbl)

    sc = "green" if shadow_pnl >= 0 else "red"
    oc = "green" if session_pnl >= 0 else "red"
    console.print(f"  Running totals : [bold {oc}]Us {'+' if session_pnl>=0 else ''}${session_pnl:.2f}[/bold {oc}]  "
                  f"vs  [bold {sc}]Bot {'+' if shadow_pnl>=0 else ''}${shadow_pnl:.2f}[/bold {sc}]")
    console.print()
    _log_improvement(slug, our_pnl, shad_pnl if s else 0, our_pos, s)
    _save_state()


# ── Kalshi price feed (BTC only) ───────────────────────────────────────────────

_KALSHI_ET = __import__("datetime").timezone(__import__("datetime").timedelta(hours=-4))

def _kalshi_ticker(ts: int) -> str:
    """KXBTC15M-26APR051830-30 — uses window CLOSE time + close-minute suffix."""
    from datetime import datetime as _dt, timedelta as _td
    w     = _dt.fromtimestamp(ts, tz=_KALSHI_ET)
    close = w + _td(minutes=15)
    return "KXBTC15M-" + close.strftime("%y%b%d%H%M").upper() + "-" + close.strftime("%M")


async def _kalshi_poll(client: httpx.AsyncClient) -> None:
    """Poll Kalshi every 2s for BTC 15-min yes/no ask prices."""
    while True:
        for offset in [0, 1]:
            ts     = _window_ts("BTC", offset)
            ktick  = _kalshi_ticker(ts)
            slug   = _slug("BTC", ts)
            try:
                r = await client.get(f"{KALSHI_API}/markets/{ktick}", timeout=5)
                if r.status_code == 200:
                    m  = r.json()["market"]
                    ya = float(m.get("yes_ask_dollars") or 0)
                    na = float(m.get("no_ask_dollars")  or 0)
                    fs = m.get("floor_strike")
                    if ya > 0 and na > 0:
                        kalshi_prices[ktick] = (ya, na)
                        # Keep position prices in sync + fire triggers
                        if slug in positions and not positions[slug]["resolved"]:
                            positions[slug]["ask_up"] = ya
                            positions[slug]["ask_dn"] = na
                            _check_triggers(slug)
                    if fs:
                        kalshi_floor[ktick] = float(fs)
            except Exception:
                pass
        await asyncio.sleep(KALSHI_POLL)


# ── Window lifecycle ───────────────────────────────────────────────────────────

async def _open_window(client: httpx.AsyncClient, coin: str, ts: int) -> None:
    slug = _slug(coin, ts)
    if slug in positions:
        return

    # ── BTC: use Kalshi prices ─────────────────────────────────────────────────
    if coin == "BTC":
        ktick = _kalshi_ticker(ts)
        try:
            r = await client.get(f"{KALSHI_API}/markets/{ktick}", timeout=5)
            if r.status_code != 200:
                return
            m  = r.json()["market"]
            if m.get("status") not in ("active", "open"):
                return
            ya = float(m.get("yes_ask_dollars") or 0)
            na = float(m.get("no_ask_dollars")  or 0)
            fs = m.get("floor_strike")
            if ya <= 0 or na <= 0:
                return
            kalshi_prices[ktick] = (ya, na)
            open_price = float(fs) if fs else live_prices.get("BTC", 0)
            if fs:
                kalshi_floor[ktick] = open_price
        except Exception:
            return

        window_end = float(ts) + 900
        title      = f"BTC Up or Down — Kalshi {ktick}"
        pos = {
            "coin":         "BTC",
            "slug":         slug,
            "title":        title,
            "window_start": float(ts),
            "window_end":   window_end,
            "open_price":   open_price,
            "market":       None,
            "kalshi_ticker": ktick,
            "ask_up":       ya,
            "ask_dn":       na,
            "spent_up":     0.0,
            "spent_down":   0.0,
            "shares_up":    0.0,
            "shares_down":  0.0,
            "trades":       [],
            "cooldowns":    {},
            "resolved":     False,
            "winner":       "",
            "pnl":          0.0,
        }
        positions[slug] = pos
        from datetime import datetime as _dt
        window_label = _dt.fromtimestamp(ts).strftime("%I:%M %p")
        console.rule(f"[bold cyan]NEW WINDOW  BTC  {window_label} (Kalshi)[/bold cyan]")
        console.print(f"  Floor strike : ${open_price:,.2f}")
        console.print(f"  Kalshi asks  : YES={ya:.3f}  NO={na:.3f}  Sum={ya+na:.3f}  ({ktick})")
        console.print()
        return

    # ── ETH/SOL/XRP: use Polymarket CLOB ──────────────────────────────────────
    market = await _fetch_market(client, slug)
    if not market or market.get("closed"):
        return

    asks = await _clob_asks_rest(client, market)
    if not asks:
        return
    ask_up, ask_dn = asks

    # Register tokens for real-time CLOB WS feed
    _register_market_tokens(slug, market)

    current_price = live_prices.get(coin, 0)
    window_end    = _parse_end(market)
    title         = market.get("question", slug)

    pos = {
        "coin":         coin,
        "slug":         slug,
        "title":        title,
        "window_start": float(ts),
        "window_end":   window_end,
        "open_price":   current_price,
        "market":       market,
        "ask_up":       ask_up,
        "ask_dn":       ask_dn,
        "spent_up":     0.0,
        "spent_down":   0.0,
        "shares_up":    0.0,
        "shares_down":  0.0,
        "trades":       [],
        "cooldowns":    {},
        "resolved":     False,
        "winner":       "",
        "pnl":          0.0,
    }
    positions[slug] = pos

    console.rule(f"[bold cyan]NEW WINDOW  {coin}  {title[title.find('April')+8:].strip()}[/bold cyan]")
    console.print(f"  Open price : ${current_price:,.2f}")
    console.print(f"  CLOB asks  : Up={ask_up:.3f}  Down={ask_dn:.3f}  Sum={ask_up+ask_dn:.3f}")
    console.print()


async def _tick_window(client: httpx.AsyncClient, slug: str) -> None:
    global session_pnl
    pos  = positions[slug]
    coin = pos["coin"]
    if pos["resolved"]:
        return

    elapsed = _elapsed(pos)
    if elapsed < 0:
        return  # window hasn't started yet — don't trade early

    # Re-fetch market dict if missing (ETH/SOL/XRP only — BTC uses Kalshi poll)
    if coin != "BTC" and not pos.get("market"):
        pos["market"] = await _fetch_market(client, slug)
        if not pos["market"]:
            return  # market not available yet, try next tick
        # Re-register tokens so WS feed picks them up after restart
        _register_market_tokens(slug, pos["market"])

    # ── Price updates ──────────────────────────────────────────────────────────
    if coin == "BTC":
        # Prices come from _kalshi_poll; no REST fallback needed here
        ktick = pos.get("kalshi_ticker") or _kalshi_ticker(int(pos["window_start"]))
        if ktick in kalshi_prices:
            pos["ask_up"], pos["ask_dn"] = kalshi_prices[ktick]
    else:
        # ETH/SOL/XRP: use Polymarket CLOB WS, REST fallback
        if slug in clob_prices:
            pos["ask_up"], pos["ask_dn"] = clob_prices[slug]
        else:
            if not pos.get("market"):
                pos["market"] = await _fetch_market(client, slug)
            if pos.get("market"):
                asks = await _clob_asks_rest(client, pos["market"])
                if asks:
                    pos["ask_up"], pos["ask_dn"] = asks

    # Trigger check as fallback (primary path is CLOB WS / Kalshi poll → _check_triggers)
    _check_triggers(slug)

    # ── Resolve ────────────────────────────────────────────────────────────────
    if time.time() >= pos["window_end"]:
        if coin == "BTC":
            # Kalshi resolution: compare final BTC price to floor_strike
            ktick      = pos.get("kalshi_ticker") or _kalshi_ticker(int(pos["window_start"]))
            floor      = kalshi_floor.get(ktick) or pos.get("open_price", 0)
            end_price  = live_prices.get("BTC", 0)
            # Give it up to 30s after expiry for final price to settle
            if end_price == 0:
                return
            winner = "Up" if end_price >= floor else "Down"
        else:
            market = await _fetch_market(client, slug)
            if not market or not market.get("closed"):
                return
            try:
                prices   = json.loads(market.get("outcomePrices", "[]"))
                outcomes = json.loads(market.get("outcomes", "[]"))
                up_idx   = next((i for i, o in enumerate(outcomes) if str(o).lower() == "up"), 0)
                up_p     = float(prices[up_idx])
                winner   = "Up" if up_p >= 0.99 else "Down" if up_p <= 0.01 else "VOID"
            except Exception:
                winner = "UNKNOWN"

        gross = pos["shares_up"] if winner == "Up" else pos["shares_down"] if winner == "Down" else \
                (pos["shares_up"] + pos["shares_down"]) * 0.5
        spent = pos["spent_up"] + pos["spent_down"]
        pnl   = gross - spent

        pos["resolved"]  = True
        pos["winner"]    = winner
        pos["pnl"]       = pnl
        pos["end_price"] = live_prices.get(coin, 0)
        session_pnl     += pnl

        await _shadow_resolve(slug, winner)
        _print_comparison(slug, pos)


# ── CLOB WebSocket feed ────────────────────────────────────────────────────────

async def _clob_ws_feed() -> None:
    """Subscribe to Polymarket CLOB WebSocket for real-time price updates."""
    while True:
        try:
            async with websockets.connect(CLOB_WS, ping_interval=20) as ws:
                console.print("[dim]CLOB WebSocket connected[/dim]")
                subscribed: set[str] = set()

                async def _subscribe_new():
                    new = set(_token_to_info.keys()) - subscribed
                    if new:
                        await ws.send(json.dumps({"assets_ids": list(new), "type": "market"}))
                        subscribed.update(new)
                        _subscribed_tokens.update(new)

                await _subscribe_new()

                async for raw in ws:
                    # Check for new tokens to subscribe to every message
                    if set(_token_to_info.keys()) - subscribed:
                        await _subscribe_new()

                    msg = json.loads(raw)

                    # Initial snapshot: list of orderbook dicts
                    if isinstance(msg, list):
                        for book in msg:
                            token_id = book.get("asset_id", "")
                            info = _token_to_info.get(token_id)
                            if not info:
                                continue
                            slug, side = info
                            asks = book.get("asks", [])
                            if asks:
                                best_ask = float(asks[0]["price"])
                                up, dn = clob_prices.get(slug, (0.5, 0.5))
                                clob_prices[slug] = (best_ask, dn) if side == "Up" else (up, best_ask)

                    # Price change update
                    elif isinstance(msg, dict) and "price_changes" in msg:
                        for change in msg["price_changes"]:
                            token_id = change.get("asset_id", "")
                            info = _token_to_info.get(token_id)
                            if not info:
                                continue
                            slug, side = info
                            best_ask = float(change.get("best_ask", 0) or 0)
                            if best_ask <= 0:
                                continue
                            up, dn = clob_prices.get(slug, (0.5, 0.5))
                            clob_prices[slug] = (best_ask, dn) if side == "Up" else (up, best_ask)
                            # Keep pos in sync for status display
                            if slug in positions:
                                positions[slug]["ask_up"], positions[slug]["ask_dn"] = clob_prices[slug]
                                # Fire triggers on every real-time price update
                                _check_triggers(slug)

        except Exception as e:
            console.print(f"[yellow]CLOB WS error: {e} — reconnecting in 3s[/yellow]")
            await asyncio.sleep(3)


# ── Price feed ─────────────────────────────────────────────────────────────────

async def _price_feed() -> None:
    cb_ids = [v["cb_id"] for v in COINS.values()]
    # Map product_id back to coin name
    reverse = {v["cb_id"]: k for k, v in COINS.items()}

    sub = {"type": "subscribe", "product_ids": cb_ids, "channel": "ticker"}

    while True:
        try:
            async with websockets.connect(CB_WS, ping_interval=20) as ws:
                await ws.send(json.dumps(sub))
                console.print(f"[dim]Price feed connected: {', '.join(cb_ids)}[/dim]")
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("channel") != "ticker":
                        continue
                    for evt in msg.get("events", []):
                        for ticker in evt.get("tickers", []):
                            pid   = ticker.get("product_id", "")
                            price = float(ticker.get("price", 0) or 0)
                            coin  = reverse.get(pid)
                            if coin and price:
                                live_prices[coin] = price
        except Exception as e:
            console.print(f"[yellow]Price feed error: {e} — reconnecting in 3s[/yellow]")
            await asyncio.sleep(3)


# ── Status bar ─────────────────────────────────────────────────────────────────

async def _status_loop() -> None:
    while True:
        await asyncio.sleep(15)
        if not live_prices:
            continue
        now = datetime.now().strftime("%H:%M:%S")
        prices_str = "  ".join(
            f"{c}=${live_prices[c]:,.2f}" for c in COINS if c in live_prices
        )
        open_pos  = [p for p in positions.values() if not p["resolved"]]
        closed    = [p for p in positions.values() if p["resolved"]]
        color     = "green" if session_pnl >= 0 else "red"
        sign      = "+" if session_pnl >= 0 else ""

        sc = "green" if shadow_pnl >= 0 else "red"
        ss = "+" if shadow_pnl >= 0 else ""

        total_unreal = sum(
            p["shares_up"] * p.get("ask_up", 0) + p["shares_down"] * p.get("ask_dn", 0)
            - (p["spent_up"] + p["spent_down"])
            for p in open_pos
        )
        uc = "green" if total_unreal >= 0 else "red"
        us = "+" if total_unreal >= 0 else ""

        console.rule(f"[dim]── STATUS {now} ──[/dim]")
        console.print(f"  [cyan]Prices[/cyan]  {prices_str}")
        console.print(
            f"  [cyan]P&L[/cyan]     Realized [{color}]{sign}${session_pnl:.2f}[/{color}]"
            f"  Unrealized [{uc}]{us}${total_unreal:.0f}[/{uc}]"
            f"  │  Bot [{sc}]{ss}${shadow_pnl:.2f}[/{sc}]"
            f"  │  {len(open_pos)} open  {len(closed)} closed  {trade_count} trades"
        )

        for pos in sorted(open_pos, key=lambda p: p["window_end"]):
            elapsed = int(_elapsed(pos))
            spent   = pos["spent_up"] + pos["spent_down"]
            secs_left  = int(pos["window_end"] - time.time())
            window_label = pos["title"]
            a = window_label.find("April")
            et = window_label.find("ET") + 2
            wl = window_label[a+6:et].strip() if a >= 0 and et > 2 else window_label[-20:]
            cur_ask_up = pos.get("ask_up", 0)
            cur_ask_dn = pos.get("ask_dn", 0)
            mkt_val    = pos["shares_up"] * cur_ask_up + pos["shares_down"] * cur_ask_dn
            unreal     = mkt_val - spent
            unreal_col = "green" if unreal >= 0 else "red"
            unreal_str = f"{'+'if unreal>=0 else ''}{unreal:.0f}"
            trades_n   = len(pos.get("trades", []))
            sum_clob   = cur_ask_up + cur_ask_dn

            console.print(
                f"  [bold]{pos['coin']}[/bold] {wl:<18}"
                f" Up ${pos['spent_up']:>4.0f}@{cur_ask_up:.3f}({pos['shares_up']:>4.0f}sh)"
                f" Dn ${pos['spent_down']:>4.0f}@{cur_ask_dn:.3f}({pos['shares_down']:>4.0f}sh)"
                f" sum={sum_clob:.2f} cost=${spent:.0f} val=${mkt_val:.0f} [{unreal_col}]{unreal_str}[/{unreal_col}]"
                f"  {trades_n}t  {secs_left}s"
            )
        console.print()


# ── Main loop ──────────────────────────────────────────────────────────────────

async def _trading_loop(client: httpx.AsyncClient) -> None:
    # Wait for at least one price tick
    for _ in range(20):
        if live_prices:
            break
        await asyncio.sleep(0.5)

    while True:
        for coin in COINS:
            for offset in [0, 1]:
                ts   = _window_ts(coin, offset)
                slug = _slug(coin, ts)
                if slug not in positions:
                    await _open_window(client, coin, ts)
                elif not positions[slug]["resolved"]:
                    await _tick_window(client, slug)
        # Also resolve any older positions that expired but weren't caught above
        for slug, p in list(positions.items()):
            if not p["resolved"] and time.time() >= p.get("window_end", 0):
                await _tick_window(client, slug)
        await _shadow_poll(client)
        await asyncio.sleep(30)


async def main() -> None:
    console.print("[bold cyan]BTC/ETH/SOL/XRP 15-min Strategy — Paper Trader[/bold cyan]")
    console.print("  Strategy : ARB (sum<0.94) + ODDS model edge (BTC, ≥10% gap, high/med conf)")
    console.print("  Prices   : Coinbase WebSocket (real-time)")
    console.print("  BTC mkt  : Kalshi REST poll (every 2s)  — bot tracked on Polymarket")
    console.print("  ETH/SOL/XRP: Polymarket CLOB WebSocket")
    console.print(f"  Stakes   : ARB=${ARB_STAKE:.0f}/side  ODDS=${ODDS_STAKE:.0f} (≥{ODDS_THRESHOLD:.0%} gap)  MAX=${MAX_PER_WINDOW:.0f}/window")
    console.print(f"  State    : {STATE_FILE}")
    console.print(f"  Log      : {IMPROVE_FILE}")
    console.print()

    _load_surface()
    _load_state()

    async with httpx.AsyncClient() as client:
        # On startup, immediately try to resolve any positions that expired while offline
        unresolved = [slug for slug, p in positions.items()
                      if not p.get("resolved") and time.time() >= p.get("window_end", 0)]
        if unresolved:
            console.print(f"[dim]Checking {len(unresolved)} expired position(s) for resolution...[/dim]")
            for slug in unresolved:
                await _tick_window(client, slug)

        await asyncio.gather(
            _price_feed(),
            _clob_ws_feed(),
            _kalshi_poll(client),
            _polygon_ws_feed(),
            _shadow_watch(client),
            _trading_loop(client),
            _status_loop(),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped.[/dim]")
        if positions:
            closed = [p for p in positions.values() if p["resolved"]]
            color  = "green" if session_pnl >= 0 else "red"
            sign   = "+" if session_pnl >= 0 else ""
            console.print(f"\nFinal: {len(closed)} resolved  Session P&L: [{color}]{sign}${session_pnl:.2f}[/{color}]")
