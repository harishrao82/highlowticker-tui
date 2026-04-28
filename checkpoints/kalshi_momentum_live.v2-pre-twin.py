#!/usr/bin/env python3
"""
kalshi_momentum_live.py — live BTC/ETH/SOL/XRP momentum trader on Kalshi.

Strategy (backtested edge from kalshi_btc_momentum_sim.py):
  - At window open (t=0), snap open_price from Coinbase WS.
  - At t=ENTRY_ELAPSED (default 300s / 5 min), compute
        delta = (price_now − open_price) / open_price * 100
  - If |delta| ≥ THRESHOLD_PCT, buy SHARES of the momentum side:
        delta > 0  →  buy YES
        delta < 0  →  buy NO
  - Hold to settlement, log P&L.

Risk: SHARES=1. Max loss per trade ≈ entry price (cents). Max 4 trades/window.

Trades are appended to ~/.kalshi_momentum_trades.jsonl (one JSON per line).

Run:   python kalshi_momentum_live.py
Stop:  Ctrl-C
"""
import asyncio
import base64
import json
import math
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import stdev

import ccxt
import httpx
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv
from rich.console import Console

import btc_vol_profile

load_dotenv()
console = Console()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COINS = {
    "BTC": dict(enabled=True, series="KXBTC15M", cb_product="BTC-USD",
                kraken="BTC/USD", bitstamp="BTC/USD"),
    "ETH": dict(enabled=True, series="KXETH15M", cb_product="ETH-USD",
                kraken="ETH/USD", bitstamp="ETH/USD"),
    # SOL re-enabled. Earlier +6.7¢ Kalshi-Poly gap was measured on a fixed-
    # threshold strategy; with dynamic thresholds + per-checkpoint ask caps,
    # structurally-rich asks are filtered out automatically. Downside per
    # trade is still bounded by the max ask cap × SHARES.
    "SOL": dict(enabled=True, series="KXSOL15M", cb_product="SOL-USD",
                kraken="SOL/USD", bitstamp=None),     # bitstamp has no SOL
    "XRP": dict(enabled=True, series="KXXRP15M", cb_product="XRP-USD",
                kraken="XRP/USD", bitstamp="XRP/USD"),
}

SHARES          = 1          # fallback when balance fetch fails
MIN_ENTRY_PRICE = 0.45       # skip if ask is below this — entries <0.45 were 0/8 wins
MAX_ENTRY_PRICE = 0.90       # absolute hard ceiling (per-checkpoint caps below)
LIMIT_BUFFER    = 0.02       # aggressive half: place limit at ask + buffer (guaranteed fill)
PATIENT_OFFSET  = 0.02       # patient half: place limit at ask - offset (2¢ cheaper, maker-fee rebate)
PATIENT_MIN_PX  = 0.01       # floor for the patient half's limit price
MARKET_FRACTION = 0.10       # 10% aggressive (guaranteed fill), 90% patient (maker
                             # rebate at ask-2¢). Patient saves 3¢/share (2¢ entry +
                             # 1¢ fee) = $13+/day over 69 fires. Worth more than signal.
PATIENT_TIMEOUT_SEC = 15     # auto-cancel the patient leg if not filled within
                             # this many seconds — otherwise a late fill lands
                             # long after the original momentum signal is stale
MAX_EVAL_GAP_SEC    = 60     # if the evaluator fires more than this many seconds
                             # past the checkpoint T, skip — signal is stale

# Portfolio-tiered share sizing. At the start of every window we fetch the
# current Kalshi balance and pick the share size from the first tier whose
# threshold the balance clears. Tiers ordered high → low.
#
#   balance >= $500   →  15 shares/entry
#   $400 - $500       →  12
#   $220 - $400       →  10
#   $150 - $220       →   5
#   below $150        →   1
SHARE_TIERS: list[tuple[float, int]] = [
    (500.0, 15),
    (400.0, 12),
    (220.0, 10),
    (150.0,  5),
    (  0.0,  1),
]

def shares_for_portfolio(value: float) -> int:
    for threshold, n in SHARE_TIERS:
        if value >= threshold:
            return n
    return 1

# Set at each window start after fetching the Kalshi balance
current_window_shares: int = SHARES

# Multi-entry checkpoints: (elapsed_sec, vol_factor, max_entry_price).
#
# vol_factor scales the instantaneous threshold against realized BTC vol
# (stddev of prior 1h 1-min log returns, expressed as %). Scallops fires
# at roughly 1.0 × realized_vol historically (Pearson +0.342 corr measured
# over 10,897 trades).
#
# Effective threshold = clip(vol_factor * vol_pct, VOL_THR_FLOOR, VOL_THR_CEILING)
#
# So in a quiet night regime (vol ≈ 0.03%):
#   T=30s   thr = 0.5 * 0.03% = 0.015%   (catches small moves)
#   T=600s  thr = 1.5 * 0.03% = 0.045%
#
# In NY open regime (vol ≈ 0.08%):
#   T=30s   thr = 0.5 * 0.08% = 0.040%
#   T=600s  thr = 1.5 * 0.08% = 0.120%
#
# max_entry_price caps — lowered Apr 14 after -$58 drawdown from high-price losses.
# At 0.80 entry: win +$2, loss -$8 → needs 80% win rate. ETH was only 64% today.
# Hard ceiling: 0.70 across the board. Scallops handles higher but only because
# he has offsetting positions; we're pure directional.
CHECKPOINTS = [
    ( 30, 0.5, 0.58),
    ( 60, 0.7, 0.64),
    (120, 1.0, 0.73),
    (180, 1.0, 0.75),
    (300, 1.2, 0.75),
    (420, 1.5, 0.75),
    (600, 1.5, 0.75),
]
VOL_THR_FLOOR   = 0.015     # minimum threshold (pct) — protects against noise
VOL_THR_CEILING = 0.25      # maximum threshold (pct) — cap in violent regimes
VOL_LOOKBACK_MIN = 60       # realized vol window length

# Per-coin cap adjustment — subtracted from every checkpoint's max_ask for
# that coin. Derived from realized per-coin win rates so that each coin's
# effective max entry stays within its EV-positive zone.
#
#   Coin realized win rate → max EV+ entry (≈ win_rate − 0.02 safety)
#   BTC   66% → 0.64   so baseline caps need −0.15 pull-back
#   ETH   85% → 0.83   no adjustment (baseline caps already ≤ 0.88)
#   SOL   72% → 0.70   −0.10
#   XRP   72% → 0.70   −0.10
COIN_CAP_ADJUSTMENT: dict[str, float] = {
    "BTC": -0.15,
    "ETH":  0.00,
    "SOL":  0.00,
    "XRP":  0.00,
}

# Per-coin per-checkpoint cap override. When a (coin, T) pair is set here,
# this REPLACES the COIN_CAP_ADJUSTMENT calc for that checkpoint. Used to
# loosen late-T BTC caps without changing the early-T tighter ones.
COIN_CAP_OVERRIDE: dict[tuple[str, int], float] = {
    # BTC late-T raised so we can capture fast aggressive moves that push ask
    # past the default 0.55 cap before we can fire.
    ("BTC", 180): 0.65,
    ("BTC", 300): 0.65,
    ("BTC", 420): 0.65,
    ("BTC", 600): 0.65,
    # Note: base caps raised +5¢ so BTC effective = base(0.75) + adj(-0.15) = 0.60
    # Override keeps late-T at 0.65 (above the new base 0.60)
}

def _effective_cap(sym: str, T: int, base_cap: float) -> float:
    """Return effective cap for (coin, checkpoint). Override > adjustment."""
    if (sym, T) in COIN_CAP_OVERRIDE:
        return COIN_CAP_OVERRIDE[(sym, T)]
    return base_cap + COIN_CAP_ADJUSTMENT.get(sym, 0.0)

# Effective vol at window open = max(profile_slot_vol, realized_last_hour).
# Profile covers time-of-week regime shifts (9:30 ET open, CME reopen, etc.)
# while realized captures current-moment spikes.
_vol_profile: dict = {}     # loaded at startup from btc_vol_profile

COOLDOWN_SEC = 180          # skip if same side fired within this many seconds (multi-share)
COOLDOWN_SEC_SMALL = 60     # shorter cooldown when only 1 share on the line

# Overnight share cap — between these hours (ET), cap shares at 1 regardless
# of portfolio tier or momentum multiplier. Still trades (collects data/patterns)
# but limits losses. Data: overnight 34-56% win rate, -$64 at midnight alone.
OVERNIGHT_START_HOUR = 23   # 11pm ET
OVERNIGHT_END_HOUR   = 5    # 5am ET
OVERNIGHT_MAX_SHARES = 6    # 1mkt+5pat — same level that proved out at 80% WR overnight

# Dead hours: stop ALL trading during these hours (ET).
# 5pm=45.5% WR (-$30), 8pm=30.8% (-$21), 12am=30.8% (-$31), 3am=39.4% (-$23).
# Trades are still logged to filtered signals for tracking, but no orders placed.
DEAD_HOURS_ET = {0, 3, 17, 20}  # midnight, 3am, 5pm, 8pm ET

# Max total shares per coin per window. Prevents runaway stacking from
# MOM + DD + Scallops + avgdn all firing on the same coin.
# At 15 shares × $0.65 avg = $9.75 max risk per coin per window (~2% of $500).
MAX_SHARES_PER_COIN_WINDOW = 15

# Worst-case per window ≈ SHARES × max(max_px) × max_entries_per_window
# Backtest: mean ~2.2 entries/window × 4 coins ~= 9 entries average,
# max 7 entries per coin × 4 coins = 28 entries theoretical max.
# At 1 share × 0.88 × 28 → ~$25 theoretical max loss / window (very rare).
# Typical window stake: ~$1-3 notional across all coins.
# Goal: prove the edge is real cheaply, then scale up SHARES once verified.

TRADE_LOG    = Path.home() / ".kalshi_momentum_trades.jsonl"
FILTERED_LOG = Path.home() / ".kalshi_filtered_signals.jsonl"

# Scallops TOD × T-bucket lookup table (built from his historical trades).
# When a cell has n >= SCALLOPS_MIN_N, we override our vol-threshold and
# max_price cap with his levels. Otherwise fall back to existing logic.
SCALLOPS_LEVELS_PATH = Path.home() / ".scallops_levels.json"
SCALLOPS_MIN_N       = 10        # need this many of his trades to trust the cell
SCALLOPS_DELTA_LOOSE = 0.5       # use 0.5 × his median |Δ| as our floor
SCALLOPS_PRICE_BUFFER = 0.05     # allow ask up to his median price + 5¢

# Until the hedge module is wired up, no naked entries above this price.
# Scallops can sit at 0.77 because his other-side insurance caps the loss;
# we'd take the full -$0.77 hit on a reversal. Lift this once hedging is live.
NAKED_MAX_ENTRY_PRICE = 0.74
HEDGE_ENABLED         = True     # hedge module live — cap is lifted
# Block fresh naked entries past this elapsed second. Late-window Scallops
# trades in our dataset are almost always insurance hedges on top of an
# existing position — copying them naked is a coin flip with 5 min to settle.
# The hedge module still runs after this; it just won't open new positions.
MAX_FRESH_ENTRY_T_SEC = 600

_SCALLOPS_TOD = [(0,4),(4,8),(8,12),(12,16),(16,20),(20,24)]
_SCALLOPS_T   = [(0,30),(30,60),(60,120),(120,180),(180,300),(300,420),(420,600),(600,900)]

def _load_scallops_levels() -> dict:
    if not SCALLOPS_LEVELS_PATH.exists():
        return {}
    try:
        with open(SCALLOPS_LEVELS_PATH) as f:
            return json.load(f)
    except Exception:
        return {}

_scallops_levels: dict = _load_scallops_levels()

# ── Scallops live position (from poly_scallops_live_shadow.py) ────────────
# Tails ~/.scallops_live_trades.jsonl to show Scallops' current-window
# direction in the status line. Updated every status tick (~10s).
SCALLOPS_LIVE_LOG = Path.home() / ".scallops_live_trades.jsonl"
_scallops_file_pos: int = 0   # file offset for incremental reads

# (coin, window_start_ts) → {side: "Up"/"Down", price: float, elapsed: int, n: int}
_scallops_live: dict[tuple[str, int], dict] = {}


def _refresh_scallops_live() -> None:
    """Incrementally read new lines from the Scallops trade log."""
    global _scallops_file_pos
    if not SCALLOPS_LIVE_LOG.exists():
        return
    try:
        size = SCALLOPS_LIVE_LOG.stat().st_size
        if size < _scallops_file_pos:
            _scallops_file_pos = 0  # file was truncated/rotated
        with open(SCALLOPS_LIVE_LOG) as f:
            f.seek(_scallops_file_pos)
            for line in f:
                try:
                    t = json.loads(line)
                except Exception:
                    continue
                coin = t.get("coin")
                ws = t.get("window_start_ts")
                side = t.get("outcome")     # "Up" or "Down"
                if not (coin and ws and side):
                    continue
                # Only track 15-min windows
                if t.get("market_type") not in ("15m", None):
                    continue
                if t.get("side") != "BUY":
                    continue
                key = (coin, ws)
                if key not in _scallops_live:
                    _scallops_live[key] = {
                        "side": side,
                        "price": float(t.get("price", 0)),
                        "elapsed": t.get("elapsed_in_window") or 0,
                        "n": 1,
                    }
                else:
                    _scallops_live[key]["n"] += 1
            _scallops_file_pos = f.tell()
    except Exception:
        pass

# Set of (coin, window_start_ts) we've already acted on for Scallops signals.
# Reset each window in run_window().
_scallops_acted: set[tuple[str, int]] = set()

SCALLOPS_ENTRY_MIN_PRICE = 0.50   # only act on his signal if his Poly entry ≥ this
SCALLOPS_DD_MAX_ASK      = 0.85   # don't DD if Kalshi ask has risen past this
SCALLOPS_ENTRY_SHARES    = 6      # same as normal MOM entry: 1 market + 5 patient

# Resting avg-down bid: when MOM + Scallops agree, place a limit bid at $0.35
# on our side. Cancel at T=300s if unfilled. Backtest: 75% WR, +$2.40/fill.
SCALLOPS_AVGDN_BID       = 0.35   # resting bid price
SCALLOPS_AVGDN_SHARES    = 6
SCALLOPS_AVGDN_CANCEL_T  = 300    # cancel unfilled bid after this elapsed

# Cross-coin direction tracking: what side has each coin fired this window?
# Reset each window. Used to detect 3-vs-1 and 2-vs-1 outliers.
# coin → "yes" / "no" / None
_cross_coin_sides: dict[str, str | None] = {}

def _cross_coin_majority() -> str | None:
    """Return the majority direction across all coins that have fired,
    or None if tied or <2 coins have fired."""
    sides = [s for s in _cross_coin_sides.values() if s is not None]
    if len(sides) < 2:
        return None
    yes_n = sides.count("yes")
    no_n = sides.count("no")
    if yes_n > no_n:
        return "yes"
    elif no_n > yes_n:
        return "no"
    return None  # tied

def _is_outlier(sym: str, side: str) -> tuple[bool, str | None]:
    """Check if firing `side` on `sym` would make it the outlier.
    Returns (is_outlier, majority_side).
    Only triggers when 2+ other coins already fired the opposite direction."""
    others = {c: s for c, s in _cross_coin_sides.items() if c != sym and s is not None}
    if len(others) < 2:
        return False, None
    opp_count = sum(1 for s in others.values() if s != side)
    same_count = sum(1 for s in others.values() if s == side)
    if opp_count >= 2 and same_count == 0:
        # All other coins disagree with this side
        majority = "no" if side == "yes" else "yes"
        return True, majority
    return False, None

# Scallops-only exit timer: if MOM hasn't confirmed within 240s, sell.
# Backtest: saves +$31 on 49 windows (46.7% WR → cut losses early).
SCALLOPS_EXIT_TIMER_SEC  = 240    # 4 minutes
# Track Scallops-initiated entries awaiting MOM confirmation
# (coin, ws) → {'side': str, 'entry_elapsed': int, 'order_ids': list}
_scallops_pending_confirm: dict[tuple[str, int], dict] = {}

# Track resting avg-down orders per (coin, ws) → order_id for cancellation
_scallops_avgdn_orders: dict[tuple[str, int], str] = {}


async def _place_scallops_avgdn_bid(client: httpx.AsyncClient,
                                     sym: str, side: str, ticker: str,
                                     elapsed: int) -> None:
    """Place a resting limit bid at $0.35 on our side. Cancel at T=300 if unfilled.
    Backtest: 75% WR when filled in first 5 min, +$2.40/fill."""
    key = (sym, current_window_start_ts)
    if key in _scallops_avgdn_orders:
        return  # already placed for this window

    bid_px = SCALLOPS_AVGDN_BID
    order = await _place_buy(client, ticker, side, bid_px,
                              SCALLOPS_AVGDN_SHARES, f"{sym}@T{elapsed}/SAVG")
    if order:
        oid = order["order_id"]
        _scallops_avgdn_orders[key] = oid
        _register_fill_tracker(order, sym, side, ticker,
                               SCALLOPS_AVGDN_SHARES, bid_px,
                               "scallops-avgdn", SCALLOPS_AVGDN_SHARES)
        console.print(
            f"  [bold blue]↓ SCALLOPS AVGDN BID {sym} {side.upper()} "
            f"{SCALLOPS_AVGDN_SHARES}sh @ ${bid_px:.2f} "
            f"(cancel at T={SCALLOPS_AVGDN_CANCEL_T}s)[/bold blue]"
        )

        # Schedule cancellation at T=300
        async def _cancel_avgdn() -> None:
            remaining = SCALLOPS_AVGDN_CANCEL_T - elapsed
            if remaining > 0:
                await asyncio.sleep(remaining)
            await _cancel_single(client, oid, tag=f"{sym}/SAVG-cancel")
            _scallops_avgdn_orders.pop(key, None)

        asyncio.create_task(_cancel_avgdn())


async def _process_scallops_signals(client: httpx.AsyncClient) -> None:
    """Check for new Scallops signals and fire trades.

    Called every status tick (~10s). For each coin in the current window:
      - If Scallops' first trade just appeared (not yet acted on):
        A. His entry ≥ $0.55 + we have NO position → new entry on his side
        B. His entry ≥ $0.55 + we have a position on SAME side → DD
        C. His entry ≥ $0.55 + we have a position on OTHER side → do nothing
        D. His entry < $0.55 → ignore
    """
    if current_window_start_ts == 0 or not kalshi_state:
        return

    elapsed = int(time.time() - current_window_start_ts)
    if elapsed < 5 or elapsed > 780:   # too early or < 2 min left
        return

    # Dead hours: no Scallops entries either
    now_et_hour = (datetime.now(timezone.utc) + ET_OFFSET).hour
    if now_et_hour in DEAD_HOURS_ET:
        return

    ws = current_window_start_ts

    for sym, st in kalshi_state.items():
        key = (sym, ws)
        if key in _scallops_acted:
            continue

        rec = _scallops_live.get(key)
        if not rec:
            continue   # no Scallops trade yet for this coin/window

        # Mark as acted regardless of outcome — one shot per (coin, window)
        _scallops_acted.add(key)

        s_price = rec["price"]
        if s_price < SCALLOPS_ENTRY_MIN_PRICE:
            console.print(
                f"  [dim]Scallops {sym} {rec['side']}@{s_price:.2f} — "
                f"below ${SCALLOPS_ENTRY_MIN_PRICE:.2f} threshold, ignoring[/dim]"
            )
            continue

        # Map Scallops side → Kalshi side
        s_kalshi = "yes" if rec["side"] == "Up" else "no"

        # Cross-coin check: if 2+ other Scallops entries this window went
        # the opposite direction, this coin is the outlier.
        # Use _scallops_live (all Scallops first trades) for cross-coin awareness.
        sc_others = {
            c: ('yes' if _scallops_live[(c, ws)]['side'] == 'Up' else 'no')
            for c in ['BTC', 'ETH', 'SOL', 'XRP']
            if c != sym and (c, ws) in _scallops_live
            and _scallops_live[(c, ws)]['price'] >= SCALLOPS_ENTRY_MIN_PRICE
        }
        if len(sc_others) >= 2:
            opp_count = sum(1 for s in sc_others.values() if s != s_kalshi)
            same_count = sum(1 for s in sc_others.values() if s == s_kalshi)
            if opp_count >= 3 and same_count == 0:
                # 3-vs-1: flip to majority
                majority = next(s for s in sc_others.values() if s != s_kalshi)
                console.print(
                    f"  [bold yellow]↔ SCALLOPS CROSS-COIN FLIP {sym} "
                    f"{s_kalshi.upper()}→{majority.upper()} "
                    f"(3 coins say {majority.upper()})[/bold yellow]"
                )
                s_kalshi = majority
            elif opp_count >= 2 and same_count == 0:
                # 2-vs-1: block
                console.print(
                    f"  [dim]Scallops {sym} {s_kalshi.upper()} blocked — "
                    f"2 other coins say opposite[/dim]"
                )
                continue

        ticker = st["ticker"]
        cur_ask = st["yes_ask"] if s_kalshi == "yes" else st["no_ask"]

        if not cur_ask or cur_ask <= 0 or cur_ask >= 1:
            continue

        # Do we already have a position in this window?
        our_trades = st.get("trades", [])
        our_side = our_trades[0]["side"] if our_trades else None

        if our_side is None:
            # ── SCENARIO A: No position yet → Scallops-initiated entry ────
            if cur_ask > SCALLOPS_DD_MAX_ASK:
                console.print(
                    f"  [dim]Scallops {sym} {rec['side']}@{s_price:.2f} — "
                    f"Kalshi ask {cur_ask:.2f} > cap {SCALLOPS_DD_MAX_ASK}, skip[/dim]"
                )
                continue
            if cur_ask < MIN_ENTRY_PRICE:
                continue

            # Per-coin cap check
            existing_sh = sum(
                tr.get("shares", 0) for tr in _fill_tracker.values()
                if tr.get("coin") == sym and tr.get("ticker") == ticker
                and tr.get("fill_at")
            )
            remaining = max(0, MAX_SHARES_PER_COIN_WINDOW - existing_sh)
            if remaining == 0:
                continue

            sc_shares = min(SCALLOPS_ENTRY_SHARES, remaining)
            n_agg = 1
            n_pat = sc_shares - 1
            agg_px = round(min(cur_ask + LIMIT_BUFFER, SCALLOPS_DD_MAX_ASK), 2)
            pat_px = round(max(cur_ask - PATIENT_OFFSET, PATIENT_MIN_PX), 2)

            console.print(
                f"  [bold yellow]★ SCALLOPS ENTRY {sym} "
                f"{s_kalshi.upper()} (he bought {rec['side']}@{s_price:.2f}) "
                f"→ {n_agg}@{agg_px:.2f}(taker)+{n_pat}@{pat_px:.2f}(maker)"
                f"[/bold yellow]"
            )

            agg_order = await _place_buy(
                client, ticker, s_kalshi, agg_px, n_agg,
                f"{sym}@T{elapsed}/SC-M"
            )
            if agg_order:
                _register_fill_tracker(
                    agg_order, sym, s_kalshi, ticker, n_agg, agg_px,
                    "scallops-market", SCALLOPS_ENTRY_SHARES
                )
                st.setdefault("trades", []).append({
                    "T_checkpoint":     0,
                    "elapsed_actual":   elapsed,
                    "side":             s_kalshi,
                    "ask_at_trigger":   cur_ask,
                    "limit_price_sent": agg_px,
                    "shares":           n_agg,
                    "role":             "scallops-market",
                    "confirmed":        False,
                    "order_id":         agg_order["order_id"],
                    "client_order_id":  agg_order["client_order_id"],
                    "sent_at":          agg_order.get("sent_at"),
                    "ack_at":           agg_order.get("ack_at"),
                })
                session_stats["pending"] += 1

            if n_pat > 0:
                pat_order = await _place_buy(
                    client, ticker, s_kalshi, pat_px, n_pat,
                    f"{sym}@T{elapsed}/SC-L"
                )
                if pat_order:
                    _register_fill_tracker(
                        pat_order, sym, s_kalshi, ticker, n_pat, pat_px,
                        "scallops-patient", SCALLOPS_ENTRY_SHARES
                    )
                    st.setdefault("trades", []).append({
                        "T_checkpoint":     0,
                        "elapsed_actual":   elapsed,
                        "side":             s_kalshi,
                        "ask_at_trigger":   cur_ask,
                        "limit_price_sent": pat_px,
                        "shares":           n_pat,
                        "role":             "scallops-patient",
                        "confirmed":        False,
                        "order_id":         pat_order["order_id"],
                        "client_order_id":  pat_order["client_order_id"],
                        "sent_at":          pat_order.get("sent_at"),
                        "ack_at":           pat_order.get("ack_at"),
                    })
                    session_stats["pending"] += 1
                    asyncio.create_task(
                        _cancel_patient_after(client, pat_order["order_id"],
                                              tag=f"{sym}@T{elapsed}/SC-L")
                    )

            # Place resting avg-down bid at $0.35
            if elapsed < SCALLOPS_AVGDN_CANCEL_T:
                await _place_scallops_avgdn_bid(
                    client, sym, s_kalshi, ticker, elapsed)

            # Exit timer disabled — Scallops-only holds to expiry.
            # Historical: 65.2% WR, +$0.10/trade. Thin edge but +EV.
            # Timer cost more in exit spread than it saved. Revisit if WR drops.

            _log_filtered({
                "ts": time.time(), "action": "scallops_entry",
                "coin": sym, "side": s_kalshi, "ticker": ticker,
                "scallops_price": s_price, "scallops_side": rec["side"],
                "cur_ask": cur_ask, "elapsed": elapsed,
                "window_start_ts": ws,
            })

        elif our_side == s_kalshi:
            # ── SCENARIO B: We agree → DD on same side ────────────────
            # MOM confirmed the Scallops entry — cancel exit timer
            _scallops_pending_confirm.pop((sym, ws), None)

            if cur_ask > SCALLOPS_DD_MAX_ASK:
                console.print(
                    f"  [dim]Scallops DD {sym} {rec['side']}@{s_price:.2f} — "
                    f"ask {cur_ask:.2f} > cap, skip DD[/dim]"
                )
                continue

            # Per-coin cap check
            existing_sh = sum(
                tr.get("shares", 0) for tr in _fill_tracker.values()
                if tr.get("coin") == sym and tr.get("ticker") == ticker
                and tr.get("fill_at")
            )
            remaining = max(0, MAX_SHARES_PER_COIN_WINDOW - existing_sh)
            if remaining == 0:
                console.print(f"  [dim]Scallops DD {sym} — at {MAX_SHARES_PER_COIN_WINDOW}sh cap[/dim]")
                continue

            sc_dd_shares = min(SCALLOPS_ENTRY_SHARES, remaining)
            n_agg = 1
            n_pat = sc_dd_shares - 1
            agg_px = round(min(cur_ask + LIMIT_BUFFER, SCALLOPS_DD_MAX_ASK), 2)
            pat_px = round(max(cur_ask - PATIENT_OFFSET, PATIENT_MIN_PX), 2)

            console.print(
                f"  [bold magenta]★ SCALLOPS DD {sym} "
                f"{s_kalshi.upper()} (he agrees: {rec['side']}@{s_price:.2f}) "
                f"→ {n_agg}@{agg_px:.2f}+{n_pat}@{pat_px:.2f}"
                f"[/bold magenta]"
            )

            agg_order = await _place_buy(
                client, ticker, s_kalshi, agg_px, n_agg,
                f"{sym}@T{elapsed}/SDD-M"
            )
            if agg_order:
                _register_fill_tracker(
                    agg_order, sym, s_kalshi, ticker, n_agg, agg_px,
                    "scallops-dd-market", SCALLOPS_ENTRY_SHARES
                )
                st.setdefault("trades", []).append({
                    "T_checkpoint":     0,
                    "elapsed_actual":   elapsed,
                    "side":             s_kalshi,
                    "ask_at_trigger":   cur_ask,
                    "limit_price_sent": agg_px,
                    "shares":           n_agg,
                    "role":             "scallops-dd-market",
                    "confirmed":        False,
                    "order_id":         agg_order["order_id"],
                    "client_order_id":  agg_order["client_order_id"],
                    "sent_at":          agg_order.get("sent_at"),
                    "ack_at":           agg_order.get("ack_at"),
                })
                session_stats["pending"] += 1

            if n_pat > 0:
                pat_order = await _place_buy(
                    client, ticker, s_kalshi, pat_px, n_pat,
                    f"{sym}@T{elapsed}/SDD-L"
                )
                if pat_order:
                    _register_fill_tracker(
                        pat_order, sym, s_kalshi, ticker, n_pat, pat_px,
                        "scallops-dd-patient", SCALLOPS_ENTRY_SHARES
                    )
                    st.setdefault("trades", []).append({
                        "T_checkpoint":     0,
                        "elapsed_actual":   elapsed,
                        "side":             s_kalshi,
                        "ask_at_trigger":   cur_ask,
                        "limit_price_sent": pat_px,
                        "shares":           n_pat,
                        "role":             "scallops-dd-patient",
                        "confirmed":        False,
                        "order_id":         pat_order["order_id"],
                        "client_order_id":  pat_order["client_order_id"],
                        "sent_at":          pat_order.get("sent_at"),
                        "ack_at":           pat_order.get("ack_at"),
                    })
                    session_stats["pending"] += 1
                    asyncio.create_task(
                        _cancel_patient_after(client, pat_order["order_id"],
                                              tag=f"{sym}@T{elapsed}/SDD-L")
                    )

            # Place resting avg-down bid at $0.35
            if elapsed < SCALLOPS_AVGDN_CANCEL_T:
                await _place_scallops_avgdn_bid(
                    client, sym, s_kalshi, ticker, elapsed)

            _log_filtered({
                "ts": time.time(), "action": "scallops_dd",
                "coin": sym, "side": s_kalshi, "ticker": ticker,
                "scallops_price": s_price, "scallops_side": rec["side"],
                "cur_ask": cur_ask, "elapsed": elapsed,
                "window_start_ts": ws,
            })

        else:
            # ── SCENARIO C: Scallops disagrees → do nothing ───────────
            console.print(
                f"  [dim]Scallops {sym} {rec['side']}@{s_price:.2f} "
                f"DISAGREES with our {our_side.upper()} — holding[/dim]"
            )


def scallops_status(coin: str, ws: int) -> str:
    """Compact string for status line: 'U.55x3' or '—'."""
    rec = _scallops_live.get((coin, ws))
    if not rec:
        return "—"
    s = "U" if rec["side"] == "Up" else "D"
    return f"{s}{rec['price']:.2f}x{rec['n']}"


# ── Prior-5m direction gate ────────────────────────────────────────────────
# Loads per-coin buckets of (prior 5-min Δ%) → next-15m-green probability.
# Built from 365d of 1-min candles per coin. Use to GATE MOM direction:
# when current prior-5m bucket says 'up_only', skip NO entries; 'down_only',
# skip YES entries. 'neutral' = allow both (no edge).
PRIOR5_PATH = Path.home() / ".prior5_direction_gate.json"
def _load_prior5() -> dict:
    if not PRIOR5_PATH.exists(): return {}
    try:
        with open(PRIOR5_PATH) as f: return json.load(f)
    except: return {}
_prior5_table: dict = _load_prior5()
PRIOR5_GATE_ENABLED = True

# Rolling per-coin blended-price history for computing live Δ5m. Keep 360
# samples at 1-sec interval (6 min buffer, 5-min lookback + margin).
from collections import deque as _deque2
_price_history: dict[str, _deque2] = {}
_PRICE_HIST_MAX = 360   # seconds
def _record_price_tick(coin: str, px: float) -> None:
    if coin not in _price_history:
        _price_history[coin] = _deque2(maxlen=_PRICE_HIST_MAX)
    _price_history[coin].append((time.time(), px))

def _delta5m_pct(coin: str) -> float | None:
    """(now − 5min_ago) / 5min_ago × 100. Returns None if insufficient history."""
    hist = _price_history.get(coin)
    if not hist or len(hist) < 60:
        return None
    now_ts, now_px = hist[-1]
    target_ts = now_ts - 300
    # find closest older sample
    past = None
    for ts, px in hist:
        if ts <= target_ts: past = (ts, px)
        else: break
    if not past:
        past = hist[0]
    past_ts, past_px = past
    if not past_px: return None
    return (now_px - past_px) / past_px * 100.0

def _prior5_decision(coin: str, delta5m_pct: float) -> str:
    """Return 'up_only' | 'down_only' | 'neutral' | 'unknown' for this coin + Δ5m."""
    stats = _prior5_table.get(coin)
    if not stats: return 'unknown'
    for b in stats.get('buckets', []):
        if b['lo'] <= delta5m_pct < b['hi']:
            return b.get('decision', 'neutral')
    return 'unknown'

# Hourly probability surfaces per coin — used in status display for live edge view
def _load_coin_surface(coin: str) -> dict:
    p = Path.home() / f".{coin.lower()}_model_surface_hourly.json"
    if not p.exists(): return {}
    try:
        with open(p) as f: return json.load(f)
    except: return {}
_hourly_surfaces: dict = {c: _load_coin_surface(c) for c in ('BTC', 'ETH', 'SOL', 'XRP')}

# Surface-edge engine config: only fire when surface says we have ≥ this much
# edge over the market price. Backtest shows below 25pp the surface is mostly
# noise; ≥25pp had 75% win rate on Apr 14 BTC data.
SURFACE_ENGINE_ENABLED = False  # disabled — losing in backtest
SURFACE_MIN_EDGE       = 0.25
SURFACE_COOLDOWN_SEC   = 60
_surface_last_ts: dict[str, float] = {}

def _surface_p_up(coin: str, hour: int, t_sec: int, d_pct: float) -> float | None:
    """Lookup p_up from the per-coin hourly surface."""
    surf = _hourly_surfaces.get(coin)
    if not surf: return None
    h = str(hour)
    if h not in surf.get('hours', {}): return None
    grid = surf['hours'][h]['surface']
    t_vals = surf['t_vals']; d_vals = surf['d_vals']
    from bisect import bisect_left
    ti = min(len(t_vals)-1, max(0, bisect_left(t_vals, t_sec)))
    di = min(len(d_vals)-1, max(0, bisect_left(d_vals, d_pct)))
    return grid[ti][di]['p_up']

# Module-level vol cache so _status_loop can show current MOM/POP thresholds
_current_vol_by_coin: dict[str, float] = {}

def _scallops_mom_level(coin: str, et_hour: int, elapsed: int) -> tuple[float, float, int] | None:
    """Return (min_delta_pct, max_price, n) for momentum entry, or None if no data."""
    coin_tbl = _scallops_levels.get(coin)
    if not coin_tbl:
        return None
    tod_key = next((f'{lo:02d}-{hi:02d}' for lo, hi in _SCALLOPS_TOD if lo <= et_hour < hi), None)
    if not tod_key or tod_key not in coin_tbl:
        return None
    t_key = next((f'{lo}-{hi}' for lo, hi in _SCALLOPS_T if lo <= elapsed < hi), None)
    if not t_key:
        return None
    cell = coin_tbl[tod_key].get(t_key, {})
    mom = cell.get('mom')
    if not mom or mom.get('n', 0) < SCALLOPS_MIN_N:
        return None
    return (mom['min_delta_pct'], mom['max_price'], mom['n'])

# TOD buckets where Scallops' contra entries had positive realized edge.
# 04-08 ET +7pp, 12-16 +4pp, 20-24 +7pp. Other buckets had negative edge
# so we explicitly skip contra in those windows.
SCALLOPS_CONTRA_GOOD_TODS = {'04-08', '12-16', '20-24'}
# Don't open contra below this price — Scallops can do penny-lottery contra
# because his other-side position cushions losses; ours is naked and the
# implied win rate gap is too steep at < $0.30 to expect real EV.
CONTRA_MIN_PRICE = 0.30   # floor — anything cheaper is penny lottery, skip
# T-scaled ceiling: early window = loose (reversal plausible), late = strict
# (little time left to recover). Each tier is (T_threshold, max_price).
CONTRA_MAX_PRICE_TIERS = [
    (0,   0.45),   # T=0-300s: full band
    (300, 0.40),   # T=300-450s: tighter
    (450, 0.35),   # T=450-600s: only clearly cheap
]
def _contra_max_price(elapsed: int) -> float:
    best = CONTRA_MAX_PRICE_TIERS[0][1]
    for t, p in CONTRA_MAX_PRICE_TIERS:
        if elapsed >= t: best = p
    return best

CONTRA_COOLDOWN_SEC = 60  # min seconds between contra fires per coin

# Rolling empirical buffer: per coin, list of (winner_min, loser_max) per window.
# Used as primary signal for CONTRA (<$0.50) AND MOM (>$0.50) entries.
REVERSAL_BUFFER_N = 20
REVERSAL_MIN_EDGE = 0.05   # need at least 5pp above ask before firing contra
MOM_EMP_MIN_EDGE  = 0.05   # need at least 5pp above implied before firing mom
from collections import deque as _deque
# each entry is (winner_min, loser_max)
_reversal_history: dict[str, _deque] = {}

def _empirical_reversal_rate(coin: str, price: float) -> tuple[float, int] | None:
    """Fraction of recent EVENTUAL WINNERS whose min price ≤ `price`.
    Used for CONTRA (buying cheap side, expecting reversal)."""
    hist = _reversal_history.get(coin)
    if not hist or len(hist) < 10:
        return None
    hits = sum(1 for (wmin, _lmax) in hist if wmin <= price)
    return (hits / len(hist), len(hist))

def _empirical_momentum_rate(coin: str, price: float) -> tuple[float, int] | None:
    """Fraction of recent windows where the LOSER's max price stayed BELOW `price`.
    = Implied P(current side at `price` is the winner because losers rarely get this high).
    Used for MOM (buying favored side, expecting continuation)."""
    hist = _reversal_history.get(coin)
    if not hist or len(hist) < 10:
        return None
    # Windows where loser never reached `price` → current side at `price` is likely winner
    loser_never_hit = sum(1 for (_wmin, lmax) in hist if lmax < price)
    return (loser_never_hit / len(hist), len(hist))
# Auto-enable contra only in TOD buckets where it had strongest realized edge
# AND has the most safety gates verified. Start conservative with 04-08 ET only
# (+7pp edge). Add more as we get comfortable: 20-24 ET (+7pp), 12-16 (+4pp).
CONTRA_AUTO_ENABLED_TODS = set()   # disabled — 37% WR, -$6/day. Revisit at 4pm Apr 23
CONTRA_FORCE_ON = False

def _contra_enabled_now() -> bool:
    if CONTRA_FORCE_ON:
        return True
    et_hour = (datetime.now(timezone.utc) + ET_OFFSET).hour
    tod_key = next((f'{lo:02d}-{hi:02d}' for lo, hi in _SCALLOPS_TOD if lo <= et_hour < hi), None)
    return tod_key in CONTRA_AUTO_ENABLED_TODS

def _scallops_con_level(coin: str, et_hour: int, elapsed: int) -> tuple[float, float, int] | None:
    """Return (min_delta_pct, max_price, n) for contra entry, or None."""
    coin_tbl = _scallops_levels.get(coin)
    if not coin_tbl:
        return None
    tod_key = next((f'{lo:02d}-{hi:02d}' for lo, hi in _SCALLOPS_TOD if lo <= et_hour < hi), None)
    if not tod_key or tod_key not in SCALLOPS_CONTRA_GOOD_TODS or tod_key not in coin_tbl:
        return None
    t_key = next((f'{lo}-{hi}' for lo, hi in _SCALLOPS_T if lo <= elapsed < hi), None)
    if not t_key:
        return None
    cell = coin_tbl[tod_key].get(t_key, {})
    con = cell.get('con')
    if not con or con.get('n', 0) < SCALLOPS_MIN_N:
        return None
    return (con['max_delta_pct'], con['max_price'], con['n'])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS   = "wss://api.elections.kalshi.com/trade-api/ws/v2"
COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
ET_OFFSET   = timedelta(hours=-4)

_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)


# ── Kalshi auth ──────────────────────────────────────────────────────────────

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

def _ws_auth_headers() -> dict:
    return _sign("GET", "/trade-api/ws/v2")


# ── Multi-exchange crypto prices ─────────────────────────────────────────────
#
# Kalshi BTC settles to the CME CF Benchmark — a volume-weighted average across
# Coinbase, Kraken, Bitstamp, LMAX and ItBit. To match that reference more
# closely (instead of just Coinbase), we collect prices from multiple venues
# and use the blended mean for delta calculation.
#
# Coinbase: WS subscription (sub-second)
# Kraken / Bitstamp: REST polling via ccxt every ~3s
#
# `exchange_prices[exchange][coin] = latest_price`
exchange_prices: dict[str, dict[str, float]] = {
    "coinbase": {},
    "kraken":   {},
    "bitstamp": {},
}

try:
    import cfb_proxy as _cfb_proxy
except Exception:
    _cfb_proxy = None

def blended_price(coin: str) -> float | None:
    """Returns CFB-proxy price (60s rolling mean of bid/ask mids across CB/Kr/Bs)
    once warm; falls back to last-trade median if CFB deque hasn't filled yet
    (~first 60s after startup).

    Also records the returned price into the rolling _price_history so the
    prior-5m direction gate can compute Δ5m.

    The CFB proxy matches Kalshi UI delta within 1-3bps consistently. The old
    last-trade median is kept as a transient fallback only.
    """
    if _cfb_proxy is not None:
        cfb = _cfb_proxy.current(coin)
        if cfb is not None:
            _record_price_tick(coin, cfb)
            return cfb
    # Fallback: last-trade median (warm-up only, or if cfb_proxy module fails)
    pxs = [p for p in (ex.get(coin) for ex in exchange_prices.values())
           if p and p > 0]
    if not pxs:
        return None
    if len(pxs) == 1:
        return pxs[0]
    pxs.sort()
    n = len(pxs)
    if n % 2:
        m = pxs[n // 2]
    else:
        m = (pxs[n // 2 - 1] + pxs[n // 2]) / 2.0
    _record_price_tick(coin, m)
    return m


async def _coinbase_price_feed() -> None:
    products         = [cfg["cb_product"] for cfg in COINS.values() if cfg["enabled"]]
    product_to_coin  = {cfg["cb_product"]: coin
                        for coin, cfg in COINS.items() if cfg["enabled"]}
    while True:
        try:
            async with websockets.connect(COINBASE_WS, ping_interval=20,
                                          open_timeout=10) as ws:
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
                                exchange_prices["coinbase"][sym] = px
                                _notify_price_update()
        except Exception as e:
            console.print(f"[yellow]Coinbase WS: {e} — reconnect 3s[/yellow]")
            await asyncio.sleep(3)


async def _ccxt_poll_loop(name: str, exchange, sym_map: dict[str, str],
                          interval: float = 3.0) -> None:
    """Poll one ccxt exchange for all enabled coins, write into exchange_prices[name]."""
    loop = asyncio.get_event_loop()
    first_ok = False
    while True:
        for coin, symbol in sym_map.items():
            if not symbol:
                continue
            try:
                ticker = await loop.run_in_executor(None, exchange.fetch_ticker, symbol)
                px = ticker.get("last") or ticker.get("close")
                if px and float(px) > 0:
                    exchange_prices[name][coin] = float(px)
                    _notify_price_update()
                    if not first_ok:
                        console.print(f"[dim]{name} REST connected — {coin}=${float(px):,.2f}[/dim]")
                        first_ok = True
            except Exception:
                # Quietly skip transient errors so the poll loop keeps running
                pass
        await asyncio.sleep(interval)


_cb_historical = ccxt.coinbase()   # used for one-shot mid-window open fetches

async def _fetch_open_at(cb_product: str, ts: int) -> float | None:
    """Fetch the 1-min candle open price at or just after `ts` (unix seconds).

    Used when we join a window already in progress and need the historical
    coin price at the 15-min boundary. Returns None on failure.
    """
    loop = asyncio.get_event_loop()
    try:
        ohlcv = await loop.run_in_executor(
            None,
            lambda: _cb_historical.fetch_ohlcv(cb_product, "1m", ts * 1000, 2),
        )
        if ohlcv:
            return float(ohlcv[0][1])   # open of first candle
    except Exception as e:
        console.print(f"  [dim yellow]historical fetch {cb_product}: {e}[/dim yellow]")
    return None


async def _realized_vol_pct(cb_product: str, end_ts: int,
                            minutes: int = VOL_LOOKBACK_MIN) -> float:
    """Stddev of 1-min log returns over the prior `minutes` minutes,
    expressed as a percentage of price. Used to scale the momentum threshold.
    Returns 0 on failure (caller will fall back to VOL_THR_FLOOR).
    """
    loop = asyncio.get_event_loop()
    start_ms = (end_ts - minutes * 60) * 1000
    try:
        ohlcv = await loop.run_in_executor(
            None,
            lambda: _cb_historical.fetch_ohlcv(cb_product, "1m", start_ms, minutes + 5),
        )
        if not ohlcv or len(ohlcv) < 10:
            return 0.0
        closes = [float(r[4]) for r in ohlcv if r[4]]
        rets = []
        for i in range(1, len(closes)):
            if closes[i-1] > 0 and closes[i] > 0:
                rets.append(math.log(closes[i] / closes[i-1]))
        if len(rets) < 5:
            return 0.0
        return stdev(rets) * 100.0
    except Exception as e:
        console.print(f"  [dim yellow]vol fetch {cb_product}: {e}[/dim yellow]")
        return 0.0


async def _start_secondary_feeds() -> None:
    """Spawn ccxt polling loops for Kraken and Bitstamp."""
    kraken   = ccxt.kraken()
    bitstamp = ccxt.bitstamp()

    kraken_map = {
        coin: cfg.get("kraken")
        for coin, cfg in COINS.items() if cfg["enabled"] and cfg.get("kraken")
    }
    bitstamp_map = {
        coin: cfg.get("bitstamp")
        for coin, cfg in COINS.items() if cfg["enabled"] and cfg.get("bitstamp")
    }

    if kraken_map:
        asyncio.create_task(_ccxt_poll_loop("kraken", kraken, kraken_map))
    if bitstamp_map:
        asyncio.create_task(_ccxt_poll_loop("bitstamp", bitstamp, bitstamp_map))


# ── Kalshi yes/no asks (WS) ──────────────────────────────────────────────────

# kalshi_state[sym] = {ticker, yes_ask, no_ask, open_price, triggered, trade, ...}
kalshi_state:   dict[str, dict] = {}
_ticker_to_sym: dict[str, str]  = {}
current_window_start_ts: int = 0     # set by run_window so status loop can compute elapsed

# ── Fill tracker — maps order_id to order metadata for instant-fill detection ──
# When the Kalshi WS pushes a fill, we match by order_id and record the fill time.
# If a patient order fills < 1s after placement → instant fill → double down.
_fill_tracker: dict[str, dict] = {}
FILL_LOG = Path.home() / ".kalshi_fill_events.jsonl"
INSTANT_FILL_SEC = 1.0   # fills within this window count as "instant"
DOUBLEDOWN_FRACTION = 0.5  # half the original shares on double-down

STATUS_INTERVAL_SEC = float(os.environ.get("STATUS_INTERVAL", "10"))  # how often the status line prints
STATUS_LOOKBACK_SEC = float(os.environ.get("STATUS_LOOKBACK", "10"))  # delta-of-delta look-back window
STATUS_SLIM         = os.environ.get("STATUS_SLIM", "0") == "1"       # 1 = just Δ + Y/N asks, no thresholds/surface
STATUS_REST_REFRESH_SEC = 10         # min seconds between REST ask refreshes (rate-limit safety)
SCALLOPS_POLL_SEC   = 2             # how often to check for new Scallops trades (independent of display)
EVAL_DEBOUNCE_SEC   = 0.5            # min seconds between evaluator runs

# ── Hedge module ─────────────────────────────────────────────────────────────
HEDGE_ENGINE_ENABLED = False  # disabled — losing $80+ in backtest, under-fills
HEDGE_MIN_T_SEC      = 180   # don't over-hedge before this many seconds elapsed
HEDGE_TICK_SEC       = 30    # 30s timer between hedge checks (also fires on fill)
# Surface-aware hedge tiers: if surface predicts our WINNING side's probability
# at current (T, Δ, hour), scale the hedge based on how confident it is.
HEDGE_SURFACE_SKIP_P     = 0.70   # ≥ this → skip hedge entirely (keep naked upside)
HEDGE_SURFACE_PARTIAL_P  = 0.55   # in [this, skip) → partial hedge (PARTIAL_FRACTION)
HEDGE_PARTIAL_FRACTION   = 0.30   # fraction of B_max when partial-hedging

# Recovery-chase: when BOTH outcomes are underwater and delta strongly favors
# one side, aggressively stack the favored side so we net positive if the
# direction holds. Differs from hedge (which requires one side winning).
RECOVERY_CHASE_ENABLED   = False  # disabled — keep things simple, MOM-only focus
RECOVERY_CHASE_MIN_T     = 180    # don't chase too early
RECOVERY_CHASE_MAX_T     = 600    # don't chase in the last 5 min
RECOVERY_CHASE_DELTA_MULT = 2.0   # |Δ| must be ≥ this × vol_thr
RECOVERY_CHASE_COOLDOWN  = 120    # longer cooldown than other engines
RECOVERY_CHASE_TARGET_PNL = 1.0   # target min $1 profit if favored side wins
_recovery_last_ts: dict[str, float] = {}
HEDGE_COOLDOWN_SEC   = 60    # min seconds between hedge fires per coin (prevents
                              # immediate re-fires on a single price-update event)
HEDGE_MIN_WIN_PNL    = 1.0   # only hedge when winning side has ≥ this much PnL.
                              # Lowered from $2 — at our share sizes (2-5 sh), win_pnl
                              # typically sits in $1-2 range and is exactly where
                              # over-hedge creates the best lock+upside shape.
HEDGE_MAX_PRICE      = 0.85  # absolute safety cap; the real gate is the math
                              # threshold_b = winning_pnl / (winning − losing). When our
                              # original fills were cheap (e.g. CONTRA @ $0.30), the
                              # threshold can sit above $0.50 and over-hedging at $0.55
                              # or even $0.70 still locks both outcomes positive.
# Over-hedge fraction of B_max (the share count that would zero out the
# winning side after hedge). 1.0 = max upside / 0 floor; 0.5 = balanced;
# 0.9 = small floor + big upside (Scallops' typical shape).
OVER_HEDGE_FRACTION  = 0.90

# Averaging-down: when our existing position's side gets cheaper, stack more.
# Matches Scallops' pattern: he opens at $0.55, holds through dips, and adds
# more shares as his side hits $0.45-$0.50 (better cost basis without flipping).
AVG_DOWN_ENABLED      = False  # DISABLED — was firing 14× in one second (no cooldown)
AVG_DOWN_MIN_T_SEC    = 60     # don't avg down before this many seconds
AVG_DOWN_MAX_T_SEC    = 600    # don't avg down too close to settle
AVG_DOWN_MIN_DROP     = 0.05   # current ask must be ≥ 5¢ below our avg cost
AVG_DOWN_SHARES       = 2      # shares per avg-down fire
AVG_DOWN_MAX_SIDE_USD = 15.0   # don't avg down further if filled side cost ≥ this
AVG_DOWN_MIN_PRICE    = 0.30   # don't avg down once side ask is already this cheap
AVG_DOWN_COOLDOWN_SEC = 60     # min seconds between avgdn fires per coin (CRITICAL)

# A side is "bleeding" when we've stacked it heavily AND it's now priced near
# zero — market thinks it's lost. Adding more (avg-down or contra) is throwing
# good money after bad. Over-hedge can still bail us out via the OPPOSITE side.
BLEED_BLOCK_SIDE_USD  = 5.0    # if filled side cost > this
BLEED_BLOCK_MAX_PRICE = 0.30   # AND that side's ask is below this → no more adds
HEDGE_MIN_GAP_USD    = 1.0   # don't bother hedging if gap is < $1 (each share closes $1)
HEDGE_NOTIONAL_RATIO = 1.0   # cap total hedge notional at this × naked notional
_hedge_pending: dict[str, bool] = {}   # per-coin: True after a fill, cleared after hedge runs
_hedge_last_ts: dict[str, float] = {}  # per-coin: last hedge fire ts (cooldown)
_avgdn_last_ts: dict[str, float] = {}  # per-coin: last avgdn fire ts (cooldown)
_contra_last_ts: dict[str, float] = {} # per-coin: last contra fire ts (cooldown)

# Snapshot log (one JSON object per coin per status tick) for offline analysis
STATUS_SNAPSHOT_LOG = Path.home() / ".kalshi_status_snapshots.jsonl"

# Event-driven evaluator: price feeds .set() this every time they get new data,
# the evaluator loop waits on it. None when no window is active.
_price_update_event: asyncio.Event | None = None

def _notify_price_update() -> None:
    """Called by every price feed after writing new data to state."""
    if _price_update_event is not None:
        _price_update_event.set()

async def _kalshi_ws_feed() -> None:
    _msg_id = 0
    def _next() -> int:
        nonlocal _msg_id
        _msg_id += 1
        return _msg_id

    subscribed: set[str] = set()

    while True:
        resub_task = None
        try:
            async with websockets.connect(
                KALSHI_WS, additional_headers=_ws_auth_headers(),
                ping_interval=20, open_timeout=10,
            ) as ws:
                console.print("[dim]Kalshi WS connected[/dim]")
                subscribed.clear()

                async def _subscribe_new() -> None:
                    new = set(_ticker_to_sym.keys()) - subscribed
                    if new:
                        await ws.send(json.dumps({
                            "id": _next(), "cmd": "subscribe",
                            "params": {
                                "channels":       ["ticker", "fill"],
                                "market_tickers": list(new),
                            },
                        }))
                        subscribed.update(new)
                        console.print(f"[dim]Kalshi WS  subscribed {len(new)} tickers (ticker+fill)[/dim]")

                # Periodic resub task — runs in parallel with the message reader
                # so new windows' tickers get subscribed within 2s of being added,
                # even when no messages are flowing.
                async def _resub_loop() -> None:
                    while True:
                        try:
                            await _subscribe_new()
                        except Exception:
                            return
                        await asyncio.sleep(2)

                await _subscribe_new()
                resub_task = asyncio.create_task(_resub_loop())

                async for raw in ws:
                    msg = json.loads(raw)
                    msg_type = msg.get("type", "")

                    if msg_type == "ticker":
                        d   = msg.get("msg", msg)
                        t   = d.get("market_ticker", "")
                        ya  = float(d.get("yes_ask_dollars", 0) or 0)
                        yb  = float(d.get("yes_bid_dollars", 0) or 0)
                        # Prefer the real NO ask from Kalshi's orderbook; fall back
                        # to the no-arb implied (1 - yes_bid) only if the field is
                        # missing or empty. The orderbook NO ask is often cheaper
                        # than the implied price (different liquidity provider).
                        na_real = float(d.get("no_ask_dollars", 0) or 0)
                        na = na_real if 0 < na_real < 1 else (round(1.0 - yb, 4) if yb > 0 else 0.0)
                        sym = _ticker_to_sym.get(t)
                        if sym and sym in kalshi_state:
                            if 0 < ya < 1:
                                kalshi_state[sym]["yes_ask"] = ya
                            if 0 < na < 1:
                                kalshi_state[sym]["no_ask"] = na
                            _notify_price_update()

                    elif msg_type == "fill":
                        d = msg.get("msg", msg)
                        oid = d.get("order_id", "")
                        fill_at = time.time()
                        tracker = _fill_tracker.get(oid)
                        if tracker and not tracker.get("fill_at"):
                            tracker["fill_at"] = fill_at
                            gap = fill_at - tracker["sent_at"]
                            tracker["fill_gap"] = round(gap, 4)
                            role = tracker.get("role", "?")
                            sym = tracker.get("coin", "?")
                            console.print(
                                f"  [dim magenta]FILL {sym} {role} "
                                f"{oid[:8]} in {gap*1000:.0f}ms[/dim magenta]")
                            # Log every fill event
                            try:
                                with open(FILL_LOG, "a") as flog:
                                    flog.write(json.dumps({
                                        "fill_at": fill_at,
                                        "sent_at": tracker["sent_at"],
                                        "ack_at":  tracker.get("ack_at"),
                                        "fill_gap_sec": round(gap, 4),
                                        "order_id": oid,
                                        "coin": sym,
                                        "side": tracker.get("side"),
                                        "role": role,
                                        "shares": tracker.get("shares"),
                                        "price": tracker.get("price"),
                                        "ticker": tracker.get("ticker"),
                                    }) + "\n")
                            except Exception:
                                pass
                            # Instant fill on patient → trigger double-down
                            if role == "patient" and gap < INSTANT_FILL_SEC:
                                asyncio.create_task(
                                    _double_down(tracker, fill_at, gap)
                                )
                            # Mark this coin for an immediate hedge check
                            if sym:
                                _hedge_pending[sym] = True
                                _notify_price_update()

        except Exception as e:
            console.print(f"[yellow]Kalshi WS: {e} — reconnect 3s[/yellow]")
            await asyncio.sleep(3)
        finally:
            if resub_task and not resub_task.done():
                resub_task.cancel()


# ── Kalshi REST helpers ──────────────────────────────────────────────────────

async def _poll_asks_once(client: httpx.AsyncClient, ticker: str) -> tuple[float, float]:
    """REST poll for the current order-book asks.

    Prefer real `no_ask_dollars` from Kalshi's orderbook; fall back to the
    no-arb implied `1 - yes_bid` only if the field is missing.
    """
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        r = await client.get(f"{KALSHI_BASE}/markets/{ticker}",
                             headers=_headers("GET", path), timeout=8)
        if r.status_code == 200:
            m   = r.json().get("market", {})
            ya  = float(m.get("yes_ask_dollars") or 0)
            yb  = float(m.get("yes_bid_dollars") or 0)
            na_real = float(m.get("no_ask_dollars") or 0)
            if ya <= 0 or ya >= 1:
                return 0.0, 0.0
            if 0 < na_real < 1:
                return ya, na_real
            if yb <= 0 or yb >= 1:
                return 0.0, 0.0
            return ya, round(1.0 - yb, 4)
    except Exception as e:
        console.print(f"  [dim yellow]poll asks {ticker}: {e}[/dim yellow]")
    return 0.0, 0.0


async def _fetch_result(client: httpx.AsyncClient, ticker: str) -> str:
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        r = await client.get(f"{KALSHI_BASE}/markets/{ticker}",
                             headers=_headers("GET", path), timeout=8)
        if r.status_code == 200:
            return r.json().get("market", {}).get("result", "") or ""
    except Exception as e:
        console.print(f"  [dim yellow]result fetch {ticker}: {e}[/dim yellow]")
    return ""


async def _cancel_single(client: httpx.AsyncClient, order_id: str,
                         tag: str = "") -> bool:
    """DELETE a single Kalshi order. Returns True if cancellation was accepted
    (200 / 202). False if already filled (404) or other error.
    """
    if not order_id:
        return False
    path = f"/trade-api/v2/portfolio/orders/{order_id}"
    try:
        r = await client.request(
            "DELETE",
            f"{KALSHI_BASE}/portfolio/orders/{order_id}",
            headers=_headers("DELETE", path), timeout=8,
        )
        if r.status_code in (200, 202):
            console.print(f"  [dim]cancel {tag or order_id[:8]} ok[/dim]")
            return True
        if r.status_code == 404:
            # Already filled or already cancelled — silent no-op
            return False
        console.print(f"  [yellow]cancel {tag or order_id[:8]} "
                      f"{r.status_code}: {r.text[:100]}[/yellow]")
    except Exception as e:
        console.print(f"  [dim yellow]cancel {tag or order_id[:8]}: {e}[/dim yellow]")
    return False


async def _cancel_patient_after(client: httpx.AsyncClient, order_id: str,
                                tag: str,
                                delay_sec: float = PATIENT_TIMEOUT_SEC) -> None:
    """Background task: wait `delay_sec`, then attempt to cancel the patient
    order. If it already filled by then, the DELETE silently no-ops."""
    await asyncio.sleep(delay_sec)
    await _cancel_single(client, order_id, tag=tag)


async def _double_down(tracker: dict, fill_at: float, gap: float) -> None:
    """Called when a patient order fills instantly (<1s). Places an additional
    half-size order on the same side — one time only per fire.
    88% win rate on instant fills justifies the extra exposure."""
    sym    = tracker["coin"]
    side   = tracker["side"]
    ticker = tracker["ticker"]
    orig_shares = tracker.get("orig_fire_shares", 10)

    # Guard: only double down once per fire
    dd_key = f"{ticker}:{side}"
    if dd_key in _doubledown_fired:
        return
    _doubledown_fired.add(dd_key)

    # Skip DD during overnight hours
    now_et_hour = (datetime.now(timezone.utc) + ET_OFFSET).hour
    if now_et_hour >= OVERNIGHT_START_HOUR or now_et_hour < OVERNIGHT_END_HOUR:
        return

    dd_shares = max(1, int(round(orig_shares * DOUBLEDOWN_FRACTION)))
    n_agg = max(1, int(round(dd_shares * MARKET_FRACTION)))
    n_pat = dd_shares - n_agg

    # Fetch current ask
    st = kalshi_state.get(sym, {})
    cur_ask = st.get("yes_ask") if side == "yes" else st.get("no_ask")
    if not cur_ask or cur_ask <= 0 or cur_ask >= 1 or cur_ask < MIN_ENTRY_PRICE:
        console.print(f"  [dim yellow]{sym} double-down skipped — ask {cur_ask}[/dim yellow]")
        return

    agg_px = round(min(cur_ask + LIMIT_BUFFER, MAX_ENTRY_PRICE), 2)
    pat_px = round(max(cur_ask - PATIENT_OFFSET, PATIENT_MIN_PX), 2)

    elapsed = int(time.time() - current_window_start_ts)
    console.print(
        f"  [bold magenta]★ INSTANT FILL {gap*1000:.0f}ms → DOUBLE DOWN {sym} "
        f"{side.upper()} {dd_shares}sh ({n_agg}@{agg_px:.2f} + {n_pat}@{pat_px:.2f})"
        f"[/bold magenta]")

    async with httpx.AsyncClient() as client:
        agg_order = await _place_buy(client, ticker, side, agg_px, n_agg,
                                      f"{sym}@T{elapsed}/DD-M")
        if agg_order:
            _register_fill_tracker(agg_order, sym, side, ticker, n_agg,
                                   agg_px, "dd-market", orig_shares)
            st.setdefault("trades", []).append({
                "T_checkpoint": tracker.get("T_checkpoint"),
                "elapsed_actual": elapsed,
                "side": side, "delta_pct": tracker.get("delta_pct"),
                "open_price": tracker.get("open_price"),
                "trigger_price": tracker.get("trigger_price"),
                "ask_at_trigger": cur_ask,
                "limit_price_sent": agg_px,
                "shares": n_agg, "role": "dd-market",
                "confirmed": True, "coin_mult": tracker.get("coin_mult"),
                "dd_trigger_gap_ms": round(gap * 1000),
                "sent_at": agg_order.get("sent_at"),
                "ack_at": agg_order.get("ack_at"),
                "order_id": agg_order["order_id"],
                "client_order_id": agg_order["client_order_id"],
            })
            session_stats["pending"] += 1

        if n_pat > 0:
            pat_order = await _place_buy(client, ticker, side, pat_px, n_pat,
                                          f"{sym}@T{elapsed}/DD-L")
            if pat_order:
                _register_fill_tracker(pat_order, sym, side, ticker, n_pat,
                                       pat_px, "dd-patient", orig_shares)
                st.setdefault("trades", []).append({
                    "T_checkpoint": tracker.get("T_checkpoint"),
                    "elapsed_actual": elapsed,
                    "side": side, "delta_pct": tracker.get("delta_pct"),
                    "open_price": tracker.get("open_price"),
                    "trigger_price": tracker.get("trigger_price"),
                    "ask_at_trigger": cur_ask,
                    "limit_price_sent": pat_px,
                    "shares": n_pat, "role": "dd-patient",
                    "confirmed": True, "coin_mult": tracker.get("coin_mult"),
                    "dd_trigger_gap_ms": round(gap * 1000),
                    "sent_at": pat_order.get("sent_at"),
                    "ack_at": pat_order.get("ack_at"),
                    "order_id": pat_order["order_id"],
                    "client_order_id": pat_order["client_order_id"],
                })
                session_stats["pending"] += 1

                async def _dd_cancel(oid: str, tag: str) -> None:
                    await asyncio.sleep(PATIENT_TIMEOUT_SEC)
                    async with httpx.AsyncClient() as cc:
                        await _cancel_single(cc, oid, tag=tag)

                asyncio.create_task(
                    _dd_cancel(pat_order["order_id"], f"{sym}@T{elapsed}/DD-L")
                )

    _log_filtered({
        "ts": fill_at, "action": "double_down",
        "coin": sym, "side": side, "ticker": ticker,
        "fill_gap_ms": round(gap * 1000),
        "dd_shares": dd_shares, "cur_ask": cur_ask,
        "elapsed": elapsed,
        "window_start_ts": current_window_start_ts,
    })


# Per-window set of (ticker:side) that already doubled down — reset each window
_doubledown_fired: set[str] = set()
_filtered_seen: set[tuple] = set()    # dedup for filtered signal logging


def _register_fill_tracker(order: dict, coin: str, side: str, ticker: str,
                           shares: int, price: float, role: str,
                           orig_fire_shares: int = 10) -> None:
    """Register an order in the fill tracker so WS fill events can match it."""
    if not order:
        return
    _fill_tracker[order["order_id"]] = {
        "sent_at":          order.get("sent_at", time.time()),
        "ack_at":           order.get("ack_at"),
        "coin":             coin,
        "side":             side,
        "ticker":           ticker,
        "shares":           shares,
        "price":            price,
        "role":             role,
        "orig_fire_shares": orig_fire_shares,
        "fill_at":          None,
        "fill_gap":         None,
    }


async def _fetch_floor(client: httpx.AsyncClient, ticker: str) -> float | None:
    """Fetch Kalshi's floor_strike for a market. This is the exact price
    reference Kalshi uses to settle the market — closer to our delta
    computation than any external spot feed (matches the CFB internal
    reference which appears to lock ~2 minutes before window open)."""
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        r = await client.get(f"{KALSHI_BASE}/markets/{ticker}",
                             headers=_headers("GET", path), timeout=8)
        if r.status_code == 200:
            fs = r.json().get("market", {}).get("floor_strike")
            if fs:
                return float(fs)
    except Exception as e:
        console.print(f"  [dim yellow]floor fetch {ticker}: {e}[/dim yellow]")
    return None


async def _fetch_balance(client: httpx.AsyncClient) -> float | None:
    """Fetch total Kalshi portfolio value (cash + open positions) in dollars.
    Uses `balance` + `portfolio_value` fields (both in cents). Returns None
    on error."""
    path = "/trade-api/v2/portfolio/balance"
    try:
        r = await client.get(f"{KALSHI_BASE}/portfolio/balance",
                             headers=_headers("GET", path), timeout=8)
        if r.status_code == 200:
            j = r.json()
            cash      = float(j.get("balance") or 0)          # cents
            positions = float(j.get("portfolio_value") or 0)  # cents
            total = (cash + positions) / 100.0
            return total
    except Exception as e:
        console.print(f"  [dim yellow]balance fetch: {e}[/dim yellow]")
    return None


async def _place_buy(client: httpx.AsyncClient, ticker: str, side: str,
                     price: float, shares: int, tag: str) -> dict | None:
    """Limit buy at `price`. Returns order dict on success else None.
    Includes sub-second timestamps: sent_at (before POST) and ack_at (after)."""
    yes_price = f"{price:.2f}" if side == "yes" else f"{1.0 - price:.2f}"
    coid = str(uuid.uuid4())
    try:
        sent_at = time.time()
        r = await client.post(
            f"{KALSHI_BASE}/portfolio/orders",
            headers=_headers("POST", "/trade-api/v2/portfolio/orders"),
            content=json.dumps({
                "ticker":            ticker,
                "action":            "buy",
                "side":              side,
                "count":             shares,
                "type":              "limit",
                "yes_price_dollars": yes_price,
                "client_order_id":   coid,
            }),
            timeout=10,
        )
        ack_at = time.time()
        if r.status_code in (200, 201):
            order = r.json().get("order", {}) or {}
            oid   = order.get("order_id", "")
            rtt   = round((ack_at - sent_at) * 1000)
            console.print(f"  [bold green]{tag}  BUY {side.upper()} "
                          f"{shares}ct @ {price:.2f}  {oid[:8]}  "
                          f"[dim]{rtt}ms[/dim][/bold green]")
            return {"order_id": oid, "client_order_id": coid,
                    "sent_at": sent_at, "ack_at": ack_at, "raw": order}
        else:
            console.print(f"  [red]{tag}  order failed {r.status_code}: {r.text[:150]}[/red]")
    except Exception as e:
        console.print(f"  [red]{tag}  order error: {e}[/red]")
    return None


# ── Scallops fast poll (decoupled from display) ─────────────────────────────

async def _scallops_poll_loop() -> None:
    """Poll Scallops JSONL every 2s and act on new signals immediately.
    Decoupled from the status display loop so display interval doesn't
    delay trade execution."""
    async with httpx.AsyncClient() as client:
        while True:
            await asyncio.sleep(SCALLOPS_POLL_SEC)
            if not kalshi_state or current_window_start_ts == 0:
                continue
            elapsed = int(time.time() - current_window_start_ts)
            if elapsed < 5 or elapsed > 900:
                continue
            _refresh_scallops_live()
            await _process_scallops_signals(client)


# ── Ticker + timing helpers ──────────────────────────────────────────────────

def _seconds_until_next_window() -> float:
    now_et = datetime.now(timezone.utc) + ET_OFFSET
    mins   = (now_et.minute // 15) * 15
    cur    = now_et.replace(minute=mins, second=0, microsecond=0)
    return (cur + timedelta(minutes=15) - now_et).total_seconds()

def _current_window_start_utc() -> datetime:
    now = datetime.now(timezone.utc)
    mins = (now.minute // 15) * 15
    return now.replace(minute=mins, second=0, microsecond=0)

def _ticker_for(series: str, close_utc: datetime) -> str:
    close_et = close_utc + ET_OFFSET
    return (series + "-"
            + close_et.strftime("%y%b%d%H%M").upper()
            + "-" + close_et.strftime("%M"))


# ── Trade log ────────────────────────────────────────────────────────────────

def _log_trade(entry: dict) -> None:
    try:
        with open(TRADE_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        console.print(f"[yellow]trade log write error: {e}[/yellow]")


def _log_filtered(entry: dict) -> None:
    """Log a signal that passed delta threshold but was rejected by a filter.
    These are the 'money bucket' — trades we didn't take. Later we resolve
    them to see if the filter saved us or cost us."""
    try:
        with open(FILTERED_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass


# ── Running P&L (this session only) ─────────────────────────────────────────

session_stats = {"trades": 0, "wins": 0, "losses": 0, "pending": 0,
                 "gross_pnl": 0.0}


# ── Per-coin momentum-based share scaling ────────────────────────────────────
#
# Rolling window of last N resolved fires per coin. Share multiplier scales
# linearly with recent win rate vs the baseline (overall average ~70%).
#
#   multiplier = clip(recent_win_rate / BASELINE_WIN_RATE, MIN_MULT, MAX_MULT)
#
# Validated: after a hot streak (>75%), next trade wins 79%. After cold (<50%),
# only 53%. BTC shows the widest gap: 87% hot → 39% cold.
COIN_MOMENTUM_LOOKBACK = 10
COIN_MOMENTUM_BASELINE = 0.70    # our overall average win rate
COIN_MOMENTUM_MIN_MULT = 0.25    # floor — keep 1 share minimum to detect turnarounds
COIN_MOMENTUM_MAX_MULT = 1.50    # ceiling — prevent overconcentration

# Stores last N (won: bool) results per coin
_coin_results: dict[str, list[bool]] = {coin: [] for coin in COINS}


def _coin_share_multiplier(coin: str) -> float:
    """Return a smooth share multiplier based on recent win rate for this coin."""
    history = _coin_results.get(coin, [])
    if len(history) < 5:
        return 1.0   # not enough data, use baseline
    recent_wr = sum(history) / len(history)
    raw = recent_wr / COIN_MOMENTUM_BASELINE
    return max(COIN_MOMENTUM_MIN_MULT, min(COIN_MOMENTUM_MAX_MULT, raw))


def _record_coin_result(coin: str, won: bool) -> None:
    """Push a win/loss into the rolling window for this coin."""
    h = _coin_results.setdefault(coin, [])
    h.append(won)
    if len(h) > COIN_MOMENTUM_LOOKBACK:
        h.pop(0)


def _seed_coin_momentum(max_age_hours: float = 3.0) -> None:
    """Seed per-coin momentum history from TRADE_LOG on startup.
    Uses the last COIN_MOMENTUM_LOOKBACK resolved fires per coin,
    but only if the most recent resolved trade is within max_age_hours.
    If the log is stale (e.g., restart after sleeping), starts fresh."""
    if not TRADE_LOG.exists():
        return
    # Collect ALL resolved trades, dedup by (window_ticker, side)
    seen: set[tuple[str, str]] = set()
    records: list[tuple[float, str, bool]] = []
    try:
        for line in TRADE_LOG.read_text().splitlines():
            t = json.loads(line)
            if t.get("won") is None:
                continue
            ts_str = t.get("ts_utc", "")
            if not ts_str:
                continue
            ts = datetime.fromisoformat(ts_str).timestamp()
            key = (t.get("window", ""), t.get("side", ""))
            if key in seen:
                continue
            seen.add(key)
            coin = t.get("coin", "")
            if coin in COINS:
                records.append((ts, coin, t["won"]))
    except Exception:
        return
    if not records:
        return
    # Check freshness: skip if most recent resolved trade is too old
    records.sort()
    newest = records[-1][0]
    if time.time() - newest > max_age_hours * 3600:
        console.print(f"  [dim]trade log too stale (last resolved "
                      f"{(time.time()-newest)/3600:.1f}h ago) — starting fresh[/dim]")
        return
    # Sort chronologically and feed into rolling window
    records.sort()
    for _, coin, won in records:
        _record_coin_result(coin, won)
    if records:
        console.print(f"  [dim]Seeded from {len(records)} resolved fires for per-coin share scaling:[/dim]")
        for coin in sorted(COINS):
            h = _coin_results.get(coin, [])
            if h:
                wr = sum(h) / len(h) * 100
                m = _coin_share_multiplier(coin)
                wins = sum(h)
                losses = len(h) - wins
                console.print(f"    {coin}  {wins}W/{losses}L = {wr:.0f}% win  →  {m:.2f}x shares")
            else:
                console.print(f"    {coin}  no recent trades  →  1.00x shares")


# ── Live status loop ─────────────────────────────────────────────────────────
#
# Prints one block every STATUS_INTERVAL_SEC showing per-coin live state.
# Also refreshes Kalshi yes/no asks via REST each tick — Kalshi WS only emits
# events when the book changes, so REST polling keeps the displayed values
# from going stale on quiet markets.

def _fmt_ask(v) -> str:
    """Show '?' for clearly-uninitialized ask values."""
    if v is None or v <= 0 or v >= 1.0:
        return "?"
    return f"{v:.2f}"


async def _status_loop() -> None:
    last_rest_refresh = 0.0
    # Per-coin rolling history: list of (ts, delta_pct, yes_ask, no_ask).
    # Used to compute look-back deltas (leading-indicator check).
    history: dict[str, list[tuple[float, float, float, float]]] = {
        sym: [] for sym in COINS
    }
    LOOKBACK_SEC = STATUS_LOOKBACK_SEC

    def _snapshot_log(rec: dict) -> None:
        try:
            with open(STATUS_SNAPSHOT_LOG, "a") as f:
                f.write(json.dumps(rec) + "\n")
        except Exception:
            pass

    async with httpx.AsyncClient() as client:
        while True:
            await asyncio.sleep(STATUS_INTERVAL_SEC)
            if not kalshi_state or current_window_start_ts == 0:
                continue
            elapsed = int(time.time() - current_window_start_ts)
            if elapsed < 0 or elapsed > 900:
                continue

            # Rate-limited REST refresh: always run at ≥ 10s interval,
            # regardless of how fast the display ticks.
            now_ts = time.time()
            if now_ts - last_rest_refresh >= STATUS_REST_REFRESH_SEC:
                last_rest_refresh = now_ts
                async def _refresh(sym: str) -> None:
                    ya, na = await _poll_asks_once(client, kalshi_state[sym]["ticker"])
                    if 0 < ya < 1: kalshi_state[sym]["yes_ask"] = ya
                    if 0 < na < 1: kalshi_state[sym]["no_ask"]  = na
                try:
                    await asyncio.gather(*[_refresh(s) for s in kalshi_state])
                except Exception:
                    pass

            ts = datetime.now().strftime("%H:%M:%S")
            lines = [f"[dim]{ts}[/dim] +{elapsed:>3}s "]
            for sym, st in kalshi_state.items():
                now_p  = blended_price(sym)
                open_p = st.get("open_price")
                ya     = st.get("yes_ask")
                na     = st.get("no_ask")

                ya_str = _fmt_ask(ya)
                na_str = _fmt_ask(na)

                floor_p = st.get("floor_strike")
                # Delta uses blended open (matches trader logic)
                if open_p and now_p:
                    delta = (now_p - open_p) / open_p * 100.0
                    d_col = "green" if delta > 0 else "red" if delta < 0 else "white"
                    # ITM = how far above/below Kalshi's strike we currently are
                    itm = None
                    if floor_p:
                        itm = (now_p - floor_p) / floor_p * 100.0
                    cb_p = exchange_prices["coinbase"].get(sym)
                    kr_p = exchange_prices["kraken"].get(sym)
                    bs_p = exchange_prices["bitstamp"].get(sym)
                    ex_pxs = [p for p in (cb_p, kr_p, bs_p) if p]
                    if len(ex_pxs) >= 2 and now_p > 0:
                        spread_pct = (max(ex_pxs) - min(ex_pxs)) / now_p * 100.0
                        spread_str = f"{spread_pct*100:.1f}bps"
                        spread_bps = round(spread_pct * 100, 2)
                    else:
                        spread_str = "—"
                        spread_bps = None

                    # Push to history + prune old (keep ~60s)
                    h = history.setdefault(sym, [])
                    h.append((now_ts, delta, ya or 0.0, na or 0.0))
                    cutoff = now_ts - 60
                    while h and h[0][0] < cutoff:
                        h.pop(0)

                    # Look back ~LOOKBACK_SEC seconds
                    target_ts = now_ts - LOOKBACK_SEC
                    prior = None
                    for entry in h:
                        if entry[0] <= target_ts:
                            prior = entry
                        else:
                            break
                    if prior is not None:
                        d_dd = delta - prior[1]
                        d_ya = (ya or 0.0) - prior[2]
                        d_na = (na or 0.0) - prior[3]
                        st["dyes_lookback"] = d_ya
                        st["dno_lookback"] = d_na
                        # (lookback values kept on st for ask-rising filter
                        #  and confirmed signal — just not displayed)
                    else:
                        d_dd = d_ya = d_na = None
                        st["dyes_lookback"] = None
                        st["dno_lookback"] = None

                    if STATUS_SLIM:
                        sc_str = scallops_status(sym, current_window_start_ts)
                        lines.append(
                            f"[{d_col}]{sym} {delta:+.3f}%[/{d_col}] "
                            f"Y{ya_str}/N{na_str} {sc_str}"
                        )
                    else:
                        floor_s = f"⌊${floor_p:,.0f}" if floor_p else "⌊?"

                        # Current effective checkpoint → MOM/POP thresholds
                        cp = None
                        for T_, factor_, cap_ in CHECKPOINTS:
                            if elapsed >= T_: cp = (T_, factor_, cap_)
                            else: break
                        mom_thr_s = pop_thr_s = "—"
                        if cp:
                            T_, factor_, cap_ = cp
                            ev = _current_vol_by_coin.get(sym, 0.0)
                            mom_thr = max(VOL_THR_FLOOR, min(VOL_THR_CEILING, factor_ * ev)) if ev > 0 else VOL_THR_FLOOR
                            mom_thr_s = f"M{mom_thr:.3f}%"
                            et_h = (datetime.now(timezone.utc) + ET_OFFSET).hour
                            tk = next((f'{lo:02d}-{hi:02d}' for lo, hi in _SCALLOPS_TOD if lo <= et_h < hi), None)
                            ek = next((f'{lo}-{hi}' for lo, hi in _SCALLOPS_T if lo <= elapsed < hi), None)
                            cell = _scallops_levels.get(sym, {}).get(tk, {}).get(ek, {}).get('con') if tk and ek else None
                            if cell and cell.get('n', 0) >= SCALLOPS_MIN_N:
                                pop_thr_s = f"P{cell['max_delta_pct']*SCALLOPS_DELTA_LOOSE:.3f}%"

                        surf_s = ""
                        p_up = _surface_p_up(sym, (datetime.now(timezone.utc)+ET_OFFSET).hour, elapsed, delta)
                        if p_up is not None:
                            surf_s = f" sY{p_up:.2f}/sN{1-p_up:.2f}"

                        lines.append(
                            f"[{d_col}]{sym} {delta:+.3f}%[/{d_col}] "
                            f"{floor_s} Y{ya_str}/N{na_str} "
                            f"[dim]{mom_thr_s}/{pop_thr_s}{surf_s}[/dim]"
                        )

                    # Persist a structured snapshot for offline analysis
                    _snapshot_log({
                        "ts":            round(now_ts, 3),
                        "elapsed":       elapsed,
                        "coin":          sym,
                        "ticker":        st.get("ticker"),
                        "open_price":    round(open_p, 6),
                        "floor_strike":  round(floor_p, 6) if floor_p else None,
                        "now_price":     round(now_p, 6),
                        "delta_pct":     round(delta, 6),
                        "itm_pct":       round(itm, 6) if itm is not None else None,
                        "coinbase_px":   cb_p,
                        "kraken_px":     kr_p,
                        "bitstamp_px":   bs_p,
                        "ex_spread_bps": spread_bps,
                        "yes_ask":       (ya if ya and 0 < ya < 1 else None),
                        "no_ask":        (na if na and 0 < na < 1 else None),
                        "lookback_sec":  LOOKBACK_SEC,
                        "dd_lookback":   round(d_dd, 6) if d_dd is not None else None,
                        "dyes_lookback": round(d_ya, 4) if d_ya is not None else None,
                        "dno_lookback":  round(d_na, 4) if d_na is not None else None,
                    })
                elif now_p:
                    lines.append(f"[dim]{sym} ?[/dim] Y{ya_str}/N{na_str}")
                else:
                    lines.append(f"[dim]{sym} —[/dim]")
            console.print(" | ".join(lines))


# ── Per-window runner ────────────────────────────────────────────────────────

async def run_window(client: httpx.AsyncClient, window_num: int,
                     window_start_utc: datetime) -> None:
    global current_window_start_ts, current_window_shares
    close_utc        = window_start_utc + timedelta(minutes=15)
    window_start_ts  = int(window_start_utc.timestamp())
    current_window_start_ts = window_start_ts
    _fill_tracker.clear()
    _hedge_pending.clear()
    _hedge_last_ts.clear()
    _avgdn_last_ts.clear()
    _contra_last_ts.clear()
    _recovery_last_ts.clear()
    _doubledown_fired.clear()
    _filtered_seen.clear()
    _scallops_acted.clear()
    _scallops_avgdn_orders.clear()
    _scallops_pending_confirm.clear()
    _cross_coin_sides.clear()
    for sym in COINS:
        _cross_coin_sides[sym] = None
    label            = (window_start_utc + ET_OFFSET).strftime("%H:%M ET")

    console.rule(f"[bold cyan]━━ Window {window_num}  {label} ━━[/bold cyan]")

    # Fetch Kalshi cash balance and pick shares/entry tier for this window
    balance = await _fetch_balance(client)
    if balance is not None:
        current_window_shares = shares_for_portfolio(balance)
        console.print(
            f"  [dim]portfolio ${balance:,.2f} → shares/entry = "
            f"[bold]{current_window_shares}[/bold][/dim]"
        )
    else:
        console.print(
            f"  [yellow]balance fetch failed — keeping last shares/entry = "
            f"{current_window_shares}[/yellow]"
        )

    # Fresh state for this window
    kalshi_state.clear()
    _ticker_to_sym.clear()
    for sym, cfg in COINS.items():
        if not cfg["enabled"]:
            continue
        ticker = _ticker_for(cfg["series"], close_utc)
        kalshi_state[sym] = {
            "ticker":          ticker,
            "yes_ask":         0.0,
            "no_ask":          0.0,
            "open_price":      None,     # blended spot at window open — for momentum delta
            "floor_strike":    None,     # Kalshi's settlement reference — for ITM display
            "trades":          [],       # list of dicts, one per fired entry
            "last_fired_yes":  -10_000,  # elapsed_sec of last yes fire
            "last_fired_no":   -10_000,  # elapsed_sec of last no fire
        }
        _ticker_to_sym[ticker] = sym
        console.print(f"  [dim]{sym}[/dim]  {ticker}")

    # Seed asks via one REST poll each (WS updates will follow)
    async def _seed(sym: str) -> None:
        ya, na = await _poll_asks_once(client, kalshi_state[sym]["ticker"])
        if ya > 0: kalshi_state[sym]["yes_ask"] = ya
        if na > 0: kalshi_state[sym]["no_ask"]  = na
    await asyncio.gather(*[_seed(s) for s in kalshi_state])

    # Snap two references per coin:
    #   (a) open_price = blended spot at window open  → for momentum delta
    #       (matches old behavior; clean "in-window move only" signal)
    #   (b) floor_strike = Kalshi's REST value        → for ITM display
    #       (what Kalshi actually settles on; informational)
    async def _snap_one(sym: str) -> None:
        st = kalshi_state[sym]

        # Momentum reference: snap current blended spot. If we joined mid-window,
        # fetch the historical 1-min candle at window_start_ts so the "open" is
        # actually the window's open, not a late blended value.
        now_ts_ = int(time.time())
        if now_ts_ - window_start_ts <= 10:
            # On-time — use current blended
            px = blended_price(sym)
            source_label = "live blended"
        else:
            # Mid-window join — fetch historical
            cb = COINS[sym]["cb_product"]
            px = await _fetch_open_at(cb, window_start_ts)
            source_label = "historical 1m candle"
            if not px:
                px = blended_price(sym)
                source_label = "fallback live"
        if px:
            st["open_price"] = px

        # Settlement reference: Kalshi floor_strike (independent fetch)
        fs = await _fetch_floor(client, st["ticker"])
        if fs:
            st["floor_strike"] = fs

        # Log both for this coin
        if px and fs:
            itm_pct = (px - fs) / fs * 100
            itm_col = "green" if itm_pct >= 0 else "red"
            console.print(
                f"  [dim]{sym} open ${px:,.2f}  ({source_label})  "
                f"floor ${fs:,.2f}  "
                f"ITM [{itm_col}]{itm_pct:+.3f}%[/{itm_col}][/dim]"
            )
        elif px:
            console.print(
                f"  [yellow]{sym} open ${px:,.2f}  ({source_label})  "
                f"floor_strike fetch failed[/yellow]"
            )
        else:
            console.print(f"  [red]{sym} no open price — will skip trigger[/red]")

    await asyncio.gather(*[_snap_one(s) for s in kalshi_state])

    # Compute per-coin effective vol = max(profile_slot, realized_1h).
    # Profile catches time-of-week regime shifts, realized catches spikes.
    vol_by_coin:     dict[str, float] = {}
    realized_by_coin: dict[str, float] = {}
    profile_by_coin: dict[str, float] = {}
    async def _compute_vol(sym: str) -> None:
        cb = COINS[sym]["cb_product"]
        realized = await _realized_vol_pct(cb, window_start_ts)
        slot_dt  = datetime.fromtimestamp(window_start_ts, tz=timezone.utc)
        slot     = btc_vol_profile.expected_vol(_vol_profile, sym, slot_dt)
        effective = max(realized, slot) if (realized or slot) else 0.0
        realized_by_coin[sym] = realized
        profile_by_coin[sym]  = slot
        vol_by_coin[sym]      = effective
        _current_vol_by_coin[sym] = effective
    await asyncio.gather(*[_compute_vol(s) for s in kalshi_state])

    console.print(f"\n  [bold]Vol profile + dynamic thresholds:[/bold]")
    for sym in kalshi_state:
        rv = realized_by_coin.get(sym, 0.0)
        pv = profile_by_coin.get(sym, 0.0)
        ev = vol_by_coin.get(sym, 0.0)
        src = "realized" if rv >= pv and rv > 0 else "profile" if pv > 0 else "floor"
        if ev <= 0:
            console.print(f"    [yellow]{sym}  no vol data — floor {VOL_THR_FLOOR}%[/yellow]")
            continue
        adj = COIN_CAP_ADJUSTMENT.get(sym, 0.0)
        thr_row = []
        cap_row = []
        for T, factor, base_cap in CHECKPOINTS:
            thr = max(VOL_THR_FLOOR, min(VOL_THR_CEILING, factor * ev))
            eff_cap = _effective_cap(sym, T, base_cap)
            mark = "*" if (sym, T) in COIN_CAP_OVERRIDE else ""
            thr_row.append(f"T{T}={thr:.3f}%")
            cap_row.append(f"T{T}={eff_cap:.2f}{mark}")
        adj_str = f" cap_adj={adj:+.2f}" if adj else ""
        console.print(
            f"    [dim]{sym}  realized={rv:.4f}%  profile={pv:.4f}%  "
            f"eff={ev:.4f}% ({src}){adj_str}[/dim]"
        )
        console.print(f"       [yellow]MOM[/yellow]  thr: {' '.join(thr_row)}")
        if adj:
            console.print(f"            cap: {' '.join(cap_row)}")
        else:
            cap_only = [f"T{T}={c:.2f}" for T, _, c in CHECKPOINTS]
            console.print(f"            cap: {' '.join(cap_only)}")
        # CONTRA line — uses Scallops cell for delta thr per (TOD, T-bucket).
        # Hard price band $0.30-$0.45 enforced regardless of cell.
        et_hr = (datetime.now(timezone.utc) + ET_OFFSET).hour
        tk = next((f'{lo:02d}-{hi:02d}' for lo, hi in _SCALLOPS_TOD if lo <= et_hr < hi), None)
        coin_tbl = _scallops_levels.get(sym, {}).get(tk, {}) if tk else {}
        contra_state = "ON" if _contra_enabled_now() else f"off (only {sorted(CONTRA_AUTO_ENABLED_TODS)})"
        contra_thrs = []
        for T, _, _ in CHECKPOINTS:
            ekey = next((f'{lo}-{hi}' for lo, hi in _SCALLOPS_T if lo <= T < hi), None)
            cell = coin_tbl.get(ekey, {}).get('con') if ekey else None
            if cell and cell.get('n', 0) >= SCALLOPS_MIN_N:
                c_thr = cell['max_delta_pct'] * SCALLOPS_DELTA_LOOSE
                contra_thrs.append(f"T{T}={c_thr:.3f}%")
            else:
                contra_thrs.append(f"T{T}=—")
        console.print(f"       [green]POP[/green]  thr: {' '.join(contra_thrs)}  [{contra_state}]")
        band_str = " / ".join(f"T≥{t}={p:.2f}" for t, p in CONTRA_MAX_PRICE_TIERS)
        console.print(f"            band: ${CONTRA_MIN_PRICE:.2f} → [{band_str}] on cheap side")

    # ── Scallops levels for current TOD bucket ────────────────────────────
    et_now = (datetime.now(timezone.utc) + ET_OFFSET)
    et_hour = et_now.hour
    tod_key = next((f'{lo:02d}-{hi:02d}' for lo, hi in _SCALLOPS_TOD if lo <= et_hour < hi), '?')
    contra_on = "ON" if tod_key in SCALLOPS_CONTRA_GOOD_TODS else "OFF (negative-edge TOD)"
    console.print(f"\n  [bold]Scallops levels — {et_hour:02d} ET → {tod_key} bucket  (CONTRA: {contra_on})[/bold]")
    for sym in kalshi_state:
        coin_tbl = _scallops_levels.get(sym, {}).get(tod_key, {})
        if not coin_tbl:
            console.print(f"    [dim]{sym}  no Scallops data for this TOD[/dim]")
            continue
        console.print(f"    [bold cyan]{sym}[/bold cyan]      [yellow]MOM (aligned, 5sh)[/yellow]              [green]CONTRA (against, 2sh)[/green]")
        for elo, ehi in _SCALLOPS_T:
            ekey = f'{elo}-{ehi}'
            cell = coin_tbl.get(ekey, {})
            mom = cell.get('mom')
            con = cell.get('con')
            if mom and mom.get('n', 0) >= SCALLOPS_MIN_N:
                m_thr = mom['min_delta_pct'] * SCALLOPS_DELTA_LOOSE
                m_cap = min(mom['max_price'] + SCALLOPS_PRICE_BUFFER, NAKED_MAX_ENTRY_PRICE)
                mom_s = f"|Δ|≥{m_thr:.3f}%  ask≤{m_cap:.2f}  (n={mom['n']:>3})"
            else:
                mom_s = "—"
            if con and con.get('n', 0) >= SCALLOPS_MIN_N and tod_key in SCALLOPS_CONTRA_GOOD_TODS:
                c_thr = con['max_delta_pct'] * SCALLOPS_DELTA_LOOSE
                c_cap = min(con['max_price'] + SCALLOPS_PRICE_BUFFER, NAKED_MAX_ENTRY_PRICE)
                con_s = f"|Δ|≥{c_thr:.3f}%  ask≤{c_cap:.2f}  (n={con['n']:>3})"
            else:
                con_s = "—"
            console.print(f"      T={elo:>4}-{ehi:<4}s   {mom_s:<38}{con_s}")

    # Show per-coin momentum multiplier
    console.print(f"\n  [bold]Per-coin momentum shares:[/bold]")
    for sym in kalshi_state:
        mult = _coin_share_multiplier(sym)
        sh = max(1, int(round(current_window_shares * mult)))
        h = _coin_results.get(sym, [])
        if len(h) >= 5:
            wr = sum(h) / len(h) * 100
            console.print(f"    {sym}  {sum(h)}W/{len(h)-sum(h)}L = {wr:.0f}%  →  {mult:.2f}x = {sh}sh")
        else:
            console.print(f"    {sym}  <5 trades  →  1.00x = {sh}sh")

    # ── Per-coin evaluator (fires when signal + cooldown allow) ────────────
    # Threshold is dynamic: factor × realized_vol (clipped to floor/ceiling).
    async def _eval_checkpoint(sym: str, T_eff: int, factor: float,
                               max_px: float, elapsed_now: int) -> None:
        if elapsed_now - T_eff > MAX_EVAL_GAP_SEC:
            return
        st     = kalshi_state[sym]
        # Δ = (blended_now − floor_strike) / floor_strike — backtest winner
        # vs the alternatives (CFB-vs-strike too trigger-happy; snap-vs-open worst).
        ref_p = st.get("floor_strike") or st["open_price"]
        now_p  = blended_price(sym)
        if not ref_p or not now_p:
            return
        delta = (now_p - ref_p) / ref_p * 100.0
        open_p = ref_p   # alias for downstream display refs

        ex_pxs = [p for p in (exchange_prices["coinbase"].get(sym),
                               exchange_prices["kraken"].get(sym),
                               exchange_prices["bitstamp"].get(sym)) if p]
        ex_spread_bps = round((max(ex_pxs) - min(ex_pxs)) / now_p * 10000, 2) if len(ex_pxs) >= 2 else None

        # Dynamic threshold from realized vol (original math-driven approach)
        vol = vol_by_coin.get(sym, 0.0)
        thr = max(VOL_THR_FLOOR, min(VOL_THR_CEILING, factor * vol)) if vol > 0 else VOL_THR_FLOOR
        thr_source = "vol"
        scallops_max_px = None  # Scallops cell override no longer used for entry decisions

        if abs(delta) < thr:
            return

        # Late-window guard: don't open fresh naked positions after T=600s.
        # Scallops fires here as hedge insurance on top of an existing book,
        # not as a momentum opener. Hedge module still operates here.
        if elapsed_now > MAX_FRESH_ENTRY_T_SEC:
            return

        side = "yes" if delta > 0 else "no"
        cur_ask = st["yes_ask"] if side == "yes" else st["no_ask"]
        our_d = st.get("dyes_lookback") if side == "yes" else st.get("dno_lookback")
        opp_d = st.get("dno_lookback") if side == "yes" else st.get("dyes_lookback")

        # Base dict for filtered signal logging — reused by all filter exits.
        # Dedup: only log once per (ticker, side, checkpoint, reason) per window.
        def _skip(reason: str) -> None:
            dedup_key = (st["ticker"], side, T_eff, reason)
            if dedup_key in _filtered_seen:
                return
            _filtered_seen.add(dedup_key)
            _log_filtered({
                "ts":             time.time(),
                "ticker":         st["ticker"],
                "coin":           sym,
                "side":           side,
                "reason":         reason,
                "elapsed":        elapsed_now,
                "T_checkpoint":   T_eff,
                "delta_pct":      round(delta, 4),
                "threshold":      round(thr, 4),
                "thr_source":     thr_source,
                "vol_pct":        round(vol, 4),
                "factor":         factor,
                "cur_ask":        cur_ask if (cur_ask and 0 < cur_ask < 1) else None,
                "our_d_lookback": our_d,
                "opp_d_lookback": opp_d,
                "ex_spread_bps":  ex_spread_bps,
                "open_price":     open_p,
                "now_price":      now_p,
                "window_start_ts": window_start_ts,
            })

        # Same-coin opposite block removed — contra (cheap buys) fires
        # opposite to MOM deliberately and was +$34 on Apr 22.

        # Cross-coin outlier detection: if 2+ other coins already fired the
        # opposite direction, this coin is the outlier.
        # 3-vs-1: flip to majority direction (backtest: +$26)
        # 2-vs-1: block the outlier (backtest: +$17)
        is_outlier, majority = _is_outlier(sym, side)
        if is_outlier and majority:
            n_others = sum(1 for c, s in _cross_coin_sides.items()
                          if c != sym and s is not None)
            n_majority = sum(1 for c, s in _cross_coin_sides.items()
                            if c != sym and s == majority)

            if n_majority >= 3:
                # 3-vs-1: FLIP to majority direction
                side = majority
                cur_ask = st["yes_ask"] if side == "yes" else st["no_ask"]
                console.print(
                    f"  [bold yellow]↔ CROSS-COIN FLIP {sym} → {side.upper()} "
                    f"(3 coins say {majority.upper()})[/bold yellow]"
                )
            elif n_majority >= 2:
                # 2-vs-1: BLOCK the outlier
                _skip("cross_coin_outlier_block")
                console.print(
                    f"  [dim]{sym} {side.upper()} blocked — "
                    f"2 coins say {majority.upper()}[/dim]"
                )
                return

        # Cooldown: skip if same side fired within COOLDOWN_SEC (using real elapsed).
        # Scallops-source entries are 1-share probes, so use the shorter cooldown.
        last = st["last_fired_yes"] if side == "yes" else st["last_fired_no"]
        cooldown = COOLDOWN_SEC_SMALL if thr_source.startswith("scallops") else COOLDOWN_SEC
        if elapsed_now - last < cooldown:
            return  # cooldown is not a filter — it's normal spacing, don't log

        if cur_ask <= 0 or cur_ask >= 1:
            return
        if cur_ask < MIN_ENTRY_PRICE:
            _skip("min_entry_price")
            return
        # Cap source: Scallops if available, else baseline w/ per-(coin,T) override
        if scallops_max_px is not None:
            eff_cap = scallops_max_px
            cap_source = "scallops"
        else:
            eff_cap = _effective_cap(sym, T_eff, max_px)
            cap_source = "override" if (sym, T_eff) in COIN_CAP_OVERRIDE else "baseline"
        # Until hedging is live, clamp eff_cap so we never enter naked above 0.74.
        if not HEDGE_ENABLED and eff_cap > NAKED_MAX_ENTRY_PRICE:
            eff_cap = NAKED_MAX_ENTRY_PRICE
            cap_source = f"{cap_source}+naked_clamp"
        if cur_ask > eff_cap or cur_ask > MAX_ENTRY_PRICE:
            _skip("cap_exceeded")
            # MOM direction is confirmed even though ask is too expensive to enter.
            # Cancel the Scallops exit timer — the direction is validated.
            _scallops_pending_confirm.pop((sym, window_start_ts), None)
            # Don't set _cross_coin_sides here — no order was placed.
            # Setting it would block other coins from trading (SOL blocked bug).
            return

        # Bleeding-side gate: if we've already stacked this side heavily AND
        # the market has crushed it to ≤ BLEED_BLOCK_MAX_PRICE, stop adding —
        # only over-hedge on the OPPOSITE side can recover the position.
        if _side_bleeding(sym, side):
            _skip("side_bleeding")
            return

        # Prior-5m direction gate — ETH only (backtest showed gate hurts BTC/XRP
        # by blocking winners; ETH vetoed trades had 47.6% win vs 64% baseline).
        if PRIOR5_GATE_ENABLED and sym == "ETH":
            d5 = _delta5m_pct(sym)
            if d5 is not None:
                decision = _prior5_decision(sym, d5)
                if decision == "up_only" and side == "no":
                    _skip("prior5_gate_up_only")
                    return
                if decision == "down_only" and side == "yes":
                    _skip("prior5_gate_down_only")
                    return

        # (MOM empirical-gate removed — losers commonly peak >$0.55 intra-window
        # so the "loser stayed below P" metric almost never passes within our
        # price caps. Existing vol-threshold + per-coin caps are sufficient.
        # CONTRA still uses empirical reversal rate in _eval_contra.)

        # Market-disagrees gate: skip if the market thinks OUR side is unfavored.
        # If cur_ask < 0.50, market-implied probability of our side winning is < 50%
        # — don't fight the book even if our delta points our way.
        if cur_ask < 0.50:
            _skip("market_disagrees")
            return

        # Ask-rising guard: if our side's ask rose 1-3¢ in the last 10s,
        # market makers are repricing against us — 0/8 win rate historically
        if our_d is not None and 0.01 <= our_d < 0.03:
            _skip("ask_rising")
            return

        # Confirmed signal: our ask dropping + opposite rising + ask ≥ 0.65
        # was 14/14 = 100% win rate — logged for analysis
        confirmed = (our_d is not None and our_d <= 0
                     and opp_d is not None and opp_d >= 0
                     and cur_ask >= 0.65)

        # Per-coin momentum scaling — smooth multiplier from rolling win rate
        coin_mult = _coin_share_multiplier(sym)
        shares_this = max(1, int(round(current_window_shares * coin_mult)))

        # Price-based share scaling: above $0.65, half size.
        if cur_ask >= 0.65:
            shares_this = max(1, shares_this // 2)


        # (Scallops-source override removed — using vol-based sizing only)

        # Dead hours: block trading entirely, log for tracking
        now_et_hour = (datetime.now(timezone.utc) + ET_OFFSET).hour
        if now_et_hour in DEAD_HOURS_ET:
            _skip("dead_hour")
            return

        # Overnight cap: 11pm-5am ET → 1 share max (still trades, limits losses)
        is_overnight = (now_et_hour >= OVERNIGHT_START_HOUR or now_et_hour < OVERNIGHT_END_HOUR)
        if is_overnight:
            shares_this = min(shares_this, OVERNIGHT_MAX_SHARES)

        # Split: MARKET_FRACTION aggressive + rest patient
        # Pairing gate: if this entry would lock the book at an unprofitable
        # per-pair price (avg_yes + avg_no > $0.90 after fill), skip.
        if _would_lock_unprofitably(sym, side, shares_this, cur_ask):
            _skip("pairing_unprofitable")
            return

        # Per-coin-per-window cap: count all filled shares, clamp new entry
        existing_sh = sum(
            tr.get("shares", 0) for tr in _fill_tracker.values()
            if tr.get("coin") == sym and tr.get("ticker") == st["ticker"]
            and tr.get("fill_at")
        )
        remaining = max(0, MAX_SHARES_PER_COIN_WINDOW - existing_sh)
        if remaining == 0:
            _skip("max_shares_per_coin")
            return
        shares_this = min(shares_this, remaining)

        # Dynamic sizing: portfolio tier × coin_mult × price_mult × overnight cap
        # Split: 1 market (taker) + rest patient (maker, 15s timeout)
        n_agg = 1
        n_pat = max(0, shares_this - 1)

        agg_px     = round(min(cur_ask + LIMIT_BUFFER, eff_cap), 2)
        patient_px = round(max(cur_ask - PATIENT_OFFSET, PATIENT_MIN_PX), 2)

        mode_str = " [bold yellow]★CONFIRMED[/bold yellow]" if confirmed else ""
        mult_str = f" [{coin_mult:.2f}x]" if coin_mult != 1.0 else ""
        console.print(
            f"  [bold cyan]T={elapsed_now:>3}s  {sym}  open=${open_p:,.2f} "
            f"now=${now_p:,.2f}  Δ={delta:+.3f}% (|Δ|={abs(delta):.3f}% ≥ {thr:.3f}%) "
            f"({thr_source}) → buy {side.upper()} "
            f"(ask={cur_ask:.2f}, cap={eff_cap:.2f}/{cap_source})  "
            f"{shares_this}sh ({n_agg}@{agg_px:.2f} + {n_pat}@{patient_px:.2f})"
            f"{mult_str}{mode_str}[/bold cyan]"
        )

        async def _record(order: dict | None, n_shares: int,
                          limit_px_used: float, role: str) -> None:
            if not order:
                return
            st["trades"].append({
                "T_checkpoint":     T_eff,
                "elapsed_actual":   elapsed_now,
                "vol_pct":          vol,
                "factor":           factor,
                "effective_thr":    thr,
                "thr_source":       thr_source,
                "max_px":           max_px,
                "eff_cap":          eff_cap,
                "cap_source":       cap_source,
                "side":             side,
                "delta_pct":        delta,
                "open_price":       open_p,
                "trigger_price":    now_p,
                "ask_at_trigger":   cur_ask,
                "limit_price_sent": limit_px_used,
                "shares":           n_shares,
                "role":             role,
                "confirmed":        confirmed,
                "coin_mult":        round(coin_mult, 3),
                "our_d_lookback":   our_d,
                "opp_d_lookback":   opp_d,
                "ex_spread_bps":    ex_spread_bps,
                "order_id":         order["order_id"],
                "client_order_id":  order["client_order_id"],
                "sent_at":          order.get("sent_at"),
                "ack_at":           order.get("ack_at"),
            })
            session_stats["pending"] += 1

        # Aggressive / guaranteed-fill leg
        agg_order = await _place_buy(client, st["ticker"], side, agg_px,
                                     n_agg, f"{sym}@T{elapsed_now}/M")
        await _record(agg_order, n_agg, agg_px, "market")
        if agg_order:
            _register_fill_tracker(agg_order, sym, side, st["ticker"],
                                   n_agg, agg_px, "market", shares_this)
            # Store context for double-down
            _fill_tracker[agg_order["order_id"]].update({
                "T_checkpoint": T_eff, "delta_pct": delta,
                "open_price": open_p, "trigger_price": now_p,
                "coin_mult": coin_mult,
            })

        # Patient 2¢-below leg (only if > 1 share total).
        pat_order = None
        if n_pat > 0:
            pat_order = await _place_buy(client, st["ticker"], side,
                                         patient_px, n_pat,
                                         f"{sym}@T{elapsed_now}/L")
            await _record(pat_order, n_pat, patient_px, "patient")
            if pat_order:
                _register_fill_tracker(pat_order, sym, side, st["ticker"],
                                       n_pat, patient_px, "patient", shares_this)
                _fill_tracker[pat_order["order_id"]].update({
                    "T_checkpoint": T_eff, "delta_pct": delta,
                    "open_price": open_p, "trigger_price": now_p,
                    "coin_mult": coin_mult,
                })
                asyncio.create_task(_cancel_patient_after(
                    client,
                    pat_order["order_id"],
                    tag=f"{sym}@T{elapsed_now}/L",
                ))

        # Cooldown updates on any successful fire
        if agg_order or pat_order:
            if side == "yes":
                st["last_fired_yes"] = elapsed_now
            else:
                st["last_fired_no"] = elapsed_now
            # Track cross-coin direction for outlier detection
            _cross_coin_sides[sym] = side
            # MOM confirmed — cancel any Scallops exit timer for this coin
            _scallops_pending_confirm.pop((sym, window_start_ts), None)

    # Max sum-of-avg-costs per locked pair (YES+NO). Above this, a paired book
    # costs > $0.90 per $1 payout, so after fees (~$0.02/pair) we barely
    # break even. Hard cap at $0.90 keeps any locked book reliably profitable.
    PAIRING_MAX_SUM = 0.90

    def _would_lock_unprofitably(sym: str, side: str, shares: int, price: float) -> bool:
        """Return True if this trade would pair with existing opposite-side
        position into a locked book whose total cost-per-pair exceeds
        PAIRING_MAX_SUM (i.e., profit would be eaten by fees)."""
        ys, yc, ns, nc = _filled_book(sym)
        if side == "yes":
            new_ys, new_yc = ys + shares, yc + shares * price
            new_ns, new_nc = ns, nc
        else:
            new_ys, new_yc = ys, yc
            new_ns, new_nc = ns + shares, nc + shares * price
        # Only block when both sides would end up non-zero (i.e., actually paired)
        if new_ys <= 0 or new_ns <= 0:
            return False
        avg_y = new_yc / new_ys
        avg_n = new_nc / new_ns
        return (avg_y + avg_n) > PAIRING_MAX_SUM

    def _side_bleeding(sym: str, side: str) -> bool:
        """True when this side is heavily stacked AND its ask is at the floor —
        adding more would just bleed more. Over-hedge on the OPPOSITE side
        is still allowed to potentially bail us out."""
        ys, yc, ns, nc = _filled_book(sym)
        side_cost = yc if side == "yes" else nc
        side_ask  = kalshi_state[sym]["yes_ask"] if side == "yes" else kalshi_state[sym]["no_ask"]
        return (side_cost > BLEED_BLOCK_SIDE_USD
                and 0 < side_ask < BLEED_BLOCK_MAX_PRICE)

    def _filled_book(sym: str) -> tuple[float, float, float, float]:
        """Aggregate filled YES/NO shares + cost for this coin's current window.
        Returns (yes_sh, yes_cost, no_sh, no_cost). Uses _fill_tracker as truth."""
        st = kalshi_state[sym]
        ticker = st["ticker"]
        ys = yc = ns = nc = 0.0
        for tr in _fill_tracker.values():
            if tr.get("coin") != sym or tr.get("ticker") != ticker:
                continue
            if not tr.get("fill_at"):
                continue
            sh = tr.get("shares", 0)
            px = tr.get("price", 0)
            if tr.get("side") == "yes":
                ys += sh; yc += sh * px
            else:
                ns += sh; nc += sh * px
        return ys, yc, ns, nc

    async def _maybe_hedge(sym: str, elapsed: int) -> None:
        if not HEDGE_ENGINE_ENABLED:
            return
        """Over-hedge the losing side ONLY when the winning side already shows
        positive PnL and the losing side's ask is cheap enough that BOTH
        outcomes can be made positive after hedge.

        Trigger: opp_ask ≤ winning_pnl / (winning_pnl + |losing_pnl|).
        Sizing : OVER_HEDGE_FRACTION × (winning_pnl / opp_ask).

        Result : losing-side outcome stays slightly positive (small floor) while
        winning-on-the-cheap-side outcome pays a multiple — the asymmetric
        upside Scallops chases."""
        if elapsed < HEDGE_MIN_T_SEC:
            return
        ys, yc, ns, nc = _filled_book(sym)
        total_cost = yc + nc
        if total_cost == 0:
            return
        if_yes = ys - total_cost
        if_no  = ns - total_cost
        # Skip if already locked (both ≥ 0) or both losing (damage control case)
        if if_yes >= 0 and if_no >= 0:
            return
        if if_yes < 0 and if_no < 0:
            return
        # One winning, one losing — set up the hedge
        if if_yes > if_no:
            winning_pnl, losing_pnl = if_yes, if_no
            hedge_side = "no"            # buy more NO (the losing-outcome side)
            cur_ask = kalshi_state[sym]["no_ask"]
            existing_hedge_cost = nc      # already-spent on hedge side
        else:
            winning_pnl, losing_pnl = if_no, if_yes
            hedge_side = "yes"
            cur_ask = kalshi_state[sym]["yes_ask"]
            existing_hedge_cost = yc
        if not cur_ask or cur_ask <= 0 or cur_ask >= 1:
            return
        if cur_ask > HEDGE_MAX_PRICE:
            return
        # Per-coin cooldown — prevents firing twice on the same price event
        if (time.time() - _hedge_last_ts.get(sym, 0.0)) < HEDGE_COOLDOWN_SEC:
            return
        # Skip if winning_pnl too small to make a meaningful over-hedge
        if winning_pnl < HEDGE_MIN_WIN_PNL:
            return
        # Math threshold: use the LIMIT price (ask + buffer), not the raw ask,
        # so slippage on aggressive orders can't push us past the math gate.
        effective_b = min(cur_ask + LIMIT_BUFFER, 0.99)
        gap = winning_pnl - losing_pnl    # always positive
        if gap <= 0:
            return
        threshold_b = winning_pnl / gap
        if effective_b >= threshold_b:
            return                          # not cheap enough at our limit
        # Surface-aware tiering: ask the surface what it thinks about the
        # WINNING side continuing to win at current (T, Δ, hour).
        st_open = kalshi_state[sym]["open_price"]
        st_now = blended_price(sym)
        fraction = OVER_HEDGE_FRACTION
        tier = "full"
        if st_open and st_now:
            d_now = (st_now - st_open) / st_open * 100.0
            et_hr = (datetime.now(timezone.utc) + ET_OFFSET).hour
            p_up = _surface_p_up(sym, et_hr, elapsed, d_now)
            if p_up is not None:
                # winning side's surface prob
                winning_is_yes = (if_yes > if_no)
                p_win_surf = p_up if winning_is_yes else (1 - p_up)
                if p_win_surf >= HEDGE_SURFACE_SKIP_P:
                    # Skip the real hedge BUT lay a passive maker bid on the
                    # cheap opposite side at surface-implied fair value × 95%.
                    # If the market suddenly dumps there, we get a dream-priced
                    # over-hedge. If not, it auto-cancels harmlessly in 15s.
                    opp_fair = (1 - p_win_surf)
                    opp_bid_px = round(max(0.01, opp_fair * 0.95), 2)
                    # Must be meaningfully below current ask to be a maker
                    if opp_bid_px < cur_ask - 0.03:
                        st = kalshi_state[sym]
                        opp_shares = 2  # small size — this is opportunistic
                        console.print(
                            f"  [dim magenta]hedge skipped {sym}: surface p_win={p_win_surf:.2f} ≥ "
                            f"{HEDGE_SURFACE_SKIP_P:.2f} — keeping naked; "
                            f"placing passive {opp_shares}sh {hedge_side.upper()} @ ${opp_bid_px:.2f} "
                            f"(fair={opp_fair:.2f})[/dim magenta]"
                        )
                        passive_order = await _place_buy(
                            client, st["ticker"], hedge_side, opp_bid_px,
                            opp_shares, f"{sym}@T{elapsed}/Hp"
                        )
                        if passive_order:
                            _hedge_last_ts[sym] = time.time()
                            _register_fill_tracker(
                                passive_order, sym, hedge_side, st["ticker"],
                                opp_shares, opp_bid_px, "hedge-passive", opp_shares
                            )
                            _fill_tracker[passive_order["order_id"]].update({
                                "T_checkpoint": elapsed, "is_hedge": True,
                                "is_passive": True,
                            })
                            asyncio.create_task(_cancel_patient_after(
                                client, passive_order["order_id"],
                                tag=f"{sym}@T{elapsed}/Hp"
                            ))
                    else:
                        console.print(
                            f"  [dim magenta]hedge skipped {sym}: surface p_win={p_win_surf:.2f} "
                            f"— keeping naked (opp_fair {opp_fair:.2f} too close to ask {cur_ask:.2f})[/dim magenta]"
                        )
                    return
                elif p_win_surf >= HEDGE_SURFACE_PARTIAL_P:
                    fraction = HEDGE_PARTIAL_FRACTION
                    tier = f"partial(p_win={p_win_surf:.2f})"
                else:
                    tier = f"full(p_win={p_win_surf:.2f})"
        # B_max = shares that zero out the winning side (upper bound for lock)
        # B_min = shares needed to zero out the losing side (lower bound for lock)
        # If fraction × B_max < ceil(B_min), we'd hedge but NOT achieve lock —
        # floor to ceil(B_min) so we always at least reach the losing-side zero.
        b_max = winning_pnl / effective_b
        import math
        b_min_needed = math.ceil(abs(losing_pnl) / (1 - effective_b)) if effective_b < 1 else b_max
        preferred_n = max(1, int(b_max * fraction))
        if preferred_n < b_min_needed and b_min_needed <= b_max:
            target_n = b_min_needed   # floor to lock-achieving size
            tier += "+floored"
        else:
            target_n = preferred_n
        # Budget cap: don't let total hedge spend exceed naked-side notional
        naked_side_cost = (yc if hedge_side == "no" else nc)
        budget_remaining = max(0.0, naked_side_cost - existing_hedge_cost)
        max_n_by_budget = int(budget_remaining / cur_ask) if budget_remaining > 0 else 0
        n_hedge = min(target_n, max_n_by_budget) if max_n_by_budget > 0 else target_n
        if n_hedge < 1:
            return
        st = kalshi_state[sym]
        limit_px = round(min(effective_b, HEDGE_MAX_PRICE), 2)
        # Project after-hedge PnL using the LIMIT price (worst case)
        after_winning = winning_pnl - n_hedge * limit_px
        after_losing  = losing_pnl  + n_hedge * (1 - limit_px)
        console.print(
            f"  [bold magenta]OVERHEDGE T={elapsed}s  {sym}  {tier}  "
            f"win=${winning_pnl:+.2f} lose=${losing_pnl:+.2f}  "
            f"b@limit={limit_px:.2f} ≤ thr={threshold_b:.2f}  "
            f"→ buy {n_hedge}sh {hedge_side.upper()}  "
            f"after: win=${after_winning:+.2f} / lose=${after_losing:+.2f}[/bold magenta]"
        )
        # Split: 1 aggressive to guarantee partial fill, rest patient (maker)
        if n_hedge > 1:
            n_agg, n_pat = 1, n_hedge - 1
        else:
            n_agg, n_pat = 1, 0
        pat_px = round(max(cur_ask - PATIENT_OFFSET, PATIENT_MIN_PX), 2)
        agg_order = await _place_buy(client, st["ticker"], hedge_side, limit_px,
                                      n_agg, f"{sym}@T{elapsed}/H")
        if agg_order:
            _hedge_last_ts[sym] = time.time()
            _register_fill_tracker(agg_order, sym, hedge_side, st["ticker"],
                                   n_agg, limit_px, "hedge", n_hedge)
            _fill_tracker[agg_order["order_id"]].update({
                "T_checkpoint": elapsed, "is_hedge": True,
            })
        if n_pat > 0:
            pat_order = await _place_buy(client, st["ticker"], hedge_side,
                                          pat_px, n_pat, f"{sym}@T{elapsed}/h")
            if pat_order:
                _register_fill_tracker(pat_order, sym, hedge_side, st["ticker"],
                                       n_pat, pat_px, "hedge-pat", n_hedge)
                _fill_tracker[pat_order["order_id"]].update({
                    "T_checkpoint": elapsed, "is_hedge": True,
                })
                asyncio.create_task(_cancel_patient_after(
                    client, pat_order["order_id"], tag=f"{sym}@T{elapsed}/h"
                ))

    async def _eval_recovery_chase(sym: str, elapsed: int) -> None:
        """When BOTH outcomes are underwater and delta strongly favors one
        side, aggressively stack the favored side to recover losses via
        direction-chasing. NOT a hedge — a directional bet with committed capital."""
        if not RECOVERY_CHASE_ENABLED:
            return
        if not (RECOVERY_CHASE_MIN_T <= elapsed <= RECOVERY_CHASE_MAX_T):
            return
        if (time.time() - _recovery_last_ts.get(sym, 0.0)) < RECOVERY_CHASE_COOLDOWN:
            return
        ys, yc, ns, nc = _filled_book(sym)
        total = yc + nc
        if total == 0:
            return
        if_yes = ys - total
        if_no  = ns - total
        # Only activate when BOTH underwater (damage control scenario)
        if if_yes >= 0 or if_no >= 0:
            return
        # Compute Δ from open
        st = kalshi_state[sym]
        open_p = st["open_price"]; now_p = blended_price(sym)
        if not open_p or not now_p:
            return
        delta = (now_p - open_p) / open_p * 100.0
        # Determine vol threshold at current checkpoint
        cp = None
        for T_, f_, c_ in CHECKPOINTS:
            if elapsed >= T_: cp = (T_, f_, c_)
            else: break
        if cp is None:
            return
        T_, factor, base_cap = cp
        vol = _current_vol_by_coin.get(sym, 0.0)
        thr = max(VOL_THR_FLOOR, min(VOL_THR_CEILING, factor * vol)) if vol > 0 else VOL_THR_FLOOR
        if abs(delta) < thr * RECOVERY_CHASE_DELTA_MULT:
            return   # Δ not decisive enough
        side = "yes" if delta > 0 else "no"
        cur_ask = st["yes_ask"] if side == "yes" else st["no_ask"]
        if not cur_ask or cur_ask <= 0 or cur_ask >= 1:
            return
        eff_cap = _effective_cap(sym, T_, base_cap)
        if cur_ask < MIN_ENTRY_PRICE or cur_ask > min(eff_cap, NAKED_MAX_ENTRY_PRICE):
            return
        if _side_bleeding(sym, side):
            return
        # Size: buy enough shares of favored side to make if_favored_wins ≥ target.
        # After adding N shares at price p:
        #   new_side_sh = (yes_sh if side=="yes" else no_sh) + N
        #   new_total = total + N*p
        #   if_favored_wins = new_side_sh - new_total = (old_side - total) + N*(1 - p)
        # Solve: old_if_favored + N*(1-p) ≥ target  →  N ≥ (target - old_if_favored) / (1-p)
        old_if_favored = if_yes if side == "yes" else if_no
        needed = (RECOVERY_CHASE_TARGET_PNL - old_if_favored) / max(0.01, 1 - cur_ask)
        import math
        n_chase = max(1, math.ceil(needed))
        # Cap by budget: don't spend more than 1.5× total losing-book cost
        max_budget = 1.5 * total
        max_by_budget = int(max_budget / cur_ask) if cur_ask > 0 else 0
        n_chase = min(n_chase, max_by_budget) if max_by_budget > 0 else n_chase
        if n_chase < 1:
            return
        # Project outcome
        new_total = total + n_chase * cur_ask
        new_side_sh = (ys if side == "yes" else ns) + n_chase
        other_sh = ns if side == "yes" else ys
        if_side_wins = new_side_sh - new_total
        if_other_wins = other_sh - new_total
        # Skip if this doesn't actually improve the favored outcome meaningfully
        if if_side_wins <= old_if_favored + 0.25:
            return
        # Split 1 aggressive + rest patient
        n_agg = 1
        n_pat = n_chase - 1
        agg_px = round(min(cur_ask + LIMIT_BUFFER, eff_cap), 2)
        pat_px = round(max(cur_ask - PATIENT_OFFSET, PATIENT_MIN_PX), 2)
        console.print(
            f"  [bold red]RECOVERY T={elapsed}s  {sym}  Δ={delta:+.3f}%  "
            f"underwater both: if_yes=${if_yes:+.2f} if_no=${if_no:+.2f}  "
            f"→ chase {side.upper()} {n_chase}sh @ {agg_px:.2f}  "
            f"projects: if_{side}=${if_side_wins:+.2f} / if_other=${if_other_wins:+.2f}[/bold red]"
        )
        agg_order = await _place_buy(client, st["ticker"], side, agg_px,
                                      n_agg, f"{sym}@T{elapsed}/R")
        if agg_order:
            _recovery_last_ts[sym] = time.time()
            _register_fill_tracker(agg_order, sym, side, st["ticker"],
                                   n_agg, agg_px, "recovery", n_chase)
            _fill_tracker[agg_order["order_id"]].update({
                "T_checkpoint": elapsed, "delta_pct": delta,
                "open_price": open_p, "trigger_price": now_p,
                "is_recovery": True,
            })
        pat_order = None
        if n_pat > 0:
            pat_order = await _place_buy(client, st["ticker"], side, pat_px,
                                          n_pat, f"{sym}@T{elapsed}/r")
            if pat_order:
                _register_fill_tracker(pat_order, sym, side, st["ticker"],
                                       n_pat, pat_px, "recovery-pat", n_chase)
                _fill_tracker[pat_order["order_id"]].update({
                    "T_checkpoint": elapsed, "is_recovery": True,
                })
                asyncio.create_task(_cancel_patient_after(
                    client, pat_order["order_id"], tag=f"{sym}@T{elapsed}/r"
                ))
        if agg_order or pat_order:
            if side == "yes": st["last_fired_yes"] = elapsed
            else: st["last_fired_no"] = elapsed

    async def _eval_surface(sym: str, elapsed: int) -> None:
        """Surface-edge engine: fire on any side with ≥ SURFACE_MIN_EDGE
        advantage vs the market ask. Independent of MOM/CONTRA."""
        if not SURFACE_ENGINE_ENABLED:
            return
        if elapsed > MAX_FRESH_ENTRY_T_SEC:
            return
        if (time.time() - _surface_last_ts.get(sym, 0.0)) < SURFACE_COOLDOWN_SEC:
            return
        st = kalshi_state[sym]
        open_p = st["open_price"]; now_p = blended_price(sym)
        if not open_p or not now_p:
            return
        delta = (now_p - open_p) / open_p * 100.0
        et_h = (datetime.now(timezone.utc) + ET_OFFSET).hour
        pu = _surface_p_up(sym, et_h, elapsed, delta)
        if pu is None:
            return
        ya, na = st["yes_ask"], st["no_ask"]
        if not (0 < ya < 1) or not (0 < na < 1):
            return
        edge_yes = pu - ya
        edge_no  = (1 - pu) - na
        if edge_yes >= edge_no and edge_yes >= SURFACE_MIN_EDGE:
            side, ask = "yes", ya
        elif edge_no >= SURFACE_MIN_EDGE:
            side, ask = "no", na
        else:
            return
        # Apply standard naked clamp + min entry
        if ask < MIN_ENTRY_PRICE or ask > NAKED_MAX_ENTRY_PRICE:
            return
        if _side_bleeding(sym, side):
            return
        # Surface-edge cooldown shared with same-side per-coin cooldowns
        last = st["last_fired_yes"] if side == "yes" else st["last_fired_no"]
        if elapsed - last < COOLDOWN_SEC_SMALL:
            return
        shares = 3   # mid-conviction probe (less than MOM 5sh, more than CONTRA 2sh)
        if _would_lock_unprofitably(sym, side, shares, ask):
            return
        # Split: 1 aggressive + 2 patient (maker @ ask-2¢, 15s timeout)
        n_agg, n_pat = 1, 2
        agg_px = round(min(ask + LIMIT_BUFFER, NAKED_MAX_ENTRY_PRICE), 2)
        pat_px = round(max(ask - PATIENT_OFFSET, PATIENT_MIN_PX), 2)
        edge = edge_yes if side == "yes" else edge_no
        console.print(
            f"  [bold blue]SURFACE T={elapsed}s  {sym}  Δ={delta:+.3f}% "
            f"p_win={(pu if side=='yes' else 1-pu):.2f} ask={ask:.2f} edge={edge:+.2f} "
            f"→ buy {side.upper()} 1@{agg_px:.2f}(taker)+2@{pat_px:.2f}(maker)[/bold blue]"
        )
        agg_order = await _place_buy(client, st["ticker"], side, agg_px,
                                      n_agg, f"{sym}@T{elapsed}/S")
        if agg_order:
            _surface_last_ts[sym] = time.time()
            _register_fill_tracker(agg_order, sym, side, st["ticker"],
                                   n_agg, agg_px, "surface", n_agg + n_pat)
            _fill_tracker[agg_order["order_id"]].update({
                "T_checkpoint": elapsed, "delta_pct": delta,
                "open_price": open_p, "trigger_price": now_p, "is_surface": True,
            })
        pat_order = await _place_buy(client, st["ticker"], side, pat_px,
                                      n_pat, f"{sym}@T{elapsed}/s")
        if pat_order:
            _register_fill_tracker(pat_order, sym, side, st["ticker"],
                                   n_pat, pat_px, "surface-pat", n_agg + n_pat)
            _fill_tracker[pat_order["order_id"]].update({
                "T_checkpoint": elapsed, "is_surface": True,
            })
            asyncio.create_task(_cancel_patient_after(
                client, pat_order["order_id"], tag=f"{sym}@T{elapsed}/s"
            ))
        if agg_order or pat_order:
            if side == "yes": st["last_fired_yes"] = elapsed
            else: st["last_fired_no"] = elapsed

    _contra_skip_seen: set = set()
    def _contra_skip(sym, side, reason, **extra):
        key = (sym, side, reason)
        if key in _contra_skip_seen: return
        _contra_skip_seen.add(key)
        try:
            _log_filtered({
                "ts": time.time(), "ticker": kalshi_state[sym]["ticker"],
                "coin": sym, "side": side, "reason": f"contra_{reason}",
                "elapsed": extra.get('elapsed'), **extra,
            })
        except Exception: pass

    async def _eval_contra(sym: str, elapsed: int) -> None:
        """Open a contrarian position on the cheap side when delta is moving
        AGAINST that side (market over-priced the move)."""
        if not _contra_enabled_now():
            return
        if elapsed > MAX_FRESH_ENTRY_T_SEC:
            return
        if (time.time() - _contra_last_ts.get(sym, 0.0)) < CONTRA_COOLDOWN_SEC:
            return
        et_hour = (datetime.now(timezone.utc) + ET_OFFSET).hour
        contra = _scallops_con_level(sym, et_hour, elapsed)
        if not contra:
            _contra_skip(sym, "?", "no_cell", elapsed=elapsed)
            return
        min_delta, max_price, n = contra
        st = kalshi_state[sym]
        open_p = st["open_price"]
        now_p = blended_price(sym)
        if not open_p or not now_p:
            return
        delta = (now_p - open_p) / open_p * 100.0
        thr = min_delta * SCALLOPS_DELTA_LOOSE
        if abs(delta) < thr:
            _contra_skip(sym, "?", "delta_too_small", elapsed=elapsed,
                         delta=round(delta, 4), thr=round(thr, 4))
            return
        # Buy the side OPPOSITE to delta direction (the cheap, beaten side)
        if delta > 0:
            side = "no";  cur_ask = st["no_ask"]
        else:
            side = "yes"; cur_ask = st["yes_ask"]
        if not cur_ask or cur_ask <= 0 or cur_ask >= 1:
            return
        if cur_ask < CONTRA_MIN_PRICE:
            _contra_skip(sym, side, "ask_too_cheap", elapsed=elapsed, cur_ask=cur_ask)
            return
        contra_max = _contra_max_price(elapsed)
        if cur_ask > contra_max:
            _contra_skip(sym, side, "ask_too_expensive", elapsed=elapsed,
                         cur_ask=cur_ask, contra_max=contra_max)
            return
        eff_cap = min(max_price + SCALLOPS_PRICE_BUFFER, contra_max)
        if cur_ask > eff_cap:
            _contra_skip(sym, side, "above_cell_cap", elapsed=elapsed,
                         cur_ask=cur_ask, eff_cap=eff_cap)
            return
        if _side_bleeding(sym, side):
            _contra_skip(sym, side, "bleeding", elapsed=elapsed)
            return
        if _would_lock_unprofitably(sym, side, 2, cur_ask):
            _contra_skip(sym, side, "pairing_unprofitable", elapsed=elapsed, cur_ask=cur_ask)
            return
        # Empirical reversal gate (primary signal for <$0.50 entries).
        # "In last N windows, how often did eventual winner touch this price?"
        emp = _empirical_reversal_rate(sym, cur_ask)
        if emp is not None:
            emp_rate, emp_n = emp
            edge = emp_rate - cur_ask
            if edge < REVERSAL_MIN_EDGE:
                _contra_skip(sym, side, "empirical_edge_too_small",
                             elapsed=elapsed, cur_ask=cur_ask,
                             emp_rate=round(emp_rate, 3), edge=round(edge, 3))
                return
        # else: buffer not warm yet — fall back to the structural gates above
        # Cooldown using same per-side timers (don't double-fire MOM+CONTRA back-to-back)
        last = st["last_fired_yes"] if side == "yes" else st["last_fired_no"]
        if elapsed - last < COOLDOWN_SEC_SMALL:
            return
        # Also respect market-disagrees gate using the OTHER side
        opp_ask = st["yes_ask"] if side == "no" else st["no_ask"]
        if opp_ask and 0 < opp_ask < 0.50:
            # If the market thinks our contra side will lose AND the other
            # side is already cheap, the trade is even worse than it looks.
            return
        # Split: 1 aggressive (taker @ ask+2¢) + 1 patient (maker @ ask-2¢, 15s timeout)
        n_agg, n_pat = 1, 1
        agg_px = round(min(cur_ask + LIMIT_BUFFER, eff_cap), 2)
        pat_px = round(max(cur_ask - PATIENT_OFFSET, PATIENT_MIN_PX), 2)
        console.print(
            f"  [bold green]CONTRA T={elapsed}s  {sym}  Δ={delta:+.3f}% "
            f"→ buy {side.upper()} 1@{agg_px:.2f}(taker)+1@{pat_px:.2f}(maker)  "
            f"[scallops_con(n={n})][/bold green]"
        )
        agg_order = await _place_buy(client, st["ticker"], side, agg_px,
                                      n_agg, f"{sym}@T{elapsed}/C")
        if agg_order:
            _contra_last_ts[sym] = time.time()
            _register_fill_tracker(agg_order, sym, side, st["ticker"],
                                   n_agg, agg_px, "contra", n_agg + n_pat)
            _fill_tracker[agg_order["order_id"]].update({
                "T_checkpoint": elapsed, "delta_pct": delta,
                "open_price": open_p, "trigger_price": now_p, "is_contra": True,
            })
        pat_order = await _place_buy(client, st["ticker"], side, pat_px,
                                      n_pat, f"{sym}@T{elapsed}/c")
        if pat_order:
            _register_fill_tracker(pat_order, sym, side, st["ticker"],
                                   n_pat, pat_px, "contra-pat", n_agg + n_pat)
            _fill_tracker[pat_order["order_id"]].update({
                "T_checkpoint": elapsed, "is_contra": True,
            })
            asyncio.create_task(_cancel_patient_after(
                client, pat_order["order_id"], tag=f"{sym}@T{elapsed}/c"
            ))
        if agg_order or pat_order:
            if side == "yes":
                st["last_fired_yes"] = elapsed
            else:
                st["last_fired_no"] = elapsed

    async def _maybe_average_down(sym: str, elapsed: int) -> None:
        """Stack more shares on our existing dominant side when its ask drops
        ≥ AVG_DOWN_MIN_DROP below our avg cost. Capped by total side notional
        and by the late-window guard."""
        if not AVG_DOWN_ENABLED:
            return
        if not (AVG_DOWN_MIN_T_SEC <= elapsed <= AVG_DOWN_MAX_T_SEC):
            return
        if (time.time() - _avgdn_last_ts.get(sym, 0.0)) < AVG_DOWN_COOLDOWN_SEC:
            return
        ys, yc, ns, nc = _filled_book(sym)
        if ys == 0 and ns == 0:
            return
        # Pick dominant side (the one we already committed to)
        if ys >= ns and ys > 0:
            side = "yes"; cur_ask = kalshi_state[sym]["yes_ask"]
            avg_cost = yc / ys; side_cost = yc
        elif ns > 0:
            side = "no"; cur_ask = kalshi_state[sym]["no_ask"]
            avg_cost = nc / ns; side_cost = nc
        else:
            return
        if not cur_ask or cur_ask <= 0 or cur_ask >= 1:
            return
        if cur_ask < AVG_DOWN_MIN_PRICE:
            return
        if avg_cost - cur_ask < AVG_DOWN_MIN_DROP:
            return
        if side_cost >= AVG_DOWN_MAX_SIDE_USD:
            return
        if _side_bleeding(sym, side):
            return
        st = kalshi_state[sym]
        limit_px = round(min(cur_ask + LIMIT_BUFFER, NAKED_MAX_ENTRY_PRICE), 2)
        console.print(
            f"  [bold yellow]AVGDN T={elapsed}s  {sym}  "
            f"side={side.upper()} avg=${avg_cost:.2f} now=${cur_ask:.2f} "
            f"(drop=${avg_cost-cur_ask:.2f}) → buy {AVG_DOWN_SHARES}sh @ {limit_px:.2f}"
            f"  [side_cost=${side_cost:.2f}/{AVG_DOWN_MAX_SIDE_USD:.0f}][/bold yellow]"
        )
        order = await _place_buy(client, st["ticker"], side, limit_px,
                                  AVG_DOWN_SHARES, f"{sym}@T{elapsed}/A")
        if order:
            _avgdn_last_ts[sym] = time.time()
            _register_fill_tracker(order, sym, side, st["ticker"],
                                   AVG_DOWN_SHARES, limit_px, "avgdn",
                                   AVG_DOWN_SHARES)
            _fill_tracker[order["order_id"]].update({
                "T_checkpoint": elapsed, "is_avgdn": True,
            })

    def _effective_checkpoint(elapsed: int) -> tuple[int, float, float] | None:
        """Return the most recent (T, factor, max_px) whose T is <= elapsed."""
        best = None
        for T, factor, cap in CHECKPOINTS:
            if elapsed >= T:
                best = (T, factor, cap)
            else:
                break
        return best

    # ── Event-driven continuous evaluator ─────────────────────────────────
    # The evaluator waits on `_price_update_event`. Each price feed (Coinbase
    # WS / Kraken / Bitstamp / Kalshi WS) .set()s the event after writing new
    # data, which wakes us up. On each wake we find the effective checkpoint
    # and evaluate every coin. Debounced so we don't spam on noisy feeds.
    global _price_update_event
    _price_update_event = asyncio.Event()
    try:
        last_announced_cp: tuple[int, float, float] | None = None
        last_eval_ts = 0.0
        last_hedge_ts = 0.0

        while True:
            now = time.time()
            elapsed = int(now - window_start_ts)
            if elapsed >= 900:
                break

            effective = _effective_checkpoint(elapsed)
            if effective is not None:
                if effective != last_announced_cp:
                    last_announced_cp = effective
                    T, factor, cap = effective
                    ts_s = datetime.now().strftime("%H:%M:%S")
                    # Show effective threshold range across coins at this factor
                    thrs = []
                    for s in kalshi_state:
                        v = vol_by_coin.get(s, 0)
                        if v > 0:
                            thrs.append(max(VOL_THR_FLOOR, min(VOL_THR_CEILING, factor * v)))
                    thr_str = (f"thr {min(thrs):.3f}–{max(thrs):.3f}%"
                               if thrs else f"thr floor {VOL_THR_FLOOR}%")
                    console.print(
                        f"\n[bold]  [{ts_s}] entering T≥{T}s zone  "
                        f"(factor={factor}, {thr_str}, max ask {cap:.2f})[/bold]"
                    )

                if now - last_eval_ts >= EVAL_DEBOUNCE_SEC:
                    last_eval_ts = now
                    T, factor, cap = effective
                    # Run momentum + contra eval for each coin in parallel.
                    # Both have their own gates and skip cheaply when not applicable.
                    coro_pairs = []
                    for s in kalshi_state:
                        coro_pairs.append(_eval_checkpoint(s, T, factor, cap, elapsed))
                        coro_pairs.append(_eval_contra(s, elapsed))
                        coro_pairs.append(_eval_surface(s, elapsed))
                        coro_pairs.append(_eval_recovery_chase(s, elapsed))
                    await asyncio.gather(*coro_pairs)

            # Hedge + averaging-down check: every HEDGE_TICK_SEC, OR
            # immediately for any coin whose _hedge_pending flag was set by
            # a recent fill. Both functions skip out cheaply if their gates
            # don't pass, so it's safe to call them on every tick.
            pending_coins = [s for s, v in _hedge_pending.items() if v]
            tick_due = now - last_hedge_ts >= HEDGE_TICK_SEC
            if (pending_coins or tick_due) and (
                elapsed >= HEDGE_MIN_T_SEC or elapsed >= AVG_DOWN_MIN_T_SEC
            ):
                last_hedge_ts = now
                coins_to_check = pending_coins if pending_coins else list(kalshi_state.keys())
                for s in coins_to_check:
                    _hedge_pending[s] = False
                tasks = []
                for s in coins_to_check:
                    if elapsed >= HEDGE_MIN_T_SEC:
                        tasks.append(_maybe_hedge(s, elapsed))
                    if AVG_DOWN_MIN_T_SEC <= elapsed <= AVG_DOWN_MAX_T_SEC:
                        tasks.append(_maybe_average_down(s, elapsed))
                await asyncio.gather(*tasks)

            # Wait for any price feed to push a new value (or 1s timeout)
            try:
                _price_update_event.clear()
                await asyncio.wait_for(_price_update_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
    finally:
        _price_update_event = None

    # Wait until window close + small buffer for result
    close_ts = window_start_ts + 895
    while time.time() < close_ts:
        await asyncio.sleep(5)
    await asyncio.sleep(8)   # let Kalshi finalize result

    # Resolve winners + log each trade
    console.print(f"\n[bold]  Window {window_num} results[/bold]")
    for sym, st in kalshi_state.items():
        if not st["trades"]:
            continue
        result = await _fetch_result(client, st["ticker"])
        # Append winner's min price touched to rolling reversal history.
        # Walks our in-memory tick history if we captured one, else skips.
        # (Snapshots are logged to disk; for real-time buffer update we use the
        # fact that the status loop tracked ya/na throughout. If absent, the
        # db-backed seed continues to work.)
        try:
            hist = _reversal_history.get(sym)
            if hist is not None and result in ('yes', 'no'):
                # Weak in-memory append — next startup re-reads from btc_windows.db
                # which has proper tick-level min/max.
                winner_ask = st.get("yes_ask") if result == "yes" else st.get("no_ask")
                loser_ask = st.get("no_ask") if result == "yes" else st.get("yes_ask")
                if winner_ask and 0 < winner_ask < 1 and loser_ask and 0 < loser_ask < 1:
                    hist.append((winner_ask, loser_ask))
        except Exception:
            pass
        coin_total_pnl = 0.0
        for trade in st["trades"]:
            entry = trade["ask_at_trigger"]
            side  = trade["side"]

            shares_trade = trade.get("shares", SHARES)
            log_entry = {
                "ts_utc":          datetime.now(timezone.utc).isoformat(),
                "window":          st["ticker"],
                "coin":            sym,
                "window_start_ts": window_start_ts,
                "T_checkpoint":    trade["T_checkpoint"],
                "elapsed_actual":  trade["elapsed_actual"],
                "vol_pct":         trade.get("vol_pct"),
                "factor":          trade.get("factor"),
                "effective_thr":   trade.get("effective_thr"),
                "open_price":      trade["open_price"],
                "trigger_price":   trade["trigger_price"],
                "delta_pct":       round(trade["delta_pct"], 4),
                "side":            side,
                "limit_price_sent":trade["limit_price_sent"],
                "ask_at_trigger":  entry,
                "shares":          shares_trade,
                "order_id":        trade["order_id"],
                "winner":          result,
                "won":             None,
                "pnl":             None,
            }

            session_stats["pending"] -= 1

            if result in ("yes", "no"):
                won = (side == result)
                pnl = (1.0 - entry) * shares_trade if won else -entry * shares_trade
                log_entry["won"] = won
                log_entry["pnl"] = round(pnl, 4)
                coin_total_pnl += pnl

                mark = "[green]WIN[/green]" if won else "[red]LOSS[/red]"
                console.print(
                    f"    {sym} T={trade['elapsed_actual']:>3}s  {side.upper()}@{entry:.2f}  "
                    f"result={result.upper()}  {mark}  P&L ${pnl:+.2f}"
                )

                session_stats["trades"]   += 1
                session_stats["wins"]     += int(won)
                session_stats["losses"]   += int(not won)
                session_stats["gross_pnl"] = round(session_stats["gross_pnl"] + pnl, 4)
            else:
                console.print(f"    {sym} T={trade['elapsed_actual']:>3}s  {side.upper()}@{entry:.2f}  "
                              f"[yellow]result=?[/yellow]")

            _log_trade(log_entry)

        if result in ("yes", "no") and st["trades"]:
            # Record one result per unique side fired this window for
            # per-coin momentum tracking (deduped by side)
            sides_fired = {t["side"] for t in st["trades"]}
            for s_fired in sides_fired:
                _record_coin_result(sym, s_fired == result)
            mult = _coin_share_multiplier(sym)
            hist = _coin_results.get(sym, [])
            wr_str = f"{sum(hist)/len(hist)*100:.0f}%" if len(hist) >= 5 else "—"

        if result in ("yes", "no") and len(st["trades"]) > 1:
            col = "green" if coin_total_pnl >= 0 else "red"
            console.print(
                f"    [bold]{sym} window total: "
                f"[{col}]${coin_total_pnl:+.2f}[/{col}] "
                f"({len(st['trades'])} entries)  "
                f"momentum: {wr_str} → {mult:.2f}x[/bold]"
            )

    # Session running totals
    s = session_stats
    if s["trades"] > 0:
        wr = s["wins"] / s["trades"] * 100
        col = "green" if s["gross_pnl"] >= 0 else "red"
        console.print(
            f"\n  [bold]Session[/bold]  trades {s['trades']}  "
            f"wins {s['wins']}  losses {s['losses']}  "
            f"win% {wr:.0f}%  "
            f"pending {s['pending']}  "
            f"gross P&L [{col}]${s['gross_pnl']:+.2f}[/{col}]"
        )


# ── Main ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    console.print("[bold cyan]Kalshi Momentum Live  (multi-entry, EV-capped, vol-adaptive)[/bold cyan]")
    enabled_coins = [s for s, c in COINS.items() if c["enabled"]]
    console.print(f"  enabled coins: {', '.join(enabled_coins)}  "
                  f"(disabled: {', '.join(s for s,c in COINS.items() if not c['enabled']) or 'none'})")
    console.print(f"  price source: Coinbase WS + Kraken REST + Bitstamp REST  "
                  f"(blended → matches Kalshi CFB more closely)")
    console.print(f"  dynamic threshold = clip(factor × max(profile_slot, realized_1h), "
                  f"{VOL_THR_FLOOR}%, {VOL_THR_CEILING}%)")
    console.print(f"  vol profile = btc_vol_profile.load_or_build (21d history, UTC dow×hour bins)")
    console.print(f"  checkpoints (T / vol-factor / max-ask):")
    for T, factor, max_px in CHECKPOINTS:
        console.print(f"    T={T:>3}s   factor={factor}   ask ≤ {max_px:.2f}")
    console.print(
        f"  shares/entry tiered by Kalshi balance at each window open:"
    )
    for threshold, n in SHARE_TIERS:
        if threshold > 0:
            console.print(f"    balance ≥ ${threshold:>6.0f}  →  {n} shares/entry")
        else:
            console.print(f"    balance <  ${SHARE_TIERS[-2][0]:>6.0f}  →  {n} shares/entry")
    console.print(
        f"  cooldown: {COOLDOWN_SEC}s between same-side fires on same coin"
    )
    console.print(
        f"  price range filter: [{MIN_ENTRY_PRICE:.2f}, {MAX_ENTRY_PRICE:.2f}]  "
        f"limit at ask + {LIMIT_BUFFER:.2f}"
    )
    console.print(
        f"  staleness guard: skip if eval > checkpoint + {MAX_EVAL_GAP_SEC}s  "
        f"ask-rising guard: skip if our ask +1-3¢ in 10s"
    )
    console.print(
        f"  overnight cap: {OVERNIGHT_START_HOUR}:00-{OVERNIGHT_END_HOUR:02d}:00 ET → "
        f"max {OVERNIGHT_MAX_SHARES} share(s), no double-down"
    )
    console.print(f"  coins: {', '.join(s for s, c in COINS.items() if c['enabled'])}")
    console.print(f"  trade log: {TRADE_LOG}")
    _seed_coin_momentum(max_age_hours=3.0)
    console.print()

    async with httpx.AsyncClient() as client:
        # Load time-of-week vol profile (builds from Coinbase on first run,
        # cached to ~/.btc_vol_profile.json for 24h after)
        global _vol_profile
        profile_coins = {
            coin: cfg["cb_product"]
            for coin, cfg in COINS.items() if cfg["enabled"]
        }
        _vol_profile = await btc_vol_profile.load_or_build(
            profile_coins,
            log=lambda m: console.print(f"[dim]{m}[/dim]"),
        )

        # Start background feeds
        asyncio.create_task(_coinbase_price_feed())
        await _start_secondary_feeds()
        asyncio.create_task(_kalshi_ws_feed())
        asyncio.create_task(_status_loop())
        asyncio.create_task(_scallops_poll_loop())
        # Start CFB proxy (3 WS feeds, 60s rolling deque). Once warm (~60s),
        # blended_price() automatically returns the CFB proxy instead of the
        # last-trade median fallback.
        if _cfb_proxy is not None:
            enabled_coins = [c for c, cfg in COINS.items() if cfg["enabled"]]
            asyncio.create_task(_cfb_proxy.start(enabled_coins))
            console.print(f"[dim]CFB proxy started for {', '.join(enabled_coins)} (warmup ~60s)[/dim]")

        # Seed per-coin reversal history from btc_windows.db (last N settled
        # windows with their tick data). Used by CONTRA to gate <$0.50 entries
        # empirically instead of via the noisier surface tails.
        try:
            import sqlite3
            db = sqlite3.connect(Path.home() / ".btc_windows.db")
            for coin in ("BTC", "ETH", "SOL", "XRP"):
                _reversal_history[coin] = _deque(maxlen=REVERSAL_BUFFER_N)
                rows = db.execute(
                    "SELECT id, winner FROM windows "
                    "WHERE ticker LIKE ? AND winner IN ('yes','no') "
                    "ORDER BY window_start_ts DESC LIMIT ?",
                    (f"KX{coin}15M%", REVERSAL_BUFFER_N)
                ).fetchall()
                for wid, winner in rows:
                    ticks = db.execute(
                        "SELECT yes_ask, no_ask FROM ticks "
                        "WHERE window_id=? AND yes_ask IS NOT NULL AND no_ask IS NOT NULL",
                        (wid,)
                    ).fetchall()
                    if not ticks: continue
                    valid = [(ya, na) for ya, na in ticks
                             if ya and na and 0 < ya < 1 and 0 < na < 1]
                    if not valid: continue
                    if winner == "yes":
                        winner_pxs = [y for y, _ in valid]
                        loser_pxs  = [n for _, n in valid]
                    else:
                        winner_pxs = [n for _, n in valid]
                        loser_pxs  = [y for y, _ in valid]
                    _reversal_history[coin].append(
                        (min(winner_pxs), max(loser_pxs))
                    )
                n = len(_reversal_history[coin])
                console.print(f"[dim]  reversal history {coin}: seeded {n} windows[/dim]")
            db.close()
        except Exception as e:
            console.print(f"[yellow]reversal-history seed failed: {e}[/yellow]")

        # Warm up crypto price feed
        console.print("[dim]Warming up price feeds (Coinbase WS + Kraken/Bitstamp REST)…[/dim]")
        for _ in range(20):
            if all(blended_price(s) for s in COINS if COINS[s]["enabled"]):
                break
            await asyncio.sleep(0.5)
        snap = "  ".join(
            f"{s}=${blended_price(s):,.2f}"
            for s in COINS if COINS[s]["enabled"] and blended_price(s)
        )
        console.print(f"[dim]Initial blended prices: {snap}[/dim]\n")

        # Start in the CURRENT in-progress window immediately (no wait for
        # next boundary). run_window will fetch historical open price if
        # we're already past the window start.
        window_num = 1
        while True:
            window_start_utc = _current_window_start_utc()
            try:
                await run_window(client, window_num, window_start_utc)
            except Exception as e:
                console.print(f"[red]window {window_num} crashed: {e}[/red]")
                await asyncio.sleep(2)
            window_num += 1


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped.[/dim]")
