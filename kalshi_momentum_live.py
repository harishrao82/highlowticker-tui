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
PATIENT_TIMEOUT_SEC = 15     # auto-cancel the TOP patient rung if not filled
                             # within this many seconds — keeps DD trigger fast
DEEP_RUNG_TIMEOUT_SEC = 90   # deeper ladder rungs (ask-3¢, ask-4¢, …) wait
                             # longer for a real dip — no DD on these.
                             # Extended 60→90 on 2026-04-27: backtest shows
                             # gap 60-90s bucket = 100% WR, +$0.28/share over
                             # 25 samples. 90+s decays sharply so don't push further.
LADDER_FORCE_CANCEL_T = 600  # force-cancel any open rung at this elapsed (last 5 min)
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
# (stddev of prior 1h 1-min log returns, expressed as %).
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
# Hard ceiling: 0.70 across the board (we're pure directional).
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
    "BTC":  0.00,   # was -0.15 — too harsh; made T=30 cap 0.43 < MIN_ENTRY_PRICE
    "ETH":  0.00,
    "SOL":  0.00,
    "XRP":  0.00,
}

# Per-coin per-checkpoint cap override. When a (coin, T) pair is set here,
# this REPLACES the COIN_CAP_ADJUSTMENT calc for that checkpoint.
# BTC: cap at 0.65 from T=120 onward — respects 66% WR breakeven with
# safety margin, while letting early-T entries take baseline (0.58 / 0.64).
COIN_CAP_OVERRIDE: dict[tuple[str, int], float] = {
    ("BTC", 120): 0.65,
    ("BTC", 180): 0.65,
    ("BTC", 300): 0.65,
    ("BTC", 420): 0.65,
    ("BTC", 600): 0.65,
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
DEAD_HOURS_ET = {17, 20, 0, 3}

# Max total shares per coin per window. Prevents runaway stacking from
# MOM + DD + avgdn all firing on the same coin.
# At 15 shares × $0.65 avg = $9.75 max risk per coin per window (~2% of $500).
# Allows MOM (5sh) + one signal-led entry (5sh) per coin per window.
MAX_SHARES_PER_COIN_WINDOW         = 10
# Tighter cap during weekends (Fri 17:00 ET → Mon 08:00 ET). Weekend WR
# historically lower (Twin: 25% wknd vs 62% wkdy); shrink size accordingly.
WEEKEND_MAX_SHARES_PER_COIN_WINDOW = 7


def _is_weekend(now_dt) -> bool:
    """True if `now_dt` (ET datetime) falls in Fri 17:00 ET → Mon 08:00 ET."""
    wd = now_dt.weekday()  # Mon=0 .. Sun=6
    h = now_dt.hour
    if wd == 4 and h >= 17: return True   # Fri 5pm+
    if wd in (5, 6):        return True   # Sat or Sun (any hour)
    if wd == 0 and h < 8:   return True   # Mon before 8am
    return False

# Worst-case per window ≈ SHARES × max(max_px) × max_entries_per_window
# Backtest: mean ~2.2 entries/window × 4 coins ~= 9 entries average,
# max 7 entries per coin × 4 coins = 28 entries theoretical max.
# At 1 share × 0.88 × 28 → ~$25 theoretical max loss / window (very rare).
# Typical window stake: ~$1-3 notional across all coins.
# Goal: prove the edge is real cheaply, then scale up SHARES once verified.

TRADE_LOG    = Path.home() / ".kalshi_momentum_trades.jsonl"
FILTERED_LOG = Path.home() / ".kalshi_filtered_signals.jsonl"

# Naked entry price ceiling — used by recovery/surface/avgdn paths.
NAKED_MAX_ENTRY_PRICE = 0.74

# ── Coinbase signal confluence ─────────────────────────────────────────────
# coinbase_signal_detector.py writes one line per detected taker-imbalance
# event. We tail it incrementally and use it as a confluence multiplier on
# MOM entry size.
#
# DISABLED 2026-04-27: backtest of MOM + CB on 1,117 trades showed the
# 1.5x confirms boost dropped per-share EV from +$0.045 to +$0.035, and
# during the CB-detector period alone (n=394) the boost flipped P&L from
# −$1.49 to −$27.62. Confirms WR was 50.9% on n=175 (vs early small-sample
# 78.7% on n=47 — sample noise). Reverting to pure MOM.
#
# Detector still runs and the log is still tailed (cheap, harmless), so we
# can re-enable later. Set CB_CONFLUENCE_ENABLED = True to re-activate.
CB_CONFLUENCE_ENABLED = False
CB_SIGNALS_LOG = Path.home() / ".coinbase_signals.jsonl"
_cb_file_pos: int = 0
# (coin, window_start_ts) -> set of sides ("yes"/"no") that fired
_cb_signals: dict[tuple[str, int], set[str]] = {}
CB_MULT_CONFIRM = 1.5
CB_MULT_SILENT  = 1.0
# Returned from the lookup; main entry path multiplies share count by this.

def _refresh_cb_signals() -> None:
    """Incrementally read new lines from coinbase_signals.jsonl."""
    global _cb_file_pos
    if not CB_SIGNALS_LOG.exists():
        return
    try:
        size = CB_SIGNALS_LOG.stat().st_size
        if size < _cb_file_pos:
            _cb_file_pos = 0
        with open(CB_SIGNALS_LOG) as f:
            f.seek(_cb_file_pos)
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                coin = r.get("coin")
                raw_side = r.get("side")
                ws = r.get("window_start_ts")
                if not (coin and raw_side and ws):
                    continue
                k_side = "yes" if str(raw_side).lower() in ("up", "yes") else "no"
                key = (coin, int(ws))
                _cb_signals.setdefault(key, set()).add(k_side)
            _cb_file_pos = f.tell()
    except Exception:
        pass


def _cb_confluence(coin: str, side: str, ws: int) -> tuple[str, float]:
    """Return ('confirms'|'silent'|'opposes', multiplier).
    multiplier is CB_MULT_CONFIRM, CB_MULT_SILENT, or 0.0 (=skip)."""
    sides = _cb_signals.get((coin, int(ws)))
    if not sides:
        return ("silent", CB_MULT_SILENT)
    if side in sides:
        return ("confirms", CB_MULT_CONFIRM)
    # Other side fired but not ours
    return ("opposes", 0.0)


# ── (coin, side, delta_bucket) WR table ────────────────────────────────────
# Built offline by build_wr_table.py from kalshi_momentum_trades.jsonl +
# kalshi_filtered_signals.jsonl + cfb_proxy_log.jsonl (earliest signal per
# (coin, side, window) at T <= 180s, with cfb-resolved winner).
#
# Bucket tags:
#   boost   -> 1.5x shares  (n>=30, ROI > +10%)
#   skip    -> 0x  (skip the trade; log to filtered with reason="wr_table_skip")
#   neutral -> 1.0x
#   thin    -> 1.0x  (n<30 — insufficient data, neutral)
WR_TABLE_PATH = Path.home() / ".mom_wr_table.json"
_wr_table: dict = {}
# Must match build_wr_table.py DELTA_BUCKETS exactly.
_DELTA_BUCKETS = [
    ("<0.015",     0.000, 0.015),
    ("0.015-0.03", 0.015, 0.030),
    ("0.03-0.05",  0.030, 0.050),
    ("0.05-0.08",  0.050, 0.080),
    ("0.08-0.15",  0.080, 0.150),
    ("0.15-0.30",  0.150, 0.300),
    ("0.30-0.50",  0.300, 0.500),
    (">=0.50",     0.500, 1e9),
]


def _delta_bucket(d: float) -> str:
    a = abs(d)
    for label, lo, hi in _DELTA_BUCKETS:
        if lo <= a < hi:
            return label
    return _DELTA_BUCKETS[-1][0]


def _load_wr_table() -> None:
    """(Re)load the WR table from disk.  Safe to call repeatedly."""
    global _wr_table
    if not WR_TABLE_PATH.exists():
        _wr_table = {}
        return
    try:
        _wr_table = json.loads(WR_TABLE_PATH.read_text())
    except Exception:
        _wr_table = {}


# ── Scallops/Twin veto filter ──────────────────────────────────────────────
# When Scallops first BUYs at ≥$0.50 in a 15m window OPPOSITE to our MOM side,
# OR Twin's first 5m-slot-0 BUY at ≥$0.50 opposes our side, SKIP the trade.
# Backtest 2026-04-27 (10 windows, 106 MOM trades): adding this filter lifted
# WR from 68.9% → 83.9% and P&L from +$8.64 → +$19.87. Sample tiny but the
# direction is consistent with intuition: when these bots commit to the
# opposite side, MOM's signal is more likely a reversal trap.
#
# Filter MODE only — these signals NEVER initiate entries themselves.
SCALLOPS_LIVE_LOG = Path.home() / ".scallops_live_trades.jsonl"
SCALLOPS_FAST_LOG = Path.home() / ".scallops_fast_signals.jsonl"   # WS-driven, ~1-2s lag
TWIN_LIVE_LOG     = Path.home() / ".twin_live_trades.jsonl"
TWIN_FAST_LOG     = Path.home() / ".twin_fast_signals.jsonl"      # WS-driven, ~1-2s lag
VETO_MIN_PRICE    = 0.50
_scal_file_pos:      int = 0
_scal_fast_pos:      int = 0
_twin_file_pos:      int = 0
_twin_fast_pos:      int = 0
# (coin, ws_15m) -> {"side": "yes"/"no", "price": float, "ts": float}
_scallops_veto: dict[tuple[str, int], dict] = {}
_twin_veto:     dict[tuple[str, int], dict] = {}


def _ingest_scallops_record(r: dict) -> None:
    """Insert one Scallops trade record into _scallops_veto if it passes filters."""
    if r.get("side") != "BUY": return
    if r.get("market_type", "15m") != "15m": return
    ws = r.get("window_start_ts")
    coin = r.get("coin")
    outcome = r.get("outcome")
    if not (ws and coin and outcome in ("Up", "Down")): return
    price = float(r.get("price", 0) or 0)
    if price < VETO_MIN_PRICE or price >= 1: return
    ts = r.get("trade_ts")
    if not ts: return
    k = (coin, int(ws))
    cur = _scallops_veto.get(k)
    if cur and cur["ts"] <= ts: return
    _scallops_veto[k] = {
        "side": "yes" if outcome == "Up" else "no",
        "price": price, "ts": ts,
    }


def _refresh_scallops_veto() -> None:
    """Tail BOTH Scallops sources: fast WS log (~1-2s lag) and REST log (~10-30s)."""
    global _scal_file_pos, _scal_fast_pos
    # Fast WS log first (lower latency)
    if SCALLOPS_FAST_LOG.exists():
        try:
            size = SCALLOPS_FAST_LOG.stat().st_size
            if size < _scal_fast_pos:
                _scal_fast_pos = 0
            with open(SCALLOPS_FAST_LOG) as f:
                f.seek(_scal_fast_pos)
                for line in f:
                    try: _ingest_scallops_record(json.loads(line))
                    except Exception: continue
                _scal_fast_pos = f.tell()
        except Exception:
            pass
    # REST poll log
    if not SCALLOPS_LIVE_LOG.exists():
        return
    try:
        size = SCALLOPS_LIVE_LOG.stat().st_size
        if size < _scal_file_pos:
            _scal_file_pos = 0
        with open(SCALLOPS_LIVE_LOG) as f:
            f.seek(_scal_file_pos)
            for line in f:
                try: _ingest_scallops_record(json.loads(line))
                except Exception: continue
            _scal_file_pos = f.tell()
    except Exception:
        pass


def _ingest_twin_record(r: dict) -> None:
    """Insert one Twin trade record into _twin_veto if it passes filters."""
    if r.get("side") != "BUY": return
    slug = r.get("slug", "") or ""
    if "updown-5m" not in slug: return
    parts = slug.split("-")
    try:
        ws_5m = int(parts[-1])
    except (ValueError, IndexError):
        return
    ws_15m = (ws_5m // 900) * 900
    if ws_5m != ws_15m: return
    coin = r.get("coin")
    outcome = r.get("outcome")
    if not (coin and outcome in ("Up", "Down")): return
    price = float(r.get("price", 0) or 0)
    if price < VETO_MIN_PRICE or price >= 1: return
    ts = r.get("trade_ts")
    if not ts: return
    k = (coin, ws_15m)
    cur = _twin_veto.get(k)
    if cur and cur["ts"] <= ts: return
    _twin_veto[k] = {
        "side": "yes" if outcome == "Up" else "no",
        "price": price, "ts": ts,
    }


def _refresh_twin_veto() -> None:
    """Tail BOTH Twin signal sources: fast WS log (~1s lag, sparse) and REST
    log (~10-30s lag, comprehensive). The fast log gets us sub-second veto
    info for early MOM fires (T=30s, T=60s); REST catches anything WS missed."""
    global _twin_file_pos, _twin_fast_pos
    # Fast WS log first (lower latency)
    if TWIN_FAST_LOG.exists():
        try:
            size = TWIN_FAST_LOG.stat().st_size
            if size < _twin_fast_pos:
                _twin_fast_pos = 0
            with open(TWIN_FAST_LOG) as f:
                f.seek(_twin_fast_pos)
                for line in f:
                    try: _ingest_twin_record(json.loads(line))
                    except Exception: continue
                _twin_fast_pos = f.tell()
        except Exception:
            pass
    # REST poll log (fills in anything fast WS missed)
    if not TWIN_LIVE_LOG.exists():
        return
    try:
        size = TWIN_LIVE_LOG.stat().st_size
        if size < _twin_file_pos:
            _twin_file_pos = 0
        with open(TWIN_LIVE_LOG) as f:
            f.seek(_twin_file_pos)
            for line in f:
                try: _ingest_twin_record(json.loads(line))
                except Exception: continue
            _twin_file_pos = f.tell()
    except Exception:
        pass


def _veto_check(coin: str, side: str, ws: int) -> tuple[bool, str | None]:
    """Returns (should_skip, reason). reason is 'twin_veto' or None.

    scallops_veto disabled 2026-04-27: 69/69 vetoes contradicted the actual
    winner (100% wrong, $28.59 missed P&L). Scallops position on the opposite
    side is anti-signal, not vetoing signal."""
    tv = _twin_veto.get((coin, int(ws)))
    if tv and tv["side"] != side:
        return (True, "twin_veto")
    return (False, None)


def _delta_bucket_mult(coin: str, side: str, delta_pct: float) -> tuple[str, float]:
    """Lookup (tag, multiplier) for this (coin, side, delta_bucket)."""
    cells = _wr_table.get("cells") or {}
    key = f"{coin}|{side}|{_delta_bucket(delta_pct)}"
    cell = cells.get(key)
    if not cell:
        return ("thin", 1.0)
    return (cell.get("tag", "neutral"), float(cell.get("mult", 1.0)))


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

# Rolling empirical buffer: per coin, list of (winner_min, loser_max) per window.
# Kept for offline analysis / future empirical gates; no live consumer right now.
REVERSAL_BUFFER_N = 20
from collections import deque as _deque
# each entry is (winner_min, loser_max)
_reversal_history: dict[str, _deque] = {}

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
CB_POLL_SEC         = 2             # how often to refresh the Coinbase signal log
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
                              # original fills were cheap, the threshold can sit above
                              # $0.50 and over-hedging at $0.55 or even $0.70 still
                              # locks both outcomes positive.
# Over-hedge fraction of B_max (the share count that would zero out the
# winning side after hedge). 1.0 = max upside / 0 floor; 0.5 = balanced;
# 0.9 = small floor + big upside.
OVER_HEDGE_FRACTION  = 0.90

# Averaging-down: when our existing position's side gets cheaper, stack more.
# Open at $0.55, hold through dips, add more as side hits $0.45-$0.50
# (better cost basis without flipping).
AVG_DOWN_ENABLED      = False  # DISABLED — was firing 14× in one second (no cooldown)
AVG_DOWN_MIN_T_SEC    = 60     # don't avg down before this many seconds
AVG_DOWN_MAX_T_SEC    = 600    # don't avg down too close to settle
AVG_DOWN_MIN_DROP     = 0.05   # current ask must be ≥ 5¢ below our avg cost
AVG_DOWN_SHARES       = 2      # shares per avg-down fire
AVG_DOWN_MAX_SIDE_USD = 15.0   # don't avg down further if filled side cost ≥ this
AVG_DOWN_MIN_PRICE    = 0.30   # don't avg down once side ask is already this cheap
AVG_DOWN_COOLDOWN_SEC = 60     # min seconds between avgdn fires per coin (CRITICAL)

# A side is "bleeding" when we've stacked it heavily AND it's now priced near
# zero — market thinks it's lost. Adding more (avg-down) is throwing good money
# after bad. Over-hedge can still bail us out via the OPPOSITE side.
BLEED_BLOCK_SIDE_USD  = 5.0    # if filled side cost > this
BLEED_BLOCK_MAX_PRICE = 0.30   # AND that side's ask is below this → no more adds
HEDGE_MIN_GAP_USD    = 1.0   # don't bother hedging if gap is < $1 (each share closes $1)
HEDGE_NOTIONAL_RATIO = 1.0   # cap total hedge notional at this × naked notional
_hedge_pending: dict[str, bool] = {}   # per-coin: True after a fill, cleared after hedge runs
_hedge_last_ts: dict[str, float] = {}  # per-coin: last hedge fire ts (cooldown)
_avgdn_last_ts: dict[str, float] = {}  # per-coin: last avgdn fire ts (cooldown)

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
    # Flipped split: 1 patient + rest market
    n_pat = 1 if dd_shares > 1 else 0
    n_agg = dd_shares - n_pat

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


# ── Coinbase signal poll (decoupled from display) ───────────────────────────

async def _cb_poll_loop() -> None:
    """Refresh the Coinbase signal log every CB_POLL_SEC seconds.
    The MOM entry path also calls _refresh_cb_signals() defensively; this
    loop just keeps _cb_signals warm between entry decisions."""
    while True:
        await asyncio.sleep(CB_POLL_SEC)
        _refresh_cb_signals()


# ── Signal-led entry path (Scallops / Twin first-BUY-at-≥$0.50) ─────────────
# Fires a small entry on the bot's own side when a SIGNAL fires before MOM
# has triggered. Backtest 2026-04-27 (small sample): Twin entries gave
# +$1.80 lift over MOM-only at $0 PM-Kalshi gap. Scallops standalone was
# negative (~-$0.50) but enabled per user request — watch closely.
SIGNAL_POLL_SEC          = 1.0    # fallback file-tail (in-process WS is primary)
SIGNAL_ENTRY_SHARES      = 5      # smaller than MOM's portfolio-tier sizing

# In-process Polymarket WS — signals fire as soon as PM emits the trade,
# no file/poll roundtrip. We still write to fast log files (off critical path)
# so external tools / analysis still has the data.
PM_WS_URL          = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PM_GAMMA_API       = "https://gamma-api.polymarket.com/events"
PM_RPC_URL         = "https://polygon-bor-rpc.publicnode.com"
PM_ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
TWIN_WALLET        = "0x3a847382ad6fff9be1db4e073fd9b869f6884d44"
SCALLOPS_WALLET    = "0xe1d6b51521bd4365769199f392f9818661bd907c"
PM_ASSETS_REFRESH_SEC = 240
# (asset_id) → {coin, mt, ws, outcome}
_pm_markets: dict[str, dict] = {}
SIGNAL_ENTRY_MAX_ASK     = 0.74
SIGNAL_ENTRY_MIN_ASK     = 0.45
# (source, coin, ws) → True after we've fired or evaluated
_signal_acted: set[tuple[str, str, int]] = set()


# Rolling lag stats per source: source -> [(lag_sec, ts), ...] (last N entries)
_signal_lags: dict[str, list[float]] = {"scallops": [], "twin": []}
_SIGNAL_LAG_KEEP = 50   # rolling window for avg


def _record_signal_lag(source: str, lag_sec: float) -> None:
    arr = _signal_lags.setdefault(source, [])
    arr.append(lag_sec)
    if len(arr) > _SIGNAL_LAG_KEEP:
        del arr[:len(arr) - _SIGNAL_LAG_KEEP]


def _signal_lag_summary() -> str:
    """One-line summary of recent signal-to-fire latency."""
    parts = []
    for source in ("scallops", "twin"):
        arr = _signal_lags.get(source, [])
        if not arr: continue
        avg = sum(arr) / len(arr)
        parts.append(f"{source}={avg:.1f}s avg/{len(arr)}n")
    return " | ".join(parts) if parts else "no signal entries yet"


# Signal-entry freshness threshold: signals older than this don't fire.
# Catches:
#   1. Post-restart file replay (bot tails entire history on first read)
#   2. REST-derived signals that arrived too late to act on
# In-process WS signals typically arrive at ~2-3s lag → well under threshold.
SIGNAL_MAX_AGE_SEC = 12


async def _process_signal_entry(client: httpx.AsyncClient, source: str,
                                 coin: str, side: str, ws: int,
                                 sig: dict) -> None:
    """Fire a signal-led entry. Mirrors MOM's gates + ladder split, smaller size."""
    # Freshness gate — don't fire on signals from before bot started
    # or signals that arrived too late via slow REST polling.
    sig_ts = float(sig.get("ts", 0) or 0)
    if sig_ts > 0:
        age = time.time() - sig_ts
        if age > SIGNAL_MAX_AGE_SEC:
            console.print(
                f"  [dim]{source} signal stale ({age:.1f}s > {SIGNAL_MAX_AGE_SEC}s) — "
                f"skip {coin} {side.upper()} entry[/dim]"
            )
            return

    if current_window_start_ts == 0 or not kalshi_state:
        return
    if coin not in kalshi_state:
        return
    elapsed_now = int(time.time() - ws)
    if elapsed_now < 5 or elapsed_now > 780:
        return
    now_et_dt = datetime.now(timezone.utc) + ET_OFFSET
    if now_et_dt.hour in DEAD_HOURS_ET:
        return

    st = kalshi_state[coin]
    cur_ask = st.get("yes_ask") if side == "yes" else st.get("no_ask")
    if not cur_ask or cur_ask <= 0 or cur_ask >= 1:
        return
    if cur_ask < SIGNAL_ENTRY_MIN_ASK or cur_ask > SIGNAL_ENTRY_MAX_ASK:
        return

    # NOTE: cross-bot veto removed — Twin and Scallops fire independently.

    # Per-coin-per-window cap (uses same logic as MOM)
    coin_cap = (WEEKEND_MAX_SHARES_PER_COIN_WINDOW
                if _is_weekend(now_et_dt) else MAX_SHARES_PER_COIN_WINDOW)
    existing_sh = sum(
        tr.get("shares", 0) for tr in _fill_tracker.values()
        if tr.get("coin") == coin and tr.get("ticker") == st["ticker"]
        and tr.get("fill_at")
    )
    remaining = max(0, coin_cap - existing_sh)
    if remaining == 0:
        return

    shares_this = min(SIGNAL_ENTRY_SHARES, remaining)
    if cur_ask >= 0.65:
        shares_this = max(1, shares_this // 2)

    # Overnight cap
    if now_et_dt.hour >= OVERNIGHT_START_HOUR or now_et_dt.hour < OVERNIGHT_END_HOUR:
        shares_this = min(shares_this, OVERNIGHT_MAX_SHARES)

    # Ladder split: 1 market + (n-1) rungs (1 share each)
    n_market = 1 if shares_this >= 1 else 0
    agg_px = round(min(cur_ask + LIMIT_BUFFER, SIGNAL_ENTRY_MAX_ASK), 2)
    rung_pxs: list[float] = []
    for k in range(1, shares_this):
        rpx = round(cur_ask - PATIENT_OFFSET - 0.01 * (k - 1), 2)
        if rpx < PATIENT_MIN_PX:
            break
        rung_pxs.append(rpx)

    src_tag = "SC" if source == "scallops" else "TW"
    rung_str = " + ".join(f"1@{p:.2f}" for p in rung_pxs)
    if rung_str: rung_str = " + " + rung_str
    # Lag = our timestamp now (about to send order) minus when the bot's PM
    # trade hit the chain/feed.
    sig_ts = float(sig.get("ts", 0) or 0)
    lag_sec = (time.time() - sig_ts) if sig_ts else None
    if lag_sec is not None:
        _record_signal_lag(source, lag_sec)
    lag_str = f" lag={lag_sec:.1f}s" if lag_sec is not None else ""
    console.print(
        f"  [bold magenta]★ {source.upper()} ENTRY {coin} "
        f"{side.upper()} T={elapsed_now}s{lag_str} "
        f"({n_market}@{agg_px:.2f}{rung_str})[/bold magenta]"
    )

    # Place market leg
    agg_order = await _place_buy(
        client, st["ticker"], side, agg_px, n_market,
        f"{coin}@T{elapsed_now}/{src_tag}-M",
    )
    if agg_order:
        _register_fill_tracker(agg_order, coin, side, st["ticker"],
                               n_market, agg_px, f"{source}-market", shares_this)
        _fill_tracker[agg_order["order_id"]].update({
            "T_checkpoint": 0, "delta_pct": 0,
            "open_price": st.get("open_price", 0), "trigger_price": 0,
            "coin_mult": 1.0,
        })
        st.setdefault("trades", []).append({
            "T_checkpoint":     0,
            "elapsed_actual":   elapsed_now,
            "side":             side,
            "ask_at_trigger":   cur_ask,
            "limit_price_sent": agg_px,
            "shares":           n_market,
            "role":             f"{source}-market",
            "confirmed":        False,
            "order_id":         agg_order["order_id"],
            "client_order_id":  agg_order["client_order_id"],
            "sent_at":          agg_order.get("sent_at"),
            "ack_at":           agg_order.get("ack_at"),
        })
        session_stats["pending"] += 1

    # Place ladder rungs
    for k, rung_px in enumerate(rung_pxs):
        is_top = (k == 0)
        timeout = PATIENT_TIMEOUT_SEC if is_top else DEEP_RUNG_TIMEOUT_SEC
        timeout = min(timeout, max(1, LADDER_FORCE_CANCEL_T - elapsed_now))
        role = f"{source}-patient" if is_top else f"{source}-rung-{k+2}"
        tag_suffix = f"{src_tag}-L" if is_top else f"{src_tag}-L{k+1}"
        rung_order = await _place_buy(
            client, st["ticker"], side, rung_px, 1,
            f"{coin}@T{elapsed_now}/{tag_suffix}",
        )
        if rung_order:
            _register_fill_tracker(rung_order, coin, side, st["ticker"],
                                   1, rung_px, role, shares_this)
            _fill_tracker[rung_order["order_id"]].update({
                "T_checkpoint": 0, "delta_pct": 0,
                "open_price": st.get("open_price", 0), "trigger_price": 0,
                "coin_mult": 1.0,
            })
            st.setdefault("trades", []).append({
                "T_checkpoint":     0,
                "elapsed_actual":   elapsed_now,
                "side":             side,
                "ask_at_trigger":   cur_ask,
                "limit_price_sent": rung_px,
                "shares":           1,
                "role":             role,
                "confirmed":        False,
                "order_id":         rung_order["order_id"],
                "client_order_id":  rung_order["client_order_id"],
                "sent_at":          rung_order.get("sent_at"),
                "ack_at":           rung_order.get("ack_at"),
            })
            session_stats["pending"] += 1
            asyncio.create_task(_cancel_patient_after(
                client, rung_order["order_id"],
                tag=f"{coin}@T{elapsed_now}/{tag_suffix}",
                delay_sec=timeout,
            ))

    if side == "yes":
        st["last_fired_yes"] = elapsed_now
    else:
        st["last_fired_no"] = elapsed_now
    _cross_coin_sides[coin] = side


async def _signal_entry_poll_loop() -> None:
    """FALLBACK file-tail. PM WS in-process is primary path; this catches anything
    the WS missed (e.g., during reconnect)."""
    while True:
        await asyncio.sleep(SIGNAL_POLL_SEC)
        if current_window_start_ts == 0 or not kalshi_state:
            continue
        _refresh_scallops_veto()
        _refresh_twin_veto()
        ws = current_window_start_ts
        async with httpx.AsyncClient(timeout=10) as client:
            for source, sigs in (("scallops", _scallops_veto), ("twin", _twin_veto)):
                for coin in COINS:
                    if not COINS[coin]["enabled"]: continue
                    key = (source, coin, ws)
                    if key in _signal_acted: continue
                    sig = sigs.get((coin, ws))
                    if not sig: continue
                    _signal_acted.add(key)
                    # No "existing MOM trade" suppression — let signals fire
                    # independently. Per-coin-window cap controls aggregate exposure.
                    await _process_signal_entry(client, source, coin, sig["side"], ws, sig)


# ── In-process Polymarket WS — primary signal path ──────────────────────────
# Subscribes to BOTH 5m and 15m crypto markets. On each trade event, verifies
# maker on Polygon RPC (~1s); if Twin (5m slot-0) or Scallops (15m), fires
# entry directly without going through file. Saves ~1s vs file-roundtrip.

async def _pm_fetch_assets(client: httpx.AsyncClient) -> dict[str, dict]:
    """Return asset_id -> {coin, mt, ws, outcome} for current+next 5m AND 15m windows."""
    markets = {}
    now = int(time.time())
    coins = [c.lower() for c in COINS]
    for coin in coins:
        for mt, period in (("5m", 300), ("15m", 900)):
            for offset in [-period, 0, period]:
                wst = (now // period) * period + offset
                slug = f"{coin}-updown-{mt}-{wst}"
                try:
                    r = await client.get(PM_GAMMA_API, params={"slug": slug}, timeout=8)
                    if r.status_code != 200 or not r.json(): continue
                    for ev in r.json():
                        for mk in ev.get("markets", []):
                            tk = mk.get("clobTokenIds")
                            if not tk: continue
                            try:
                                tk = json.loads(tk) if isinstance(tk, str) else tk
                            except Exception: continue
                            outcomes = mk.get("outcomes", '["Up","Down"]')
                            try:
                                outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
                            except Exception:
                                outcomes = ["Up","Down"]
                            for i, asset_id in enumerate(tk):
                                outcome = outcomes[i] if i < len(outcomes) else f"out{i}"
                                markets[asset_id] = {
                                    "coin": coin.upper(), "mt": mt, "ws": wst,
                                    "outcome": outcome, "slug": slug,
                                }
                            break
                except Exception: continue
    return markets


async def _pm_verify_maker(client: httpx.AsyncClient, tx_hash: str) -> str | None:
    """Query Polygon receipt; return matching wallet (twin/scallops/None)."""
    twin_topic = "0x" + "0" * 24 + TWIN_WALLET[2:].lower()
    sc_topic   = "0x" + "0" * 24 + SCALLOPS_WALLET[2:].lower()
    for attempt in range(3):
        try:
            r = await client.post(PM_RPC_URL, json={
                "jsonrpc": "2.0", "id": 1,
                "method": "eth_getTransactionReceipt",
                "params": [tx_hash],
            }, timeout=8)
            if r.status_code != 200:
                await asyncio.sleep(0.5); continue
            result = r.json().get("result")
            if result is None:
                await asyncio.sleep(0.7); continue
            for log in result.get("logs", []):
                topics = log.get("topics", [])
                if not topics or len(topics) < 3: continue
                if topics[0].lower() != PM_ORDER_FILLED_TOPIC.lower(): continue
                m = topics[2].lower()
                if m == twin_topic: return "twin"
                if m == sc_topic:   return "scallops"
            return None
        except Exception:
            await asyncio.sleep(0.5)
    return None


def _write_signal_log_async(source: str, entry: dict) -> None:
    """Off-critical-path file write for backup/analysis. Schedule on default executor."""
    target = TWIN_FAST_LOG if source == "twin" else SCALLOPS_FAST_LOG
    try:
        with open(target, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass


async def _pm_handle_trade(rpc_client: httpx.AsyncClient,
                            kalshi_client: httpx.AsyncClient,
                            trade: dict, market_info: dict) -> None:
    """Verify maker, fire entry if Twin (5m slot-0) or Scallops (15m)."""
    if current_window_start_ts == 0 or not kalshi_state: return
    tx_hash = trade.get("transaction_hash")
    if not tx_hash: return
    try:
        price = float(trade.get("price", 0))
    except Exception: return
    if price <= 0 or price >= 1: return
    try:
        trade_ts = int(trade.get("timestamp", 0)) / 1000.0
    except Exception:
        trade_ts = time.time()

    coin = market_info["coin"]; mt = market_info["mt"]
    ws_window = market_info["ws"]
    outcome = market_info["outcome"]

    # Twin filter: 5m slot-0 only
    if mt == "5m":
        ws_15m = (ws_window // 900) * 900
        if ws_window != ws_15m: return
        ws_for_act = ws_15m
    else:
        ws_for_act = ws_window

    # Verify maker on-chain
    maker = await _pm_verify_maker(rpc_client, tx_hash)
    if maker is None: return
    if maker == "twin" and mt != "5m": return    # Twin only on 5m
    if maker == "scallops" and mt != "15m": return # Scallops only on 15m

    side = "yes" if outcome == "Up" else "no"
    sig = {"side": side, "price": price, "ts": trade_ts}

    # Update veto state
    if maker == "twin":
        cur = _twin_veto.get((coin, ws_for_act))
        if not cur or cur.get("ts", 0) > trade_ts:
            if price >= VETO_MIN_PRICE:
                _twin_veto[(coin, ws_for_act)] = sig
    else:
        cur = _scallops_veto.get((coin, ws_for_act))
        if not cur or cur.get("ts", 0) > trade_ts:
            if price >= VETO_MIN_PRICE:
                _scallops_veto[(coin, ws_for_act)] = sig

    # Fire entry (idempotent: _signal_acted gates)
    if price < VETO_MIN_PRICE: return
    if ws_for_act != current_window_start_ts: return
    key = (maker, coin, ws_for_act)
    if key in _signal_acted: return
    _signal_acted.add(key)
    # No "existing MOM trade" suppression — sources fire independently.

    # Background log to fast file (off critical path)
    log_entry = {
        "fetch_ts": time.time(), "trade_ts": trade_ts,
        "lag_sec": round(time.time() - trade_ts, 2),
        "bot": maker.capitalize(), "slug": market_info.get("slug"),
        "coin": coin, "market_type": mt,
        "side": "BUY", "outcome": outcome,
        "price": price, "size": float(trade.get("size", 0)),
        "tx": tx_hash, "window_start_ts": ws_window,
        "elapsed_in_window": int(trade_ts - ws_window),
        "source": "in_process_ws",
    }
    asyncio.get_running_loop().run_in_executor(None, _write_signal_log_async, maker, log_entry)

    # Trigger entry
    await _process_signal_entry(kalshi_client, maker, coin, side, ws_for_act, sig)


async def _pm_ws_loop() -> None:
    """Subscribe to PM WS for current+next 5m AND 15m crypto markets.
    On every last_trade_price event, verify + maybe fire entry.

    Reconnect policy:
    - SCHEDULED reconnect once per 15m window at offset 810s (13.5 min in).
      By then Twin/Scallops conviction signals for the current window have
      already fired (they happen in slot-0 = first 5 min). Refreshing the
      asset list + reconnecting now gives us a fresh WS for the NEXT window
      90 seconds before it opens. We deliberately do nothing during the
      window's first 13.5 min — Twin/Scallops detection is the priority.
    - WATCHDOG reconnect any time we see >45s of silence (silent dead conn).
    - EXCEPTION reconnect on any WS error.
    """
    global _pm_markets
    STALE_THRESHOLD_SEC      = 45
    SAFE_RECONNECT_OFFSET_S  = 810   # 13:30 into window — Twin/Scallops slot-0 done
    last_event_ts            = time.time()
    last_scheduled_reconnect = 0     # unix ts of the last scheduled reconnect target hit
    ws = None
    async with httpx.AsyncClient(timeout=10) as rpc_client:
        async with httpx.AsyncClient(timeout=15) as kalshi_client:
            while True:
                try:
                    now = time.time()

                    # ── scheduled reconnect at minute 13:30 of any 15m window ──
                    cur_window_start = (int(now) // 900) * 900
                    target = cur_window_start + SAFE_RECONNECT_OFFSET_S
                    if target <= now and last_scheduled_reconnect < target:
                        last_scheduled_reconnect = target
                        console.print(
                            f"[dim]PM WS: scheduled reconnect at T+{int(now-cur_window_start)}s "
                            f"(end-of-window dead time)[/dim]"
                        )
                        if ws:
                            try: await ws.close()
                            except Exception: pass
                            ws = None
                        # Refresh asset list while we're disconnected anyway
                        new_markets = await _pm_fetch_assets(rpc_client)
                        if new_markets:
                            _pm_markets = new_markets

                    # ── connect (initial OR after scheduled/watchdog reconnect) ──
                    if ws is None:
                        if not _pm_markets:
                            new_markets = await _pm_fetch_assets(rpc_client)
                            if new_markets: _pm_markets = new_markets
                        ws = await websockets.connect(PM_WS_URL, ping_interval=20)
                        asset_ids = list(_pm_markets.keys())
                        await ws.send(json.dumps({"type": "market", "assets_ids": asset_ids}))
                        console.print(f"[dim]PM WS: subscribed to {len(asset_ids)} assets[/dim]")
                        last_event_ts = time.time()

                    # ── recv with watchdog ──
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=10)
                    except asyncio.TimeoutError:
                        if time.time() - last_event_ts > STALE_THRESHOLD_SEC:
                            console.print(
                                f"[dim red]PM WS stale ({int(time.time()-last_event_ts)}s "
                                f"no events) → reconnect[/dim red]"
                            )
                            try: await ws.close()
                            except Exception: pass
                            ws = None
                            last_event_ts = time.time()
                        continue

                    last_event_ts = time.time()
                    m = json.loads(raw)
                    items = m if isinstance(m, list) else [m]
                    for item in items:
                        if item.get("event_type") != "last_trade_price": continue
                        asset_id = item.get("asset_id")
                        if not asset_id or asset_id not in _pm_markets: continue
                        asyncio.create_task(_pm_handle_trade(
                            rpc_client, kalshi_client, item, _pm_markets[asset_id]))
                except Exception as e:
                    console.print(f"[dim red]PM WS reconnect: {type(e).__name__} {str(e)[:80]}[/dim red]")
                    if ws:
                        try: await ws.close()
                        except Exception: pass
                        ws = None
                    await asyncio.sleep(2)


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
                        lines.append(
                            f"[{d_col}]{sym} {delta:+.3f}%[/{d_col}] "
                            f"Y{ya_str}/N{na_str}"
                        )
                    else:
                        floor_s = f"⌊${floor_p:,.0f}" if floor_p else "⌊?"

                        # Current effective checkpoint → MOM threshold
                        cp = None
                        for T_, factor_, cap_ in CHECKPOINTS:
                            if elapsed >= T_: cp = (T_, factor_, cap_)
                            else: break
                        mom_thr_s = "—"
                        if cp:
                            T_, factor_, cap_ = cp
                            ev = _current_vol_by_coin.get(sym, 0.0)
                            mom_thr = max(VOL_THR_FLOOR, min(VOL_THR_CEILING, factor_ * ev)) if ev > 0 else VOL_THR_FLOOR
                            mom_thr_s = f"M{mom_thr:.3f}%"

                        surf_s = ""
                        p_up = _surface_p_up(sym, (datetime.now(timezone.utc)+ET_OFFSET).hour, elapsed, delta)
                        if p_up is not None:
                            surf_s = f" sY{p_up:.2f}/sN{1-p_up:.2f}"

                        lines.append(
                            f"[{d_col}]{sym} {delta:+.3f}%[/{d_col}] "
                            f"{floor_s} Y{ya_str}/N{na_str} "
                            f"[dim]{mom_thr_s}{surf_s}[/dim]"
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
    _recovery_last_ts.clear()
    _doubledown_fired.clear()
    _signal_acted.clear()
    _filtered_seen.clear()
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

        if abs(delta) < thr:
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
            ask_str = f"{cur_ask:.2f}" if (cur_ask and 0 < cur_ask < 1) else "--"
            # Context-aware suffix per reason
            extra = ""
            display_reason = reason
            if reason == "cap_exceeded":
                display_reason = "ask_above_cap"
                try: extra = f" > price_cap {eff_cap:.2f}"
                except Exception: pass
            elif reason == "max_shares_per_coin":
                display_reason = "share_cap_full"
                try: extra = f"  ({existing_sh}/{coin_cap} filled)"
                except Exception: pass
            elif reason == "min_entry_price":
                display_reason = "ask_below_min"
                extra = f" < min_entry {MIN_ENTRY_PRICE:.2f}"
            elif reason == "market_disagrees":
                display_reason = "ask_below_50"
                extra = " < 0.50 (market < 50%)"
            elif reason == "ask_rising":
                try: extra = f"  (our_d={our_d:+.3f})"
                except Exception: pass
            console.print(
                f"  [dim yellow]MOM skip  {sym} {side.upper():3} T={T_eff:>3}s  "
                f"Δ={delta:+.3f}% ask={ask_str}{extra}  "
                f"reason={display_reason}[/dim yellow]"
            )
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
        last = st["last_fired_yes"] if side == "yes" else st["last_fired_no"]
        if elapsed_now - last < COOLDOWN_SEC:
            return  # cooldown is not a filter — it's normal spacing, don't log

        if cur_ask <= 0 or cur_ask >= 1:
            return
        if cur_ask < MIN_ENTRY_PRICE:
            _skip("min_entry_price")
            return
        # Per-(coin, T) override else baseline-with-coin-adjustment
        eff_cap = _effective_cap(sym, T_eff, max_px)
        cap_source = "override" if (sym, T_eff) in COIN_CAP_OVERRIDE else "baseline"
        if cur_ask > eff_cap or cur_ask > MAX_ENTRY_PRICE:
            _skip("cap_exceeded")
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
        # price caps. Existing vol-threshold + per-coin caps are sufficient.)

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

        # NOTE: Scallops/Twin VETO removed per user request — each source runs
        # independently. Veto state still populated (for end-of-day analysis)
        # but no longer blocks MOM trades.
        _refresh_scallops_veto()
        _refresh_twin_veto()

        # Per-coin momentum scaling — smooth multiplier from rolling win rate
        coin_mult = _coin_share_multiplier(sym)

        # (coin, side, delta_bucket) WR-table lookup. Skip cells fail fast.
        db_tag, db_mult = _delta_bucket_mult(sym, side, delta)
        if db_mult == 0.0:
            _log_filtered({
                "ts":              time.time(),
                "ticker":          st["ticker"],
                "coin":            sym,
                "side":            side,
                "reason":          "wr_table_skip",
                "wr_bucket":       _delta_bucket(delta),
                "elapsed":         elapsed_now,
                "T_checkpoint":    T_eff,
                "delta_pct":       round(delta, 4),
                "cur_ask":         cur_ask if (cur_ask and 0 < cur_ask < 1) else None,
                "window_start_ts": window_start_ts,
            })
            return

        # Fixed sizing: 5 shares per MOM fire. Same as Twin/Scallops signal-led.
        # Per-coin-per-window cap (8 weekday, 7 weekend) limits aggregate exposure.
        shares_this = 5

        # Coinbase signal confluence — DISABLED (see CB_CONFLUENCE_ENABLED).
        # Detector log still kept warm so we can re-enable later. When
        # disabled, every trade gets ("off", 1.0) — no skip, no boost.
        if CB_CONFLUENCE_ENABLED:
            _refresh_cb_signals()
            cb_state, cb_mult = _cb_confluence(sym, side, window_start_ts)
            if cb_mult == 0.0:
                _log_filtered({
                    "ts":              time.time(),
                    "ticker":          st["ticker"],
                    "coin":            sym,
                    "side":            side,
                    "reason":          "cb_opposes",
                    "elapsed":         elapsed_now,
                    "T_checkpoint":    T_eff,
                    "delta_pct":       round(delta, 4),
                    "cur_ask":         cur_ask if (cur_ask and 0 < cur_ask < 1) else None,
                    "window_start_ts": window_start_ts,
                })
                return
            shares_this = max(1, int(round(shares_this * cb_mult)))
        else:
            cb_state, cb_mult = ("off", 1.0)

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

        # Per-coin-per-window cap: count all filled shares, clamp new entry.
        # Weekend (Fri 17:00 ET → Mon 08:00 ET): tighter cap.
        now_et_dt = datetime.now(timezone.utc) + ET_OFFSET
        coin_cap = (WEEKEND_MAX_SHARES_PER_COIN_WINDOW
                    if _is_weekend(now_et_dt)
                    else MAX_SHARES_PER_COIN_WINDOW)
        existing_sh = sum(
            tr.get("shares", 0) for tr in _fill_tracker.values()
            if tr.get("coin") == sym and tr.get("ticker") == st["ticker"]
            and tr.get("fill_at")
        )
        remaining = max(0, coin_cap - existing_sh)
        if remaining == 0:
            _skip("max_shares_per_coin")
            return
        shares_this = min(shares_this, remaining)

        # Ladder pricing: 1 share market + (shares_this - 1) ladder rungs.
        # Each rung is 1 share, 1¢ deeper than the previous, capped at PATIENT_MIN_PX.
        #   k=1: ask - 0.02  (top rung, 15s timeout, DD-eligible)
        #   k=2: ask - 0.03  (60s timeout, no DD)
        #   k=3: ask - 0.04
        #   ...
        n_market = 1 if shares_this >= 1 else 0
        agg_px = round(min(cur_ask + LIMIT_BUFFER, eff_cap), 2)
        # Build rung price list (deepest last). Skip rungs below the floor.
        rung_pxs: list[float] = []
        for k in range(1, shares_this):
            rpx = round(cur_ask - PATIENT_OFFSET - 0.01 * (k - 1), 2)
            if rpx < PATIENT_MIN_PX:
                break
            rung_pxs.append(rpx)
        n_rungs = len(rung_pxs)

        mode_str = " [bold yellow]★CONFIRMED[/bold yellow]" if confirmed else ""
        mult_str = f" [{coin_mult:.2f}x]" if coin_mult != 1.0 else ""
        # Rich strips [tag=...] as malformed markup — use parentheses instead.
        db_str = f" (db={db_tag}{f'×{db_mult:.1f}' if db_mult != 1.0 else ''})" if db_tag != "thin" else ""
        cb_str = f" (cb={cb_state}{f'×{cb_mult:.1f}' if cb_mult != 1.0 else ''})"
        # Show veto state on every entry — passive observability
        sv = _scallops_veto.get((sym, window_start_ts))
        tv = _twin_veto.get((sym, window_start_ts))
        scal_state = "agree" if (sv and sv["side"] == side) else "silent" if not sv else "OPP"
        twin_state = "agree" if (tv and tv["side"] == side) else "silent" if not tv else "OPP"
        veto_str = f" (scal={scal_state} twin={twin_state})"
        rung_str = " + ".join(f"1@{p:.2f}" for p in rung_pxs) if rung_pxs else ""
        if rung_str: rung_str = " + " + rung_str
        console.print(
            f"  [bold cyan]T={elapsed_now:>3}s  {sym}  open=${open_p:,.2f} "
            f"now=${now_p:,.2f}  Δ={delta:+.3f}% (|Δ|={abs(delta):.3f}% ≥ {thr:.3f}%) "
            f"({thr_source}) → buy {side.upper()} "
            f"(ask={cur_ask:.2f}, cap={eff_cap:.2f}/{cap_source})  "
            f"{shares_this}sh ({n_market}@{agg_px:.2f}{rung_str})"
            f"{mult_str}{db_str}{cb_str}{veto_str}{mode_str}[/bold cyan]"
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

        # Aggressive / guaranteed-fill leg (1 share at ask + 0.02)
        agg_order = await _place_buy(client, st["ticker"], side, agg_px,
                                     n_market, f"{sym}@T{elapsed_now}/M")
        await _record(agg_order, n_market, agg_px, "market")
        if agg_order:
            _register_fill_tracker(agg_order, sym, side, st["ticker"],
                                   n_market, agg_px, "market", shares_this)
            # Store context for double-down (DD reads from this tracker)
            _fill_tracker[agg_order["order_id"]].update({
                "T_checkpoint": T_eff, "delta_pct": delta,
                "open_price": open_p, "trigger_price": now_p,
                "coin_mult": coin_mult,
            })

        # Ladder rungs: each is 1 share. Top rung (k=0) keeps the "patient"
        # role + 15s timeout + DD-eligibility. Deeper rungs are tagged
        # rung-3, rung-4, … with longer timeouts and no DD.
        any_rung_placed = False
        for k, rung_px in enumerate(rung_pxs):
            is_top = (k == 0)
            role = "patient" if is_top else f"rung-{k + 2}"
            timeout = PATIENT_TIMEOUT_SEC if is_top else DEEP_RUNG_TIMEOUT_SEC
            # Force-cancel-by-T cap: never hold past LADDER_FORCE_CANCEL_T
            timeout = min(timeout, max(1, LADDER_FORCE_CANCEL_T - elapsed_now))
            tag_suffix = "L" if is_top else f"L{k + 1}"
            rung_order = await _place_buy(
                client, st["ticker"], side, rung_px, 1,
                f"{sym}@T{elapsed_now}/{tag_suffix}",
            )
            await _record(rung_order, 1, rung_px, role)
            if rung_order:
                any_rung_placed = True
                _register_fill_tracker(rung_order, sym, side, st["ticker"],
                                       1, rung_px, role, shares_this)
                _fill_tracker[rung_order["order_id"]].update({
                    "T_checkpoint": T_eff, "delta_pct": delta,
                    "open_price": open_p, "trigger_price": now_p,
                    "coin_mult": coin_mult,
                })
                asyncio.create_task(_cancel_patient_after(
                    client,
                    rung_order["order_id"],
                    tag=f"{sym}@T{elapsed_now}/{tag_suffix}",
                    delay_sec=timeout,
                ))

        # Cooldown updates on any successful fire
        if agg_order or any_rung_placed:
            if side == "yes":
                st["last_fired_yes"] = elapsed_now
            else:
                st["last_fired_no"] = elapsed_now
            # Track cross-coin direction for outlier detection
            _cross_coin_sides[sym] = side

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
        winning-on-the-cheap-side outcome pays a multiple — asymmetric upside."""
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
        advantage vs the market ask. Independent of MOM."""
        if not SURFACE_ENGINE_ENABLED:
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
        shares = 3   # mid-conviction probe
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
                    lag_summary = _signal_lag_summary()
                    console.print(
                        f"\n[bold]  [{ts_s}] entering T≥{T}s zone  "
                        f"(factor={factor}, {thr_str}, max ask {cap:.2f})  "
                        f"[dim]signal lag: {lag_summary}[/dim][/bold]"
                    )

                if now - last_eval_ts >= EVAL_DEBOUNCE_SEC:
                    last_eval_ts = now
                    T, factor, cap = effective
                    # Run momentum + surface + recovery eval for each coin in
                    # parallel. Each has its own gates and skips cheaply when
                    # not applicable.
                    coro_pairs = []
                    for s in kalshi_state:
                        coro_pairs.append(_eval_checkpoint(s, T, factor, cap, elapsed))
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
                "role":            trade.get("role"),
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
        # Prime the Coinbase signal log once at startup, then keep it warm.
        _refresh_cb_signals()
        asyncio.create_task(_cb_poll_loop())
        asyncio.create_task(_signal_entry_poll_loop())   # fallback file-tail
        asyncio.create_task(_pm_ws_loop())               # primary signal path (in-process WS)
        # Load the (coin, side, delta_bucket) WR multiplier table.
        _load_wr_table()
        if _wr_table:
            cells = _wr_table.get("cells", {})
            n_boost = sum(1 for c in cells.values() if c.get("tag") == "boost")
            n_skip  = sum(1 for c in cells.values() if c.get("tag") == "skip")
            console.print(
                f"[dim]WR table loaded: {len(cells)} cells "
                f"({n_boost} boost / {n_skip} skip) "
                f"from {_wr_table.get('n_samples', 0)} samples[/dim]"
            )
        else:
            console.print("[dim]WR table not found — all buckets default to 1.0×[/dim]")
        # Start CFB proxy (3 WS feeds, 60s rolling deque). Once warm (~60s),
        # blended_price() automatically returns the CFB proxy instead of the
        # last-trade median fallback.
        if _cfb_proxy is not None:
            enabled_coins = [c for c, cfg in COINS.items() if cfg["enabled"]]
            asyncio.create_task(_cfb_proxy.start(enabled_coins))
            console.print(f"[dim]CFB proxy started for {', '.join(enabled_coins)} (warmup ~60s)[/dim]")

        # Seed per-coin reversal history from btc_windows.db (last N settled
        # windows with their tick data). Kept for offline analysis / future
        # empirical gates; no live consumer right now.
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
