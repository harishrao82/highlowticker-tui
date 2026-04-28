# Kalshi Crypto Trading — Project Handoff

*Last updated: 2026-04-21 8am ET.*

---

## What This Project Is

A live trading system for **Kalshi 15-minute crypto Up/Down binary markets** (BTC, ETH, SOL, XRP). Every 15 minutes a new window opens; at expiry Kalshi settles based on whether the 60-second average of CF Benchmarks' BRTI went up or down vs a floor_strike.

The **momentum strategy** (`kalshi_momentum_live.py`) is the active trader.

---

## Current Live Config (as of Apr 21)

- **Engine**: MOM only (CONTRA auto-on 4-8am ET only, all others disabled)
- **Delta**: `(blended_now − floor_strike) / floor_strike` with CFB proxy (3-exchange WS bid/ask mid, 60s rolling mean via `cfb_proxy.py`)
- **Threshold**: `clip(vol_factor × max(profile_vol, realized_1h_vol), 0.015%, 0.25%)`
- **Checkpoints**: T=30(.5/.58) T=60(.7/.64) T=120(1.0/.73) T=180(1.0/.75) T=300(1.2/.75) T=420(1.5/.75) T=600(1.5/.75)
- **BTC cap adj**: −0.15 (override T≥180 → 0.65). ETH/SOL/XRP: 0.00
- **Sizing**: 1 market (taker) + 5 patient (maker @ ask−2¢, 15s timeout)
- **Prior-5m gate**: ETH only (skip trades fighting 5-min mean-reversion edge)
- **Market disagrees**: skip if `cur_ask < 0.50`
- **365-day backtest**: 75% WR, +$12k P&L, +$33/day on BTC alone
- **Live 5-day result**: +$13, 64% WR across 4 coins

---

## Scallops: The Bot We Reverse-Engineered

"Idolized-Scallops" is a profitable Polymarket bot trading the same 15-min crypto markets.

### Data Files

| File | Contents |
|---|---|
| `~/.scallops_live_trades.jsonl` | ~40k+ trades. Fields: `trade_ts, coin, outcome(Up/Down), side(BUY), price, size, notional, slug, window_start_ts, elapsed_in_window, delta_pct` |
| `~/.poly_book_snapshots.jsonl` | Polymarket bid/ask per second per coin per window |
| `~/.kalshi_status_snapshots.jsonl` | Kalshi yes_ask/no_ask + blended price per coin per ~10s |
| `~/.btc_windows.db` | SQLite: `windows` (ticker, floor_strike, winner) + `ticks` (yes_ask, no_ask per second) |
| `~/.scallops_levels.json` | Lookup: per (coin, TOD_bucket, T_bucket) → {mom: {n, min_delta_pct, max_price}, con: {n, max_delta_pct, max_price}} |

### His Three-Mode Strategy

**Mode 1: Momentum Opener (T=30–60s)**
- Buys the **delta-aligned** side at $0.50–$0.65
- Heavy size ($50–$200 per window)
- Example: BTC falling → buys Down@$0.55 at T=42s with $1,064

**Mode 2: Controlled Build + Averaging Down (T=60–400s)**
- Winning side: adds more at same or better prices
- Cheapening side: averages down (buys same side cheaper)
- Targets `if_loss / if_win ≈ -1.0` by T=300s (symmetric risk)

**Mode 3: Cheap Insurance / Over-Hedge (T=300–900s)**
- Buys opposite side cheap ($0.20–$0.35) for **asymmetric upside**
- If original wins: small profit. If cheap side wins: **massive payout** (3-5×)
- Final 2-3 min: penny lottery tickets ($0.02–$0.10) on losing side
- Over-hedges so both outcomes are positive, skewed toward cheap side

**Mode 4: Damage Control (when wrong)**
- Original side drops to $0.20–$0.30: **stops adding**
- Switches to buying OTHER side aggressively at $0.70–$0.90
- Example: opened D@$0.51, D fell to $0.36 → bought U@$0.76, $0.84, $0.93

### Key Differences: Scallops vs Us

| | Scallops (Polymarket) | Us (Kalshi) |
|---|---|---|
| Fills at | $0.50 (tighter spread) | $0.59 (wider spread) |
| Fees | ~$0 (gas only) | $0.01/share |
| Position mgmt | Both sides, over-hedge | MOM only, naked |
| Size | $500–$2,000/window | $3–$5/window |
| Break-even WR at avg price | ~55% | ~63% |

### How to Analyze Scallops

```python
import json
from collections import defaultdict
from pathlib import Path

trades = []
with open(Path.home() / '.scallops_live_trades.jsonl') as f:
    for line in f:
        t = json.loads(line)
        if t.get('side') == 'BUY' and '-15m-' in t.get('slug', ''):
            trades.append(t)

# Group by window
by_window = defaultdict(list)
for t in trades:
    by_window[(t['coin'], t['window_start_ts'])].append(t)
for k in by_window:
    by_window[k].sort(key=lambda x: x['elapsed_in_window'])

# Walk a window's running book state
for t in by_window[('BTC', some_ws)]:
    # Track yes_sh, no_sh, yes_cost, no_cost
    # if_yes = yes_sh - total_cost
    # if_no = no_sh - total_cost
```

### What to Look For

1. **Entry timing**: when does he fire first? what |Δ| and price?
2. **Position shape over time**: plot if_yes/if_no trajectory per window
3. **Over-hedge trigger**: at what price ratio does he start buying the other side?
4. **Penny lottery**: how many shares at <$0.10? what's the expected payoff?
5. **Win rate by mode**: momentum-aligned entries vs contra-cheap entries
6. **Polymarket vs Kalshi spread**: compare his fill prices to Kalshi asks at same timestamp (join via `window_start_ts + elapsed_in_window`)

### Viewer URLs

- `http://localhost:7333/?hours=6` — Scallops trades + Poly/Kalshi ticks stitched (`poly_viewer.py`)
- `http://localhost:7332` — Scallops-only trades (`scallops_viewer.py`)

### Start All Processes

```bash
./start_scallops.sh   # recorder + both viewers
nohup python3 poly_book_recorder.py > /tmp/poly_book.log 2>&1 & disown  # Poly orderbook
nohup python3 btc_recorder.py > /tmp/btc_recorder.log 2>&1 & disown     # Kalshi ticks + DB
```

---

## Our Strategy Analysis Tooling

| File | Purpose |
|---|---|
| `kalshi_momentum_live.py` | Live trader |
| `cfb_proxy.py` | WS-based 3-exchange bid/ask mid → 60s rolling CFB proxy |
| `btc_vol_profile.py` | 21-day time-of-week vol profile |
| `btc_recorder.py` | Records Kalshi + Coinbase data to `~/.btc_windows.db` |
| `btc_model_builder.py` | Builds BTC probability surface from 365d candles |
| `poly_scallops_live_shadow.py` | Records Scallops trades to JSONL |
| `poly_book_recorder.py` | Records Polymarket orderbook to JSONL |
| `poly_viewer.py` (port 7333) | Stitched viewer: Scallops + Kalshi + Poly ticks |
| `scallops_viewer.py` (port 7332) | Scallops-only trade viewer |
| `start_scallops.sh` | Starts recorder + viewers |

### Additional Data Files

| Path | Contents |
|---|---|
| `~/.kalshi_momentum_trades.jsonl` | Our trades with settlement results |
| `~/.kalshi_fill_events.jsonl` | Fill confirmations (role, price, shares, timing) |
| `~/.kalshi_filtered_signals.jsonl` | 66k+ skipped signals with reasons |
| `~/.cfb_proxy_log.jsonl` | CFB proxy price per coin per second |
| `~/.btc_odds_cache.json` | 365d of 1-min BTC candles (48MB) |
| `~/.btc_model_surface_hourly.json` | BTC hourly probability surface (24 surfaces) |
| `~/.prior5_direction_gate.json` | Per-coin prior-5m → next-15m direction edge table |

---

## Findings That Held Up

1. **Vol-threshold MOM is +EV** — 75% WR over 365-day backtest, every month positive
2. **Scallops' levels shift by TOD** — 8am-12pm ET strongest for momentum, 4-8am/8pm-12am for contrarian
3. **Prior-5m reversion** — sharp 5-min moves predict opposite 15-min direction (+4pp edge, confirmed on 35k candles per coin)
4. **Cap_exceeded trades at real prices are ~breakeven** — 82% WR but avg ask $0.80, asymmetric payoff kills edge above $0.70
5. **CFB proxy (3-exchange bid/ask mid, 60s mean) matches Kalshi UI delta within 1-3bps**

## Findings That Were Overfitting (5 days only)

1. BB %B per-coin skip zones — pattern didn't hold cleanly with more data
2. RSI/ROC/Body-wick indicator gates — all ~47% WR over 365 days (noise)
3. Time-of-day gate — 5-day pattern was real but very specific to that week's regime
4. Scallops-level override of vol-threshold — noisier than our own vol-based thresholds (fewer samples)
5. Over-hedge module — lost $80 in backtest (partial fills + wrong-side insurance)

---

## Lead-Lag: Coinbase vs CFB vs Kalshi Odds (Apr 24)

Cross-correlation analysis on 48h / 146 windows (`lead_lag_analysis.py`):

- **Kalshi odds track instant Coinbase mid**, not the smoothed CFB proxy.
- BTC: Coinbase leads odds by ~1s (peak r=0.858 at lag +1). ETH/SOL/XRP: simultaneous (peak at lag 0).
- CFB proxy peaks at lag −5 to −6s (odds lead CFB) because the 60s rolling mean is mechanically lagged.
- Supports the thesis: retail drives the Kalshi book from free Coinbase data, not the $5k CFB feed.

---

## Scallops Reverse Engineering (Apr 24)

### What Scallops actually does (34,556 trades analyzed)

Scallops is NOT a momentum bot. He runs a **passive limit ladder + active top-of-book quoting + momentum add**:

| Archetype | Share | Mechanic |
|---|---|---|
| BELOW_BID (resting ladder rung swept) | **54%** | Pre-placed at T=0, filled as book moves through |
| AT_BID (top-of-book maker fill) | 15% | Continuous bid tracking, fills when seller hits |
| SELF_CHASE (own ask rose ≥2¢/15s) | 14% | Adds to winning side aggressively |
| SELF_DIP (own ask fell ≥2¢/15s) | 9% | Opportunistic dip add |

- Hedges 90% of BTC windows (buys both Up and Down), 75-92% across coins
- Median dominant/hedge notional ratio ~3x (75/25 split)
- Fires at tiny deltas (median |delta| = 0.04-0.05%)
- Early window (0-300s): mostly ladder fills. Late window (300-900s): shifts to active quoting.

### Kalshi Scallops-style strategy backtest

Built `kalshi_sim.py` (simulator calibrated against 1,534 real fills) and `kalshi_scallops.py` (strategy + backtest).

**Strategy rules:**
1. Dense ladder at T=0 on both YES and NO, clipped below current ask. Cheap-side sizing: 3 shares at ≤30¢, 2 at ≤45¢, 1 at ≤55¢, 0 above.
2. Active top-of-book quoting: refresh bid when book moves ≥3¢, 30s minimum dwell.
3. Momentum add: when own ask rises ≥2¢ in 15s, fire at current ask (2-min cooldown).
4. Late-window rebalance (T=600-720): add to leading side (0.55-0.75 zone) at bid+1¢; hedge deep loser (<0.25) by buying opposite at bid+1¢.
5. No BTC ladder (market too efficient); BTC runs quote + momentum only.

**Backtest results (3,701 windows, maker rebate −1¢, taker fee +2¢):**

| Coin | PnL | ROI |
|---|---|---|
| BTC (no ladder) | −$440 | −1.86% |
| ETH | +$420 | +1.26% |
| SOL | +$697 | +2.28% |
| XRP | +$610 | +2.23% |
| **TOTAL** | **+$1,287** | **+1.12%** |

Edge is structural (maker rebates + buying cheap), not directional. Maker rebate contributes ~$1,083 of the $1,287 total.

### Market calibration finding

From `hedge_timing_analysis.py` (2,940 windows):
- Leading side (0.55-0.75 zone after T=300) wins 6-9% MORE than market implies → hold/add, never hedge.
- Deep loser (<0.25 after T=600) wins 5-7% LESS than implied → sell/hedge has edge.
- The 0.85-1.00 zone at T=840 is 99.1% actual vs 93% implied (+6.1%) → near-certain winners are underpriced.

### Key files

| File | Purpose |
|---|---|
| `kalshi_sim.py` | Kalshi simulator (top-of-book, 67.5% fill/cancel accuracy) |
| `kalshi_scallops.py` | Scallops-style strategy + 3-scenario backtest |
| `lead_lag_analysis.py` | CB vs CFB vs odds cross-correlation |
| `hedge_timing_analysis.py` | Market calibration by (T, price_bucket) |
| `scallops_trigger_analysis.py` | Trigger-level Scallops trade classification |

### Simulator limitations

- Top-of-book only, no order-book depth → 35% over-fill bias on passive limits
- Conservative maker model: "bid crosses down" proxy for "order hit"
- No partial fills (assumes unlimited depth at top)

---

## Open Questions

1. Should BTC run a different strategy entirely (pure momentum, no ladder)?
2. Can the cheap-side ladder be dynamic — widen/narrow step by realized vol?
3. How does the strategy perform overnight vs daytime? (momentum live loses overnight)
4. At what capital level does the ladder start moving the Kalshi book?
5. Can Polymarket book data predict Kalshi price movements (lead-lag across venues)?
