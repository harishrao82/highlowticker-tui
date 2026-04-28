# BTC Odds Monitor — `btc_odds_research.py`

Real-time streaming monitor that compares Polymarket CLOB prices against a
historically-fitted probability model, alerts on mispricing, and tracks staged
arb opportunities.

---

## How to run

```bash
# One-time: build (or refresh) the probability surface
python3 btc_model_builder.py --days 365

# Start the live monitor
python3 btc_odds_research.py --live

# Print summary table only (no streams)
python3 btc_odds_research.py
```

---

## Architecture — 4 async tasks

```
_window_manager()   — detects 15-min window rolls, fetches open price + CLOB tokens
_coinbase_feed()    — Coinbase Advanced Trade WS → real-time BTC price
_clob_feed()        — Polymarket CLOB WS → real-time Up/Down ask prices → triggers _check_signal()
_render_loop()      — redraws terminal every second
```

All tasks run concurrently via `asyncio.gather()`.

---

## Probability surface

Built by `btc_model_builder.py` and saved to `~/.btc_model_surface.json`.

- **Source data:** 365 days of 1-min BTC/USD candles from Coinbase REST
- **Training:** Logistic regression (`sklearn`) with isotonic calibration on ~490k observations
- **Features:** `[t, delta, t², delta², t×delta]` where `t = elapsed_sec / 900`
- **Grid:** 61 time steps (every 15s, 0–900s) × 301 delta steps (0.01%, range −1.5% to +1.5%)
- **Confidence:** based on kernel effective sample size (`n_eff`)
  - `n_eff ≥ 30` → High
  - `n_eff ≥ 10` → Med
  - `n_eff < 10`  → Low (ignored for signals)
- **Lookup:** O(1) index arithmetic — no sklearn at runtime

```python
get_odds(elapsed_sec, delta_pct)
# → {"p_up": 0.591, "p_down": 0.409, "n_eff": 36851, "confidence": "high"}
```

---

## Signal logic (`_check_signal`)

Called on every CLOB tick. Guards applied in order:

| Guard | Value | Reason |
|---|---|---|
| CLOB ready + price + open set | required | can't compute without all three |
| `elapsed_sec > 30` | first 30s blocked | CLOB prices stale at window start |
| `elapsed_sec < 900` | last second blocked | window ending |
| CLOB price range | `0.05 – 0.95` | reject garbage/stale ticks |
| Confidence | `high` or `med` only | `n_eff < 10` too sparse |

### Entry condition (buy leg 1)

```
model_p_up  > CLOB_ask_up  + MISMATCH_THRESHOLD   AND   model_p_up  > 0.50
model_p_down > CLOB_ask_dn + MISMATCH_THRESHOLD   AND   model_p_down > 0.50
```

- `MISMATCH_THRESHOLD = 0.07` (7%)
- Model must be **above 50%** — only buy the side that is historically favored
- Won't re-enter a side that already has an open unfired position this window

### Arb lock condition (buy leg 2)

After leg 1 is staged, monitors the other side on every tick:

```
leg1_entry + other_side_ask  ≤  ARB_TARGET  (0.90)
```

Profit per share = `1.00 − total_cost`.  Min profit = **10¢/share**.

### Direct arb

If `CLOB_up + CLOB_dn < 0.90` at any point (both sides together cheap enough),
fires a direct arb alert with no staged position needed.

---

## Staged positions

Multiple positions can be open per window — one per side (Up/Down).

```
_staged = {
    window_ts: [
        {"side": "Down", "other": "Up", "entry_price": 0.520,
         "entry_sec": 313, "entry_model": 0.591, "entry_neff": 36851,
         "limit_target": 0.380, "arb_fired": False},
        ...
    ]
}
```

- Once `arb_fired = True`, that slot is done but the same side can be re-entered
  if a new edge appears later in the window
- `_staged` is pruned on each window roll — only current window kept

---

## Display

```
━━━  BTC ODDS MONITOR  ━━━  20:35:13  ━━━  window +313s / 587s left  (min 5/14)  ━━━

  BTC  $ 67,221.99   delta  -0.030%   open $ 67,242.41
         (green bg = up tick, red bg = down tick)

  Mod Up   Mod Dn   nEff   Cf   CLOB Up   CLOB Dn    Sum     ΔUp     ΔDn   Signal
  ──────────────────────────────────────────────────────────────────────────────────
   59.1%    40.9%  36851    H     0.520     0.490   1.010   -7.1%   +9.1%  ▼ BUY DOWN +0.10

  STAGED: BTC Down @ 0.520  (t=313s  model 59.1%  n=36851)
     Up side now: 0.360   combined: 0.880   limit target ≤ 0.380   gap -0.020 to target

  ── Alerts ──────────────────────────────────────────────
  20:04:59  ▼ BUY DOWN @ 0.500  model=57.4%  edge=+0.07  n=41597  | limit UP target ≤ 0.400
  20:06:12  *** ARB LOCKED — buy Up @ 0.400  total=0.900  profit=0.100/share (10.0¢) ***

  Surface: 35,003 windows  Threshold: 7%  ARB target: 0.90  Streaming live  Ctrl+C to stop.
```

- **ΔUp / ΔDn** — CLOB minus model. Green = CLOB underpriced (edge). Red = CLOB overpriced.
- **Signal** column fires only when edge > 7% AND model > 50%
- **BTC price** shows green background on uptick, red on downtick

---

## Key constants

| Constant | Value | Meaning |
|---|---|---|
| `MISMATCH_THRESHOLD` | `0.07` | Minimum model-vs-CLOB gap to fire a signal |
| `ARB_TARGET` | `0.90` | Max combined cost for arb lock (≥10¢ profit guaranteed) |
| `WINDOW_SEC` | `900` | 15-minute window size in seconds |
| `MAX_ALERTS` | `12` | Alert log lines kept on screen |

---

## Data sources

| Source | What | How |
|---|---|---|
| Coinbase REST | Window open price | `GET /products/BTC-USD/candles` once per window |
| Coinbase WS | Live BTC price | `wss://advanced-trade-ws.coinbase.com` ticker channel |
| Polymarket Gamma API | Token IDs for Up/Down | `GET gamma-api.polymarket.com/markets?slug=btc-updown-15m-{ts}` |
| Polymarket CLOB WS | Live ask prices | `wss://ws-subscriptions-clob.polymarket.com/ws/market` |
