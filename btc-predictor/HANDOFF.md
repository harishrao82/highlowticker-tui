# BTC Predictor — n-5 Auto-Tune Setup

Reference for the live arb-trading system layered on top of the BTC 15-min candle predictor. Last updated 2026-05-26.

## TL;DR — what is n-5 (follow-last-5)?

Every 15 minutes, the server grid-searches `(K_yes, K_no, Δ_yes, Δ_no)` over the **last 5 settled windows** and picks the combination with the highest summed P&L. Those thresholds drive the next window's arb trade decisions. The "5" is the **lookback** — empirically beats N=1 and N=10 on this market's regime persistence (~75-min correlation length).

```
                 ┌─────── settled window N ────────┐
        threshold θ from windows [N-5..N-1]    →    used to trade window N
                 └─────────────────────────────────┘
```

## Live components

| component | file | role |
|---|---|---|
| Prediction publisher | [src/api/server.py](src/api/server.py) `prediction_publisher()` | 5 Hz model runs + Kalshi state merge; pushes `pred` over WS |
| Arb signal | [src/collector/kalshi_feed.py](src/collector/kalshi_feed.py) `compute_arb_signal()` | Boolean firing logic given `pred` + thresholds |
| Arb tracker | [src/api/server.py](src/api/server.py) `ArbTradeTracker` | Authoritative per-window trade list; throttle, mark-to-market, settle |
| Auto-tuner | [src/api/server.py](src/api/server.py) `on_window_settled()` | On window roll: settles, writes ledger, retunes from last 5 |
| Grid search | [src/collector/kalshi_feed.py](src/collector/kalshi_feed.py) `find_optimal_thresholds()` | 7×7×7×7 = 2401-combo sweep; ties broken toward most-conservative |
| Simulator | [src/collector/kalshi_feed.py](src/collector/kalshi_feed.py) `simulate_window_breakdown()` | Replays one window's ticks; returns P&L + per-outcome avg entries |
| Outcome | [src/api/server.py](src/api/server.py) `_determine_outcome()` | **Kalshi YES_mid > 0.5 at sec ≥ 870** (Kalshi's market verdict, not TWAP) |
| Executor | [src/api/kalshi_executor.py](src/api/kalshi_executor.py) `place_arb_buy()` | Live Kalshi order placement: maker @ bid (3s TTL) → market @ ask+5¢ cap → drop. Gated by `LIVE_TRADING=1` env var. Defaults off. |

## Constants worth knowing

| constant | location | value | meaning |
|---|---|---|---|
| `AUTOTUNE_WINDOW_LOOKBACK` | `AppState.__init__` | **5** | the "n" in n-5 |
| `AUTOTUNE_AFTER_N_WINDOWS` | `AppState.__init__` | **5** | tuner activates after this many settled windows |
| `ARB_THROTTLE_SEC` | `kalshi_feed.py` | **3** | same-side throttle in backtest simulator |
| `LIVE_ARB_THROTTLE_SEC` | `server.py` | **3** | same-side throttle in live tracker (matches simulator) |
| `ARB_MIN_SAMPLE_SEC` | `kalshi_feed.py` | **30** | gate; first 30s of each window not traded |
| `K_GRID` | `kalshi_feed.py` | `[0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60]` | K_yes / K_no candidates |
| `D_GRID` | `kalshi_feed.py` | `[0.04, 0.06, 0.08, 0.10, 0.12, 0.15, 0.20]` | Δ_yes / Δ_no candidates |
| `REHYDRATE_HISTORY_DEPTH` | `server.py` | **20** | how many past windows surface in dashboard table |
| `CLUSTER_GAP_SEC` | `live_monitor.html` | **8** | gap above which consecutive trades become a new cluster on chart |
| `LIVE_TRADING_ENABLED` | `kalshi_executor.py` (env-driven) | **False** (default) | when True, ArbTradeTracker spawns real Kalshi orders alongside paper records |
| `MAKER_OFFSET_CENTS` | `kalshi_executor.py` | **0** | maker limit posts at the best bid (0¢ below). Backtest: dominated all `(1¢, 2¢, 3¢)` alternatives by $100-$200 |
| `MAKER_TTL_SEC` | `kalshi_executor.py` | **3** | seconds maker has to fill before market fallback fires |
| `MARKET_CAP_CENTS` | `kalshi_executor.py` | **5** | market fallback cap = current_ask + 5¢. Above this → drop |
| `KILL_SWITCH_FILE` | `kalshi_executor.py` (env-overridable) | `/tmp/btc_predictor_stop` | touch this file to halt all live-order placement |

## Decision rules (live)

```
BUY YES if (model_p − YES_mid) > Δ_yes  AND  YES_mid > K_yes  AND  sec ≥ 30
BUY NO  if (NO_p     − NO_mid)  > Δ_no   AND  NO_mid  > K_no   AND  sec ≥ 30
```

Both gated by a 3-second same-side throttle (so a continuous signal fires every 3 s, not every prediction tick).

Entry = `market_ask` at decision time. Hold to settle. Win = $1, loss = $0, net per trade = `payoff − entry_ask`.

## Persistent storage

All decision history lives on disk so the server can rebuild itself on restart:

| file | written by | source of truth for |
|---|---|---|
| `data/predictions.jsonl` | publisher, every (window_open_ts, sample_sec) | per-second tick state (model_p, Kalshi bid/ask, current_price, window_open_px); chart history rehydration |
| `data/window_pnl.jsonl` | `on_window_settled` | per-window summary with thresholds active at settle, outcome, W/L counts, per-outcome avg entries; **auto-tuner's authoritative input** |
| `data/arb_trades.jsonl` | `ArbTradeTracker` on window roll | per-trade ledger (every trade that fired, with entry, status, P&L, settled outcome). In live mode, extra fields: `execution_path`, `kalshi_order_id`, `actual_fill_price`, `fee_dollars` |
| `data/execution_results.jsonl` | `kalshi_executor.place_arb_buy` | one line per live-order attempt with `trade_id`, path, fill, fees, latency. Survives mid-window restarts; join with arb_trades.jsonl by `trade_id` |
| `data/window_pnl.jsonl.bak` | one-off backfill script | snapshot before the outcome-ground-truth correction (2026-05-26) |

## Rehydration on startup

Three things happen on server boot:

1. **`rehydrate_autotune_state()`** — reads `window_pnl.jsonl` for already-LIVE rows; for missing windows, walk-forward reconstructs what θ would have been (using only prior windows = deterministic walk-forward). Sets initial `STATE.arb_thresholds` from last 5.
2. **`rehydrate_current_window_history()`** — replays the current 15-min window's predictions into `STATE.history` AND into `ArbTradeTracker` so mid-window restarts preserve trades + chart points.
3. **Open `predictions.jsonl` in append mode** — continues writing.

Net effect: a server restart at sec 600 of a window loses nothing visible. Trades fired in sec 30–600 are reconstructed in-memory; the chart paints from sec 0; thresholds picked up from last 5 settled.

## Endpoints

| route | purpose |
|---|---|
| `GET /` | dashboard HTML |
| `WS /ws/predictions` | live tick stream; sends `latest_prediction` + history slice on connect |
| `GET /window_pnl_log?limit=N` | last N settled rows for the settlement-history table |
| `GET /arb_window` | current 15-min in-flight trades (server-side mirror of dashboard table) |
| `GET /arb_trades?limit=N` | historical trade ledger |
| `GET /optimal_settings?n_windows=N` | grid-search summary over last N windows (powers Last-1 / Last-5 panels) |
| `GET /btc_candles?hours=H` | 1-min OHLC proxy for the BTC candle chart |
| `GET /model_info` | model metadata (currently unused by dashboard) |
| `GET /health` | basic up-check |

## Recent fixes (running log of non-obvious changes)

| date | change | reason |
|---|---|---|
| 2026-05-23 | Switched from N=10 to N=5 lookback | Walk-forward backtest: N=5 dominated (better hit-rate, drawdown profile, regime adaptivity) |
| 2026-05-23 | Throttle 5s → 3s | Matched observed live click cadence; live & sim now consistent |
| 2026-05-23 | Per-window auto-tune wired into `on_window_settled` | Auto-tune was previously manual |
| 2026-05-23 | Walk-forward threshold reconstruction in rehydrate | Past windows used to show current thresholds — now show the θ the tuner would have picked at the time |
| 2026-05-24 | Per-trade settle bug fixed | `process_tick` was clearing trades before `_refresh_pnl` settled them → `arb_trades.jsonl` was empty |
| 2026-05-24 | Dashboard refactored to pure renderer | All arb logic on server; dashboard reads `pred.arb_window` |
| 2026-05-26 | **`_determine_outcome` uses Kalshi YES_mid as ground truth** | Our TWAP-based `cpx > opx` disagreed with Kalshi's actual settlement on 2.8% of windows (sub-1-bps moves). Backfilled 5 historical rows. |
| 2026-05-26 | Dashboard times converted to ET | Previously displayed UTC |
| 2026-05-26 | Performance: per-tick dirty checks added | YES chart line/markers and arb log were re-rendering at 5 Hz; now only on actual data changes |
| 2026-05-26 | **Live executor added** (`kalshi_executor.py`) | Maker @ bid (3s) → market @ ask+5¢ → drop. Behind `LIVE_TRADING=1` env var. Backtest on 270 windows: maker @ 0¢ beat 1-3¢ alternatives by $100-200; market fallback added $128 / $1.30 per fill over drop-only. |

## How to verify the system is working

```bash
# Server health
curl -s localhost:8000/health | python3 -m json.tool

# Latest live arb window
curl -s localhost:8000/arb_window | python3 -m json.tool

# Last 5 settled windows + active thresholds
curl -s 'localhost:8000/window_pnl_log?limit=5' | python3 -m json.tool

# Hindsight optimum on last 5 (should match active thresholds after a settle)
curl -s 'localhost:8000/optimal_settings?n_windows=5' | python3 -m json.tool

# Per-trade ledger
wc -l data/arb_trades.jsonl
tail -3 data/arb_trades.jsonl | python3 -m json.tool

# Look for autotune retunes in server log
grep -E 'AUTOTUNE|WINDOW.*settled|outcome flip' /tmp/btc_server.log | tail -20
```

## Known caveats / not-yet-fixed

- **Selection bias on N=5**: N was picked by looking at the same data we then evaluated it on. Going-forward live performance is the only honest validation; backtest agreement so far (+$167 live vs +$172 backtest) suggests it generalizes.
- **No slippage / market-impact model**: Both backtest and live assume fills at the recorded ask. Real Kalshi fills will be slightly worse — expect $1-2/window haircut.
- **Closed-loop**: Our own past trades influenced the Kalshi book state recorded in `predictions.jsonl`. Negligible at 1-share size.
- **Late-window outcome uses last recorded tick**: 12% of windows have last_sec < 895 (data gaps from server restarts). Those windows fall back to TWAP-based outcome (which can disagree with Kalshi by 1-2%).

## Where to look first when something looks wrong

1. Window outcome flipped vs Kalshi → check `[WINDOW HH:MM] outcome flip` warning in server log (`_determine_outcome` flagged a TWAP-vs-Kalshi disagreement)
2. No trades firing for >5 min → check `K_yes` setting vs current `YES_mid` (if YES is deep OTM, K_yes=0.60 will block; this is by design)
3. Dashboard stale → check WS reconnect status in browser console; restart `python scripts/run_live.py` if needed
4. Server hung after restart → `[rehydrate]` takes ~20s for walk-forward (20 windows × 2401-combo grid). If it never logs `Application startup complete`, look for tracebacks
5. P&L looks wildly different from Kalshi statement → outcome ground-truth fix (2026-05-26) — pre-fix rows may still show TWAP outcomes; use `original_outcome` field to spot them
