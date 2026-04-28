# Scallops Extension — Longer Timeframe Markets

*Created: 2026-04-22*

---

## Goal

Use Scallops' signals from Polymarket 4h/1h/named markets to trade on Kalshi's "above/below" strike markets (KXBTCD, KXETHD, KXSOLD, KXXRPD).

---

## What Scallops Trades (Beyond 15m)

| Market | Trades | Notional | Kalshi Map |
|---|---|---|---|
| 15m up/down | 82,401 | $2.5M | KXBTC15M (active) |
| 5m up/down | 72,732 | $1.9M | None on Kalshi |
| Named (hourly) | 26,631 | $1.3M | No direct 1h market |
| 4h up/down | 8,901 | $494k | KXBTCD (daily above/below) |

---

## Kalshi Strike Markets (KXBTCD)

**Structure:**
- Settles at **2pm ET** and **5pm ET** daily
- Strikes every **$100** (2pm event) or **$250** (5pm event)
- Binary: "BTC above $78,800 at 2pm ET?" → YES/NO
- ATM (near current price): YES ~$0.55, NO ~$0.45
- All 4 coins: KXBTCD, KXETHD, KXSOLD, KXXRPD

**Key difference from 15m:**
- 15m = relative to open ("did price go up or down?")
- Daily = absolute strike ("is price above $78,800?")
- Multiple strikes available simultaneously (ladder from deep ITM to deep OTM)

---

## Research Findings

### 4h Signal → Kalshi Daily Strikes
- **16 backtestable windows, 43.8% WR — not viable**
- Signal decays over 6-14h gap between Scallops trade and Kalshi settlement
- His 4h WR is 62.5% on Poly but doesn't survive the time gap to Kalshi

### Hourly Named Signal
- **92 windows, 50% overall WR — coin flip**
- Alpha zone ($0.55+ entry): 72.7% WR on 11 windows (small sample)
- High conviction ($5k+ notional) is actually worst (34.4% WR)
- No direct Kalshi hourly market exists

### His 4h Activity
- **BTC**: 63 windows, $3,926 avg/window, 62.5% WR on Poly
- **ETH**: 63 windows, $1,163 avg/window, 56.2% WR
- **SOL**: 61 windows, $481 avg/window, 59.4% WR
- **XRP**: 62 windows, $482 avg/window, 65.6% WR
- Fires every 4h = 6 windows/day, all 4 coins

### His first trade in 4h: $0.55 median entry
- Direction correct 62.5% on Poly (relative to open)
- When mapped to "above ATM strike at settlement" → 43.8% (loses)
- The gap: Poly settles 4h from open, Kalshi settles at fixed clock times

---

## Viable Strategy Framework

### Approach: Scalp the ATM Strike (don't hold to settlement)

1. Scallops fires on 4h/named Poly market with entry ≥$0.55
2. Find Kalshi KXBTCD strike nearest current BTC price (ATM)
3. Buy YES if he says Up, NO if he says Down
4. **Exit within 1-2 hours** (don't wait for 2pm/5pm settlement)
5. Target: ride the directional move, exit when move plays out

**Why scalp instead of hold:**
- His signal is "price going up from HERE" not "price above $X at 2pm"
- The ATM strike will move $0.10-$0.20 on a $200 BTC move
- Capture that move, exit before the signal decays

### Requirements to Build
- Real-time KXBTCD strike prices (bid/ask per strike)
- ATM strike selection logic (find closest to spot)
- Entry logic (map Scallops direction → YES/NO on ATM)
- Exit logic (time-based or price-based)
- Risk limits (max position per strike)

---

## Data Collection TODO

- [ ] **Record KXBTCD/KXETHD/KXSOLD/KXXRPD strike prices** — poll Kalshi REST for all open strikes every 30s, store to DB. Need: ticker, strike, yes_ask, no_ask, yes_bid, no_bid, timestamp.
- [ ] **Record which strikes are ATM** at each timestamp (nearest to live spot price)
- [ ] **Record Scallops' 4h and named market trades** — already captured in `.scallops_live_trades.jsonl` via the shadow recorder
- [ ] **Build historical strike → settlement mapping** — for backtesting, need to know: for each past KXBTCD event, what strikes existed and how did each settle?
- [ ] **Backtest scalping strategy** — using recorded tick data: enter ATM on Scallops signal, measure P&L at 30m/1h/2h exit vs hold-to-settlement

## Implementation TODO

- [ ] Create `kxbtcd_recorder.py` — polls Kalshi REST for all KXBTCD/KXETHD/KXSOLD/KXXRPD strikes every 30s, writes to `~/.kxbtcd_ticks.db` (SQLite)
- [ ] Create `kxbtcd_backtest.py` — runs the scalp strategy on historical data
- [ ] Create `kxbtcd_trader.py` — live trader (after backtest validates)
- [ ] Wire Scallops 4h/named signals into the trader
- [ ] ATM strike picker (find strike nearest spot, account for bid-ask spread)
- [ ] Exit logic (time-based: exit after N minutes; or profit-target/stop-loss)

---

## Open Questions

1. How liquid are the KXBTCD ATM strikes? Can we fill 10+ shares at the ask?
2. Can we sell (exit) mid-window, or only hold to settlement?
3. Do the strikes reprice fast enough to capture a $200 BTC move?
4. Is there a Kalshi WebSocket for strike price updates, or REST-only?
5. What's the bid-ask spread on ATM strikes? (Saw ~$0.02-$0.03 in our query)
6. How many events per day? (Currently 2: 2pm and 5pm ET. More on some days?)
