# Polymarket 15-min Strategy Research

## 1. What We Are Trying To Do

Polymarket runs short-window binary markets: **BTC/ETH/SOL/XRP Up or Down in the next 15 minutes**.
Each market resolves to exactly **$1.00 per share** on the winning side, **$0.00** on the losing side.

We discovered a wallet (**Idolized-Scallops**) making consistent profit on these markets and are
reverse-engineering its logic while running our own paper strategy alongside it.

---

## 2. Current Strategy Logic (as of April 4, 2026)

### The Core Insight

The Polymarket CLOB (order book) lags real price moves by ~55 seconds. When the window opens,
one side gets bought heavily early — pushing its ask price up — making the **other side temporarily
cheap relative to fair value**. The bot exploits this by buying the cheap side.

We confirmed this by analysing the bot's actual trades: it **always opens on the lower-ask (cheaper)
side**, then adds to both sides opportunistically as prices move during the window.

### Phase 1 — Contrarian Open (fires at 90s into window)

**Signal:** CLOB ask imbalance — whichever side has the **lower ask** is underpriced.  
**Action:** Buy $200 of the cheaper side.  
**Reason:** Early buyers push one side expensive. The other side is mispriced and will recover.

```
ask_up < ask_dn  →  bet Up   (Up is cheaper, market over-sold it)
ask_up > ask_dn  →  bet Down (Down is cheaper, market over-sold it)
```

> **Why not follow coin price direction?**  
> We tested coin-price direction (34% accuracy) — worse than random. The CLOB is the signal.

### Phase 2 — Mid-window Adapt (fires at 10 min)

**Signal:** Which side is cheaper NOW vs what we bought in Ph1.  
**Two paths:**
- Our Ph1 side is **still the cheaper side** → add conviction ($300) to the same side
- The OTHER side is now cheaper (CLOB flipped) → buy that side cheap ($100 hedge)

```
cheap_now == ph1_side  →  add $300 to ph1_side    (ph2-add-conviction)
cheap_now != ph1_side  →  add $100 to cheap_now   (ph2-cheap-{side})
```

### Phase 3 — Conviction Load (fires at 14.5 min)

**Signal:** CLOB has nearly settled. The higher-priced side is the confirmed winner.  
**Action:** Load $500 on the higher-priced side.  
**Skip if:** winner price already > 0.95 (no value left, would pay too much).

```
ask_up >= ask_dn  →  load $500 Up   (market confirmed Up)
ask_up <  ask_dn  →  load $500 Down (market confirmed Down)
```

### Stakes per window

| Phase | Amount | Signal used |
|-------|--------|-------------|
| Ph1 | $200 | CLOB lower ask (contrarian) |
| Ph2 winning | +$300 | Same side still cheap |
| Ph2 hedge | +$100 | Other side now cheap |
| Ph3 | +$500 | CLOB higher ask (confirmed winner) |
| **Max total** | **~$1000** | All 3 phases fire |

### Resolution

Polymarket officially closes the market 10–20 min after the window ends and sets one side to $1.00.
We poll for `closed=true` every 10s and settle P&L when confirmed.

---

## 3. Price Feeds

### Coin prices — Coinbase Advanced Trade WebSocket
```
wss://advanced-trade-ws.coinbase.com
```
- Real-time BTC/ETH/SOL/XRP prices, ~10ms latency, no auth needed
- Used to track coin direction for context (not for Ph1 signal anymore)

### CLOB prices — Polymarket WebSocket (switched April 4, 2026)
```
wss://ws-subscriptions-clob.polymarket.com/ws/market
```
- **Sub-second** push updates for every order book change
- Previously polled REST every 10s — now real-time
- Sends `price_changes` events with `best_ask` per token as trades happen
- This is the primary signal for Ph1, Ph2, Ph3 decisions

> **Why not pay for faster data?** Coinbase WS is already 8-15ms. The bottleneck is strategy
> accuracy, not data speed. Institutional feeds ($10k+/mo) would not improve results.

---

## 4. The Setup

### Files

| File | Purpose |
|------|---------|
| `btc_strategy_paper.py` | Main paper trader — runs 24/7, trades all 4 coins |
| `btc_dashboard.py` | Web dashboard at http://localhost:7331 |
| `run_all.sh` | Starts everything, prevents Mac sleep, auto-restarts on crash |

### Persistent data (home directory)

| File | Contents |
|------|---------|
| `~/.btc_strategy_state.json` | All positions (open + closed), running P&L |
| `~/.btc_strategy_improvements.json` | One entry per resolved window — strategy tuning log |

### Running

```bash
cd /Users/Harish/highlowticker-tui
bash run_all.sh          # start everything
bash run_all.sh stop     # stop everything
```

Fresh start (wipe all history):
```bash
bash run_all.sh stop
rm -f ~/.btc_strategy_state.json ~/.btc_strategy_improvements.json
bash run_all.sh
```

Monitor live:
```bash
tail -f /tmp/btc_strategy.log    # trader activity
tail -f /tmp/btc_dashboard.log   # dashboard server
```

---

## 5. Idolized-Scallops — The Bot We Are Learning From

| Field | Value |
|-------|-------|
| Name | Idolized-Scallops |
| Wallet | `0xe1d6b51521bd4365769199f392f9818661bd907c` |
| Profile | https://polymarket.com/profile/0xe1d6b51521bd4365769199f392f9818661bd907c |

### Observed behaviour

- Trades BTC, ETH, SOL, XRP 15-min windows exclusively
- 30–60 trades per window — fully automated bot
- **Always opens on the cheaper (lower-ask) side** — confirmed from trade analysis
- Uses small probe buys first, then loads heavy on confirmed direction
- Buys the opposite side when it gets cheap mid-window (both sides held at expiry)
- Never sells — always holds to resolution

### Specific example decoded (BTC 1:00-1:15PM, winner=Up)

```
13:00:15  Bot buys Up @0.460  (Up was cheaper — contrarian open)
13:01-09  Bot buys Down @0.70-0.81  (Down now getting expensive but bot adds)
13:10:13  Bot pivots back to Up @0.14  (Down at 0.86, Up now ultra-cheap again)
13:11:09  Bot loads Up @0.383  (Up recovering, bot adds conviction)
Result: Up wins. Bot holds 1310sh Up + 1184sh Down. P&L = -$37 (near breakeven)
```

Our trade that window: 3 trades all Down, -$1000 loss.

### How we shadow the bot

Every 10 seconds we poll:
```
GET https://data-api.polymarket.com/trades?user=0xe1d6b51521bd4365769199f392f9818661bd907c&limit=200
```
New trades on windows we're trading are recorded. When the window resolves we compute their P&L
and log it alongside ours in `~/.btc_strategy_improvements.json`.

---

## 6. Reading the Dashboard (http://localhost:7331)

Auto-refreshes every 10 seconds.

### Header KPIs
- **Realized P&L (us/bot)** — settled windows only
- **Open** — windows currently trading
- **Closed** — fully resolved windows

### Open Positions table

| Column | Meaning |
|--------|---------|
| Phase | Which phase last fired (Ph1/Ph2/Ph3) |
| Up/Down shares | Shares held, avg price paid, current CLOB ask |
| Cost → Val | Spent vs current mark-to-market |
| Unreal P&L | Live unrealized gain/loss |
| Time left | Seconds to expiry |

### Closed Windows

Each closed window is a card — click **"view trades →"** to see:
- Step-by-step trade table for us (with reason tags: ph1-contrarian, ph2-add-conviction, ph3-conviction)
- Step-by-step trade table for the bot
- Running totals per side

### Improvement Log

One row per resolved window showing: coin, window, winner, Ph1 guess (✓/✗), our P&L, bot P&L, delta.

---

## 7. Strategy Evolution Log

| Date | Change | Reason | Result |
|------|--------|--------|--------|
| Apr 3 | Initial: follow coin price direction in Ph1 | First attempt | 34% Ph1 accuracy — worse than random |
| Apr 4 | Switched to CLOB higher-ask signal in Ph1 | Tested 8 windows | Still MISS — same direction as coin price |
| Apr 4 | **Switched to CLOB lower-ask (contrarian) in Ph1** | Bot analysis showed it always buys cheaper side | Testing now |
| Apr 4 | Ph2 also contrarian — buy whichever side is cheaper at 10min | Follows same bot logic | Testing now |
| Apr 4 | Switched CLOB from REST poll (10s) to WebSocket (real-time) | Sub-second price updates | Active |

**Current clean baseline started: April 4, 2026 ~1:15 PM ET.**

---

## 8. Bugs Fixed

| Bug | Impact | Fix |
|-----|--------|-----|
| `window_start = time.time()` | Phases fired ~15 min early | Changed to `float(ts)` (actual market start timestamp) |
| No guard for `elapsed < 0` | Ph1 fired immediately on future-window creation | Added `if elapsed < 0: return` |
| Main loop only swept `offset=[0,1]` | Old expired positions never resolved | Added full sweep of all unresolved expired positions |
| `_load_state` dropped positions >5 min past expiry | Crash lost unresolved positions | Extended cutoff to 2 hours |

---

## 9. Next Steps

- [ ] Accumulate 30+ windows with contrarian Ph1 — measure accuracy vs 34% baseline
- [ ] If accuracy still low: add minimum spread threshold (skip Ph1 if `|ask_up - ask_dn| < 0.08`)
- [ ] Track CLOB trend over first 90s (rising/falling) not just single snapshot
- [ ] Consider copying bot's multi-trade approach within each phase instead of single buy
- [ ] Analyse which coins have highest win rate — consider dropping worst performer
