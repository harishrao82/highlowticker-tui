# Bot Finder — Side Projects

*Created: 2026-04-23*

---

## Project 1: Signal Finder — Discover New Scallops-like Bots

### Goal
Scan Polymarket 15m/5m/1h/4h crypto up/down markets and identify **profitable bots** by analyzing all users who trade in these events.

### Approach
1. **Crawl recent events** — for each 15m window in the past 7 days, fetch all trades (not just one user)
2. **Identify frequent wallets** — who shows up in 80%+ of windows? Those are bots.
3. **Profile each bot:**
   - How many windows do they trade per day?
   - What time of day are they active?
   - What's their avg entry price? (favorite buyer at $0.60+ vs underdog at $0.35?)
   - Do they trade all 4 coins or specialize?
   - What's their estimated WR? (compare their side to settlement winner)
4. **Rank by edge** — sort bots by estimated WR × volume. The ones with 65%+ WR and high volume are the next Scallops.
5. **Output**: leaderboard of wallets with WR, volume, activity pattern, and whether they're worth shadowing.

### Data Source
- `GET https://data-api.polymarket.com/trades?market={conditionId}&limit=500` — all trades for a market
- Each trade has `proxyWallet`, `side`, `price`, `size`, `outcome`, `timestamp`
- Need to paginate through all trades per window (could be 500+ per window across all users)
- Settlement winners: infer from late-window book state (bid=0.999) or from our btc_windows.db

### Implementation Plan
```
bot_finder.py:
  1. For each 15m window in past 7 days (96/day × 7 × 4 coins = ~2,688 windows):
     - Fetch all trades via /trades?market=conditionId
     - Group by proxyWallet
  2. Build wallet profiles:
     - windows_traded: set of (coin, ws) they appeared in
     - total_trades, total_notional
     - side_distribution: % Up vs % Down
     - avg_entry_price
     - hours_active: histogram of trading hours
  3. For each wallet, compute WR:
     - For each window they traded, check if their first-trade side matches the winner
  4. Filter: wallets with ≥50 windows + ≥60% WR
  5. Output: ranked list with stats
```

### Rate Limiting
- Polymarket data API: ~2 req/sec safe
- 2,688 windows × ~3 pages each = ~8,000 requests
- At 2/sec = ~67 minutes to crawl 7 days
- Run once, cache results, update daily

### TODO
- [ ] Build `bot_finder.py` — crawl all trades per window, profile wallets
- [ ] Build wallet WR calculator (join with settlement outcomes)
- [ ] Build leaderboard output (terminal table or HTML)
- [ ] Identify top 5 bots besides Scallops
- [ ] Test: can we shadow any of them profitably?

---

## Project 2: Scallops Signature Detector — Find Him If He Moves

### Goal
If Scallops changes wallets or moves to a new market, detect him by his **trading signature** — the pattern of how he trades is unique even if the wallet is unknown.

### His Known Signature
From our reverse-engineering:

1. **Timing**: enters within first 30-120s of a 15m window
2. **All 4 coins**: trades BTC, ETH, SOL, XRP in same window
3. **Both sides**: buys Up AND Down (two-sided market maker)
4. **Ladder**: multiple small orders across price levels (2-6¢ steps)
5. **Size**: $50-200 per side per window on 15m, $500+ on 4h
6. **Frequency**: trades 90%+ of all windows during active hours
7. **Cross-coin correlation**: all 4 coins same direction when entry ≥$0.50
8. **Named + 4h + 5m**: also active on hourly, 4h, and 5m markets simultaneously

### Signature Matching Algorithm
```
For each unknown wallet in a crypto up/down market:
  Score = 0
  
  if trades_per_day >= 80:                    score += 2  (high frequency)
  if trades_all_4_coins:                      score += 2  (multi-coin)
  if buys_both_up_and_down:                   score += 2  (two-sided)
  if avg_trades_per_window >= 5:              score += 1  (ladder pattern)
  if active_in_15m AND 5m AND 4h:            score += 2  (multi-timeframe)
  if entry_within_first_60s >= 50%:           score += 1  (early mover)
  if daily_notional >= $10,000:               score += 1  (serious size)
  
  if score >= 8: LIKELY SCALLOPS or similar bot
```

### Implementation Plan
```
scallops_detector.py:
  Input: conditionId of any crypto up/down market (or scan all active ones)
  
  1. Fetch all trades for the past 24h on that market
  2. Group by wallet
  3. For each wallet, compute signature scores
  4. Flag any wallet scoring ≥ 8
  5. If flagged: 
     - Compare trade pattern to known Scallops signature
     - Alert: "Possible Scallops at wallet 0x..."
     - Start shadowing via /activity endpoint
```

### When to Run
- **Daily**: scan yesterday's trades across all active crypto markets
- **On alert**: if Scallops goes quiet (no trades for 4h), run detector on all markets to find where he moved
- **New market launch**: when Polymarket adds new crypto pairs, scan for bot activity

### TODO
- [ ] Build signature scoring function
- [ ] Build `scallops_detector.py` — scan markets, score wallets
- [ ] Test on known Scallops wallet (should score 10+)
- [ ] Test on random wallets (should score <5)
- [ ] Build alert system: notify when Scallops goes quiet + new high-score wallet appears
- [ ] Store wallet profiles for trend tracking (is a new bot ramping up?)

---

## Shared Infrastructure

Both projects need:
- [ ] Polymarket trade crawler (paginated /trades by market, cached)
- [ ] Settlement outcome mapper (conditionId → winner)
- [ ] Wallet profile storage (SQLite or JSONL)

Build the crawler first — both projects use it.
