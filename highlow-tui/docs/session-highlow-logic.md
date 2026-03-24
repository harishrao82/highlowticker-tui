# Session High/Low Logic

How a new session high or low is created, how it appears in the UI, and how often the list refreshes.

---

## 1. Startup — Seeding Baselines

When the app starts, `TradierProvider.connect()` makes REST calls to Tradier (`/v1/markets/quotes`) for all 621 symbols in batches of 200.

For each symbol it stores:

| Variable | What it holds |
|---|---|
| `_open_prices[sym]` | Today's regular-session open price (used for % change) |
| `_session_highs[sym]` | Highest price so far today |
| `_session_lows[sym]` | Lowest price so far today |
| `_high_counts[sym]` | 0 (no new highs detected yet this run) |
| `_low_counts[sym]` | 0 (no new lows detected yet this run) |

These baselines represent the state of the market **at the moment the app connected**. Any symbol whose price later exceeds `_session_highs` or drops below `_session_lows` will trigger a new entry in the table.

---

## 2. Real-Time Stream — How Events Arrive

Tradier's SSE stream is subscribed in **3 parallel connections** (Tradier rejects requests with more than ~318 symbols due to a URL length limit of ~1330 chars):

- Connection 1: symbols 1–300
- Connection 2: symbols 301–600
- Connection 3: symbols 601–621

All three connections push raw JSON lines into a single shared `asyncio.Queue`. The `stream()` loop drains this queue continuously.

Two event types are processed:

**`trade`** — an actual transaction on an exchange:
```json
{"type":"trade","symbol":"AAPL","price":"253.10","size":"200","cvol":"38000000"}
```
Price = `price` field directly. Trade `size` is also fed into the volume spike tracker.

**`quote`** — a bid/ask update:
```json
{"type":"quote","symbol":"AAPL","bid":"253.08","ask":"253.12"}
```
Price = `(bid + ask) / 2`

---

## 3. New Session High — Detection Logic

For every incoming event, `_handle_event()` runs this check:

```
if price > _session_highs[sym]:
    _session_highs[sym] = price          # update the high watermark
    _high_counts[sym] += 1               # increment cumulative count
    record timestamp                     # for rate bars
    → emit HIGHLOW_UPDATE
```

A `HIGHLOW_UPDATE` is only emitted when `updated = True` (price broke the high **or** the low). If the price is between the current high and low, nothing is emitted and the UI is not touched.

---

## 4. New Session Low — Same Logic, Opposite Direction

```
if price < _session_lows[sym]:
    _session_lows[sym] = price
    _low_counts[sym] += 1
    record timestamp
    → emit HIGHLOW_UPDATE
```

---

## 5. How a Row Appears in the UI Table

`HIGHLOW_UPDATE` carries the full `newHighs` dict: `{symbol: cumulative_count}` for every symbol that has made at least one new high this run.

`_apply_highlow_update()` in `app.py` adds a new row **only if** the count for that symbol increased since the last update:

```
if count <= prev_highs.get(symbol, 0):
    skip   # already shown this count
```

So AAPL hitting a new high for the 3rd time today produces a new row showing count=3, pushing the previous AAPL row down. The table keeps the **50 most recent entries** (newest at the top).

Each row stores:
- Symbol, price at the new high/low
- Cumulative count (how many times it has made a new high/low since app started)
- % change from open
- Timestamp

---

## 6. Refresh Rate

There is **no polling interval**. The table updates are purely event-driven:

| Condition | UI update |
|---|---|
| Trade or quote event breaks session high/low | Immediate — new row inserted at top of table |
| Price between high and low | No update, no redraw |
| Market closed / no trades | No update (queue stays empty) |

In practice during market hours, active stocks like SPY can trigger updates every few seconds. Less liquid names may only update a handful of times per day.

The rate bars (30s / 1m / 5m / 20m) show how many new highs/lows occurred within each wall-clock-aligned window. These are recomputed on every update using the rolling timestamp list, pruned to a 20-minute window.

---

## 7. Daily Reset at 9:30 AM ET

A background task (`_market_open_reseed`) wakes up exactly at 9:30 AM ET each day and:

1. Clears `_high_counts`, `_low_counts`, all timestamps, volume spikes, and resets `VolumeTracker`
2. Re-fetches REST quotes — at this moment Tradier has the real regular-session open price
3. Re-seeds `_open_prices`, `_session_highs`, `_session_lows` from scratch

This ensures:
- `% change` is computed from the actual 9:30 open, not yesterday's close
- Any pre-market moves don't contaminate the session high/low baselines
- Counts start fresh for the new trading day

After the reset the SSE connections remain open — no reconnection needed.

---

## 8. Row Highlight Colors

Each row in the Highs/Lows table is color-coded. Priority is top-to-bottom — the first matching rule wins.

| Color | Trigger |
|---|---|
| **Flash** (no background) | The single most recent entry — the last new high/low that just arrived |
| **Dark green bg** (Highs) / **Dark red bg** (Lows) | 52-week high or low |
| **Yellow** | First time this symbol has appeared this session (`count = 1`) |
| **Orange** | Same symbol appears 3+ times consecutively in the list — keeps breaking out |
| **Purple** | % change accelerated — moved more than 0.5% between this row and the previous row for the same symbol |
| **Pink** | Volume spike — current 1-minute trade volume is 2x or more above its rolling average |
| No color | None of the above |

The thresholds for orange (default: 3 consecutive), purple (default: 0.5% move), and pink (default: 2× volume) are configurable via `s` → Settings, or by editing `config/highlight.json` directly.

---

## 9. Summary Flow

```
9:30 AM ET
  └─ _market_open_reseed() fires
       └─ REST seed: sets open, high, low baselines for all 621 symbols

During session (real-time)
  └─ Tradier SSE → 3 connections → shared queue
       └─ stream() drains queue → _handle_event()
            └─ price > session_high?
                 └─ YES → update watermark, increment count, emit HIGHLOW_UPDATE
                      └─ _apply_highlow_update() → count increased?
                           └─ YES → insert row at top of Highs table → _refresh_ui()
            └─ price < session_low?  (same path → Lows table)
```
