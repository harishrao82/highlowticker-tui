# Tradier Real-Time Data Guide

How we get real-time equity prices via Tradier, and how to extend it to options.

## Auth

All requests use a Bearer token:

```
Authorization: Bearer <TRADIER_ACCESS_TOKEN>
Accept: application/json
```

Token is stored in `highlow-tui/.env` as `TRADIER_ACCESS_TOKEN`.

---

## 1. Real-Time Stock Prices (What We Do Today)

### Step 1: Create a streaming session

```
POST https://api.tradier.com/v1/markets/events/session
```

Returns a `sessionid` used for the SSE connection.

### Step 2: Seed baselines via REST quotes

```
GET https://api.tradier.com/v1/markets/quotes?symbols=AAPL,MSFT,NVDA,...&greeks=false
```

- Batch up to 200 symbols per request
- Returns: `last`, `high`, `low`, `open`, `close`, `prevclose`, `bid`, `ask`, `volume`
- We use this to set session high/low baselines before the stream starts
- Re-fetched at 9:30 AM ET daily to reset for regular session open

Response shape:
```json
{
  "quotes": {
    "quote": [
      {
        "symbol": "AAPL",
        "last": 195.23,
        "high": 196.10,
        "low": 194.50,
        "open": 195.00,
        "close": 194.80,
        "prevclose": 194.80,
        "bid": 195.22,
        "ask": 195.24,
        "volume": 12345678
      }
    ]
  }
}
```

### Step 3: Connect to SSE stream

```
GET https://stream.tradier.com/v1/markets/events
    ?symbols=AAPL,MSFT,NVDA,...
    &sessionid=<from step 1>
    &filter=trade,quote
    &linebreak=true
```

- Max ~300 symbols per connection (URL length limit ~1330 chars)
- For more symbols, open multiple SSE connections (each needs its own session ID)
- Uses `httpx` async streaming (`client.stream("GET", ...)` + `resp.aiter_lines()`)

Each line is a JSON event:

**Trade event:**
```json
{
  "type": "trade",
  "symbol": "AAPL",
  "price": 195.23,
  "size": 100,
  "date": "2026-04-13T10:30:00"
}
```

**Quote event:**
```json
{
  "type": "quote",
  "symbol": "AAPL",
  "bid": 195.22,
  "ask": 195.24,
  "bidsize": 200,
  "asksize": 300
}
```

We compute mid-price from quotes: `(bid + ask) / 2`

### How events flow in our code

```
SSE stream → _sse_reader() → asyncio.Queue → stream() → _handle_event()
```

- `_sse_reader()` is a background task that reads lines and pushes to a shared queue
- `stream()` is an async generator that drains the queue and yields `HIGHLOW_UPDATE` dicts
- `_handle_event()` updates session highs/lows, tracks volume spikes, computes % change

### Key implementation details

- **Session reset**: At 9:30 AM ET, all counts/baselines are cleared and REST quotes re-fetched
- **Volume tracking**: Trade `size` is fed to `VolumeTracker` for rolling spike detection
- **Breadth counting**: High/low timestamps pruned to a 20-minute rolling window
- **Reconnect**: If SSE drops, the app's watchdog detects silence >90s and reconnects

---

## 2. Extending to Options Prices

Tradier has full options support. Here's what's available:

### Option Chain Lookup (REST)

```
GET https://api.tradier.com/v1/markets/options/chains
    ?symbol=SPY
    &expiration=2026-04-13
    &greeks=true
```

Returns all strikes for that expiration with: `bid`, `ask`, `last`, `volume`, `open_interest`, `greeks` (delta, gamma, theta, vega, IV).

### Get Available Expirations

```
GET https://api.tradier.com/v1/markets/options/expirations
    ?symbol=SPY
    &includeAllRoots=true
```

Returns list of expiration dates. For 0DTE, filter to today's date.

### Option Quotes (REST)

```
GET https://api.tradier.com/v1/markets/quotes
    ?symbols=SPY260413C00550000
    &greeks=true
```

Option symbols use OCC format: `SPY260413C00550000` = SPY, 2026-04-13 expiry, Call, $550 strike.

You can batch option symbols the same way we batch stock symbols (up to 200 per request).

### Real-Time Option Streaming (SSE)

**The same SSE stream supports option symbols.** You can mix stock and option symbols in one connection:

```
GET https://stream.tradier.com/v1/markets/events
    ?symbols=SPY,SPY260413C00550000,SPY260413P00540000
    &sessionid=<session_id>
    &filter=trade,quote
    &linebreak=true
```

Trade and quote events come through identically — the `symbol` field will be the OCC option symbol. You get real-time bid/ask/trade for options the same way as equities.

### Option Symbol Format (OCC)

```
SPY260413C00550000
│  │     ││       │
│  │     ││       └─ strike × 1000 (8 digits, zero-padded) → $550.000
│  │     │└── C = Call, P = Put
│  │     └─── expiration YYMMDD → 2026-04-13
│  └── underlying symbol (left-padded to 6 chars with spaces, but usually just the ticker)
```

### Lookup helper (Tradier)

```
GET https://api.tradier.com/v1/markets/options/lookup
    ?underlying=SPY
    &expiration=2026-04-13
    &strike=550
    &type=call
```

Returns the OCC symbol so you don't have to construct it manually.

---

## 3. Minimal Example: Stream SPY + a few 0DTE options

```python
import asyncio, json, os, httpx

TOKEN = os.getenv("TRADIER_ACCESS_TOKEN")
REST  = "https://api.tradier.com/v1"
SSE   = "https://stream.tradier.com/v1"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}

async def main():
    # 1. Get today's SPY expirations & chain
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{REST}/markets/options/chains",
                        headers=HEADERS,
                        params={"symbol": "SPY", "expiration": "2026-04-13", "greeks": "true"})
        chain = r.json()["options"]["option"]
        # pick a few ATM calls/puts
        atm_options = [o["symbol"] for o in chain
                       if abs(o.get("strike", 0) - 550) < 5][:4]

    # 2. Create stream session
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{REST}/markets/events/session", headers=HEADERS)
        session_id = r.json()["stream"]["sessionid"]

    # 3. Stream SPY + those options
    symbols = ["SPY"] + atm_options
    params = {
        "symbols": ",".join(symbols),
        "sessionid": session_id,
        "filter": "trade,quote",
        "linebreak": "true",
    }
    async with httpx.AsyncClient(timeout=None) as c:
        async with c.stream("GET", f"{SSE}/markets/events",
                            headers=HEADERS, params=params) as resp:
            async for line in resp.aiter_lines():
                if line.strip():
                    ev = json.loads(line)
                    print(f"{ev.get('type'):8} {ev.get('symbol'):24} "
                          f"price={ev.get('price', ev.get('bid', ''))}")

asyncio.run(main())
```

---

## 4. Files in This Repo

| File | What it does |
|------|-------------|
| `highlow-tui/providers/tradier_provider.py` | Full provider: REST seed + SSE stream + session high/low tracking |
| `stream_check.py` | Minimal SSE diagnostic — prints every raw event |
| `highlow-tui/core/app_config.py` | Reads config, validates `TRADIER_ACCESS_TOKEN` |
| `highlow-tui/providers/_volume.py` | Volume spike detection fed by trade events |

---

## 5. Rate Limits & Gotchas

- **SSE URL length**: ~1330 chars max → keep to ~300 symbols per connection
- **REST batch**: 200 symbols per `/markets/quotes` call
- **Session expiry**: Stream sessions are ephemeral — create a new one on reconnect
- **Market hours**: SSE only sends events during market hours (pre-market through after-hours)
- **Option volume**: Many OTM options have zero volume — you'll mostly get quote (bid/ask) events, not trades
- **Greeks**: Set `greeks=true` on REST calls to get delta/gamma/theta/vega/IV; not available on SSE events
