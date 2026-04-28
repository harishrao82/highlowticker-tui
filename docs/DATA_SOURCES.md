# Market Data Sources & How We Fetch Them

## 1. Real-Time Equity Streaming

### Tradier (Primary — Pro tier)
- **API**: REST + Server-Sent Events (SSE)
- **Endpoint**: `https://stream.tradier.com/v1/markets/events` (stream), `https://api.tradier.com/v1/markets/quotes` (REST seed)
- **Auth**: `TRADIER_ACCESS_TOKEN` (Bearer token)
- **Latency**: Sub-second (event-driven, no polling)
- **How it works**: Creates an SSE session, then streams trade/quote events. REST seeds session baselines at connect and daily at 9:30 AM ET. Batches 300 symbols per SSE connection (URL length limit), 200 per REST request.
- **Data fields**: high, low, open, close, bid, ask, volume
- **File**: `providers/tradier_provider.py`

### Yahoo Finance (Free tier fallback)
- **Library**: `yfinance`
- **Auth**: None
- **Polling interval**: ~90 seconds
- **Timeframe**: 1-minute bars (daily snapshot)
- **File**: `providers/yahoo_provider.py`

---

## 2. Real-Time Crypto Streaming

### Coinbase Advanced Trade WebSocket
- **Endpoint**: `wss://advanced-trade-ws.coinbase.com`
- **Auth**: `COINBASE_API_KEY_USERNAME` + `COINBASE_API_PRIVATE_KEY` (JWT signed by SDK)
- **Channel**: `ticker_batch`
- **Assets**: BTC, ETH, SOL, XRP, etc.
- **Latency**: Sub-second
- **Session boundary**: Midnight UTC
- **File**: `providers/coinbase_provider.py`

### Multi-Exchange BTC Composite (BRTI approximation)
Equal-weight average of 4 exchange feeds for a robust BTC price:

| Exchange | Endpoint | Auth |
|----------|----------|------|
| Coinbase | `wss://advanced-trade-ws.coinbase.com` | JWT |
| Bitstamp | `wss://ws.bitstamp.net` | None |
| Kraken | `wss://ws.kraken.com/v2` | None |
| Gemini | `wss://api.gemini.com/v1/marketdata/BTCUSD` | None |

- **Staleness threshold**: 10 seconds per exchange
- **File**: `btc_price_sources.py`

---

## 3. Prediction Markets (15-min Binary Options)

### Kalshi
- **REST**: `https://api.elections.kalshi.com/trade-api/v2`
- **WebSocket**: `wss://api.elections.kalshi.com/trade-api/ws/v2`
- **Auth**: `KALSHI_API_KEY` + `KALSHI_API_SECRET` (RSA-PSS signed headers)
- **Markets**: KXBTC15M, KXETH15M, KXSOL15M, KXXRP15M (15-minute windows)
- **Data**: floor_strike, yes_ask, no_ask, winner at settlement (895s into window)
- **Files**: `btc_recorder.py`, `btc_kalshi_trader.py`, `kalshi_auth.py`

### Polymarket
- **Gamma API**: `https://gamma-api.polymarket.com/markets` (market discovery)
- **CLOB API**: `https://clob.polymarket.com` (token IDs, orderbooks)
- **CLOB WebSocket**: `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- **Data API**: `https://data-api.polymarket.com/` (trades, positions, activity)
- **Auth**: None (all public)
- **Markets**: BTC/ETH/SOL/XRP 15-min Up/Down
- **Data**: ask_up, ask_dn, real-time orderbook updates
- **Files**: `btc_recorder.py`, `polymarket_scan.py`, various `poly_*.py`

---

## 4. Historical Data

### Polygon.io (SPY 1-min bars)
- **Endpoint**: `https://api.polygon.io/v2/aggs/ticker/SPY/range/1/minute`
- **Auth**: API key via CLI `--apikey`
- **Rate limit**: 5 req/min (free plan) — 13s between requests, 60s retry on 429
- **Pagination**: Cursor-based (`next_url`), 50k bars per page
- **Use case**: Build 0DTE option probability surface
- **File**: `spy_0dte_builder.py`

### CCXT (Multi-exchange crypto OHLCV)
- **Exchanges**: Coinbase, Kraken, Bitstamp, Binance.US, OKX, Huobi
- **Timeframe**: 1-minute candles
- **Auth**: None (public OHLCV endpoints)
- **Assets**: BTC, ETH, SOL, XRP
- **Use case**: Measure which exchange best predicts Kalshi floor_strike
- **File**: `crypto_floor_predict.py`

### Yahoo Finance (yfinance)
- **Timeframes**: Daily, weekly, minute bars
- **Auth**: None
- **Use case**: General historical lookups, backtesting
- **File**: `fetch.py`

---

## 5. Environment Variables

```
TRADIER_ACCESS_TOKEN          # Tradier broker (equity streaming)
COINBASE_API_KEY_USERNAME     # Coinbase (crypto streaming)
COINBASE_API_PRIVATE_KEY      # Coinbase private key (PEM)
KALSHI_API_KEY                # Kalshi (prediction markets)
KALSHI_API_SECRET             # Kalshi private key (PEM, \n-escaped)
```

---

## 6. Quick Reference Table

| Source | Transport | Timeframe | Auth | Assets |
|--------|-----------|-----------|------|--------|
| Tradier | SSE + REST | Real-time | Token | US equities |
| Yahoo Finance | REST (yfinance) | 90s poll | None | Equities |
| Coinbase WS | WebSocket | Real-time | JWT | Crypto |
| Bitstamp WS | WebSocket | Real-time | None | BTC |
| Kraken WS | WebSocket | Real-time | None | Crypto |
| Gemini WS | WebSocket | Real-time | None | BTC |
| Kalshi | REST + WS | Real-time + on-demand | RSA-PSS | 15-min binaries |
| Polymarket | REST + WS | Real-time + on-demand | None | 15-min binaries |
| Polygon.io | REST | Historical 1-min bars | API key | SPY |
| CCXT | REST | Historical 1-min candles | None | Crypto |
