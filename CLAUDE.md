# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Setup

```bash
cd highlow-tui
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Coinbase (crypto) credentials go in `highlow-tui/.env` (see `.env.example`). User broker config lives at `~/.highlowticker/config.toml`.

## Running

```bash
cd highlow-tui
python app.py
```

## Tests

```bash
cd highlow-tui
pytest tests/                  # all tests
pytest tests/test_yahoo_provider.py  # single file
```

## Architecture

**Data flow:**

```
Provider.stream()  →  HighLowTUI._data_loop()  →  _apply_highlow_update()  →  _refresh_ui()
```

- `app.py` — Textual `App` subclass; owns the UI, keyboard bindings (`s` settings, `m` mode switch, `q` quit), and the async data loop
- `core/high_low_ticker.py` — session high/low state machine; `process_stock()` detects new extremes and manages counts
- `core/provider_loader.py` — factory that instantiates the correct provider from broker config
- `core/app_config.py` — reads `~/.highlowticker/config.toml`; equity broker raises (Pro tier only), crypto broker validates env vars
- `providers/base.py` — `DataProvider` protocol: `connect()`, `stream()` (async generator), `disconnect()`, `get_metadata()`
- `providers/yahoo_provider.py` — free tier; polls Yahoo Finance ~every 90 s, no API key needed
- `providers/coinbase_provider.py` — real-time crypto via Coinbase Advanced Trade WebSocket SDK; requires `COINBASE_API_KEY_USERNAME` and `COINBASE_API_PRIVATE_KEY`
- `providers/_volume.py` — wall-clock-aligned rolling volume spike detection (`VolumeTracker`)
- `core/license.py` — RSA-SHA256 offline license validation; machine-bound (hostname + MAC); activate with `python app.py --activate <key>`

**Highlight priority** (computed O(n) in `_refresh_ui`):
flash (newest) > 52-week extreme > yellow (first occurrence) > orange (3+ consecutive same symbol) > purple (≥15% move) > volume spike > default

**Configuration:**
- `config/highlight.json` — highlight thresholds and colors; hot-reloadable at runtime via `s` key
- `tickers/tickers.json` — watchlist (571 S&P 500 symbols by default; editable)

## Free vs. Pro Tier

Free tier includes Yahoo Finance polling and Coinbase crypto streaming. Pro tier (equity live data via Schwab/Alpaca/IBKR) is intentionally excluded — those files are listed in `.gitignore` and should not be committed.
