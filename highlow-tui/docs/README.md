# HighLow TUI

Terminal UI for the HighLow Ticker: session new highs/lows, indices (SPY, DIA, QQQ), and MAG8. Uses the same core logic as the React app in `../highlow` but runs as a single Python process with Textual for low RAM use.

## What it shows

- **Indices**: SPY, DIA, QQQ — last price and advancers/decliners (from constituent lists).
- **MAG8**: Magnificent 8 aggregate and per-symbol % change.
- **Session tables**: New highs and new lows with symbol, count, price, % change, 52w flag.
- **Highlighting**: Same rules as the React app — flash (newest row), 52-week high/low, yellow (count=1), orange (consecutive same symbol), purple (significant % change). Thresholds and colors are configurable.

## Setup

1. Create a virtual environment and install dependencies:

   ```bash
   cd highlow-tui
   python3 -m venv venv
   source venv/bin/activate   # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. Configure environment:

   - Copy `.env.example` to `.env` in the project root.
   - Set `SCH_DIR` to the path of your Schwab API SDK (Python).
   - Set `TOKEN_PATH` to your Schwab token file (e.g. `token.json` in the project root).
   - Set `API_KEY` and `CLIENT_SECRET` in `.env` for live auth.

3. Ensure `tickers/tickers.json` contains the symbols you want to stream (SPY, DIA, QQQ, and MAG8 symbols are required for indices/MAG8). You can copy from `../highlow/tickers.json` or use the included tickers.

## Run

From the `highlow-tui` directory:

```bash
python app.py
```

- **s**: Open Settings (view highlight thresholds; edit `config/highlight.json` for values and colors).
- **q**: Quit.

## Highlight thresholds and color settings

- **Thresholds**: `consecutiveCount` (default 1) and `significantPercentChange` (default 0.5) in `config/highlight.json`. Press **s** to open Settings and reload after editing the file.
- **Colors**: Same structure as the React app; edit `config/highlight.json` and restart. The TUI maps these to row styles (flash, 52w high/low, yellow, orange, purple).

## No simulation mode

This app uses live Schwab data only. For a simulated or full React setup, use the main `../highlow` project.

## Reference

For the full React dashboard (option screener, hot stocks, etc.), see the parent [highlow](../highlow) app.
