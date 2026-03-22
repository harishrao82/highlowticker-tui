# HighlowTicker TUI

A terminal UI for active equity traders. Monitors real-time session highs and lows across a watchlist of stocks and ETFs — built on the thesis that option prices lag stock prices by 30–120 seconds after rapid intraday moves.

![Python](https://img.shields.io/badge/python-3.9%2B-blue) ![License](https://img.shields.io/badge/license-MIT-green)

---

## What it shows

- **Session tables** — new highs and lows with symbol, count, price, % change, 52-week flag
- **Rate bars** — breakout velocity across 30s / 1m / 5m / 20m windows
- **Index cards** — SPY, DIA, QQQ last price and advancers/decliners from constituent lists
- **MAG8** — Magnificent 8 aggregate and per-symbol % change
- **Highlighting** — flash (newest), 52-week extreme, first occurrence, consecutive runs, significant % move. Thresholds are hot-reloadable via the settings screen (`s`)

## Quick start

```bash
git clone https://github.com/jach8/highlowticker-tui.git
cd highlowticker-tui/highlow-tui

python3 -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt

python app.py
```

Free tier runs on Yahoo Finance with ~90 second polling. No account or API key required.

## Watchlist

Edit `tickers/tickers.json` to customize your symbols. SPY, DIA, and QQQ constituents live in their respective JSON files and drive the advancer/decliner counts.

## Pro tier

Live 1 Hz data and additional features are available with a Pro license.
Visit **[highlowtick.com](https://highlowtick.com)** for pricing and setup.

## Requirements

- Python 3.9+
- Terminal with 256-color support (iTerm2, Windows Terminal, Alacritty, etc.)

## License

MIT
