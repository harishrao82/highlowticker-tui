# HighLow TUI

Real-time session highs and lows across 571 S&P 500 components, in your terminal.

Tracks every stock making a new intraday high or low — color-coded by frequency, volume spikes, and % change. Built for 0DTE traders watching the equal-weight tape before it shows up on the index.

![screenshot placeholder]

Live demo: **[highlowtick.com](https://highlowtick.com)**

---

## Quickstart

```bash
git clone https://github.com/jach8/highlowticker-tui
cd highlowticker-tui
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

That's it. No config required — runs on Yahoo Finance (90s polling) out of the box.

---

## Coinbase (real-time crypto)

To stream live crypto data from Coinbase instead:

**1. Create `~/.highlowticker/config.toml`:**

```toml
[crypto]
broker = "coinbase"
```

**2. Add credentials to `.env` in the project root:**

```bash
cp .env.example .env
# fill in your Coinbase API key and private key
```

Get your API key from [Coinbase Developer Platform](https://www.coinbase.com/settings/api) — create a key with `read` scope on `advanced_trade`.

**3. Run:**

```bash
python app.py
```

---

## Controls

| Key | Action |
|-----|--------|
| `s` | Settings — view and edit highlight thresholds |
| `Tab` | Switch between equity / crypto (if both configured) |
| `q` | Quit |

---

## Highlight colors

| Color | Meaning |
|-------|---------|
| Flash | Most recent new high/low |
| Yellow | First occurrence this session |
| Orange | Same symbol hitting new extremes repeatedly |
| Purple | Significant % move vs prior entry |
| Pink | Volume spike |
| Green bg | 52-week high |
| Red bg | 52-week low |

Thresholds are in `config/highlight.json` and hot-reloadable via `s`.

---

## Want real-time equity data?

The free tier polls Yahoo Finance every 90 seconds. For live equity streaming across all 571 symbols (Schwab, Alpaca, IBKR, and more), see **[HighlowTicker Pro](https://highlowtick.com)** — $149 one-time.
