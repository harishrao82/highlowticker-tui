# HighLow Ticker TUI

A terminal UI for active equity traders. Streams real-time session highs and lows across 600+ symbols via Tradier SSE — built on the thesis that breadth tells you where the market is going before price confirms it.

![Python](https://img.shields.io/badge/python-3.11%2B-blue) ![License](https://img.shields.io/badge/license-MIT-green)

---

![HighLow TUI screenshot](docs/screenshot.png)

---

## What it does

Most trading dashboards show you price. This one shows you **who is breaking out and at what rate** — across every symbol in your watchlist simultaneously, in real time.

The core signal is simple: when more stocks make new session highs than session lows, the market is healthy. When lows dominate — even if SPY looks flat — something is breaking underneath. You see it here before it shows in the index.

---

## Layout

```
┌─ Live feed ──────────────────────────────────┬─ Index prices ──────────────┐
│  stream heartbeat + active symbols           │  SPY  QQQ  IWM  VXX        │
│                                              │  TLT  GLD  (% from close)  │
├─ Rate bars ───────────────────────────────────────────────────────────────┤
│  Lows  ████████████████████████  20m  ████  Highs                         │
│        ████████████████          5m   ███                                  │
│        ████████                  1m   █                                    │
│        ████                      30s  █                                    │
├─ Session new lows ───────┬─ Breadth + Sectors ──────┬─ Session new highs ─┤
│  Symbol  Count  Price    │  BREADTH  NEUTRAL  556   │  Symbol  Count  ...  │
│  ON      70    58.29     │  HIGH ████████     21    │  CME     8    298    │
│  TXN     7     190.65    │       ██           23    │  GOOGL   17   279    │
│  AAPL    37    252.26    │  MID  ██           31    │  AMT     7    170    │
│  ...                     │       ████         36    │  ...                  │
│                          │  LOW  ████████     41    │                       │
│                          ├──────────────────────────┤                       │
│                          │  SECTORS  intraday       │                       │
│                          │  Technology  ████ -1.45% │                       │
│                          │    BEARISH  ▲2  ▼27      │                       │
│                          │  Financials  ███ -0.48%  │                       │
│                          │    FADING   ▲5  ▼2       │                       │
│                          │  ...                     │                       │
│                          ├──────────────────────────┤                       │
│                          │  BREADTH  NEUTRAL        │                       │
│                          │  1m  ████  0.36  H:33 L:59                      │
│                          │  5m  ███   0.18  H:82 L:363                     │
│                          │  20m ████  0.21  H:398 L:1513                   │
└──────────────────────────┴──────────────────────────┴───────────────────────┘
```

---

## Panels

### Rate bars
Rolling breakout velocity across four windows — 30s, 1m, 5m, 20m. The left side is lows, right side is highs. When lows start building on the right side of the screen faster than they decay on the left, selling is accelerating. The bar widths are proportional so you can read momentum at a glance without looking at numbers.

### Breadth histogram
Every symbol in your watchlist is placed in a bucket based on where its current price sits between its session high and low:

```
position = (current_price − session_low) / (session_high − session_low)
```

`HIGH` = symbols trading near their session highs. `LOW` = symbols near their lows. The shape of the distribution matters more than the signal label — a bimodal histogram (big bars at both ends, thin middle) means the market is split across sector lines. A histogram piling at `LOW` with SPY still flat is a divergence worth watching.

Only symbols with live SSE activity are included. ETFs are excluded to avoid double-counting (VXX rising and QQQ rising cancel each other out in a breadth count).

### Sector breadth
Twelve sectors, fixed in SPY-weight order (Technology → Utilities → ETF). Each row shows:

- **Bar** — average % change from open across all symbols in the sector
- **▲/▼ counters** — distinct symbols making new session highs vs lows
- **Signal** — derived from the combination of direction and breadth:

| Signal | Meaning |
|--------|---------|
| `BULLISH` | Price above open + more symbols hitting new highs |
| `BEARISH` | Price below open + more symbols hitting new lows |
| `BOUNCING` | Price above open but more symbols hit lows earlier — recovery, watch if it holds |
| `FADING` | Price below open but more symbols hit highs earlier — was strong, now rolling over |
| `MIXED` | Roughly equal highs and lows, no conviction |

`FADING` is often the most actionable — it identifies sectors that led earlier but are now distributing.

### Session tables
New highs and lows, sorted by most recent. Each row shows symbol, cumulative count (how many times it has broken out today), current price, and % change from open. Highlight colours indicate first occurrence (yellow), consecutive runs (orange), significant % moves (purple), and volume spikes (pink).

### Index prices
SPY, QQQ, IWM, VXX, TLT, GLD — % change calculated from **previous day's close**, matching what you see in TradingView and thinkorswim. VXX is colour-inverted: green when it falls (volatility easing), red when it rises.

### Breadth gauge (bottom)
1m, 5m, and 20m rolling H/L ratios using raw SSE event timestamps. The 1m ratio drives the BULLISH/BEARISH/NEUTRAL signal — the fastest read on whether buying or selling is accelerating right now.

---

## Reading the market

**The simplest rule:** when Technology and Financials are both `BULLISH` and the breadth histogram is piled at `HIGH`, buy the dip in SPY. When both are `BEARISH` and the histogram is piled at `LOW`, fade the bounce.

**The early warning:** watch for `FADING` in Technology while Financials is still `BULLISH`. That divergence often precedes a 30–60 minute SPY pullback before Financials catches down.

**The breadth trap:** if the 1m breadth ratio is 0.65+ (bullish) but only 15 symbols are in the histogram (pre-market or thin tape), the signal is noise. The count shown in the header tells you how many live symbols back the reading.

---

## Setup

```bash
git clone https://github.com/your-handle/highlowticker-tui.git
cd highlowticker-tui/highlow-tui

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Add your Tradier token
echo 'TRADIER_ACCESS_TOKEN=your_token_here' > .env

# Set broker in config
mkdir -p ~/.highlowticker
echo '[equity]\nbroker = "tradier"' > ~/.highlowticker/config.toml

python app.py
```

### Requirements
- Python 3.11+
- Tradier account with streaming access
- Terminal with 256-colour support (iTerm2, Alacritty, Windows Terminal)

### Keyboard shortcuts

| Key | Action |
|-----|--------|
| `s` | Settings — adjust highlight thresholds |
| `q` | Quit |
| `m` | Switch between equity / crypto mode |
| `←` / `→` | Scroll momentum chart back / forward |

---

## Session persistence

State is saved to `~/.highlowticker/session_state.json` on exit and autosaved every 60 seconds. On restart during the same trading day, cumulative high/low counts and table history are restored automatically — you pick up exactly where you left off without losing the session context.

---

## Watchlist

621 symbols by default — S&P 500 constituents plus major ETFs, classified by GICS sector in `tickers/sectors.json`. Edit `tickers/tickers.json` to add or remove symbols. ETFs are tracked for price but excluded from breadth counts.

---

## License

MIT
