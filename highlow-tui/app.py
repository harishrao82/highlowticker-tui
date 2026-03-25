#!/usr/bin/env python3
"""
HighLow TUI: terminal UI for session highs/lows.
Run from highlow-tui directory: python app.py
"""
import asyncio
import json
import math
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import os, certifi                             
os.environ.setdefault("SSL_CERT_FILE", certifi.where())                                  
os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where()) 

# Ensure project root and core are on path
_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / 'core'))

from dataclasses import dataclass, field

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Static, Header, Footer, Button
from textual.screen import Screen
from rich.text import Text
from rich.style import Style

_ET = ZoneInfo("America/New_York")

MAX_TABLE_ROWS = 50
RATE_BAR_WIDTH = 18
RATE_TIMEFRAMES = ["20m", "5m", "1m", "30s"]
MOMENTUM_WINDOW = 1200  # seconds of history to keep (20 min)

CHART_Y_W           = 8    # y-axis column width (chars including separator │)
CHART_VIEW_SECS     = 7200 # 2-hour viewport window
CHART_SCROLL_STEP   = 300  # seconds per scroll keypress (5 min)
CHART_RENDER_INTERVAL = 0.5  # max chart render rate (seconds) — prevents lag at high event rates


def make_bar(value: float, max_val: float, width: int = RATE_BAR_WIDTH, reverse: bool = False) -> str:
    filled = min(int(value / max_val * width), width) if max_val > 0 else 0
    bar = "█" * filled + "░" * (width - filled)
    return bar[::-1] if reverse else bar


HIGHLIGHT_STYLES = {
    "flash_high": Style(),
    "flash_low": Style(),
    "week52_high": Style(color="white", bgcolor="rgb(20,83,45)"),
    "week52_low": Style(color="white", bgcolor="rgb(127,29,29)"),
    "yellow": Style(color="black", bgcolor="yellow"),
    "orange": Style(color="black", bgcolor="orange1"),
    "purple":       Style(color="white", bgcolor="purple"),
    "volume_spike": Style(color="black", bgcolor="rgb(244,114,182)"),
    "default":      Style(),
}


def load_highlight_config():
    path = _ROOT / "config" / "highlight.json"
    default = {
        "thresholds": {
            "consecutiveCount": 1,
            "significantPercentChange": 0.5,
            "volumeSpikeRatio": 2.0,
            "volumeSpikeWindow": 60,
        },
        "colors": {},
    }
    if not path.exists():
        return default
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return default


def save_highlight_config(config):
    path = _ROOT / "config" / "highlight.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(config, f, indent=2)


def compute_highlights(data, is_highs, week52_set, thresholds, suppress_yellow=False, volume_spikes=None):
    """Return highlight type for every entry in O(n) — no per-row scanning."""
    if not data:
        return []
    volume_spikes = volume_spikes or set()
    n = len(data)
    consec_threshold = thresholds.get("consecutiveCount", 1) + 1
    sig_pct = thresholds.get("significantPercentChange", 0.5)
    flash_type = "flash_high" if is_highs else "flash_low"
    week52_type = "week52_high" if is_highs else "week52_low"

    # Two-pass O(n) contiguous run-size: run_size[i] = length of the same-symbol
    # run that contains index i (both directions from i).
    run_end = [1] * n
    for i in range(1, n):
        if data[i]["symbol"] == data[i - 1]["symbol"]:
            run_end[i] = run_end[i - 1] + 1
    run_size = list(run_end)
    for i in range(n - 2, -1, -1):
        if data[i]["symbol"] == data[i + 1]["symbol"]:
            run_size[i] = run_size[i + 1]

    highlights = []
    last_pct: dict = {}  # nearest lower-index pct per symbol (for purple)
    for i, e in enumerate(data):
        sym = e["symbol"]
        pct = e.get("percentChange") or 0
        if i == 0:
            h = flash_type
        elif sym in week52_set:
            h = week52_type
        elif e.get("count") == 1 and not suppress_yellow:
            h = "yellow"
        elif run_size[i] >= consec_threshold:
            h = "orange"
        elif sym in last_pct and abs(pct - last_pct[sym]) > sig_pct:
            h = "purple"
        elif sym in volume_spikes:
            h = "volume_spike"
        else:
            h = "default"
        highlights.append(h)
        last_pct[sym] = pct
    return highlights


@dataclass
class SessionState:
    session_highs: list = field(default_factory=list)
    session_lows:  list = field(default_factory=list)
    prev_highs:    dict = field(default_factory=dict)
    prev_lows:     dict = field(default_factory=dict)
    prev_entries_highs: dict = field(default_factory=dict)
    prev_entries_lows:  dict = field(default_factory=dict)
    week52_highs: set = field(default_factory=set)
    week52_lows:  set = field(default_factory=set)


class HighLowTUI(App):
    CSS = """
    Screen {
        layout: vertical;
    }
    #header-row {
        height: 1;
        padding: 0 1;
        layout: horizontal;
    }
    #app-title {
        width: 1fr;
    }
    #ticker {
        height: 1;
        padding: 0 0;
    }
    #rate-bars {
        height: auto;
        padding: 0 0;
        margin: 0 0;
    }
    #tables-container {
        height: 1fr;
        layout: horizontal;
    }
    .table-box {
        width: 1fr;
        height: 1fr;
        border: solid cyan;
        margin: 0 0;
    }
    #momentum-box {
        width: 1fr;
        height: 1fr;
        border: solid cyan;
        margin: 0 0;
        overflow: hidden hidden;
    }
    #momentum-chart {
        height: 1fr;
        overflow: hidden hidden;
    }
    #spy-chart {
        height: 1fr;
        border-top: dashed $primary-darken-3;
        overflow: hidden hidden;
    }
    #system-health {
        height: 4;
        border-top: dashed $primary-darken-3;
        padding: 0 1;
    }
    DataTable {
        height: 1fr;
    }
    #connection-status {
        width: auto;
        text-align: right;
    }
    #mode-toggle {
        width: auto;
        padding: 0 2;
    }
    """

    BINDINGS = [
        ("s", "settings", "Settings"),
        ("q", "quit", "Quit"),
        ("m", "switch_mode", "Mode"),
        ("left",  "chart_scroll_back", "◀ chart"),
        ("right", "chart_scroll_fwd",  "▶ live"),
    ]

    def __init__(
        self,
        equity_provider=None,
        crypto_provider=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._equity_provider = equity_provider
        self._crypto_provider = crypto_provider
        self._active_mode = "crypto" if crypto_provider and not equity_provider else "equity"
        self._provider = equity_provider or crypto_provider  # active provider
        self.last_state = {}
        # Per-mode session state
        self._states = {
            "equity": SessionState(),
            "crypto": SessionState(),
        }
        # Flat attributes mirror the active state (unchanged hot path)
        self.session_highs = self._states[self._active_mode].session_highs
        self.session_lows  = self._states[self._active_mode].session_lows
        self.prev_highs    = self._states[self._active_mode].prev_highs
        self.prev_lows     = self._states[self._active_mode].prev_lows
        self.prev_entries_highs = self._states[self._active_mode].prev_entries_highs
        self.prev_entries_lows  = self._states[self._active_mode].prev_entries_lows
        self.week52_highs  = self._states[self._active_mode].week52_highs
        self.week52_lows   = self._states[self._active_mode].week52_lows
        self.volume_spikes: set = set()
        self.connection_status = "connecting"
        self.last_update_time = None
        self._highs_dirty = False
        self._lows_dirty  = False
        self.highlight_config = load_highlight_config()
        self._w_status = None
        self._w_rate_bars = None
        self._w_highs = None
        self._w_lows  = None
        self._w_momentum = None
        self._w_spy      = None
        self._w_ticker = None
        self._w_mode_toggle = None
        self._momentum_history: deque = deque(maxlen=30000)  # full session, ~30k max
        self._spy_history:      deque = deque(maxlen=5000)   # full session; 5s interval × 5000 = ~7h
        self._last_valid_spy:   float = 0.0      # last non-zero SPY price seen
        self._chart_offset_secs: float = 0.0    # 0 = live, positive = scrolled back
        self._last_chart_render: float = 0.0    # timestamp of last chart render
        self._event_count: int = 0
        self._event_timestamps: deque = deque(maxlen=600)  # last 10 min of event times
        self._w_health = None
        self._ticker_text    = Text("")
        self._ticker_doubled = Text("")
        self._ticker_offset  = 0
        self._stream_task = None
        self._start_time = time.time()

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="header-row"):
            yield Static("[bold]HighLow TUI[/]  [dim]s: Settings  q: Quit[/]", id="app-title")
            if self._equity_provider and self._crypto_provider:
                mode_label = "[bold cyan][Equity][/]  Crypto" if self._active_mode == "equity" else "Equity  [bold cyan][Crypto][/]"
                yield Static(mode_label, id="mode-toggle")
            yield Static("● connecting", id="connection-status")
        yield Static("", id="ticker")
        yield Static(id="rate-bars")
        with Horizontal(id="tables-container"):
            with Vertical(classes="table-box"):
                yield Static("Session new lows", id="lows-label")
                yield DataTable(id="lows-table", cursor_type="row", zebra_stripes=True)
            with Vertical(id="momentum-box"):
                yield Static("", id="momentum-chart")
                yield Static("", id="spy-chart")
                yield Static("", id="system-health")
            with Vertical(classes="table-box"):
                yield Static("Session new highs", id="highs-label")
                yield DataTable(id="highs-table", cursor_type="row", zebra_stripes=True)
        yield Footer()

    def on_mount(self) -> None:
        self._w_status    = self.query_one("#connection-status", Static)
        self._w_ticker    = self.query_one("#ticker", Static)
        self._w_rate_bars = self.query_one("#rate-bars", Static)
        self._w_highs     = self.query_one("#highs-table", DataTable)
        self._w_lows      = self.query_one("#lows-table", DataTable)
        self._w_momentum  = self.query_one("#momentum-chart", Static)
        self._w_spy       = self.query_one("#spy-chart", Static)
        self._w_health    = self.query_one("#system-health", Static)
        if self._equity_provider and self._crypto_provider:
            self._w_mode_toggle = self.query_one("#mode-toggle", Static)
        for table in (self._w_highs, self._w_lows):
            table.add_column("Symbol", width=6)
            table.add_column("Count",  width=5)
            table.add_column("Price",  width=9)
            table.add_column("% Chg", width=8)
        self.set_interval(1 / 12, self._scroll_ticker)
        self._stream_task = asyncio.create_task(self._data_loop())

    def action_chart_scroll_back(self) -> None:
        oldest = self._momentum_history[0][0] if self._momentum_history else time.time()
        max_offset = time.time() - oldest - CHART_VIEW_SECS
        self._chart_offset_secs = min(self._chart_offset_secs + CHART_SCROLL_STEP, max(max_offset, 0))
        self._render_momentum_chart()
        self._render_spy_chart()

    def action_chart_scroll_fwd(self) -> None:
        self._chart_offset_secs = max(0.0, self._chart_offset_secs - CHART_SCROLL_STEP)
        self._render_momentum_chart()
        self._render_spy_chart()

    async def _data_loop(self) -> None:
        try:
            await self._provider.connect()
            self.connection_status = "connected"
            self._refresh_status()
            async for data in self._provider.stream():
                if data.get("type") == "HIGHLOW_UPDATE":
                    self._apply_highlow_update(data.get("data", {}))
                    self.last_update_time = time.time()
                    self._refresh_ui()
                    await asyncio.sleep(0)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.connection_status = f"error: {e}"
            self._refresh_status()

    def _apply_highlow_update(self, data):
        if not data:
            return
        self.last_state = data
        new_highs = data.get("newHighs") or {}
        new_lows = data.get("newLows") or {}
        last_high = data.get("lastHigh") or {}
        last_low = data.get("lastLow") or {}
        percent_change = data.get("percentChange") or {}
        ts = time.time()

        # New high entries
        new_high_entries = []
        for symbol, count in new_highs.items():
            if count <= 0 or symbol not in last_high:
                continue
            if count <= self.prev_highs.get(symbol, 0):
                continue
            prev_entry = self.prev_entries_highs.get(symbol, {})
            new_high_entries.append({
                "symbol": symbol,
                "count": count,
                "timestamp": ts,
                "price": last_high[symbol],
                "percentChange": percent_change.get(symbol, 0.0),
                "prevCount": prev_entry.get("count", 0),
                "prevPercentChange": prev_entry.get("percentChange", 0),
            })
        for e in new_high_entries:
            self.session_highs.insert(0, e)
            self.prev_entries_highs[e["symbol"]] = {
                "count": e["count"],
                "percentChange": e["percentChange"],
                "timestamp": e["timestamp"],
            }
        self.prev_highs = dict(new_highs)
        self.session_highs = self.session_highs[:MAX_TABLE_ROWS]

        # New low entries
        new_low_entries = []
        for symbol, count in new_lows.items():
            if count <= 0 or symbol not in last_low:
                continue
            if count <= self.prev_lows.get(symbol, 0):
                continue
            prev_entry = self.prev_entries_lows.get(symbol, {})
            new_low_entries.append({
                "symbol": symbol,
                "count": count,
                "timestamp": ts,
                "price": last_low[symbol],
                "percentChange": percent_change.get(symbol, 0.0),
                "prevCount": prev_entry.get("count", 0),
                "prevPercentChange": prev_entry.get("percentChange", 0),
            })
        for e in new_low_entries:
            self.session_lows.insert(0, e)
            self.prev_entries_lows[e["symbol"]] = {
                "count": e["count"],
                "percentChange": e["percentChange"],
                "timestamp": e["timestamp"],
            }
        self.prev_lows = dict(new_lows)
        self.session_lows = self.session_lows[:MAX_TABLE_ROWS]

        self.week52_highs = set(data.get("week52Highs") or [])
        self.week52_lows = set(data.get("week52Lows") or [])
        spike_ratio = self.highlight_config.get("thresholds", {}).get("volumeSpikeRatio", 2.0)
        self.volume_spikes = {
            sym for sym, ratio in (data.get("volumeSpikes") or {}).items()
            if ratio >= spike_ratio
        }
        self._highs_dirty = len(new_high_entries) > 0
        self._lows_dirty = len(new_low_entries) > 0
        self._event_count += 1
        self._event_timestamps.append(ts)

    @staticmethod
    def _compute_momentum_score(high_counts: dict, low_counts: dict) -> float:
        """Weighted momentum: recent timeframes count more.
        Score > 0 = more new highs, Score < 0 = more new lows.
        """
        h1  = high_counts.get("1m",  0);  l1  = low_counts.get("1m",  0)
        h5  = high_counts.get("5m",  0);  l5  = low_counts.get("5m",  0)
        h20 = high_counts.get("20m", 0);  l20 = low_counts.get("20m", 0)
        return 4 * (h1 - l1) + 2 * (h5 - l5) + (h20 - l20)

    SPY_SAMPLE_INTERVAL = 5  # seconds between SPY price samples (5s → ~12 points/min for accurate OHLC)

    def _update_momentum(self, high_counts: dict, low_counts: dict) -> None:
        score = self._compute_momentum_score(high_counts, low_counts)
        raw_spy = (self.last_state.get("indexPrices") or {}).get("SPY", 0.0)
        if raw_spy and raw_spy > 1.0:
            self._last_valid_spy = raw_spy
        spy = self._last_valid_spy
        now   = time.time()
        self._momentum_history.append((now, score))
        if spy and (not self._spy_history or now - self._spy_history[-1][0] >= self.SPY_SAMPLE_INTERVAL):
            self._spy_history.append((now, spy))
        # Throttle chart renders: at high event rates (20-50/sec) rendering every event
        # would copy/iterate large deques continuously and lag the asyncio loop.
        if now - self._last_chart_render >= CHART_RENDER_INTERVAL:
            self._last_chart_render = now
            self._render_momentum_chart()
            self._render_spy_chart()

    @staticmethod
    def _x_axis_marks(view_start: float, view_end: float, chart_w: int):
        """Return list of (col, label, is_major) for time marks in the viewport.
        Tick spacing adapts to viewport duration so labels never crowd together."""
        duration = view_end - view_start
        if duration <= 1800:    # ≤ 30 min → every 5 min
            step = 300
        elif duration <= 7200:  # ≤ 2 h   → every 15 min
            step = 900
        else:                   # > 2 h   → every 30 min
            step = 1800
        t = math.ceil(view_start / step) * step
        marks = []
        while t <= view_end:
            col = round((t - view_start) / (view_end - view_start) * (chart_w - 1))
            dt = datetime.fromtimestamp(t, tz=_ET)
            label = dt.strftime("%H:%M")
            marks.append((col, label, dt.minute % 30 == 0))
            t += step
        return marks

    @staticmethod
    def _ohlc_1min(history, view_start: float, view_end: float):
        """Bucket [(ts, value)] into 1-minute OHLC candles within the viewport."""
        from collections import defaultdict
        buckets: dict = defaultdict(list)
        for ts, val in history:
            if view_start <= ts <= view_end:
                bucket = int(ts // 60) * 60
                buckets[bucket].append((ts, val))
        candles = []
        for bucket_ts in sorted(buckets):
            items = sorted(buckets[bucket_ts])
            vals = [v for _, v in items]
            candles.append({
                "bucket_ts": bucket_ts,
                "open":  vals[0],
                "high":  max(vals),
                "low":   min(vals),
                "close": vals[-1],
            })
        return candles

    @staticmethod
    def _build_candle_grid(candles, view_start: float, view_end: float,
                           chart_w: int, chart_h: int,
                           y_min: float, y_max: float,
                           zero_row: int = -1,
                           current_row: int = -1):
        """
        Render OHLC candles into a grid[row][col] = (char, style_str).
        zero_row >= 0 draws a dashed zero line behind candles.
        current_row >= 0 draws a subtle horizontal current-value line.
        """
        DIM_GRID = "#1e3a1e"
        grid = [[("·", DIM_GRID)] * chart_w for _ in range(chart_h)]

        def to_row(val: float) -> int:
            frac = 1.0 - (val - y_min) / (y_max - y_min)
            return max(0, min(chart_h - 1, int(frac * (chart_h - 1))))

        # Zero line (drawn first, candles paint over it)
        if 0 <= zero_row < chart_h:
            for c in range(chart_w):
                grid[zero_row][c] = ("─", "dim white")

        span_secs = view_end - view_start

        # Pre-compute column ranges so we can draw close→open connectors afterward
        col_ranges = []
        for candle in candles:
            bt = candle["bucket_ts"]
            c_left  = max(0, round((bt      - view_start) / span_secs * (chart_w - 1)))
            c_right = min(chart_w - 1, round((bt + 60 - view_start) / span_secs * (chart_w - 1)))
            col_ranges.append((c_left, c_right))

        for idx, candle in enumerate(candles):
            c_left, c_right = col_ranges[idx]
            candle_width = c_right - c_left
            # Leave a 1-char gap on the right so adjacent bars are visually separated;
            # skip the gap when there is only 1 column available.
            body_right = c_right - 1 if candle_width > 1 else c_right

            bullish    = candle["close"] >= candle["open"]
            bar_color  = "bright_green" if bullish else "bright_red"

            high_row = to_row(candle["high"])
            low_row  = to_row(candle["low"])

            # Fill the entire high-to-low range solid — no separate wick/body distinction.
            # Color alone (green/red) conveys direction; solid fill reads clearly at any width.
            for r in range(high_row, low_row + 1):
                for c in range(c_left, body_right + 1):
                    if 0 <= c < chart_w:
                        grid[r][c] = ("█", bar_color)

        # Current-value line — drawn after candles, only overwrites empty cells
        # so candle bodies/wicks always take visual priority
        if 0 <= current_row < chart_h:
            for c in range(chart_w):
                if grid[current_row][c][0] == "·":
                    grid[current_row][c] = ("─", "#3a6060")

        return grid

    def _render_momentum_chart(self) -> None:
        if not self._w_momentum:
            return

        now        = time.time()
        view_end   = now - self._chart_offset_secs
        view_start = view_end - CHART_VIEW_SECS
        width      = max(self._w_momentum.size.width  or 60, CHART_Y_W + 4)
        height     = max(self._w_momentum.size.height or 20, 5)
        chart_w    = width - CHART_Y_W
        chart_h    = max(height - 2, 3)

        current_score = self._momentum_history[-1][1] if self._momentum_history else 0.0
        sign        = "+" if current_score > 0 else ""
        score_color = ("bright_green" if current_score > 0
                       else ("bright_red" if current_score < 0 else "white"))
        scroll_tag  = (f"  ◀ -{int(self._chart_offset_secs / 60)}m"
                       if self._chart_offset_secs else "  LIVE")

        out = Text()
        out.append("MOMENTUM  ", style="bold dim white")
        out.append(f"Score: {sign}{current_score:.0f}", style=f"bold {score_color}")
        out.append(scroll_tag, style="dim yellow")
        out.append("\n")

        # Pre-filter to viewport so _ohlc_1min doesn't iterate the full 30k deque
        history_view = [(t, s) for t, s in self._momentum_history if t >= view_start]
        candles = self._ohlc_1min(history_view, view_start, view_end)

        if not candles:
            out.append("  No data in this window\n", style="dim")
            self._w_momentum.update(out)
            return

        all_vals = ([c["high"] for c in candles] + [c["low"] for c in candles])
        span     = max(abs(v) for v in all_vals) or 1.0
        y_max    = span * 1.1
        y_min    = -y_max

        def to_row_m(v: float) -> int:
            frac = 1.0 - (v - y_min) / (y_max - y_min)
            return max(0, min(chart_h - 1, int(frac * (chart_h - 1))))

        zero_row    = to_row_m(0.0)
        current_row = to_row_m(current_score)

        grid = self._build_candle_grid(
            candles, view_start, view_end, chart_w, chart_h, y_min, y_max,
            zero_row=zero_row, current_row=current_row,
        )

        y_lbl = {
            0:            f"{y_max:+.0f}",
            zero_row:     "   0",
            chart_h - 1:  f"{y_min:+.0f}",
        }

        for r_idx, row in enumerate(grid):
            line = Text(overflow="fold")
            for ch, st in row:
                line.append(ch, style=st)
            if r_idx == current_row:
                # Floating current-score label; overrides any static label at this row
                lbl = f"{sign}{current_score:.0f}"
                line.append(f"{lbl:>{CHART_Y_W - 1}}", style=f"bold {score_color}")
                line.append("◄", style=f"bold {score_color}")
            else:
                lbl = y_lbl.get(r_idx, "")
                line.append(f"{lbl:>{CHART_Y_W - 1}}", style="dim white")
                line.append("│", style="dim")
            out.append_text(line)
            out.append("\n")

        DIM_GRID = "#1e3a1e"
        marks   = self._x_axis_marks(view_start, view_end, chart_w)
        x_chars: dict = {}
        for col, label, is_30min in marks:
            for i, ch in enumerate(label):
                c = col - len(label) // 2 + i
                if 0 <= c < chart_w:
                    x_chars[c] = (ch, "bright_white" if is_30min else "dim")

        x_line = Text(overflow="fold")
        for c in range(chart_w):
            if c in x_chars:
                ch, st = x_chars[c]
                x_line.append(ch, style=st)
            else:
                x_line.append("─", style=DIM_GRID)
        x_line.append(" " * (CHART_Y_W - 1), style="")
        x_line.append("┘", style="dim")
        out.append_text(x_line)

        self._w_momentum.update(out)

    def _render_spy_chart(self) -> None:
        if not self._w_spy:
            return

        now        = time.time()
        view_end   = now - self._chart_offset_secs
        view_start = view_end - CHART_VIEW_SECS
        width      = max(self._w_spy.size.width  or 60, CHART_Y_W + 4)
        height     = max(self._w_spy.size.height or 10, 5)
        chart_w    = width - CHART_Y_W
        chart_h    = max(height - 2, 3)

        current_spy = self._last_valid_spy
        scroll_tag  = (f"  ◀ -{int(self._chart_offset_secs / 60)}m"
                       if self._chart_offset_secs else "  LIVE")

        spy_history = [(t, p) for t, p in self._spy_history if p > 1.0 and t >= view_start]
        candles = self._ohlc_1min(spy_history, view_start, view_end)

        if not candles:
            out = Text()
            out.append("SPY  ", style="bold dim white")
            out.append(f"{current_spy:.2f}", style="bold bright_cyan")
            out.append(scroll_tag, style="dim yellow")
            out.append("\n")
            out.append("  No data in this window\n", style="dim")
            self._w_spy.update(out)
            return

        all_highs = [c["high"] for c in candles]
        all_lows  = [c["low"]  for c in candles]
        p_min = min(all_lows)
        p_max = max(all_highs)

        min_span = max(current_spy, p_max) * 0.003
        span     = max(p_max - p_min, min_span)
        center   = (p_max + p_min) / 2
        y_min    = center - span * 0.6
        y_max    = center + span * 0.6

        baseline    = candles[0]["open"]
        last_close  = candles[-1]["close"]
        delta       = last_close - baseline
        sign        = "+" if delta >= 0 else ""
        delta_color = "bright_green" if delta >= 0 else "bright_red"

        out = Text()
        out.append("SPY  ", style="bold dim white")
        out.append(f"{current_spy:.2f}  ", style="bold bright_cyan")
        out.append(f"{sign}{delta:.2f}", style=f"bold {delta_color}")
        out.append(scroll_tag, style="dim yellow")
        out.append("\n")

        mid_row = chart_h // 2

        def to_row_spy(p: float) -> int:
            frac = 1.0 - (p - y_min) / (y_max - y_min)
            return max(0, min(chart_h - 1, int(frac * (chart_h - 1))))

        current_row = to_row_spy(current_spy) if current_spy > 1.0 else -1

        grid = self._build_candle_grid(
            candles, view_start, view_end, chart_w, chart_h, y_min, y_max,
            current_row=current_row,
        )

        y_lbl = {
            0:            f"{y_max:.2f}",
            mid_row:      f"{(y_max + y_min) / 2:.2f}",
            chart_h - 1:  f"{y_min:.2f}",
        }

        for r_idx, row in enumerate(grid):
            line = Text(overflow="fold")
            for ch, st in row:
                line.append(ch, style=st)
            if r_idx == current_row:
                lbl = f"{current_spy:.2f}"
                line.append(f"{lbl:>{CHART_Y_W - 1}}", style="bold #4a9a9a")
                line.append("◄", style="bold #4a9a9a")
            else:
                lbl = y_lbl.get(r_idx, "")
                line.append(f"{lbl:>{CHART_Y_W - 1}}", style="dim white")
                line.append("│", style="dim")
            out.append_text(line)
            out.append("\n")

        DIM_GRID = "#1e3a1e"
        marks   = self._x_axis_marks(view_start, view_end, chart_w)
        x_chars: dict = {}
        for col, label, is_30min in marks:
            for i, ch in enumerate(label):
                c = col - len(label) // 2 + i
                if 0 <= c < chart_w:
                    x_chars[c] = (ch, "bright_white" if is_30min else "dim")

        x_line = Text(overflow="fold")
        for c in range(chart_w):
            if c in x_chars:
                ch, st = x_chars[c]
                x_line.append(ch, style=st)
            else:
                x_line.append("─", style=DIM_GRID)
        x_line.append(" " * (CHART_Y_W - 1), style="")
        x_line.append("┘", style="dim")
        out.append_text(x_line)

        self._w_spy.update(out)

    def _refresh_status(self):
        dot = "[green]●[/green]" if self.connection_status == "connected" else "[red]●[/red]"
        name = self._provider.get_metadata()["name"]
        self._w_status.update(f"{dot} [dim]{name}[/dim]  {self.connection_status}")

    def _scroll_ticker(self) -> None:
        n = len(self._ticker_text)
        if not n:
            return
        w = self._w_ticker.size.width or 80
        self._w_ticker.update(self._ticker_doubled[self._ticker_offset : self._ticker_offset + w])
        self._ticker_offset = (self._ticker_offset + 1) % n

    def _build_ticker_text(self) -> None:
        # Merge highs and lows, sort by timestamp newest-first (matches tables)
        tagged = (
            [(e, "high") for e in self.session_highs[:25]] +
            [(e, "low")  for e in self.session_lows[:25]]
        )
        tagged.sort(key=lambda x: x[0].get("timestamp", 0), reverse=True)
        t = Text()
        for e, kind in tagged[:50]:
            pct   = e.get("percentChange") or 0
            arrow = "▲" if kind == "high" else "▼"
            style = "green" if kind == "high" else "red"
            t.append(f"  {e['symbol']} {arrow}{e['price']:.2f} ({pct:+.2f}%)  ", style=style)
        if not len(t):
            t.append("  Waiting for data...  ", style="dim")
        self._ticker_text = t
        self._ticker_doubled = t + t
        self._ticker_offset = min(self._ticker_offset, max(1, len(t)) - 1)

    @staticmethod
    def _render_rate_bars(high_counts: dict, low_counts: dict, width: int) -> str:
        max_val = max(
            max((high_counts.get(t, 0) for t in RATE_TIMEFRAMES), default=1),
            max((low_counts.get(t, 0) for t in RATE_TIMEFRAMES), default=1),
            1,
        )
        # Fixed non-bar chars per line: count(3) + sp(1) + bar + sp(1) + label(3) + sp(1) + bar + sp(1) + count(3) = 13
        bar_w = max(4, (width - 13) // 2)
        lines = [f"[dim]{'Lows':>{bar_w + 4}}  Highs[/dim]"]
        for tf in RATE_TIMEFRAMES:
            lc = low_counts.get(tf, 0)
            hc = high_counts.get(tf, 0)
            l_bar = make_bar(lc, max_val, bar_w, reverse=True)
            h_bar = make_bar(hc, max_val, bar_w)
            lines.append(
                f"[dim]{lc:>3d}[/dim] [red]{l_bar}[/red] [dim]{tf:>3s}[/dim] [green]{h_bar}[/green] [dim]{hc:<3d}[/dim]"
            )
        return "\n".join(lines)

    @staticmethod
    def _build_table(table: DataTable, entries, is_highs, week52_set, thresholds, prefix, suppress_yellow=False, volume_spikes=None):
        table.clear()
        highlights = compute_highlights(entries, is_highs, week52_set, thresholds, suppress_yellow=suppress_yellow, volume_spikes=volume_spikes)
        for i, (e, h) in enumerate(zip(entries, highlights)):
            style = HIGHLIGHT_STYLES.get(h, HIGHLIGHT_STYLES["default"])
            pct = e.get("percentChange") or 0
            sign = "+" if pct >= 0 else ""
            pct_style = style + Style(color="green" if pct >= 0 else "red")
            table.add_row(
                Text(f"{e['symbol']:<6}", style=style),
                Text(f"{e['count']:>5}", style=style),
                Text(f"{e.get('price', 0):>9.2f}", style=style),
                Text(f"{sign}{pct:.2f}%".rjust(8), style=pct_style),
                key=f"{prefix}_{i}_{e['symbol']}",
            )

    def _refresh_health(self) -> None:
        if not self._w_health:
            return
        now = time.time()

        # Time since last HIGHLOW_UPDATE — primary choking indicator
        if self.last_update_time:
            lag = now - self.last_update_time
            if lag < 3:
                lag_str, lag_style = f"{lag:.1f}s ago", "bright_green"
            elif lag < 15:
                lag_str, lag_style = f"{lag:.1f}s ago", "yellow"
            else:
                lag_str, lag_style = f"{lag:.0f}s ago ⚠", "bright_red"
        else:
            lag_str, lag_style = "waiting...", "dim"

        # Rolling 60-second event rate
        cutoff60 = now - 60
        rate60 = sum(1 for t in self._event_timestamps if t > cutoff60)

        # Uptime
        uptime_s = int(now - self._start_time)
        h, rem = divmod(uptime_s, 3600)
        m, s   = divmod(rem, 60)
        uptime_str = f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s"

        provider_name = self._provider.get_metadata()["name"] if self._provider else "—"

        out = Text()
        # Row 1: last event + rate
        out.append("LAST EVT ", style="dim")
        out.append(lag_str, style=f"bold {lag_style}")
        out.append("   RATE ", style="dim")
        out.append(f"{rate60}/min", style="bold")
        out.append("   TOTAL ", style="dim")
        out.append(f"{self._event_count:,}", style="bold")
        out.append("\n")
        # Row 2: data sizes + uptime + provider
        out.append("HIGHS ", style="dim")
        out.append(f"{len(self.session_highs)}", style="bold bright_green")
        out.append("  LOWS ", style="dim")
        out.append(f"{len(self.session_lows)}", style="bold bright_red")
        out.append("  MEM ", style="dim")
        out.append(f"{len(self._momentum_history):,}", style="bold")
        out.append("/", style="dim")
        out.append(f"{len(self._spy_history)}", style="bold")
        out.append("  UP ", style="dim")
        out.append(uptime_str, style="bold")
        out.append(f"  {provider_name}", style="dim cyan")

        self._w_health.update(out)

    def _refresh_ui(self):
        self._refresh_status()
        # Rate bars — use live widget width so bars fill the terminal
        high_counts = self.last_state.get("highCounts") or {}
        low_counts = self.last_state.get("lowCounts") or {}
        bar_width = self._w_rate_bars.size.width or 80
        self._w_rate_bars.update(self._render_rate_bars(high_counts, low_counts, bar_width))
        self._update_momentum(high_counts, low_counts)
        self._refresh_health()

        thresholds = self.highlight_config.get("thresholds", {})

        if self._highs_dirty or self._lows_dirty:
            self._build_ticker_text()

        suppress = time.time() - self._start_time < 300
        if self._highs_dirty:
            self._build_table(self._w_highs, self.session_highs, True,  self.week52_highs, thresholds, "h", suppress_yellow=suppress, volume_spikes=self.volume_spikes)
            self._highs_dirty = False

        if self._lows_dirty:
            self._build_table(self._w_lows,  self.session_lows,  False, self.week52_lows,  thresholds, "l", suppress_yellow=suppress, volume_spikes=self.volume_spikes)
            self._lows_dirty = False

    def action_settings(self) -> None:
        self.push_screen(SettingsScreen(self.highlight_config, self._on_settings_save))

    def _on_settings_save(self, config):
        self.highlight_config = config
        save_highlight_config(config)
        self._refresh_ui()

    def check_action(self, action: str, parameters: tuple):
        if action == "switch_mode":
            return bool(self._equity_provider and self._crypto_provider)
        return True

    async def action_switch_mode(self) -> None:
        await self._switch_mode()

    async def _switch_mode(self) -> None:
        """5-step provider switch: cancel stream → disconnect → swap state → reconnect → restart stream."""
        # Step 1: cancel the active stream task
        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass
        self._stream_task = None

        # Step 2+3: disconnect active provider
        await self._provider.disconnect()

        # Step 4: swap mode and session state
        new_mode = "crypto" if self._active_mode == "equity" else "equity"
        # Save current state
        self._states[self._active_mode].session_highs = self.session_highs
        self._states[self._active_mode].session_lows  = self.session_lows
        self._states[self._active_mode].prev_highs    = self.prev_highs
        self._states[self._active_mode].prev_lows     = self.prev_lows
        self._states[self._active_mode].prev_entries_highs = self.prev_entries_highs
        self._states[self._active_mode].prev_entries_lows  = self.prev_entries_lows
        self._states[self._active_mode].week52_highs  = self.week52_highs
        self._states[self._active_mode].week52_lows   = self.week52_lows
        # Restore new mode state
        self._active_mode  = new_mode
        self.session_highs = self._states[new_mode].session_highs
        self.session_lows  = self._states[new_mode].session_lows
        self.prev_highs    = self._states[new_mode].prev_highs
        self.prev_lows     = self._states[new_mode].prev_lows
        self.prev_entries_highs = self._states[new_mode].prev_entries_highs
        self.prev_entries_lows  = self._states[new_mode].prev_entries_lows
        self.week52_highs  = self._states[new_mode].week52_highs
        self.week52_lows   = self._states[new_mode].week52_lows
        # Switch active provider
        self._provider = self._equity_provider if new_mode == "equity" else self._crypto_provider
        # Clear tables
        self._w_highs.clear()
        self._w_lows.clear()
        # Update mode toggle label
        if self._w_mode_toggle:
            label = "[bold cyan][Equity][/]  Crypto" if new_mode == "equity" else "Equity  [bold cyan][Crypto][/]"
            self._w_mode_toggle.update(label)

        # Step 5: reconnect and restart stream
        await self._provider.connect()
        self.connection_status = "connecting"
        self._stream_task = asyncio.create_task(self._data_loop())


class SettingsScreen(Screen):
    def __init__(self, initial_config, on_save, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.initial_config = initial_config
        self.on_save_cb = on_save

    def compose(self) -> ComposeResult:
        t = self.initial_config.get("thresholds", {})
        yield Static("[bold]Highlight settings[/] (edit config/highlight.json for colors)")
        yield Static(f"Consecutive count (orange): {t.get('consecutiveCount', 1)}")
        yield Static(f"Significant % change (purple): {t.get('significantPercentChange', 0.5)}")
        yield Static("\n[dim]Close with Escape. Edit config/highlight.json and press s again to reload.[/]")
        yield Button("Close", variant="primary", id="close-btn")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close-btn":
            try:
                cfg = load_highlight_config()
                if self.on_save_cb:
                    self.on_save_cb(cfg)
            except Exception:
                pass
            self.dismiss()


def main():
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=_ROOT / ".env")

    import json as _json
    from core.app_config import load_config, get_equity_broker, get_crypto_broker, ConfigError
    from core.provider_loader import load_equity_provider, load_crypto_provider, ProviderLoadError
    from core.license import get_license_key, validate, activate, save_license_key
    from providers.yahoo_provider import YahooFinanceProvider

    # --activate <key>  — bind key to this machine and exit
    if "--activate" in sys.argv:
        idx = sys.argv.index("--activate")
        key = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else get_license_key()
        if not key:
            print("Usage: python app.py --activate <key>", file=sys.stderr)
            sys.exit(1)
        try:
            bound_key = activate(key)
            save_license_key(bound_key)
            print("Key activated and saved to ~/.highlowticker/config.toml")
        except RuntimeError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)
        sys.exit(0)

    # Validate key if present — warn only, never block
    result = validate(get_license_key())
    if result.message:
        print(f"[license] {result.message}", file=sys.stderr)
    if result.valid and not result.machine_bound:
        print("[license] Key not yet bound to this machine. Run: python app.py --activate", file=sys.stderr)

    equity_symbols = _load_symbols()
    crypto_symbols = _load_crypto_symbols()

    try:
        cfg = load_config()
        equity_broker = get_equity_broker(cfg)
        crypto_broker = get_crypto_broker(cfg)
    except ConfigError as e:
        print(f"[HighlowTicker] Config error: {e}", file=sys.stderr)
        sys.exit(1)

    equity_provider = None
    crypto_provider = None

    if equity_broker:
        try:
            equity_provider = load_equity_provider(equity_broker, equity_symbols)
        except ProviderLoadError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)

    if crypto_broker:
        try:
            crypto_provider = load_crypto_provider(crypto_broker, crypto_symbols)
        except ProviderLoadError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)

    # Fallback: neither configured → Yahoo free tier
    if not equity_provider and not crypto_provider:
        equity_provider = YahooFinanceProvider(equity_symbols)

    app = HighLowTUI(
        equity_provider=equity_provider,
        crypto_provider=crypto_provider,
    )
    app.run()


def _load_symbols() -> list[str]:
    """Load equity symbol list from tickers.json with a safe fallback."""
    import json as _json
    tickers_path = _ROOT / "tickers" / "tickers.json"
    try:
        return _json.loads(tickers_path.read_text())["symbols"]
    except Exception:
        return ["SPY", "QQQ", "DIA", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN"]


def _load_crypto_symbols() -> list[str]:
    """Load crypto symbol list from crypto_tickers.json with a safe fallback."""
    import json as _json
    tickers_path = _ROOT / "tickers" / "crypto_tickers.json"
    try:
        return _json.loads(tickers_path.read_text())["symbols"]
    except Exception:
        return ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD"]


if __name__ == "__main__":
    main()
