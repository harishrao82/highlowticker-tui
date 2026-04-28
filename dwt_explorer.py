#!/usr/bin/env python3
"""
dwt_explorer.py — interactive DWT pattern match explorer.

Pick any recorded window (or live latest) from a dropdown.
For each symbol, finds the best DWT match at 5 checkpoints:
  45s, 90s, 3min, 5min, 9min

Only shows a match if similarity >= 0.70.
Matched window: first T seconds bright, remainder dimmed.
Click legend items to show/hide any line.

Run:  python dwt_explorer.py
"""
import http.server
import os
import sqlite3
import threading
import urllib.parse
import webbrowser
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import pywt
import stumpy
from plotly.subplots import make_subplots

# ── Config ────────────────────────────────────────────────────────────────────

DB           = Path.home() / ".btc_windows.db"
SYMBOLS      = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "XRP": "KXXRP15M", "SOL": "KXSOL15M"}
PORT         = 8766   # different port from hmm_explorer

DWT_WAVELET  = "db4"
DWT_LEVEL    = 1
SIM_THRESH   = 0.70

CHECKPOINTS  = [45, 90, 180, 300, 540]
CP_LABELS    = ["45s", "90s", "3min", "5min", "9min"]
CP_COLORS    = ["#3498db", "#e74c3c", "#2ecc71", "#f39c12", "#9b59b6"]

CURRENT_COLOR = "white"

# ── Distance helpers ──────────────────────────────────────────────────────────

def load_ticks(con, wid, length=900):
    df = pd.read_sql(
        "SELECT elapsed_sec, yes_ask FROM ticks WHERE window_id=? ORDER BY elapsed_sec",
        con, params=(wid,)
    )
    arr = (df.set_index("elapsed_sec").reindex(range(length))
             .ffill().bfill()["yes_ask"].to_numpy(dtype=np.float64))
    return np.nan_to_num(arr, nan=0.5)

def znorm(a):
    return (a - a.mean()) / (a.std() + 1e-10)

# DWT distance
def dwt_approx(arr):
    return pywt.wavedec(arr, DWT_WAVELET, level=DWT_LEVEL)[0]

def dwt_dist(a, b):
    ca, cb = znorm(dwt_approx(a)), znorm(dwt_approx(b))
    n = min(len(ca), len(cb))
    return float(np.sqrt(np.sum((ca[:n] - cb[:n]) ** 2)))

# Stumpy (MASS) distance — z-normalised Euclidean on raw ticks
def stumpy_dist(a, b):
    """Z-normalised Euclidean distance via MASS (stumpy)."""
    n = min(len(a), len(b))
    q = a[:n].astype(np.float64)
    t = b[:n].astype(np.float64)
    # MASS requires len(T) >= len(Q); pad t with one extra point if equal
    t_pad = np.append(t, t[-1])
    dist_profile = stumpy.mass(q, t_pad)
    return float(dist_profile[0])   # distance of query vs start of t

def compute_dist(a, b, method: str) -> float:
    if method == "stumpy":
        return stumpy_dist(a, b)
    return dwt_dist(a, b)

def similarity(d, avg_d):
    return 1.0 / (1.0 + d / avg_d) if avg_d > 0 else 0.0

# ── Chart builder ─────────────────────────────────────────────────────────────

def build_chart(window_ts: int, method: str = "dwt") -> str:
    con       = sqlite3.connect(DB)
    is_live   = (window_ts == 0)

    if is_live:
        window_ts = con.execute(
            "SELECT MAX(window_start_ts) FROM windows"
        ).fetchone()[0]

    win_label = datetime.fromtimestamp(window_ts).strftime("%Y-%m-%d %H:%M ET")

    # How far into the window are we?
    now = int(datetime.now(timezone.utc).timestamp())
    elapsed = now - window_ts
    T_live  = min(elapsed, 900) if is_live else 900
    T_live  = max(T_live, 1)

    fig = make_subplots(
        rows=len(SYMBOLS), cols=1,
        subplot_titles=list(SYMBOLS.keys()),
        vertical_spacing=0.06,
    )

    for row_idx, (sym, prefix) in enumerate(SYMBOLS.items(), 1):

        # ── Load selected window ──────────────────────────────────────────────
        cur_row = con.execute(
            "SELECT w.id FROM windows w WHERE w.window_start_ts=? AND w.ticker LIKE ?",
            (window_ts, prefix + "%")
        ).fetchone()
        if not cur_row:
            fig.layout.annotations[row_idx-1].text = f"{sym} — no data"
            continue

        cur_arr  = load_ticks(con, cur_row[0])
        T_show   = T_live  # how much of current window to show solid
        cur_ya   = float(cur_arr[min(T_show - 1, 899)])
        cur_side = "YES" if cur_ya >= 0.5 else "NO"
        side_color = "#2ecc71" if cur_side == "YES" else "#e74c3c"

        # ── Load all past labeled windows for this coin ───────────────────────
        past_rows = con.execute(
            "SELECT w.id, w.window_start_ts, w.winner FROM windows w "
            "WHERE w.winner IS NOT NULL AND w.window_start_ts < ? AND w.ticker LIKE ? "
            "AND (SELECT COUNT(*) FROM ticks WHERE window_id=w.id) > 100 "
            "ORDER BY w.window_start_ts",
            (window_ts, prefix + "%")
        ).fetchall()

        past_arrays = {wid: load_ticks(con, wid) for wid, _, _ in past_rows}

        # ── For each checkpoint: find best match ──────────────────────────────
        matches = {}   # key → match dict

        for cp, cp_label, cp_color in zip(CHECKPOINTS, CP_LABELS, CP_COLORS):
            if cp > T_show and is_live:
                continue

            cp_actual = min(cp, T_show)
            cur_clip  = cur_arr[:cp_actual]
            if len(cur_clip) < 16:
                continue

            dists = []
            for wid, wts, winner in past_rows:
                past_clip = past_arrays[wid][:cp_actual]
                if len(past_clip) < 16:
                    continue
                d = compute_dist(cur_clip, past_clip, method)
                dists.append((d, wid, wts, winner))

            if not dists:
                continue

            avg_d    = float(np.mean([d for d, *_ in dists]))
            dists.sort(key=lambda x: x[0])
            best_d, best_wid, best_wts, best_winner = dists[0]
            best_sim = similarity(best_d, avg_d)

            if best_sim < SIM_THRESH:
                continue

            t_str = datetime.fromtimestamp(best_wts).strftime("%m-%d %H:%M")
            matches[cp] = {
                "arr":    past_arrays[best_wid],
                "winner": best_winner,
                "sim":    best_sim,
                "wts":    best_wts,
                "label":  f"{cp_label} match: {t_str} → {best_winner.upper()}  sim={best_sim:.2f}",
                "color":  cp_color,
                "cp":     cp_actual,
            }

        # ── Rolling best: scan clip lengths [T-30 … T], find overall best ────
        if T_show >= 32:
            roll_start = max(T_show - 30, 16)
            # avg_d baseline: use full T clip
            base_dists = [compute_dist(cur_arr[:T_show], past_arrays[wid][:T_show], method)
                          for wid, _, _ in past_rows]
            avg_d_roll = float(np.mean(base_dists)) if base_dists else 1.0

            roll_best = None   # (sim, clip_len, arr, winner, wts)
            for clip_len in range(roll_start, T_show + 1):
                cur_clip = cur_arr[:clip_len]
                for wid, wts, winner in past_rows:
                    d       = compute_dist(cur_clip, past_arrays[wid][:clip_len], method)
                    sim_val = similarity(d, avg_d_roll)
                    if sim_val >= SIM_THRESH:
                        if roll_best is None or sim_val > roll_best[0]:
                            roll_best = (sim_val, clip_len, past_arrays[wid], winner, wts)

            if roll_best:
                rb_sim, rb_clip, rb_arr, rb_winner, rb_wts = roll_best
                rb_tstr = datetime.fromtimestamp(rb_wts).strftime("%m-%d %H:%M")
                matches["rolling"] = {
                    "arr":    rb_arr,
                    "winner": rb_winner,
                    "sim":    rb_sim,
                    "wts":    rb_wts,
                    "label":  f"rolling @{rb_clip}s: {rb_tstr} → {rb_winner.upper()}  sim={rb_sim:.2f}",
                    "color":  "#ffffff",
                    "cp":     rb_clip,
                }

        # ── Plot current window first (appears at top of symbol group) ──────────
        fig.add_trace(go.Scatter(
            x=list(range(T_show)),
            y=cur_arr[:T_show].tolist(),
            mode="lines",
            name="current",
            line=dict(color=CURRENT_COLOR, width=3),
            legendgroup=f"{sym}_current",
            legendgrouptitle=dict(
                text=f"▪ {sym}  {cur_side}  {cur_ya:.2f}",
                font=dict(size=13, color=side_color),
            ),
            showlegend=True,
        ), row=row_idx, col=1)

        # ── Plot matched windows ──────────────────────────────────────────────
        for cp, m in matches.items():
            arr   = m["arr"]
            color = m["color"]
            cp_t  = m["cp"]
            grp   = f"{sym}_{cp}"
            short = f"{m['label'].split(':')[0]}  {m['label'].split('→')[1].strip()}"

            # Bright segment: first cp_t seconds
            fig.add_trace(go.Scatter(
                x=list(range(cp_t)),
                y=arr[:cp_t].tolist(),
                mode="lines",
                name=short,
                line=dict(color=color, width=2),
                opacity=0.9,
                legendgroup=grp,
                legendgrouptitle=dict(text=""),
                showlegend=True,
            ), row=row_idx, col=1)

            # Dim segment: rest of window
            if cp_t < 900:
                fig.add_trace(go.Scatter(
                    x=list(range(cp_t, 900)),
                    y=arr[cp_t:].tolist(),
                    mode="lines",
                    name=short,
                    line=dict(color=color, width=1.5, dash="dot"),
                    opacity=0.55,
                    legendgroup=grp,
                    showlegend=False,
                ), row=row_idx, col=1)

        # Vertical line at current time (live only)
        if is_live and T_show < 900:
            fig.add_vline(x=T_show, line_dash="dash",
                          line_color="white", opacity=0.4,
                          row=row_idx, col=1)

        # 0.5 reference
        fig.add_hline(y=0.5, line_dash="dot", line_color="gray",
                      opacity=0.3, row=row_idx, col=1)

        # Checkpoint markers: bright colored vline if match found, faint grey otherwise
        for cp, cp_color in zip(CHECKPOINTS, CP_COLORS):
            if is_live and cp > T_show:
                continue
            if cp in matches:
                fig.add_vline(x=matches[cp]["cp"], line_dash="dash",
                              line_color=matches[cp]["color"], line_width=1.5,
                              opacity=0.8, row=row_idx, col=1)
            else:
                fig.add_vline(x=cp, line_dash="dot", line_color="#444",
                              opacity=0.4, row=row_idx, col=1)
        # Rolling match vline (white dashed)
        if "rolling" in matches:
            fig.add_vline(x=matches["rolling"]["cp"], line_dash="dash",
                          line_color="#cccccc", line_width=1,
                          opacity=0.6, row=row_idx, col=1)

        n_matches = len(matches)
        outcomes_str = "  (" + ", ".join(m["winner"].upper() for m in matches.values()) + ")" if matches else ""
        fig.layout.annotations[row_idx-1].text = (
            f"{sym}  —  {n_matches} match{'es' if n_matches != 1 else ''} ≥ {SIM_THRESH:.0%}"
            + outcomes_str
            + (f"  |  t={T_show}s" if is_live else "")
        )

    fig.update_yaxes(range=[0, 1], tickformat=".2f")
    fig.update_xaxes(range=[0, 900], title_text="Elapsed seconds")
    fig.update_layout(
        height=380 * len(SYMBOLS),
        template="plotly_dark",
        title_text=f"Pattern Explorer [{method.upper()}] — {win_label}  (click legend to hide/show)",
        legend=dict(
            orientation="v", x=1.01, y=1,
            tracegroupgap=12,
            bgcolor="rgba(20,20,40,0.85)",
            bordercolor="#334",
            borderwidth=1,
            font=dict(size=11),
        ),
        hovermode="x unified",
    )
    con.close()
    back_btn = (
        '<div style="position:fixed;top:12px;left:12px;z-index:9999">'
        '<a href="/" style="background:#2e86c1;color:#fff;padding:8px 16px;'
        'border-radius:8px;text-decoration:none;font-family:system-ui;font-weight:600;'
        'font-size:0.95rem;">← Back</a></div>'
    )
    raw = pio.to_html(fig, full_html=True, include_plotlyjs="cdn")
    return raw.replace("<body>", "<body>" + back_btn)

# ── HTTP server ───────────────────────────────────────────────────────────────

def get_windows():
    con  = sqlite3.connect(DB)
    latest = con.execute("SELECT MAX(window_start_ts) FROM windows").fetchone()[0]
    rows = con.execute(
        "SELECT DISTINCT window_start_ts FROM windows "
        "WHERE (SELECT COUNT(*) FROM ticks WHERE window_id=windows.id) > 100 "
        "ORDER BY window_start_ts DESC LIMIT 200"
    ).fetchall()
    con.close()
    return latest, [r[0] for r in rows]

INDEX_TMPL = """<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Pattern Explorer</title>
<style>
  body  {{ background:#1a1a2e; color:#eee; font-family:system-ui,sans-serif;
           display:flex; flex-direction:column; align-items:center;
           justify-content:center; height:100vh; margin:0; }}
  h1    {{ font-size:1.6rem; margin-bottom:0.4rem; color:#58d6ff; }}
  p     {{ font-size:0.9rem; color:#888; margin:0 0 1rem; }}
  select{{ font-size:1.05rem; padding:10px 14px; border-radius:8px;
           background:#16213e; color:#eee; border:1px solid #334; width:340px; }}
  button{{ margin-left:10px; font-size:1.05rem; padding:10px 20px;
           border-radius:8px; background:#2e86c1; color:#fff; border:none;
           cursor:pointer; font-weight:600; }}
  button:hover {{ background:#1a5276; }}
  #live {{ background:#1e8449; }}
  #live:hover {{ background:#145a32; }}
  #status {{ margin-top:1rem; font-size:0.95rem; color:#aaa; min-height:1.4em; }}
  .row  {{ display:flex; align-items:center; margin-bottom:0.8rem; }}
  .method-toggle {{ display:flex; gap:10px; margin-bottom:1rem; }}
  .method-btn {{
    padding:8px 20px; border-radius:8px; font-size:1rem; font-weight:600;
    border:2px solid #334; background:#16213e; color:#aaa; cursor:pointer;
  }}
  .method-btn.active {{ border-color:#58d6ff; color:#58d6ff; background:#1a2a3e; }}
</style></head>
<body>
<h1>Pattern Explorer</h1>
<p>Checkpoints: 45s · 90s · 3min · 5min · 9min &nbsp;|&nbsp; sim ≥ 0.70 only &nbsp;|&nbsp; click legend to toggle</p>
<div class="method-toggle">
  <button class="method-btn active" id="btn-dwt"     onclick="setMethod('dwt')">DWT</button>
  <button class="method-btn"        id="btn-stumpy"  onclick="setMethod('stumpy')">Stumpy (MASS)</button>
</div>
<div class="row">
  <select id="win">{options}</select>
  <button onclick="run()">&#9654; Show</button>
  <button id="live" onclick="runLive()">⚡ Latest Live</button>
</div>
<div id="status">Select a window or click Latest Live.</div>
<script>
var _method = 'dwt';
function setMethod(m) {{
  _method = m;
  document.getElementById('btn-dwt').classList.toggle('active', m === 'dwt');
  document.getElementById('btn-stumpy').classList.toggle('active', m === 'stumpy');
}}
function openChart(url) {{
  document.getElementById('status').innerText = 'Building chart… (may take a few seconds)';
  window.location.href = url;
}}
function run()     {{ openChart('/chart?ts=' + document.getElementById('win').value + '&method=' + _method); }}
function runLive() {{ openChart('/chart?ts=0&method=' + _method); }}
</script>
</body></html>"""

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/":
            latest, windows = get_windows()
            opts = f'<option value="0">⚡ Latest live (auto-detect)</option>\n'
            opts += "\n".join(
                f'<option value="{ts}"{"  selected" if ts == latest else ""}>'
                f'{datetime.fromtimestamp(ts).strftime("%Y-%m-%d  %H:%M ET")}'
                f'</option>'
                for ts in windows
            )
            body = INDEX_TMPL.format(options=opts).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)

        elif parsed.path == "/chart":
            params = urllib.parse.parse_qs(parsed.query)
            ts     = int(params["ts"][0])
            method = params.get("method", ["dwt"])[0]
            if method not in ("dwt", "stumpy"):
                method = "dwt"
            label  = "live" if ts == 0 else datetime.fromtimestamp(ts).strftime("%H:%M ET")
            print(f"\nBuilding {method.upper()} chart for window: {label}…")
            html   = build_chart(ts, method).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html)

        else:
            self.send_response(404); self.end_headers()

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    server = http.server.HTTPServer(("127.0.0.1", PORT), Handler)
    url    = f"http://127.0.0.1:{PORT}"
    print(f"DWT Explorer running at {url}")
    print("Checkpoints: 45s, 90s, 3min, 5min, 9min  |  sim threshold: {SIM_THRESH}")
    print("Stop with Ctrl-C\n")
    threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")

if __name__ == "__main__":
    main()
