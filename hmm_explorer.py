#!/usr/bin/env python3
"""
hmm_explorer.py — interactive HMM window explorer.

Pick any recorded window from a dropdown, see the HMM state chart.
Run:  python hmm_explorer.py   (opens browser automatically)
"""
import http.server
import json
import os
import sqlite3
import threading
import urllib.parse
import webbrowser
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
from scipy.special import logsumexp
from scipy.stats import multivariate_normal
from sklearn.cluster import KMeans

# ── Config ────────────────────────────────────────────────────────────────────

DB           = Path.home() / ".btc_windows.db"
SYMBOLS      = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "XRP": "KXXRP15M", "SOL": "KXSOL15M"}
N_STATES     = 3
MAX_ITER     = 15
STATE_COLORS = ["#e74c3c", "#f39c12", "#2ecc71"]
STATE_NAMES  = ["Bearish", "Uncertain", "Bullish"]
PORT         = 8765
HTML_OUT     = os.path.expanduser("~/hmm_explorer_chart.html")

_HMM_CACHE: dict = {}

# ── HMM core ──────────────────────────────────────────────────────────────────

def load_ticks(con, wid, length=900):
    df = pd.read_sql(
        "SELECT elapsed_sec, yes_ask FROM ticks WHERE window_id=? ORDER BY elapsed_sec",
        con, params=(wid,)
    )
    arr = (df.set_index("elapsed_sec").reindex(range(length))
             .ffill().bfill()["yes_ask"].to_numpy(dtype=np.float64))
    return np.nan_to_num(arr, nan=0.5)

def make_features(arr, T):
    x     = arr[:T]
    x_sm  = pd.Series(x).rolling(15, min_periods=1).mean().to_numpy()
    delta = np.diff(x_sm, prepend=x_sm[0])
    return np.column_stack([x_sm, delta, np.abs(delta)])

def log_emission(obs, means, covs):
    T, N  = len(obs), len(means)
    log_b = np.full((T, N), -1e10)
    for k in range(N):
        try:
            rv = multivariate_normal(mean=means[k], cov=covs[k], allow_singular=True)
            log_b[:, k] = np.log(np.clip(rv.pdf(obs), 1e-300, None))
        except Exception:
            pass
    return log_b

def forward(log_pi, log_A, log_b):
    T, N  = log_b.shape
    alpha = np.full((T, N), -np.inf)
    alpha[0] = log_pi + log_b[0]
    for t in range(1, T):
        alpha[t] = logsumexp(alpha[t-1][:, None] + log_A, axis=0) + log_b[t]
    return alpha

def backward(log_A, log_b):
    T, N = log_b.shape
    beta = np.zeros((T, N))
    for t in range(T-2, -1, -1):
        beta[t] = logsumexp(log_A + log_b[t+1] + beta[t+1], axis=1)
    return beta

def viterbi(log_pi, log_A, log_b):
    T, N  = log_b.shape
    delta = np.full((T, N), -np.inf)
    psi   = np.zeros((T, N), dtype=int)
    delta[0] = log_pi + log_b[0]
    for t in range(1, T):
        trans    = delta[t-1][:, None] + log_A
        psi[t]   = trans.argmax(axis=0)
        delta[t] = trans.max(axis=0) + log_b[t]
    states     = np.zeros(T, dtype=int)
    states[-1] = delta[-1].argmax()
    for t in range(T-2, -1, -1):
        states[t] = psi[t+1, states[t+1]]
    return states

def train_hmm(obs_seqs):
    X_all = np.vstack(obs_seqs)
    D     = X_all.shape[1]
    km    = KMeans(n_clusters=N_STATES, n_init=5, random_state=42).fit(X_all)
    means = km.cluster_centers_.copy()
    labels= km.labels_
    covs  = []
    for k in range(N_STATES):
        pts = X_all[labels == k]
        if len(pts) < 2: pts = X_all
        covs.append(np.diag(np.maximum(np.var(pts, axis=0), 1e-6)))
    covs    = np.array(covs)
    pi      = np.full(N_STATES, 1.0 / N_STATES)
    A       = np.full((N_STATES, N_STATES), 1.0 / N_STATES)
    prev_ll = -np.inf
    for _ in range(MAX_ITER):
        log_pi = np.log(pi + 1e-300)
        log_A  = np.log(A  + 1e-300)
        sum_g0, sum_xi = np.zeros(N_STATES), np.zeros((N_STATES, N_STATES))
        sum_g,  sum_go = np.zeros(N_STATES), np.zeros((N_STATES, D))
        sum_go2 = np.zeros((N_STATES, D))
        total_ll = 0.0
        for obs in obs_seqs:
            log_b = log_emission(obs, means, covs)
            alpha = forward(log_pi, log_A, log_b)
            beta  = backward(log_A, log_b)
            ll    = logsumexp(alpha[-1]);  total_ll += ll
            gamma = np.exp(np.clip(alpha + beta - ll, -500, 0))
            gamma /= gamma.sum(axis=1, keepdims=True) + 1e-300
            log_xi = (alpha[:-1, :, None] + log_A[None] +
                      log_b[1:, None, :] + beta[1:, None, :] - ll)
            xi = np.exp(np.clip(log_xi, -500, 0))
            xi /= xi.sum(axis=(1, 2), keepdims=True) + 1e-300
            sum_g0  += gamma[0];  sum_xi  += xi.sum(axis=0)
            sum_g   += gamma.sum(axis=0)
            sum_go  += gamma.T @ obs;  sum_go2 += gamma.T @ (obs ** 2)
        pi    = sum_g0 / (sum_g0.sum() + 1e-300)
        A     = sum_xi / (sum_xi.sum(axis=1, keepdims=True) + 1e-300)
        means = sum_go / (sum_g[:, None] + 1e-300)
        var   = sum_go2 / (sum_g[:, None] + 1e-300) - means**2
        covs  = np.array([np.diag(np.maximum(var[k], 1e-6)) for k in range(N_STATES)])
        if abs(total_ll - prev_ll) < 1.0: break
        prev_ll = total_ll
    order = np.argsort(means[:, 0])
    return {"pi": pi[order], "A": A[np.ix_(order, order)],
            "means": means[order], "covs": covs[order]}

def get_or_train(con, sym, prefix):
    """Train once per coin on all labeled windows, cache by sym only."""
    rows = con.execute(
        "SELECT w.id, w.winner FROM windows w "
        "WHERE w.winner IS NOT NULL AND w.ticker LIKE ? "
        "AND (SELECT COUNT(*) FROM ticks WHERE window_id=w.id) > 100 "
        "ORDER BY w.window_start_ts",
        (prefix+"%",)
    ).fetchall()
    n = len(rows)
    if sym in _HMM_CACHE and _HMM_CACHE[sym]["n_windows"] == n:
        return _HMM_CACHE[sym]
    print(f"  Training {sym} on {n} windows…")
    obs_seqs = [make_features(load_ticks(con, wid), 900) for wid, _ in rows]
    model    = train_hmm(obs_seqs)
    log_pi   = np.log(model["pi"] + 1e-300)
    log_A    = np.log(model["A"]  + 1e-300)
    sy, sn   = np.zeros(N_STATES), np.zeros(N_STATES)
    for (wid, winner), obs in zip(rows, obs_seqs):
        sts = viterbi(log_pi, log_A, log_emission(obs, model["means"], model["covs"]))
        dom = np.bincount(sts, minlength=N_STATES).argmax()
        if winner == "yes": sy[dom] += 1
        else:               sn[dom] += 1
    model["win_rates"]    = sy / (sy + sn + 1e-10)
    model["state_counts"] = (sy + sn).astype(int)
    model["n_windows"]    = n
    _HMM_CACHE[sym]       = model
    return model

# ── Chart builder ─────────────────────────────────────────────────────────────

def build_chart(window_ts: int) -> str:
    con       = sqlite3.connect(DB)
    win_label = datetime.fromtimestamp(window_ts).strftime("%Y-%m-%d %H:%M ET")
    fig       = make_subplots(rows=len(SYMBOLS), cols=1,
                              subplot_titles=list(SYMBOLS.keys()),
                              vertical_spacing=0.08)

    for row_idx, (sym, prefix) in enumerate(SYMBOLS.items(), 1):
        model  = get_or_train(con, sym, prefix)
        log_pi = np.log(model["pi"] + 1e-300)
        log_A  = np.log(model["A"]  + 1e-300)

        row = con.execute(
            "SELECT w.id, w.winner FROM windows w WHERE w.window_start_ts=? AND w.ticker LIKE ?",
            (window_ts, prefix+"%")
        ).fetchone()
        if not row:
            fig.layout.annotations[row_idx-1].text = f"{sym} — no data"
            continue

        wid, winner = row
        arr    = load_ticks(con, wid)
        T_max  = con.execute(
            "SELECT MAX(elapsed_sec) FROM ticks WHERE window_id=? AND yes_ask IS NOT NULL", (wid,)
        ).fetchone()[0] or 899
        T      = min(T_max + 1, 900)
        feat   = make_features(arr, T)
        states = viterbi(log_pi, log_A, log_emission(feat, model["means"], model["covs"]))

        # Colored state bands
        boundaries = np.concatenate([[0], np.where(np.diff(states))[0] + 1, [T]])
        for i in range(len(boundaries) - 1):
            s0, s1 = int(boundaries[i]), int(boundaries[i+1])
            fig.add_shape(type="rect", x0=s0, x1=s1, y0=0, y1=1,
                          yref="y domain", fillcolor=STATE_COLORS[states[s0]],
                          opacity=0.35, line_width=0, layer="below",
                          row=row_idx, col=1)

        # Price line
        fig.add_trace(go.Scatter(
            x=list(range(T)), y=arr[:T].tolist(), mode="lines",
            name=f"{sym} yes_ask", line=dict(color="white", width=2),
            showlegend=(row_idx == 1),
        ), row=row_idx, col=1)

        # Transition markers
        ti = np.where(np.diff(states))[0] + 1
        if len(ti):
            fig.add_trace(go.Scatter(
                x=ti.tolist(), y=arr[ti].tolist(), mode="markers",
                name="Transition", marker=dict(color="yellow", size=9, symbol="diamond"),
                showlegend=(row_idx == 1),
            ), row=row_idx, col=1)

        fig.add_hline(y=0.5, line_dash="dot", line_color="gray",
                      opacity=0.4, row=row_idx, col=1)

        final_state = int(states[-1])
        dom_state   = int(np.bincount(states, minlength=N_STATES).argmax())
        wr          = model["win_rates"][final_state]
        outcome     = f"  →  {'✅ YES' if winner=='yes' else '❌ NO'}" if winner else "  (open)"
        fig.layout.annotations[row_idx-1].text = (
            f"{sym}  dominant={STATE_NAMES[dom_state]}  "
            f"final={STATE_NAMES[final_state]}  YES rate={wr:.0%}{outcome}"
        )

    for k in range(N_STATES):
        fig.add_trace(go.Scatter(x=[None], y=[None], mode="markers",
            marker=dict(color=STATE_COLORS[k], size=12, symbol="square"),
            name=STATE_NAMES[k], showlegend=True), row=1, col=1)

    fig.update_yaxes(range=[0, 1], tickformat=".2f")
    fig.update_xaxes(range=[0, 900], title_text="Elapsed seconds")
    fig.update_layout(height=400*len(SYMBOLS), template="plotly_dark",
                      title_text=f"HMM Explorer — {win_label}",
                      legend=dict(orientation="v", x=1.01, y=1),
                      hovermode="x unified")
    con.close()
    return pio.to_html(fig, full_html=True, include_plotlyjs="cdn")

# ── HTTP server ───────────────────────────────────────────────────────────────

def get_windows():
    con  = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT DISTINCT window_start_ts FROM windows "
        "WHERE (SELECT COUNT(*) FROM ticks WHERE window_id=windows.id) > 100 "
        "ORDER BY window_start_ts DESC"
    ).fetchall()
    con.close()
    return [r[0] for r in rows]

INDEX_TMPL = """<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>HMM Explorer</title>
<style>
  body  {{ background:#1a1a2e; color:#eee; font-family:system-ui,sans-serif;
           display:flex; flex-direction:column; align-items:center;
           justify-content:center; height:100vh; margin:0; }}
  h1    {{ font-size:1.6rem; margin-bottom:1.4rem; color:#58d6ff; }}
  select{{ font-size:1.1rem; padding:10px 14px; border-radius:8px;
           background:#16213e; color:#eee; border:1px solid #334; width:320px; }}
  button{{ margin-left:12px; font-size:1.1rem; padding:10px 22px;
           border-radius:8px; background:#2e86c1; color:#fff; border:none;
           cursor:pointer; font-weight:600; }}
  button:hover {{ background:#1a5276; }}
  #status {{ margin-top:1rem; font-size:0.95rem; color:#aaa; min-height:1.4em; }}
</style></head>
<body>
<h1>HMM Window Explorer</h1>
<div>
  <select id="win">{options}</select>
  <button onclick="run()">&#9654; Show Chart</button>
</div>
<div id="status">Select a window and click Show Chart.</div>
<script>
function run() {{
  var ts = document.getElementById('win').value;
  document.getElementById('status').innerText = 'Building chart… (first run trains the model, ~30s)';
  fetch('/chart?ts=' + ts)
    .then(r => r.text())
    .then(html => {{
      var w = window.open('', '_blank');
      w.document.write(html);
      w.document.close();
      document.getElementById('status').innerText = 'Done.';
    }})
    .catch(e => document.getElementById('status').innerText = 'Error: ' + e);
}}
</script>
</body></html>"""

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass   # silence access log

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/":
            windows = get_windows()
            opts    = "\n".join(
                f'<option value="{ts}">'
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
            print(f"\nBuilding chart for window {datetime.fromtimestamp(ts).strftime('%H:%M ET')}…")
            html   = build_chart(ts).encode()
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
    print(f"HMM Explorer running at {url}")
    print("Stop with Ctrl-C\n")
    threading.Timer(0.5, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")

if __name__ == "__main__":
    main()
