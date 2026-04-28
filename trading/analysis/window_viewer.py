#!/usr/bin/env python3
"""
window_viewer.py — Pick one 15-min window, see per-symbol per-price-level P&L.

Run:  python window_viewer.py
Open: http://localhost:8771
"""
import base64
import http.server
import json
import os
import sqlite3
import time
import urllib.parse
import webbrowser
from datetime import datetime
from pathlib import Path

import httpx
import numpy as np
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv

load_dotenv(Path("/Users/Harish/highlowticker-tui/.env"), override=True)
KALSHI_BASE  = "https://api.elections.kalshi.com/trade-api/v2"
_api_key     = os.environ["KALSHI_API_KEY"]
_private_key = serialization.load_pem_private_key(
    os.environ["KALSHI_API_SECRET"].encode(), password=None
)

DB      = Path.home() / ".btc_windows.db"
PORT    = 8771
SYMBOLS = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M", "XRP": "KXXRP15M"}

# Per-coin limits — must match expire_maker.py COINS config
COIN_LIMITS = {
    "BTC": (0.02, 0.40),
    "ETH": (0.02, 0.40),
    "SOL": (0.02, 0.43),
    "XRP": (0.02, 0.34),
}


def _kalshi_hdrs(method, path):
    ts  = str(round(time.time() * 1000))
    msg = ts + method.upper() + path
    sig = _private_key.sign(
        msg.encode(),
        asym_padding.PSS(mgf=asym_padding.MGF1(hashes.SHA256()),
                         salt_length=asym_padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       _api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "Content-Type":            "application/json",
    }


def backfill_winners():
    """Fetch results for any closed unlabeled windows. Returns count filled."""
    cutoff = int(time.time()) - 300  # closed more than 5 min ago
    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT id, ticker FROM windows "
        "WHERE winner IS NULL AND window_start_ts + 900 < ? "
        "ORDER BY window_start_ts",
        (cutoff,),
    ).fetchall()

    filled = 0
    with httpx.Client() as client:
        for wid, ticker in rows:
            path = f"/trade-api/v2/markets/{ticker}"
            try:
                r = client.get(f"{KALSHI_BASE}/markets/{ticker}",
                               headers=_kalshi_hdrs("GET", path), timeout=8)
                if r.status_code == 200:
                    result = r.json().get("market", {}).get("result", "")
                    if result in ("yes", "no"):
                        con.execute("UPDATE windows SET winner=? WHERE id=?", (result, wid))
                        con.commit()
                        filled += 1
            except Exception:
                pass
            time.sleep(0.05)

    con.close()
    return filled


def load_arrays(con, wid):
    ticks = con.execute(
        "SELECT elapsed_sec, yes_ask, no_ask FROM ticks "
        "WHERE window_id=? AND elapsed_sec < 900 ORDER BY elapsed_sec", (wid,)
    ).fetchall()
    yes_arr = np.full(900, np.nan)
    no_arr  = np.full(900, np.nan)
    for sec, ya, na in ticks:
        if 0 <= sec < 900:
            if ya is not None: yes_arr[sec] = ya
            if na is not None: no_arr[sec]  = na
    ly, ln = 0.5, 0.5
    for i in range(900):
        if np.isnan(yes_arr[i]): yes_arr[i] = ly
        else: ly = yes_arr[i]
        if np.isnan(no_arr[i]):  no_arr[i]  = ln
        else: ln = no_arr[i]
    return yes_arr, no_arr


def simulate(con, wts, lo, hi):
    prices = [round(p / 100, 2) for p in range(int(lo * 100), int(hi * 100) + 1)]
    sym_results = {}

    for sym, prefix in SYMBOLS.items():
        row = con.execute(
            "SELECT id, winner, ticker FROM windows "
            "WHERE ticker LIKE ? AND window_start_ts=? "
            "AND winner IS NOT NULL",
            (prefix + "%", wts)
        ).fetchone()
        if not row:
            sym_results[sym] = None
            continue

        wid, winner, ticker = row
        yes_arr, no_arr = load_arrays(con, wid)

        coin_lo, coin_hi = COIN_LIMITS.get(sym, (lo, hi))

        fills = []
        for p in prices:
            if p < coin_lo - 0.001 or p > coin_hi + 0.001:
                fills.append({"price": p, "filled": False, "out_of_range": True})
                continue

            yes_mask = yes_arr <= p
            no_mask  = no_arr  <= p
            t_yes = int(np.argmax(yes_mask)) if yes_mask.any() else 9999
            t_no  = int(np.argmax(no_mask))  if no_mask.any()  else 9999
            if t_yes == 9999 and t_no == 9999:
                fills.append({"price": p, "filled": False, "out_of_range": False})
                continue

            if t_yes <= t_no:
                side, t_hit = "YES", t_yes
            else:
                side, t_hit = "NO", t_no

            won = (side == "YES" and winner == "yes") or (side == "NO" and winner == "no")
            pnl = round((1.0 - p) if won else -p, 2)
            mins, secs = divmod(t_hit, 60)
            fills.append({
                "price":        p,
                "filled":       True,
                "out_of_range": False,
                "side":         side,
                "t_hit":        t_hit,
                "time_fmt":     f"{mins}m{secs:02d}s",
                "won":          won,
                "pnl":          pnl,
            })

        total_pnl = sum(f["pnl"] for f in fills if f["filled"] and not f.get("out_of_range"))
        sym_results[sym] = {
            "winner":    winner.upper(),
            "fills":     fills,
            "total_pnl": round(total_pnl, 2),
        }

    return sym_results, prices


def render(wts, sym_results, prices, lo, hi):
    label = datetime.fromtimestamp(wts).strftime("%b %d  %H:%M")

    # Build per-symbol column HTML
    sym_headers = ""
    sym_cols    = {s: [] for s in SYMBOLS}
    sym_totals  = ""

    for sym in SYMBOLS:
        res = sym_results.get(sym)
        if res is None:
            winner_tag = '<span style="color:#546e7a">no data</span>'
        else:
            wc = "#4caf50" if res["winner"] == "YES" else "#f44336"
            winner_tag = f'<span style="color:{wc}">{res["winner"]}</span>'
        sym_headers += f'<th colspan="2">{sym}<br><small>{winner_tag}</small></th>'

    price_rows = ""
    for p in reversed(prices):
        row = f'<td style="color:#80cbc4"><b>${p:.2f}</b></td>'
        for sym in SYMBOLS:
            res = sym_results.get(sym)
            if res is None:
                row += '<td colspan="2" style="color:#546e7a">—</td>'
                continue
            f = next((x for x in res["fills"] if abs(x["price"] - p) < 0.001), None)
            if not f or not f["filled"]:
                if f and f.get("out_of_range"):
                    row += '<td colspan="2" style="color:#2a3a4a;font-size:11px">N/A</td>'
                else:
                    row += '<td colspan="2" style="color:#333">—</td>'
            else:
                sc = "#4caf50" if f["side"] == "YES" else "#e57373"
                pc = "#4caf50" if f["won"] else "#f44336"
                ps = f'+${f["pnl"]:.2f}' if f["won"] else f'-${abs(f["pnl"]):.2f}'
                row += (
                    f'<td style="color:{sc}">{f["side"]}<br>'
                    f'<small style="color:#546e7a">{f["time_fmt"]}</small></td>'
                    f'<td style="color:{pc}">{ps}</td>'
                )
        price_rows += f"<tr>{row}</tr>"

    # Totals row
    totals_row = '<td style="color:#80cbc4"><b>NET</b></td>'
    grand = 0.0
    for sym in SYMBOLS:
        res = sym_results.get(sym)
        if res is None:
            totals_row += '<td colspan="2" style="color:#546e7a">—</td>'
        else:
            t = res["total_pnl"]
            grand += t
            tc = "#4caf50" if t >= 0 else "#f44336"
            ts = f"+${t:.2f}" if t >= 0 else f"-${abs(t):.2f}"
            totals_row += f'<td colspan="2" style="color:{tc};font-weight:700">{ts}</td>'

    gc = "#4caf50" if grand >= 0 else "#f44336"
    gs = f"+${grand:.2f}" if grand >= 0 else f"-${abs(grand):.2f}"

    return f"""
    <div style="color:#80cbc4;font-size:12px;margin-bottom:10px;padding:8px 12px;
                background:#0d1e1e;border-radius:4px">
      Window: <b>{label}</b> &nbsp;·&nbsp; limits ${lo:.2f}–${hi:.2f}
      &nbsp;·&nbsp; Grand net: <b style="color:{gc}">{gs}</b>
    </div>
    <table class="mtbl">
      <thead>
        <tr>
          <th class="left">Price</th>
          {sym_headers}
        </tr>
        <tr style="background:#0d1520">
          <th></th>
          {''.join(f'<th style="color:#78909c;font-size:10px">Side / Time</th><th style="color:#78909c;font-size:10px">P&L</th>' for _ in SYMBOLS)}
        </tr>
      </thead>
      <tbody>
        {price_rows}
        <tr style="border-top:2px solid #2e4057;background:#101820">
          {totals_row}
        </tr>
      </tbody>
    </table>"""


PAGE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Window Viewer</title>
<style>
  * {{ box-sizing:border-box; }}
  body {{ background:#0e1117; color:#e0e0e0; font-family:monospace; font-size:13px;
          margin:0; padding:20px; }}
  h2   {{ color:#4fc3f7; margin:0 0 16px; font-size:18px; }}
  .controls {{ background:#151b27; padding:14px 18px; border-radius:8px;
               margin-bottom:20px; display:flex; flex-wrap:wrap; gap:14px;
               align-items:flex-end; border:1px solid #1e2d3a; }}
  .cg  {{ display:flex; flex-direction:column; gap:4px; }}
  label {{ color:#78909c; font-size:11px; text-transform:uppercase; letter-spacing:.5px; }}
  input, select {{ background:#1e2d3a; color:#e0e0e0; border:1px solid #2e4057;
                   border-radius:4px; padding:6px 10px; font-family:monospace;
                   font-size:13px; outline:none; }}
  button {{ background:#0277bd; color:white; border:none; border-radius:4px;
            padding:8px 22px; cursor:pointer; font-size:13px; font-weight:600; }}
  button:hover {{ background:#0288d1; }}
  .mtbl {{ width:100%; border-collapse:collapse; }}
  .mtbl th {{ background:#131c2b; padding:7px 10px; text-align:center;
              color:#78909c; border-bottom:2px solid #1e2d3a; font-size:11px;
              text-transform:uppercase; }}
  .mtbl th.left {{ text-align:left; }}
  .mtbl td {{ padding:5px 10px; border-bottom:1px solid #131c2b; text-align:center; }}
  .mtbl tr:hover td {{ background:#151f2e; }}
  .error {{ color:#ef5350; padding:20px; }}
  .hint  {{ color:#546e7a; padding:40px 20px; }}
</style>
<script>
function run() {{
  var wts = document.getElementById('wts').value;
  var lo  = document.getElementById('lo').value;
  var hi  = document.getElementById('hi').value;
  if (!wts) return;
  document.getElementById('out').innerHTML = '<p style="color:#546e7a;padding:20px">Loading…</p>';
  fetch('/query?wts=' + wts + '&lo=' + lo + '&hi=' + hi)
    .then(r => r.text())
    .then(h => {{ document.getElementById('out').innerHTML = h; }});
}}
function refreshWindows() {{
  var btn = document.getElementById('refresh-btn');
  var sel = document.getElementById('wts');
  var cur = sel.value;
  btn.textContent = 'Fetching…';
  btn.disabled = true;
  fetch('/refresh')
    .then(r => r.json())
    .then(data => {{
      sel.innerHTML = '';
      data.windows.forEach(function(w) {{
        var o = document.createElement('option');
        o.value = w.ts;
        o.textContent = w.label;
        if (String(w.ts) === String(cur)) o.selected = true;
        sel.appendChild(o);
      }});
      btn.textContent = data.filled > 0
        ? '↻ +' + data.filled + ' labeled'
        : '↻ Refresh';
      btn.disabled = false;
      setTimeout(function() {{ btn.textContent = '↻ Refresh'; }}, 3000);
    }});
}}
</script>
</head>
<body>
<h2>Window Viewer</h2>
<div class="controls">
  <div class="cg">
    <label>Window</label>
    <div style="display:flex;gap:6px;align-items:center">
      <select id="wts">{window_options}</select>
      <button id="refresh-btn" onclick="refreshWindows()"
              style="padding:6px 10px;font-size:12px;background:#1e3a4a">↻ Refresh</button>
    </div>
  </div>
  <div class="cg">
    <label>Low limit</label>
    <input type="number" id="lo" min="0.01" max="0.49" step="0.01" value="0.10" style="width:75px">
  </div>
  <div class="cg">
    <label>High limit</label>
    <input type="number" id="hi" min="0.01" max="0.49" step="0.01" value="0.45" style="width:75px">
  </div>
  <button onclick="run()">&#9654; Run</button>
</div>
<div id="out"><p class="hint">Pick a window and click Run.</p></div>
</body>
</html>"""


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/refresh":
            self._serve_refresh_json()
        elif parsed.path == "/windows":
            self._serve_windows_json()
        elif parsed.path == "/query":
            try:
                wts = int(params["wts"][0])
                lo  = float(params.get("lo", ["0.10"])[0])
                hi  = float(params.get("hi", ["0.45"])[0])
                con = sqlite3.connect(DB)
                sym_results, prices = simulate(con, wts, lo, hi)
                con.close()
                self._respond(render(wts, sym_results, prices, lo, hi))
            except Exception as e:
                import traceback
                self._respond(f"<p class='error'>{e}<br><pre>{traceback.format_exc()}</pre></p>")
        else:
            con = sqlite3.connect(DB)
            rows = con.execute(
                "SELECT DISTINCT window_start_ts FROM windows "
                "WHERE winner IS NOT NULL "
                "AND (SELECT COUNT(*) FROM ticks WHERE window_id=windows.id) > 100 "
                "ORDER BY window_start_ts DESC"
            ).fetchall()
            con.close()
            opts = ""
            for (ts,) in rows:
                label = datetime.fromtimestamp(ts).strftime("%b %d  %H:%M")
                opts += f'<option value="{ts}">{label}</option>\n'
            self._respond(PAGE.format(window_options=opts))

    def _serve_refresh_json(self):
        filled = backfill_winners()
        con = sqlite3.connect(DB)
        rows = con.execute(
            "SELECT DISTINCT window_start_ts FROM windows "
            "WHERE winner IS NOT NULL "
            "AND (SELECT COUNT(*) FROM ticks WHERE window_id=windows.id) > 100 "
            "ORDER BY window_start_ts DESC"
        ).fetchall()
        con.close()
        data = {
            "filled": filled,
            "windows": [{"ts": r[0], "label": datetime.fromtimestamp(r[0]).strftime("%b %d  %H:%M")}
                        for r in rows],
        }
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_windows_json(self):
        import json
        con = sqlite3.connect(DB)
        rows = con.execute(
            "SELECT DISTINCT window_start_ts FROM windows "
            "WHERE winner IS NOT NULL "
            "AND (SELECT COUNT(*) FROM ticks WHERE window_id=windows.id) > 100 "
            "ORDER BY window_start_ts DESC"
        ).fetchall()
        con.close()
        data = [{"ts": r[0], "label": datetime.fromtimestamp(r[0]).strftime("%b %d  %H:%M")}
                for r in rows]
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _respond(self, html):
        data = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    import threading
    server = http.server.HTTPServer(("", PORT), Handler)
    url = f"http://localhost:{PORT}"
    print(f"Window Viewer  →  {url}")
    threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    server.serve_forever()
