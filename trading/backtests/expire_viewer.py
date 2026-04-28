#!/usr/bin/env python3
"""
expire_viewer.py — interactive web viewer for the expire/fade backtest.

Pick symbol, date window, and limit price range.
For each limit price, shows every individual trade (when hit, side, cost)
and a summary (total cost, total profit).

Run:  python expire_viewer.py
Open: http://localhost:8770
"""
import http.server
import sqlite3
import traceback
import urllib.parse
import webbrowser
from datetime import datetime
from pathlib import Path

import numpy as np

DB      = Path.home() / ".btc_windows.db"
PORT    = 8770
SYMBOLS = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M", "XRP": "KXXRP15M"}


# ── Data helpers ──────────────────────────────────────────────────────────────

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


def run_backtest(sym, prefix, from_ts, to_ts, lo, hi):
    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT w.id, w.winner, w.window_start_ts FROM windows w "
        "WHERE w.winner IS NOT NULL AND w.ticker LIKE ? "
        "AND w.window_start_ts >= ? AND w.window_start_ts <= ? "
        "AND (SELECT COUNT(*) FROM ticks WHERE window_id=w.id) > 100 "
        "ORDER BY w.window_start_ts",
        (prefix + "%", from_ts, to_ts)
    ).fetchall()

    # Cache arrays — one DB read per window regardless of price levels
    arrays = {}
    for wid, winner, wts in rows:
        arrays[wid] = load_arrays(con, wid)
    con.close()

    prices  = [round(p / 100, 2) for p in range(int(lo * 100), int(hi * 100) + 1)]
    results = []

    for p in prices:
        trades = []
        for wid, winner, wts in rows:
            yes_arr, no_arr = arrays[wid]
            yes_mask = yes_arr <= p
            no_mask  = no_arr  <= p
            t_yes = int(np.argmax(yes_mask)) if yes_mask.any() else 9999
            t_no  = int(np.argmax(no_mask))  if no_mask.any()  else 9999
            if t_yes == 9999 and t_no == 9999:
                continue

            if t_yes <= t_no:
                side      = "YES"
                t_hit     = t_yes
                price_hit = float(yes_arr[t_yes])
            else:
                side      = "NO"
                t_hit     = t_no
                price_hit = float(no_arr[t_no])

            won = (side == "YES" and winner == "yes") or (side == "NO" and winner == "no")
            pnl = round((1.0 - p) if won else -p, 2)

            trades.append({
                "wts":       wts,
                "wlabel":    datetime.fromtimestamp(wts).strftime("%m-%d %H:%M"),
                "t_hit":     t_hit,
                "side":      side,
                "price_hit": price_hit,
                "cost":      p,
                "winner":    winner.upper(),
                "won":       won,
                "pnl":       pnl,
            })

        if not trades:
            continue

        wins         = sum(1 for t in trades if t["won"])
        total_profit = sum(t["pnl"] for t in trades if t["won"])
        total_loss   = abs(sum(t["pnl"] for t in trades if not t["won"]))
        net          = total_profit - total_loss
        results.append({
            "price":        p,
            "pid":          int(round(p * 100)),   # safe HTML id (e.g. 10, 32)
            "trades":       trades,
            "n_trades":     len(trades),
            "wins":         wins,
            "win_pct":      wins / len(trades),
            "total_profit": total_profit,
            "total_loss":   total_loss,
            "net":          net,
            "net_100":      net * 100,
        })

    return results, len(rows)


# ── HTML rendering ────────────────────────────────────────────────────────────

def render_results(sym, results, n_windows, from_label, to_label, lo, hi):
    summary_rows = ""
    detail_blocks = ""

    for r in results:
        pid       = r["pid"]
        net_col   = "#4caf50" if r["net"] > 0 else "#f44336"
        net_sign  = "+" if r["net"] > 0 else ""
        breakeven = r["price"]
        edge_col  = "#4caf50" if r["win_pct"] > breakeven else "#f44336"

        summary_rows += f"""
        <tr class="srow" onclick="toggle({pid})">
          <td class="left"><b>${r['price']:.2f}</b></td>
          <td>{r['n_trades']}</td>
          <td>{r['wins']}</td>
          <td style="color:{edge_col}">{r['win_pct']:.0%}</td>
          <td style="color:#888;font-size:11px">{breakeven:.0%} b/e</td>
          <td style="color:#4caf50">+${r['total_profit']:.2f}</td>
          <td style="color:#f44336">-${r['total_loss']:.2f}</td>
          <td style="color:{net_col}">{net_sign}{r['net']:.2f}</td>
          <td style="color:{net_col}">{net_sign}{r['net_100']:.0f}</td>
          <td class="expand-btn" id="btn-{pid}">▶</td>
        </tr>"""

        trade_rows = ""
        for t in r["trades"]:
            sc = "#4caf50" if t["side"] == "YES" else "#e57373"
            rc = "#4caf50" if t["won"] else "#f44336"
            rl = "WIN" if t["won"] else "LOSS"
            ps = f"+${t['pnl']:.2f}" if t["won"] else f"-${abs(t['pnl']):.2f}"
            mins, secs = divmod(t["t_hit"], 60)
            time_fmt = f"{mins}m {secs:02d}s"
            trade_rows += f"""
            <tr>
              <td>{t['wlabel']}</td>
              <td>{t['t_hit']}s &nbsp;<span style="color:#666">({time_fmt})</span></td>
              <td style="color:{sc};font-weight:600">{t['side']}</td>
              <td style="color:#546e7a">${t['price_hit']:.3f}</td>
              <td><b>${t['cost']:.2f}</b></td>
              <td>{t['winner']}</td>
              <td style="color:{rc};font-weight:600">{rl}</td>
              <td style="color:{rc}">{ps}</td>
            </tr>"""

        detail_blocks += f"""
        <tr id="detail-{pid}" class="detail-row" style="display:none">
          <td colspan="10" style="padding:0 0 12px 24px">
            <table class="dtbl">
              <thead><tr>
                <th>Window</th><th>Hit at</th><th>Side</th>
                <th>Mkt price</th><th>Fill @ limit</th><th>Winner</th>
                <th>Result</th><th>P&amp;L</th>
              </tr></thead>
              <tbody>{trade_rows}</tbody>
            </table>
            <div class="trade-summary">
              {r['n_trades']} trades &nbsp;·&nbsp;
              {r['wins']} wins &nbsp;·&nbsp;
              total cost <b>${r['total_loss'] + sum(t['cost'] for t in r['trades'] if t['won']):.2f}</b> &nbsp;·&nbsp;
              total profit <b style="color:#4caf50">+${r['total_profit']:.2f}</b> &nbsp;·&nbsp;
              net <b style="color:{'#4caf50' if r['net']>0 else '#f44336'}">{'+' if r['net']>0 else ''}{r['net']:.2f}</b>
            </div>
          </td>
        </tr>"""

    # ── Grand totals ──────────────────────────────────────────────────────────
    all_trades   = sum(r["n_trades"]     for r in results)
    all_wins     = sum(r["wins"]         for r in results)
    all_profit   = sum(r["total_profit"] for r in results)
    all_loss     = sum(r["total_loss"]   for r in results)
    all_cost     = sum(
        t["cost"] for r in results for t in r["trades"]
    )
    all_net      = all_profit - all_loss
    all_net_col  = "#4caf50" if all_net > 0 else "#f44336"
    all_net_sign = "+" if all_net > 0 else ""
    all_win_pct  = all_wins / all_trades if all_trades else 0

    totals_row = f"""
        <tr style="border-top:2px solid #2e4057;background:#101820;font-weight:600">
          <td class="left" style="color:#80cbc4">TOTAL</td>
          <td style="text-align:right">{all_trades}</td>
          <td style="text-align:right">{all_wins}</td>
          <td style="text-align:right;color:#80cbc4">{all_win_pct:.0%}</td>
          <td style="text-align:right;color:#546e7a;font-size:11px">0% fees (MM)</td>
          <td style="text-align:right;color:#4caf50">+${all_profit:.2f}</td>
          <td style="text-align:right;color:#f44336">-${all_loss:.2f}</td>
          <td style="text-align:right;color:{all_net_col}">{all_net_sign}{all_net:.2f}</td>
          <td style="text-align:right;color:{all_net_col}">{all_net_sign}{all_net*100:.0f}</td>
          <td></td>
        </tr>
        <tr style="background:#0a1218">
          <td colspan="10" style="padding:8px 12px;font-size:12px;color:#546e7a">
            Total capital deployed: <b style="color:#e0e0e0">${all_cost:.2f}</b>
            &nbsp;·&nbsp;
            Return on deployed: <b style="color:{all_net_col}">{all_net_sign}{(all_net/all_cost*100) if all_cost else 0:.1f}%</b>
            &nbsp;·&nbsp;
            Maker fee: <b style="color:#4caf50">$0.00</b>
          </td>
        </tr>"""

    return f"""
    <div class="results-hdr">
      {sym} &nbsp;·&nbsp; {n_windows} windows &nbsp;·&nbsp;
      {from_label} → {to_label} &nbsp;·&nbsp;
      limits ${lo:.2f} – ${hi:.2f}
    </div>
    <table class="mtbl">
      <thead>
        <tr>
          <th class="left">Limit</th>
          <th>Trades</th><th>Wins</th><th>Win%</th><th>Break-even</th>
          <th>Profit</th><th>Loss</th><th>Net/ct</th><th>Net 100ct</th><th></th>
        </tr>
      </thead>
      <tbody>
        {summary_rows}
        {detail_blocks}
        {totals_row}
      </tbody>
    </table>"""


# ── Page template ─────────────────────────────────────────────────────────────

PAGE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Expire Backtest Viewer</title>
<style>
  *    {{ box-sizing:border-box; }}
  body {{ background:#0e1117; color:#e0e0e0; font-family:monospace; font-size:13px;
          margin:0; padding:20px; }}
  h2   {{ color:#4fc3f7; margin:0 0 16px; font-size:18px; }}

  /* Controls */
  .controls {{ background:#151b27; padding:14px 18px; border-radius:8px;
               margin-bottom:20px; display:flex; flex-wrap:wrap; gap:14px;
               align-items:flex-end; border:1px solid #1e2d3a; }}
  .cg  {{ display:flex; flex-direction:column; gap:4px; }}
  label {{ color:#78909c; font-size:11px; text-transform:uppercase; letter-spacing:.5px; }}
  input, select {{ background:#1e2d3a; color:#e0e0e0; border:1px solid #2e4057;
                   border-radius:4px; padding:6px 10px; font-family:monospace;
                   font-size:13px; outline:none; }}
  input:focus, select:focus {{ border-color:#4fc3f7; }}
  button {{ background:#0277bd; color:white; border:none; border-radius:4px;
            padding:8px 22px; cursor:pointer; font-size:13px; font-weight:600; }}
  button:hover {{ background:#0288d1; }}

  /* Main summary table */
  .mtbl {{ width:100%; border-collapse:collapse; margin-bottom:4px; }}
  .mtbl th {{ background:#131c2b; padding:7px 10px; text-align:right;
              color:#78909c; border-bottom:2px solid #1e2d3a; font-size:11px;
              text-transform:uppercase; }}
  .mtbl th.left {{ text-align:left; }}
  .srow {{ cursor:pointer; transition:background 0.1s; }}
  .srow:hover {{ background:#151f2e; }}
  .srow td {{ padding:5px 10px; border-bottom:1px solid #131c2b; text-align:right; }}
  .srow td.left {{ text-align:left; }}
  .expand-btn {{ color:#546e7a; font-size:11px; }}

  /* Detail table */
  .detail-row td {{ padding:0; }}
  .dtbl {{ width:96%; border-collapse:collapse; background:#0d1520;
           border-radius:6px; margin:4px 0 4px 0; }}
  .dtbl th {{ background:#0a1828; padding:5px 12px; text-align:left;
              color:#4fc3f7; font-size:11px; text-transform:uppercase; }}
  .dtbl td {{ padding:4px 12px; border-bottom:1px solid #131c2b; font-size:12px; }}
  .dtbl tr:last-child td {{ border-bottom:none; }}
  .dtbl tr:hover td {{ background:#0f1e2e; }}

  .trade-summary {{ padding:8px 12px; color:#78909c; font-size:12px; }}
  .results-hdr {{ color:#80cbc4; font-size:12px; margin-bottom:10px;
                  padding:8px 12px; background:#0d1e1e; border-radius:4px; }}
  .error {{ color:#ef5350; padding:20px; }}
  .hint  {{ color:#546e7a; padding:40px 20px; }}
</style>
<script>
var _open = {{}};
function toggle(pid) {{
  var det = document.getElementById('detail-' + pid);
  var btn = document.getElementById('btn-' + pid);
  if (!det) return;
  if (_open[pid]) {{
    det.style.display = 'none';
    btn.textContent = '▶';
    delete _open[pid];
  }} else {{
    det.style.display = '';
    btn.textContent = '▼';
    _open[pid] = true;
  }}
}}
function run() {{
  var sym  = document.getElementById('sym').value;
  var fts  = document.getElementById('from_ts').value;
  var tts  = document.getElementById('to_ts').value;
  var lo   = document.getElementById('lo').value;
  var hi   = document.getElementById('hi').value;
  if (!sym || !fts || !tts || !lo || !hi) return;
  document.getElementById('out').innerHTML =
    '<p style="color:#546e7a;padding:20px">Running…</p>';
  fetch('/query?sym=' + encodeURIComponent(sym) +
        '&from_ts=' + fts + '&to_ts=' + tts +
        '&lo=' + lo + '&hi=' + hi)
    .then(r => r.text())
    .then(h => {{ document.getElementById('out').innerHTML = h; }})
    .catch(e => {{ document.getElementById('out').innerHTML =
      '<p class="error">Error: ' + e + '</p>'; }});
}}
</script>
</head>
<body>
<h2>Expire Backtest Viewer</h2>
<div class="controls">
  <div class="cg">
    <label>Symbol</label>
    <select id="sym">{sym_options}</select>
  </div>
  <div class="cg">
    <label>From window</label>
    <select id="from_ts">{window_options}</select>
  </div>
  <div class="cg">
    <label>To window</label>
    <select id="to_ts">{window_options_rev}</select>
  </div>
  <div class="cg">
    <label>Low limit</label>
    <input type="number" id="lo" min="0.01" max="0.49" step="0.01"
           value="0.10" style="width:75px">
  </div>
  <div class="cg">
    <label>High limit</label>
    <input type="number" id="hi" min="0.01" max="0.49" step="0.01"
           value="0.45" style="width:75px">
  </div>
  <button onclick="run()">&#9654; Run</button>
</div>
<div id="out">
  <p class="hint">Pick parameters above and click Run.</p>
</div>
</body>
</html>"""


# ── HTTP server ───────────────────────────────────────────────────────────────

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass   # silence access log

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/query":
            self._handle_query(params)
        else:
            self._serve_index()

    def _serve_index(self):
        con = sqlite3.connect(DB)
        # Distinct window timestamps that have labeled data with enough ticks
        wrows = con.execute(
            "SELECT DISTINCT window_start_ts FROM windows "
            "WHERE winner IS NOT NULL "
            "AND (SELECT COUNT(*) FROM ticks WHERE window_id=windows.id) > 100 "
            "ORDER BY window_start_ts"
        ).fetchall()
        con.close()

        timestamps = [r[0] for r in wrows]

        def make_options(ts_list):
            opts = ""
            for ts in ts_list:
                label = datetime.fromtimestamp(ts).strftime("%b %d  %H:%M")
                opts += f'<option value="{ts}">{label}</option>\n'
            return opts

        sym_options      = "\n".join(f'<option value="{s}">{s}</option>' for s in SYMBOLS)
        window_options     = make_options(timestamps)           # oldest first (From)
        window_options_rev = make_options(reversed(timestamps)) # newest first (To)

        html = PAGE.format(
            sym_options=sym_options,
            window_options=window_options,
            window_options_rev=window_options_rev,
        )
        self._respond(html)

    def _handle_query(self, params):
        try:
            sym     = params.get("sym",     ["BTC"])[0]
            from_ts = int(params.get("from_ts", [0])[0])
            to_ts   = int(params.get("to_ts",   [9999999999])[0])
            lo      = float(params.get("lo", ["0.10"])[0])
            hi      = float(params.get("hi", ["0.45"])[0])
            prefix  = SYMBOLS.get(sym, "KXBTC15M")

            results, n_windows = run_backtest(sym, prefix, from_ts, to_ts, lo, hi)

            if not results:
                self._respond(
                    "<p class='error'>No labeled windows found in that range.</p>"
                )
                return

            from_label = datetime.fromtimestamp(from_ts).strftime("%b %d %H:%M")
            to_label   = datetime.fromtimestamp(to_ts).strftime("%b %d %H:%M")
            html = render_results(sym, results, n_windows, from_label, to_label, lo, hi)
            self._respond(html)

        except Exception as e:
            self._respond(
                f"<p class='error'>{e}<br><pre>{traceback.format_exc()}</pre></p>"
            )

    def _respond(self, html: str):
        data = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    import threading
    server = http.server.HTTPServer(("", PORT), Handler)
    url = f"http://localhost:{PORT}"
    print(f"Expire Backtest Viewer  →  {url}")
    threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    server.serve_forever()


if __name__ == "__main__":
    main()
