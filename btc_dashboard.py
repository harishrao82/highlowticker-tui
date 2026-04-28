#!/usr/bin/env python3
"""
Paper trader dashboard — http://localhost:7331

Reads ~/.btc_strategy_state.json and ~/.btc_strategy_improvements.json
and serves a live-updating web UI.

Run: python3 btc_dashboard.py
"""
import json
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn

STATE_FILE   = Path.home() / ".btc_strategy_state.json"
IMPROVE_FILE = Path.home() / ".btc_strategy_improvements.json"

app = FastAPI()


def _load(path: Path) -> dict | list:
    try:
        return json.loads(path.read_text()) if path.exists() else {}
    except Exception:
        return {}


def _pnl_class(v: float) -> str:
    return "pos" if v >= 0 else "neg"


def _fmt_pnl(v: float) -> str:
    return f"{'+'if v>=0 else ''}{v:.2f}"


def _wl(title: str, slug: str = "") -> str:
    a = title.find("April"); et = title.find("ET")
    return title[a+6:et+2].strip() if a >= 0 else slug[-12:]


@app.get("/", response_class=HTMLResponse)
def dashboard():
    state   = _load(STATE_FILE)
    improve = _load(IMPROVE_FILE)

    positions  = state.get("positions", {})
    shadow_pos = state.get("shadow", {})
    session_pnl = state.get("session_pnl", 0.0)
    shadow_pnl  = state.get("shadow_pnl",  0.0)
    trade_count = state.get("trade_count",  0)
    saved_at    = state.get("saved_at", "—")

    open_pos   = [(s, p) for s, p in positions.items() if not p.get("resolved")]
    closed_pos = [(s, p) for s, p in positions.items() if p.get("resolved")]
    open_pos.sort(key=lambda x: x[1].get("window_end", 0))
    closed_pos.sort(key=lambda x: x[1].get("window_end", 0), reverse=True)

    now = datetime.now().strftime("%H:%M:%S")

    # ── Open positions ─────────────────────────────────────────────────────────
    def open_rows():
        if not open_pos:
            return "<tr><td colspan='11' class='dim'>No open positions</td></tr>"
        rows = []
        for slug, p in open_pos:
            coin   = p.get("coin", "?")
            title  = p.get("title", slug)
            ph     = "Ph3" if p.get("phase3_done") else "Ph2" if p.get("phase2_done") else "Ph1" if p.get("phase1_done") else "Ph1"
            secs   = int(p.get("window_end", 0) - datetime.now().timestamp())
            secs_s = f"{secs}s" if secs > 0 else f"<span class='neg'>{secs}s</span>"
            su = p.get("spent_up", 0);   sd = p.get("spent_down", 0)
            shu = p.get("shares_up", 0); shd = p.get("shares_down", 0)
            ask_up = p.get("ask_up", 0); ask_dn = p.get("ask_dn", 0)
            mkt    = shu * ask_up + shd * ask_dn
            cost   = su + sd
            unreal = mkt - cost
            uc     = _pnl_class(unreal)
            avg_u  = su / shu if shu else 0
            avg_d  = sd / shd if shd else 0
            open_p = p.get("open_price", 0)

            # bot open activity on same slug
            bot = shadow_pos.get(slug, {})
            bot_trades = bot.get("trades", [])
            bot_su = bot.get("spent_up", 0);   bot_sd = bot.get("spent_down", 0)
            bot_shu = bot.get("shares_up", 0); bot_shd = bot.get("shares_down", 0)
            bot_avg_u = bot_su / bot_shu if bot_shu else 0
            bot_avg_d = bot_sd / bot_shd if bot_shd else 0
            bot_cell = f"""<span class='up'>{bot_shu:.0f}sh@{bot_avg_u:.3f}</span> /
                           <span class='dn'>{bot_shd:.0f}sh@{bot_avg_d:.3f}</span>
                           <small>({len(bot_trades)} trades)</small>""" if bot_trades else "<span class='dim'>not trading</span>"

            rows.append(f"""
            <tr style='cursor:pointer' onclick="window.location='/window/{slug}'">
              <td><b>{coin}</b></td>
              <td class='dim'>{_wl(title, slug)}</td>
              <td>{ph}</td>
              <td>${open_p:,.2f}</td>
              <td class='up'>{shu:.0f}sh<br><small>avg {avg_u:.3f} now {ask_up:.3f}</small></td>
              <td class='dn'>{shd:.0f}sh<br><small>avg {avg_d:.3f} now {ask_dn:.3f}</small></td>
              <td>${cost:.0f} → <b>${mkt:.0f}</b><br><span class='{uc}'>{'+' if unreal>=0 else ''}{unreal:.0f}</span></td>
              <td>{bot_cell}</td>
              <td>{secs_s}</td>
            </tr>""")
        return "\n".join(rows)

    # ── Closed positions with bot comparison ───────────────────────────────────
    def closed_rows():
        if not closed_pos:
            return "<tr><td colspan='1' class='dim'>No closed positions yet</td></tr>"
        rows = []
        for slug, p in closed_pos[:20]:
            coin   = p.get("coin", "?")
            title  = p.get("title", slug)
            winner = p.get("winner", "?")
            pnl    = p.get("pnl", 0)
            pc     = _pnl_class(pnl)
            su     = p.get("spent_up", 0);   sd = p.get("spent_down", 0)
            shu    = p.get("shares_up", 0);  shd = p.get("shares_down", 0)
            avg_u  = su / shu if shu else 0
            avg_d  = sd / shd if shd else 0
            open_p = p.get("open_price", 0)
            end_p  = p.get("end_price", 0)
            moved  = "↑" if end_p >= open_p else "↓"
            pct    = abs((end_p - open_p) / open_p * 100) if open_p else 0
            our_trades = p.get("trades", [])

            # bot data
            bot    = shadow_pos.get(slug, {})
            bot_pnl   = bot.get("pnl")
            bot_trades = bot.get("trades", [])
            bot_su = bot.get("spent_up", 0);   bot_sd = bot.get("spent_down", 0)
            bot_shu = bot.get("shares_up", 0); bot_shd = bot.get("shares_down", 0)
            bot_avg_u = bot_su / bot_shu if bot_shu else 0
            bot_avg_d = bot_sd / bot_shd if bot_shd else 0

            # build bot timeline
            def bot_timeline():
                if not bot_trades:
                    return "<span class='dim'>No bot activity on this window</span>"
                lines = []
                for t in sorted(bot_trades, key=lambda x: x.get("time",""))[:20]:
                    side  = t.get("side", "?")
                    price = t.get("price", 0)
                    cost  = t.get("cost", 0)
                    tm    = t.get("time", "?")
                    sc    = "up" if side == "Up" else "dn"
                    lines.append(f"<span class='dim'>{tm}</span> <span class='{sc}'>{side}</span> @{price:.3f} ${cost:.0f}")
                more = len(bot_trades) - 20
                if more > 0:
                    lines.append(f"<span class='dim'>... +{more} more trades</span>")
                return "<br>".join(lines)

            # our trade timeline
            def our_timeline():
                if not our_trades:
                    return "<span class='dim'>—</span>"
                lines = []
                for t in our_trades:
                    side  = t.get("side","?")
                    price = t.get("price",0)
                    cost  = t.get("cost",0)
                    tm    = t.get("time","?")
                    reason= t.get("reason","")
                    sc    = "up" if side=="Up" else "dn"
                    lines.append(f"<span class='dim'>{tm}</span> <span class='{sc}'>{side}</span> @{price:.3f} ${cost:.0f} <span class='dim'>{reason}</span>")
                return "<br>".join(lines)

            w_cls = "up" if winner == "Up" else "dn"
            bot_pnl_html = f"<span class='{_pnl_class(bot_pnl)}'><b>{_fmt_pnl(bot_pnl)}</b></span>" \
                           if bot_pnl is not None else "<span class='dim'>—</span>"

            rows.append(f"""
            <tr class='window-row'>
              <td colspan='1'>
                <div class='window-card'>
                  <div class='window-header'>
                    <span class='coin-badge'>{coin}</span>
                    <a href='/window/{slug}' style='color:#e0e0e0;text-decoration:none;font-weight:bold'>{_wl(title, slug)}</a>
                    <span class='winner-badge {w_cls}'>▶ {winner} won</span>
                    <span class='dim'>${open_p:,.2f} {moved} ${end_p:,.2f} ({pct:.3f}%)</span>
                    <a href='/window/{slug}' style='color:#555;font-size:11px'>view trades →</a>
                  </div>
                  <div class='compare-grid'>
                    <div class='compare-col'>
                      <div class='compare-label'>★ Our Paper</div>
                      <div class='compare-stats'>
                        <span class='up'>{shu:.0f}sh @ avg {avg_u:.3f}</span> Up &nbsp;|&nbsp;
                        <span class='dn'>{shd:.0f}sh @ avg {avg_d:.3f}</span> Down<br>
                        Spent <b>${su+sd:.0f}</b> &nbsp; P&L <span class='{pc}'><b>{_fmt_pnl(pnl)}</b></span>
                        &nbsp; ({len(our_trades)} trades)
                      </div>
                      <div class='timeline'>{our_timeline()}</div>
                    </div>
                    <div class='compare-col'>
                      <div class='compare-label'>◆ Idolized-Scallops</div>
                      <div class='compare-stats'>
                        <span class='up'>{bot_shu:.0f}sh @ avg {bot_avg_u:.3f}</span> Up &nbsp;|&nbsp;
                        <span class='dn'>{bot_shd:.0f}sh @ avg {bot_avg_d:.3f}</span> Down<br>
                        Spent <b>${bot_su+bot_sd:.0f}</b> &nbsp; P&L {bot_pnl_html}
                        &nbsp; ({len(bot_trades)} trades)
                      </div>
                      <div class='timeline'>{bot_timeline()}</div>
                    </div>
                  </div>
                </div>
              </td>
            </tr>""")
        return "\n".join(rows)

    # ── Strategy stats ─────────────────────────────────────────────────────────
    def strategy_stats():
        if not improve:
            return "<p class='dim'>No resolved windows yet.</p>"
        wins  = [e for e in improve if e.get("our_pnl", 0) > 0]
        loss  = [e for e in improve if e.get("our_pnl", 0) <= 0]
        ph1_correct = [e for e in improve if e.get("phase1_side") == e.get("winner")]
        total_pnl   = sum(e.get("our_pnl", 0) for e in improve)
        bot_total   = sum(e.get("bot_pnl", 0) for e in improve if e.get("bot_pnl") is not None)
        avg_win  = sum(e["our_pnl"] for e in wins) / len(wins)   if wins else 0
        avg_loss = sum(e["our_pnl"] for e in loss) / len(loss)   if loss else 0
        return f"""
        <div class='stats-grid'>
          <div class='stat'><div class='stat-val {'pos' if total_pnl>=0 else 'neg'}'>${total_pnl:+.2f}</div><div class='stat-label'>Total P&L (us)</div></div>
          <div class='stat'><div class='stat-val {'pos' if bot_total>=0 else 'neg'}'>${bot_total:+.2f}</div><div class='stat-label'>Total P&L (bot)</div></div>
          <div class='stat'><div class='stat-val'>{len(wins)}/{len(improve)}</div><div class='stat-label'>Win rate</div></div>
          <div class='stat'><div class='stat-val pos'>${avg_win:.2f}</div><div class='stat-label'>Avg win</div></div>
          <div class='stat'><div class='stat-val neg'>${avg_loss:.2f}</div><div class='stat-label'>Avg loss</div></div>
          <div class='stat'><div class='stat-val'>{len(ph1_correct)}/{len(improve)}</div><div class='stat-label'>Ph1 correct</div></div>
        </div>"""

    # ── Improvement log ────────────────────────────────────────────────────────
    def improve_rows():
        if not improve:
            return "<tr><td colspan='10' class='dim'>No data yet</td></tr>"
        rows = []
        for e in reversed(improve[-30:]):
            pnl    = e.get("our_pnl", 0)
            bpnl   = e.get("bot_pnl")
            pc     = _pnl_class(pnl)
            bc     = _pnl_class(bpnl) if bpnl is not None else ""
            winner = e.get("winner", "?")
            ph1    = e.get("phase1_side", "?")
            correct = ph1 == winner
            delta  = (pnl - bpnl) if bpnl is not None else None
            dc     = _pnl_class(delta) if delta is not None else ""
            rows.append(f"""
            <tr>
              <td><b>{e.get('coin','?')}</b></td>
              <td class='dim'>{_wl(e.get('title','?'))}</td>
              <td class='{'up' if winner=='Up' else 'dn'}'>{winner}</td>
              <td class='{'pos' if correct else 'neg'}'>{ph1} {'✓' if correct else '✗'}</td>
              <td class='up'>{e.get('our_shares_up',0):.0f}sh@{e.get('our_up_avg',0):.3f}</td>
              <td class='dn'>{e.get('our_shares_dn',0):.0f}sh@{e.get('our_dn_avg',0):.3f}</td>
              <td class='up'>{e.get('bot_shares_up') or 0:.0f}sh@{e.get('bot_up_avg') or 0:.3f}</td>
              <td class='dn'>{e.get('bot_shares_dn') or 0:.0f}sh@{e.get('bot_dn_avg') or 0:.3f}</td>
              <td class='{pc}'><b>{_fmt_pnl(pnl)}</b></td>
              <td class='{bc}'>{_fmt_pnl(bpnl) if bpnl is not None else '—'}</td>
              <td class='{dc}'>{''+_fmt_pnl(delta) if delta is not None else '—'}</td>
              <td class='dim'>{e.get('our_trades',0)}/{e.get('bot_trades','—')}</td>
              <td class='dim'>{e.get('timestamp','?')[11:16]}</td>
            </tr>""")
        return "\n".join(rows)

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Paper Trader Dashboard</title>
<meta http-equiv="refresh" content="10">
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0a0a0f; color: #e0e0e0; font-family: 'SF Mono','Fira Code',monospace; font-size: 13px; padding: 16px; }}
  h1 {{ color: #fbbf24; font-size: 18px; margin-bottom: 4px; }}
  h2 {{ color: #60a5fa; font-size: 14px; margin: 24px 0 8px; border-bottom: 1px solid #1e1e3a; padding-bottom: 4px; }}
  .meta {{ color: #555; font-size: 11px; margin-bottom: 16px; }}
  table {{ width: 100%; border-collapse: collapse; margin-bottom: 8px; }}
  th {{ background: #111128; color: #60a5fa; text-align: left; padding: 6px 10px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; }}
  td {{ padding: 5px 10px; border-bottom: 1px solid #111; vertical-align: top; }}
  tr:hover td {{ background: #0d0d1a; }}
  small {{ color: #666; font-size: 11px; }}
  .up {{ color: #4ade80; }}
  .dn {{ color: #f87171; }}
  .pos {{ color: #4ade80; }}
  .neg {{ color: #f87171; }}
  .dim {{ color: #555; }}
  .header-bar {{ display: flex; gap: 24px; align-items: baseline; margin-bottom: 20px; flex-wrap: wrap; }}
  .kpi {{ background: #111128; border: 1px solid #1e1e3a; border-radius: 6px; padding: 10px 18px; }}
  .kpi-val {{ font-size: 22px; font-weight: bold; }}
  .kpi-label {{ color: #555; font-size: 11px; margin-top: 2px; }}
  .stats-grid {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 12px; }}
  .stat {{ background: #111128; border: 1px solid #1e1e3a; border-radius: 6px; padding: 10px 16px; min-width: 120px; }}
  .stat-val {{ font-size: 18px; font-weight: bold; }}
  .stat-label {{ color: #555; font-size: 11px; margin-top: 2px; }}

  /* Window cards for closed positions */
  .window-card {{ background: #0e0e1c; border: 1px solid #1e1e3a; border-radius: 8px; padding: 12px 16px; margin-bottom: 10px; }}
  .window-header {{ display: flex; align-items: center; gap: 16px; margin-bottom: 10px; flex-wrap: wrap; }}
  .coin-badge {{ background: #1e1e3a; color: #fbbf24; font-weight: bold; padding: 2px 8px; border-radius: 4px; }}
  .winner-badge {{ font-weight: bold; padding: 2px 8px; border-radius: 4px; }}
  .winner-badge.up {{ background: #052e16; color: #4ade80; }}
  .winner-badge.dn {{ background: #450a0a; color: #f87171; }}
  .compare-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  .compare-col {{ background: #0a0a14; border: 1px solid #1a1a2e; border-radius: 6px; padding: 10px 12px; }}
  .compare-label {{ color: #60a5fa; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 6px; font-weight: bold; }}
  .compare-stats {{ margin-bottom: 8px; line-height: 1.6; }}
  .timeline {{ font-size: 11px; line-height: 1.8; color: #aaa; border-top: 1px solid #1a1a2e; padding-top: 6px; margin-top: 4px; max-height: 180px; overflow-y: auto; }}
  .window-row td {{ padding: 4px 0; border: none; }}
</style>
</head>
<body>
<h1>★ BTC/ETH/SOL/XRP Paper Trader vs Idolized-Scallops</h1>
<div class="meta">State: {saved_at}  │  {now}  │  Auto-refresh 10s</div>

<div class="header-bar">
  <div class="kpi"><div class="kpi-val {'pos' if session_pnl>=0 else 'neg'}">{_fmt_pnl(session_pnl)}</div><div class="kpi-label">Realized P&L (us)</div></div>
  <div class="kpi"><div class="kpi-val {'pos' if shadow_pnl>=0 else 'neg'}">{_fmt_pnl(shadow_pnl)}</div><div class="kpi-label">Realized P&L (bot)</div></div>
  <div class="kpi"><div class="kpi-val">{len(open_pos)}</div><div class="kpi-label">Open windows</div></div>
  <div class="kpi"><div class="kpi-val">{len(closed_pos)}</div><div class="kpi-label">Closed</div></div>
  <div class="kpi"><div class="kpi-val">{trade_count}</div><div class="kpi-label">Our trades</div></div>
</div>

<h2>Open Positions</h2>
<table>
  <tr>
    <th>Coin</th><th>Window</th><th>Phase</th><th>Open $</th>
    <th>Up shares</th><th>Down shares</th>
    <th>Cost → Val / Unreal</th><th>Bot activity</th><th>Time left</th>
  </tr>
  {open_rows()}
</table>

<h2>Strategy Stats</h2>
{strategy_stats()}

<h2>Closed Windows — Us vs Bot (last 20)</h2>
<table><tr><td style="padding:0;border:none;">
{closed_rows()}
</td></tr></table>

<h2>Improvement Log (last 30)</h2>
<table>
  <tr>
    <th>Coin</th><th>Window</th><th>Winner</th><th>Ph1</th>
    <th>Our Up</th><th>Our Dn</th>
    <th>Bot Up</th><th>Bot Dn</th>
    <th>Our P&L</th><th>Bot P&L</th><th>Delta</th>
    <th>Trades</th><th>Time</th>
  </tr>
  {improve_rows()}
</table>

</body>
</html>"""
    return HTMLResponse(html)


@app.get("/window/{slug}", response_class=HTMLResponse)
def window_detail(slug: str):
    state = _load(STATE_FILE)
    positions  = state.get("positions", {})
    shadow_pos = state.get("shadow", {})

    p = positions.get(slug)
    if not p:
        return HTMLResponse(f"<h2>Window not found: {slug}</h2>", status_code=404)

    coin     = p.get("coin", "?")
    title    = p.get("title", slug)
    winner   = p.get("winner", "?")
    pnl      = p.get("pnl", 0)
    open_p   = p.get("open_price", 0)
    end_p    = p.get("end_price", 0)
    our_trades = p.get("trades", [])
    su = p.get("spent_up", 0); sd = p.get("spent_down", 0)
    shu = p.get("shares_up", 0); shd = p.get("shares_down", 0)
    avg_u = su / shu if shu else 0
    avg_d = sd / shd if shd else 0

    bot = shadow_pos.get(slug, {})
    bot_trades = bot.get("trades", [])
    bot_su = bot.get("spent_up", 0); bot_sd = bot.get("spent_down", 0)
    bot_shu = bot.get("shares_up", 0); bot_shd = bot.get("shares_down", 0)
    bot_pnl = bot.get("pnl")
    bot_avg_u = bot_su / bot_shu if bot_shu else 0
    bot_avg_d = bot_sd / bot_shd if bot_shd else 0

    def trade_rows(trades, is_bot=False):
        if not trades:
            return "<tr><td colspan='5' style='color:#555'>No trades</td></tr>"
        rows = []
        running_up = 0; running_dn = 0
        for i, t in enumerate(sorted(trades, key=lambda x: x.get("time", "")), 1):
            side       = t.get("side", "?")
            price      = t.get("price", 0)
            cost       = t.get("cost", 0)
            shares     = t.get("shares", cost / price if price else 0)
            tm         = t.get("time", "?")
            fetched_at = t.get("fetched_at", "")
            reason     = t.get("reason", "") if not is_bot else ""
            sc         = "up" if side == "Up" else "dn"
            if side == "Up":
                running_up += cost
            else:
                running_dn += cost
            if is_bot and fetched_at:
                time_cell = f"<td style='color:#888'>{tm}<br><span style='color:#555;font-size:10px'>got {fetched_at}</span></td>"
            else:
                time_cell = f"<td style='color:#888'>{tm}</td>"
            reason_cell = f"<td style='color:#555;font-size:11px'>{reason}</td>" if not is_bot else ""
            rows.append(f"""<tr>
              <td style='color:#555'>{i}</td>
              {time_cell}
              <td class='{sc}'><b>{side}</b></td>
              <td>@<b>{price:.3f}</b></td>
              <td>${cost:.0f} ({shares:.1f}sh)</td>
              <td style='color:#555;font-size:11px'>↑${running_up:.0f} ↓${running_dn:.0f}</td>
              {reason_cell}
            </tr>""")
        return "\n".join(rows)

    w_cls = "up" if winner == "Up" else "dn"
    pnl_cls = _pnl_class(pnl)
    bot_pnl_html = f"<span class='{_pnl_class(bot_pnl)}'><b>{_fmt_pnl(bot_pnl)}</b></span>" if bot_pnl is not None else "—"
    pct = abs((end_p - open_p) / open_p * 100) if open_p else 0
    moved = "↑" if end_p >= open_p else "↓"
    reason_th = "<th>Reason</th>" if our_trades else ""

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{coin} {_wl(title, slug)}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0a0a0f; color: #e0e0e0; font-family: 'SF Mono','Fira Code',monospace; font-size: 13px; padding: 20px; max-width: 1100px; margin: 0 auto; }}
  h1 {{ color: #fbbf24; font-size: 16px; margin-bottom: 4px; }}
  h2 {{ color: #60a5fa; font-size: 13px; margin: 20px 0 8px; border-bottom: 1px solid #1e1e3a; padding-bottom: 4px; }}
  a {{ color: #60a5fa; text-decoration: none; font-size: 12px; }}
  a:hover {{ text-decoration: underline; }}
  table {{ width: 100%; border-collapse: collapse; margin-bottom: 8px; }}
  th {{ background: #111128; color: #60a5fa; text-align: left; padding: 6px 10px; font-size: 11px; text-transform: uppercase; }}
  td {{ padding: 6px 10px; border-bottom: 1px solid #111; }}
  tr:hover td {{ background: #0d0d1a; }}
  .up {{ color: #4ade80; }} .dn {{ color: #f87171; }}
  .pos {{ color: #4ade80; }} .neg {{ color: #f87171; }}
  .summary-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin: 16px 0; }}
  .panel {{ background: #0e0e1c; border: 1px solid #1e1e3a; border-radius: 8px; padding: 14px 16px; }}
  .panel-title {{ color: #60a5fa; font-size: 11px; text-transform: uppercase; margin-bottom: 10px; font-weight: bold; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-weight: bold; }}
  .badge.up {{ background: #052e16; color: #4ade80; }}
  .badge.dn {{ background: #450a0a; color: #f87171; }}
</style>
</head>
<body>
<a href="/">← Back to dashboard</a>
<h1 style="margin-top:12px">{coin} · {title}</h1>
<p style="color:#555;font-size:11px;margin:4px 0 16px">
  Open ${open_p:,.2f} {moved} Close ${end_p:,.2f} ({pct:.3f}% move) &nbsp;│&nbsp;
  Winner: <span class='{w_cls}'><b>{winner}</b></span>
</p>

<div class="summary-grid">
  <div class="panel">
    <div class="panel-title">★ Our Paper</div>
    <span class='up'>{shu:.0f}sh @ avg {avg_u:.3f}</span> Up &nbsp;|&nbsp;
    <span class='dn'>{shd:.0f}sh @ avg {avg_d:.3f}</span> Down<br>
    <span style="margin-top:6px;display:inline-block">
      Spent <b>${su+sd:.0f}</b> &nbsp; P&L <span class='{pnl_cls}'><b>{_fmt_pnl(pnl)}</b></span>
      &nbsp; {len(our_trades)} trades
    </span>
  </div>
  <div class="panel">
    <div class="panel-title">◆ Idolized-Scallops</div>
    <span class='up'>{bot_shu:.0f}sh @ avg {bot_avg_u:.3f}</span> Up &nbsp;|&nbsp;
    <span class='dn'>{bot_shd:.0f}sh @ avg {bot_avg_d:.3f}</span> Down<br>
    <span style="margin-top:6px;display:inline-block">
      Spent <b>${bot_su+bot_sd:.0f}</b> &nbsp; P&L {bot_pnl_html}
      &nbsp; {len(bot_trades)} trades
    </span>
  </div>
</div>

<h2>★ Our Trades — step by step</h2>
<table>
  <tr><th>#</th><th>Time</th><th>Side</th><th>Price</th><th>Cost (shares)</th><th>Running total</th>{reason_th}</tr>
  {trade_rows(our_trades, is_bot=False)}
</table>

<h2>◆ Idolized-Scallops Trades</h2>
<table>
  <tr><th>#</th><th>Time</th><th>Side</th><th>Price</th><th>Cost (shares)</th><th>Running total</th></tr>
  {trade_rows(bot_trades, is_bot=True)}
</table>
</body>
</html>"""
    return HTMLResponse(html)


@app.get("/state")
def state_json():
    return _load(STATE_FILE)


@app.get("/log")
def log_json():
    return _load(IMPROVE_FILE)


if __name__ == "__main__":
    print("Dashboard → http://localhost:7331")
    uvicorn.run(app, host="0.0.0.0", port=7331, log_level="warning")
