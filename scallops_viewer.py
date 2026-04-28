#!/usr/bin/env python3
"""
scallops_viewer.py — web viewer for Idolized-Scallops trades.

Reads ~/.scallops_live_trades.jsonl, groups by coin → window,
resolves winners from btc_windows.db.

Run:   python3 scallops_viewer.py
Open:  http://localhost:7332
"""
import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn

LOG = Path.home() / ".scallops_live_trades.jsonl"
DB  = Path.home() / ".btc_windows.db"
ET  = timezone(timedelta(hours=-4))

app = FastAPI()


def _ticker_for(coin: str, wts: int) -> str:
    close_dt = datetime.fromtimestamp(wts + 900, tz=ET)
    return (f"KX{coin}15M-"
            + close_dt.strftime("%y%b%d%H%M").upper()
            + "-" + close_dt.strftime("%M"))


def _load_winners() -> dict[str, str]:
    winners = {}
    if not DB.exists():
        return winners
    try:
        db = sqlite3.connect(DB)
        for ticker, winner in db.execute(
            "SELECT ticker, winner FROM windows WHERE winner IS NOT NULL"
        ):
            winners[ticker] = winner
        db.close()
    except Exception:
        pass
    return winners


def _fmt_pnl(v: float) -> str:
    return f"{'+'if v>=0 else ''}{v:,.0f}"


def _pnl_class(v: float) -> str:
    return "pos" if v >= 0 else "neg"


@app.get("/", response_class=HTMLResponse)
def index(hours: int = 12, coin: str = ""):
    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
    winners = _load_winners()
    db = sqlite3.connect(DB) if DB.exists() else None

    trades = []
    if LOG.exists():
        with open(LOG) as f:
            for line in f:
                try:
                    t = json.loads(line)
                    # Only 15-minute markets
                    if "-15m-" not in t.get("slug", ""):
                        continue
                    if t.get("window_start_ts", 0) >= cutoff:
                        if not coin or t.get("coin") == coin:
                            trades.append(t)
                except Exception:
                    continue

    # Group by coin → window
    by_coin = defaultdict(lambda: defaultdict(list))
    for t in trades:
        by_coin[t.get("coin", "?")][t.get("window_start_ts", 0)].append(t)

    # Build per-coin summaries
    coin_stats = {}
    all_windows = []
    total_cost = 0
    total_pnl = 0
    total_wins = 0
    total_resolved = 0

    for c in sorted(by_coin):
        c_cost = 0
        c_pnl = 0
        c_wins = 0
        c_resolved = 0
        c_windows = []

        for wts in sorted(by_coin[c], reverse=True):
            wt = by_coin[c][wts]
            ticker = _ticker_for(c, wts)
            winner_raw = winners.get(ticker)
            winner = "Up" if winner_raw == "yes" else "Down" if winner_raw == "no" else None

            up_cost = sum(t["notional"] for t in wt if t["outcome"] == "Up" and t["side"] == "BUY")
            dn_cost = sum(t["notional"] for t in wt if t["outcome"] == "Down" and t["side"] == "BUY")
            up_shares = sum(t["size"] for t in wt if t["outcome"] == "Up" and t["side"] == "BUY")
            dn_shares = sum(t["size"] for t in wt if t["outcome"] == "Down" and t["side"] == "BUY")
            cost = up_cost + dn_cost
            lean = "Up" if up_cost > dn_cost else "Down"
            n = len(wt)

            pnl = None
            match = None
            if winner:
                payout = up_shares if winner == "Up" else dn_shares
                pnl = payout - cost
                match = lean == winner
                c_pnl += pnl
                c_resolved += 1
                if match:
                    c_wins += 1
            c_cost += cost

            up_avg = up_cost / up_shares if up_shares else 0
            dn_avg = dn_cost / dn_shares if dn_shares else 0

            dt = datetime.fromtimestamp(wts, tz=ET)
            w = dict(
                dt=dt, coin=c, wts=wts, ticker=ticker, n=n, cost=cost,
                up_cost=up_cost, dn_cost=dn_cost,
                up_shares=up_shares, dn_shares=dn_shares,
                up_avg=up_avg, dn_avg=dn_avg,
                lean=lean, winner=winner, match=match, pnl=pnl,
                trades=sorted(wt, key=lambda t: t.get("elapsed_in_window", 0)),
            )
            c_windows.append(w)
            all_windows.append(w)

        coin_stats[c] = dict(cost=c_cost, pnl=c_pnl, wins=c_wins,
                             resolved=c_resolved, windows=len(c_windows))
        total_cost += c_cost
        total_pnl += c_pnl
        total_wins += c_wins
        total_resolved += c_resolved

    # ── KPIs ──
    lean_pct = f"{total_wins}/{total_resolved}" if total_resolved else "—"
    lean_rate = f"{total_wins/total_resolved*100:.0f}%" if total_resolved else ""
    ret = f"{total_pnl/total_cost*100:+.1f}%" if total_cost else "—"

    # ── Coin tabs ──
    coin_tabs = [f"<a href='/?hours={hours}' class='{'active' if not coin else ''}'>All</a>"]
    for c in sorted(by_coin):
        cs = coin_stats[c]
        pc = _pnl_class(cs['pnl'])
        coin_tabs.append(
            f"<a href='/?hours={hours}&coin={c}' class='{'active' if coin==c else ''}'>"
            f"{c} <span class='{pc}'>${cs['pnl']:+,.0f}</span></a>"
        )

    # ── Coin summary cards ──
    coin_cards = []
    for c in sorted(by_coin):
        cs = coin_stats[c]
        pc = _pnl_class(cs['pnl'])
        wr = f"{cs['wins']}/{cs['resolved']}" if cs['resolved'] else "—"
        rt = f"{cs['pnl']/cs['cost']*100:+.1f}%" if cs['cost'] else "—"
        coin_cards.append(f"""
        <div class='coin-card'>
          <div class='coin-name'>{c}</div>
          <div class='coin-stat'><span class='{pc}'>${cs['pnl']:+,.0f}</span></div>
          <div class='coin-detail'>${cs['cost']:,.0f} deployed · {wr} lean · {rt}</div>
        </div>""")

    # ── Window cards (grouped by coin) ──
    def window_card(w):
        if w['pnl'] is not None:
            pnl_str = f"${w['pnl']:+,.2f}"
            ret_pct = (w['pnl'] / w['cost'] * 100) if w['cost'] else 0
            ret_str = f"({ret_pct:+.1f}%)"
        else:
            pnl_str = "..."
            ret_str = ""
        pnl_cls = _pnl_class(w['pnl']) if w['pnl'] is not None else "pend"
        w_cls = "up" if w['winner'] == "Up" else "dn" if w['winner'] == "Down" else ""
        lean_cls = "up" if w['lean'] == "Up" else "dn"
        match_str = "✓" if w['match'] else "✗" if w['match'] is False else "..."
        match_cls = "pos" if w['match'] else "neg" if w['match'] is False else "pend"
        hedge_pct = min(w['up_cost'], w['dn_cost']) / w['cost'] * 100 if w['cost'] else 0
        # Max delta seen during the window (how far price moved at its peak)
        deltas = [t.get('delta_pct') for t in w['trades'] if t.get('delta_pct') is not None]
        final_delta = w['trades'][-1].get('delta_pct') if w['trades'] else None
        max_abs_delta = max((abs(d) for d in deltas), default=None)
        delta_str = ""
        if final_delta is not None:
            dc = "up" if final_delta > 0 else "dn" if final_delta < 0 else "dim"
            delta_str = f"<span class='{dc}'>Δfin{final_delta:+.3f}%</span>"
            if max_abs_delta is not None and max_abs_delta > abs(final_delta or 0):
                delta_str += f" <span class='dim'>|max {max_abs_delta:.3f}%</span>"

        # Trade timeline with delta at trade time
        trade_lines = []
        for t in w['trades'][:40]:
            sc = "up" if t["outcome"] == "Up" else "dn"
            elapsed = t.get("elapsed_in_window", 0)
            # delta_pct is the coin's % change from window open at trade time
            dpct = t.get("delta_pct")
            if dpct is not None:
                d_col = "up" if dpct > 0 else "dn" if dpct < 0 else "dim"
                d_str = f"<span class='{d_col}'>Δ{dpct:+.3f}%</span>"
            else:
                d_str = "<span class='dim'>Δ—</span>"

            # Flag trades going against coin direction (contrarian)
            contra = ""
            if dpct is not None:
                if (t['outcome']=='Up' and dpct < -0.02) or (t['outcome']=='Down' and dpct > 0.02):
                    contra = " <span style='color:#fbbf24'>◆</span>"

            trade_lines.append(
                f"<span class='dim'>{elapsed:>3}s</span> "
                f"{d_str} "
                f"<span class='{sc}'>{t['side']} {t['outcome']}</span> "
                f"@{t['price']:.3f} ${t['notional']:.0f}{contra}"
            )
        more = len(w['trades']) - 40
        if more > 0:
            trade_lines.append(f"<span class='dim'>... +{more} more</span>")
        timeline = "<br>".join(trade_lines)

        return f"""
        <div class='window-card'>
          <div class='window-header'>
            <span class='coin-badge'>{w['coin']}</span>
            <span style='font-weight:bold'>{w['dt'].strftime('%H:%M ET')}</span>
            <span class='winner-badge {w_cls}'>▶ {w['winner'] or '...'}</span>
            <span class='{match_cls}' style='font-weight:bold'>{match_str}</span>
            <span class='pnl-badge {pnl_cls}'>{pnl_str} <small>{ret_str}</small></span>
            {delta_str}
            <span class='dim'>${w['cost']:,.0f} · {w['n']} trades · hedge {hedge_pct:.0f}%</span>
          </div>
          <div class='window-body'>
            <div class='pos-summary'>
              <span class='up'>{w['up_shares']:.0f}sh @ {w['up_avg']:.3f}</span> Up ${w['up_cost']:,.0f}
              &nbsp;│&nbsp;
              <span class='dn'>{w['dn_shares']:.0f}sh @ {w['dn_avg']:.3f}</span> Down ${w['dn_cost']:,.0f}
              &nbsp;│&nbsp;
              Lean <span class='{lean_cls}'><b>{w['lean']}</b></span>
            </div>
            <details><summary class='dim' style='cursor:pointer;font-size:11px'>
              {len(w['trades'])} trades · click to expand
            </summary>
            <div class='timeline'>{timeline}</div>
            </details>
          </div>
        </div>"""

    # Group by time slot (window_start_ts) — all coins in the same 15-min slot together
    by_timeslot = defaultdict(list)
    for w in all_windows:
        by_timeslot[w['wts']].append(w)

    # Filter by coin if requested
    if coin:
        by_timeslot = {wts: [w for w in ws if w['coin'] == coin]
                       for wts, ws in by_timeslot.items()}
        by_timeslot = {k: v for k, v in by_timeslot.items() if v}

    # Preload Kalshi ticks for all timeslots we'll render, per coin
    # ticks_by_slot[(wts, coin)] = dict[elapsed_sec] = (yes_ask, no_ask)
    ticks_by_slot = {}
    if db:
        for wts in by_timeslot:
            for c in ['BTC','ETH','SOL','XRP']:
                close_dt = datetime.fromtimestamp(wts + 900, tz=ET)
                ticker = f"KX{c}15M-{close_dt.strftime('%y%b%d%H%M').upper()}-{close_dt.strftime('%M')}"
                row = db.execute('SELECT id FROM windows WHERE ticker=?', (ticker,)).fetchone()
                if not row: continue
                wid = row[0]
                rows = db.execute(
                    'SELECT elapsed_sec, yes_ask, no_ask FROM ticks WHERE window_id=? ORDER BY elapsed_sec',
                    (wid,)
                ).fetchall()
                d = {}
                for sec, ya, na in rows:
                    d[sec] = (ya, na)
                ticks_by_slot[(wts, c)] = d
        db.close()

    def nearest_tick(wts, coin, target_sec):
        d = ticks_by_slot.get((wts, coin))
        if not d: return None
        if target_sec in d: return d[target_sec]
        prior = None
        for s in sorted(d):
            if s <= target_sec: prior = d[s]
            else: break
        return prior

    # Build per (wts, coin) sorted list of (elapsed, delta_pct) from trade data
    # for interpolating delta at arbitrary seconds
    delta_points = defaultdict(list)  # (wts, coin) -> [(sec, delta)]
    for w in all_windows:
        for t in w['trades']:
            dp = t.get('delta_pct')
            if dp is None: continue
            delta_points[(w['wts'], w['coin'])].append((t.get('elapsed_in_window', 0), dp))
    for key in delta_points:
        delta_points[key].sort()

    def interp_delta(wts, coin, target_sec):
        pts = delta_points.get((wts, coin))
        if not pts: return None
        # Find bracketing points
        if target_sec <= pts[0][0]: return pts[0][1]
        if target_sec >= pts[-1][0]: return pts[-1][1]
        for i in range(1, len(pts)):
            if pts[i][0] >= target_sec:
                s0, d0 = pts[i-1]
                s1, d1 = pts[i]
                if s1 == s0: return d1
                frac = (target_sec - s0) / (s1 - s0)
                return d0 + frac * (d1 - d0)
        return pts[-1][1]

    sections_html = []
    for wts in sorted(by_timeslot, reverse=True):
        ws = sorted(by_timeslot[wts], key=lambda w: w['coin'])
        dt = datetime.fromtimestamp(wts, tz=ET)
        slot_cost = sum(w['cost'] for w in ws)
        slot_pnl = sum(w['pnl'] for w in ws if w['pnl'] is not None)
        slot_resolved = [w for w in ws if w['pnl'] is not None]
        slot_wins = sum(1 for w in slot_resolved if w['match'])
        pc = _pnl_class(slot_pnl) if slot_resolved else "pend"
        pnl_str = f"${slot_pnl:+,.0f}" if slot_resolved else "..."
        coins_in = " ".join(w['coin'] for w in ws)

        # Compact coin badges for the header
        coin_badges = ""
        for w in ws:
            w_pnl = f"${w['pnl']:+,.0f}" if w['pnl'] is not None else "..."
            w_pc = _pnl_class(w['pnl']) if w['pnl'] is not None else "pend"
            w_match = "✓" if w['match'] else "✗" if w['match'] is False else ""
            w_mc = "pos" if w['match'] else "neg" if w['match'] is False else ""
            coin_badges += (
                f"<span class='coin-badge'>{w['coin']}</span>"
                f"<span class='{w_pc}'>{w_pnl}</span>"
                f"<span class='{w_mc}'>{w_match}</span>&nbsp;&nbsp;"
            )

        # Build unified trade table — rows=elapsed_sec, columns=4 coins
        # Each trade keyed by (coin, elapsed) — if multiple trades at same sec, show all
        rows_by_sec = defaultdict(lambda: defaultdict(list))
        # Also pre-compute running totals per coin (sorted trades by time)
        running_state = {c: {
            'up_cost': 0.0, 'up_shares': 0.0,
            'dn_cost': 0.0, 'dn_shares': 0.0,
            'total': 0.0,
        } for c in ['BTC','ETH','SOL','XRP']}
        # Sort all trades by elapsed, then apply running totals into a per-(sec,coin) snapshot
        all_coin_trades = []
        for w in ws:
            for t in w['trades']:
                all_coin_trades.append((t.get('elapsed_in_window', 0), w['coin'], t))
        all_coin_trades.sort(key=lambda x: x[0])
        # snapshot_after[(sec, coin)] = state after applying all trades up to and including this row
        snapshot_after = {}
        for sec, coin, t in all_coin_trades:
            rows_by_sec[sec][coin].append(t)
            st = running_state[coin]
            out = t['outcome']
            if out == 'Up':
                st['up_cost'] += t['notional']
                st['up_shares'] += t['size']
            else:
                st['dn_cost'] += t['notional']
                st['dn_shares'] += t['size']
            st['total'] = st['up_cost'] + st['dn_cost']
            snapshot_after[(sec, coin)] = {k: v for k, v in st.items()}

        # Summary row per coin (winner + P&L)
        summary_cells = {}
        for w in ws:
            w_pnl = f"${w['pnl']:+,.2f}" if w['pnl'] is not None else "..."
            w_pc = _pnl_class(w['pnl']) if w['pnl'] is not None else "pend"
            w_match = "✓" if w['match'] else "✗" if w['match'] is False else "?"
            w_mc = "pos" if w['match'] else "neg" if w['match'] is False else "pend"
            w_cls_ = "up" if w['winner'] == "Up" else "dn" if w['winner'] == "Down" else ""
            summary_cells[w['coin']] = (
                f"<td class='summary-cell'>"
                f"<div><span class='{w_cls_}'><b>{w['winner'] or '—'}</b></span> "
                f"<span class='{w_mc}'>{w_match}</span></div>"
                f"<div class='{w_pc}'><b>{w_pnl}</b></div>"
                f"<div class='dim'>{w['n']} · ${w['cost']:,.0f}</div>"
                f"</td>"
            )

        # Build table rows
        table_rows = []
        for sec in sorted(rows_by_sec):
            cells = []
            for c in ['BTC','ETH','SOL','XRP']:
                trades_here = rows_by_sec[sec].get(c, [])
                if not trades_here:
                    # Check if this coin even has data for this slot
                    has_coin = any(w['coin'] == c for w in ws)
                    if has_coin:
                        tick = nearest_tick(wts, c, sec)
                        interp = interp_delta(wts, c, sec)
                        parts = []
                        if tick:
                            ya, na = tick
                            ya_s = f"{ya:.2f}" if ya and 0 < ya < 1 else "—"
                            na_s = f"{na:.2f}" if na and 0 < na < 1 else "—"
                            parts.append(
                                f"Y<span class='up'>{ya_s}</span>/N<span class='dn'>{na_s}</span>"
                            )
                        if interp is not None:
                            d_col = "up" if interp > 0 else "dn" if interp < 0 else "dim"
                            parts.append(f"<span class='{d_col}'>Δ{interp:+.3f}%</span>")
                        if parts:
                            cells.append(
                                f"<td class='empty'>"
                                f"<span class='dim' style='font-size:10px'>"
                                + " ".join(parts)
                                + "</span></td>"
                            )
                        else:
                            cells.append(f"<td class='empty'></td>")
                    else:
                        cells.append(f"<td class='na'></td>")
                    continue
                # Combine all trades at this sec for this coin
                parts = []
                for t in trades_here:
                    out = t['outcome']
                    oc = "up" if out == "Up" else "dn"
                    oc_short = "U" if out == "Up" else "D"
                    dpct = t.get('delta_pct')
                    d_str = f"{dpct:+.3f}%" if dpct is not None else "—"
                    d_col = "up" if dpct and dpct > 0 else "dn" if dpct and dpct < 0 else "dim"
                    parts.append(
                        f"<div class='trade-cell'>"
                        f"<span class='{oc}'><b>{oc_short}</b></span> "
                        f"@{t['price']:.2f} "
                        f"<span class='money'>${t['notional']:.0f}</span> "
                        f"<span class='{d_col}'>{d_str}</span>"
                        f"</div>"
                    )
                # Running totals after these trades
                snap = snapshot_after.get((sec, c))
                if snap:
                    u_avg = snap['up_cost']/snap['up_shares'] if snap['up_shares'] else 0
                    d_avg = snap['dn_cost']/snap['dn_shares'] if snap['dn_shares'] else 0
                    cum_parts = []
                    if snap['up_shares'] > 0:
                        cum_parts.append(
                            f"<span class='up'>U:{snap['up_shares']:.0f}sh@{u_avg:.2f}</span>"
                            f" <span class='dim'>${snap['up_cost']:.0f}</span>"
                        )
                    if snap['dn_shares'] > 0:
                        cum_parts.append(
                            f"<span class='dn'>D:{snap['dn_shares']:.0f}sh@{d_avg:.2f}</span>"
                            f" <span class='dim'>${snap['dn_cost']:.0f}</span>"
                        )
                    if cum_parts:
                        parts.append(
                            f"<div class='running'>Σ {' / '.join(cum_parts)}"
                            f" <span class='money'>${snap['total']:.0f}</span></div>"
                        )
                        # If-Up / If-Down outcomes
                        if_up = snap['up_shares'] - snap['total']
                        if_dn = snap['dn_shares'] - snap['total']
                        u_cls = 'pos' if if_up >= 0 else 'neg'
                        d_cls = 'pos' if if_dn >= 0 else 'neg'
                        parts.append(
                            f"<div class='outcome'>"
                            f"if↑ <span class='{u_cls}'>${if_up:+.1f}</span> · "
                            f"if↓ <span class='{d_cls}'>${if_dn:+.1f}</span>"
                            f"</div>"
                        )
                cells.append(f"<td>{''.join(parts)}</td>")
            table_rows.append(f"<tr><td class='sec'>{sec}s</td>{''.join(cells)}</tr>")

        # Summary row at the top (sticky)
        summary_row = (
            "<tr class='summary-row'>"
            "<td class='sec'><b>RESULT</b></td>"
            + "".join(summary_cells.get(c, "<td class='na'></td>") for c in ['BTC','ETH','SOL','XRP'])
            + "</tr>"
        )

        slot_id = f"slot_{wts}"
        sections_html.append(f"""
        <div class='timeslot' data-slot-id='{slot_id}'>
          <div class='timeslot-header' onclick="toggleSlot('{slot_id}')">
            <span style='font-weight:bold;min-width:110px'>{dt.strftime('%b %d %H:%M')}</span>
            {coin_badges}
            <span class='dim' style='margin-left:auto'>${slot_cost:,.0f}</span>
            <span class='{pc}' style='font-weight:bold;min-width:60px;text-align:right'>{pnl_str}</span>
          </div>
          <div class='timeslot-body'>
            <table class='trade-matrix'>
              <thead>
                <tr>
                  <th class='sec-hdr'>sec</th>
                  <th>BTC</th><th>ETH</th><th>SOL</th><th>XRP</th>
                </tr>
              </thead>
              <tbody>
                {summary_row}
                {''.join(table_rows)}
              </tbody>
            </table>
          </div>
        </div>""")

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset='utf-8'><title>Scallops Viewer</title>
<meta http-equiv='refresh' content='30'>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0a0a0f; color: #e0e0e0; font-family: 'SF Mono','Fira Code',monospace; font-size: 13px; padding: 16px; }}
  h1 {{ color: #fbbf24; font-size: 18px; margin-bottom: 4px; }}
  h2 {{ color: #60a5fa; font-size: 14px; margin: 24px 0 8px; border-bottom: 1px solid #1e1e3a; padding-bottom: 4px; }}
  .meta {{ color: #555; font-size: 11px; margin-bottom: 16px; }}
  .up {{ color: #4ade80; }} .dn {{ color: #f87171; }}
  .pos {{ color: #4ade80; }} .neg {{ color: #f87171; }}
  .dim {{ color: #555; }} .pend {{ color: #888; }}

  .tabs {{ display: flex; gap: 8px; margin: 12px 0 16px; flex-wrap: wrap; }}
  .tabs a {{ background: #111128; border: 1px solid #1e1e3a; border-radius: 6px;
             padding: 6px 14px; color: #e0e0e0; text-decoration: none; font-size: 12px; }}
  .tabs a:hover {{ background: #1e1e3a; }}
  .tabs a.active {{ background: #1e1e3a; border-color: #60a5fa; color: #60a5fa; }}

  .kpis {{ display: flex; gap: 16px; margin: 16px 0; flex-wrap: wrap; }}
  .kpi {{ background: #111128; border: 1px solid #1e1e3a; border-radius: 6px; padding: 10px 18px; }}
  .kpi-val {{ font-size: 22px; font-weight: bold; }}
  .kpi-label {{ color: #555; font-size: 11px; margin-top: 2px; }}

  .coin-cards {{ display: flex; gap: 12px; margin: 12px 0; flex-wrap: wrap; }}
  .coin-card {{ background: #111128; border: 1px solid #1e1e3a; border-radius: 8px; padding: 12px 18px; min-width: 140px; }}
  .coin-name {{ color: #fbbf24; font-weight: bold; font-size: 14px; }}
  .coin-stat {{ font-size: 20px; font-weight: bold; margin: 4px 0; }}
  .coin-detail {{ color: #888; font-size: 11px; }}

  .window-card {{ background: #0e0e1c; border: 1px solid #1e1e3a; border-radius: 8px;
                  padding: 12px 16px; margin-bottom: 8px; }}
  .window-header {{ display: flex; align-items: center; gap: 12px; margin-bottom: 8px; flex-wrap: wrap; }}
  .coin-badge {{ background: #1e1e3a; color: #fbbf24; font-weight: bold; padding: 2px 8px; border-radius: 4px; }}
  .winner-badge {{ font-weight: bold; padding: 2px 8px; border-radius: 4px; }}
  .winner-badge.up {{ background: #052e16; color: #4ade80; }}
  .winner-badge.dn {{ background: #450a0a; color: #f87171; }}
  .pnl-badge {{ font-weight: bold; padding: 2px 10px; border-radius: 4px; font-size: 14px; }}
  .pnl-badge.pos {{ background: #052e16; color: #4ade80; }}
  .pnl-badge.neg {{ background: #450a0a; color: #f87171; }}
  .pnl-badge.pend {{ background: #1a1a2e; color: #888; }}
  .pnl-badge small {{ font-weight: normal; opacity: 0.85; }}
  .pos-summary {{ font-size: 12px; margin-bottom: 6px; }}
  .timeline {{ font-size: 11px; line-height: 1.7; color: #aaa; border-top: 1px solid #1a1a2e;
               padding-top: 6px; margin-top: 6px; max-height: 200px; overflow-y: auto; }}

  .time-links {{ margin: 8px 0; }}
  .time-links a {{ color: #60a5fa; text-decoration: none; margin-right: 12px; font-size: 12px; }}
  .time-links a:hover {{ text-decoration: underline; }}

  .timeslot {{ margin-bottom: 4px; }}
  .timeslot-header {{ display: flex; align-items: center; gap: 8px; padding: 8px 12px;
                      background: #111128; border: 1px solid #1e1e3a; border-radius: 6px;
                      cursor: pointer; flex-wrap: wrap; }}
  .timeslot-header:hover {{ background: #1a1a2e; }}
  .timeslot-body {{ padding: 8px 0 12px 0; display: none; }}

  .trade-matrix {{ width: 100%; border-collapse: collapse; font-size: 11px; table-layout: fixed; }}
  .trade-matrix th {{ background: #1a1a2e; color: #60a5fa; padding: 6px 8px; text-align: left;
                       font-size: 10px; text-transform: uppercase; border-bottom: 1px solid #2a2a4a; }}
  .trade-matrix th.sec-hdr {{ width: 60px; }}
  .trade-matrix td {{ padding: 4px 8px; border-bottom: 1px solid #111; vertical-align: top;
                      width: 23%; }}
  .trade-matrix td.sec {{ color: #666; width: 60px; font-family: monospace; }}
  .trade-matrix td.empty {{ background: #0c0c14; }}
  .trade-matrix td.na {{ background: #060608; }}
  .trade-matrix .trade-cell {{ margin-bottom: 2px; }}
  .trade-matrix .money {{ color: #fbbf24; }}
  .trade-matrix tr:hover td {{ background: #12122a; }}
  .trade-matrix .summary-row td {{ background: #0e0e1c; padding: 8px 10px; border-bottom: 2px solid #2a2a4a; }}
  .trade-matrix .summary-cell {{ font-size: 12px; line-height: 1.5; }}
  .trade-matrix .running {{ font-size: 10px; margin-top: 3px; padding-top: 3px;
                            border-top: 1px dashed #222; color: #888; }}
  .trade-matrix .outcome {{ font-size: 10px; color: #888; margin-top: 1px; }}
</style></head>
<body>
<h1>◆ Idolized-Scallops Trade Viewer</h1>
<div class='meta'>{datetime.now(tz=ET).strftime('%H:%M:%S ET')} · auto-refresh 30s</div>

<div class='time-links'>
  <a href='/?hours=6{"&coin="+coin if coin else ""}'>6h</a>
  <a href='/?hours=12{"&coin="+coin if coin else ""}'>12h</a>
  <a href='/?hours=24{"&coin="+coin if coin else ""}'>24h</a>
  <a href='/?hours=48{"&coin="+coin if coin else ""}'>48h</a>
</div>

<div class='tabs'>{"".join(coin_tabs)}</div>

<div class='kpis'>
  <div class='kpi'><div class='kpi-val'>{len(all_windows)}</div><div class='kpi-label'>Windows</div></div>
  <div class='kpi'><div class='kpi-val'>{len(trades):,}</div><div class='kpi-label'>Trades</div></div>
  <div class='kpi'><div class='kpi-val'>${total_cost:,.0f}</div><div class='kpi-label'>Deployed</div></div>
  <div class='kpi'><div class='kpi-val {_pnl_class(total_pnl)}'>${_fmt_pnl(total_pnl)}</div><div class='kpi-label'>P&L</div></div>
  <div class='kpi'><div class='kpi-val'>{ret}</div><div class='kpi-label'>Return</div></div>
  <div class='kpi'><div class='kpi-val'>{lean_pct} {lean_rate}</div><div class='kpi-label'>Lean Match</div></div>
</div>

<div class='coin-cards'>{"".join(coin_cards)}</div>

{"".join(sections_html)}

<script>
function expandedSlots() {{
  try {{ return new Set(JSON.parse(localStorage.getItem('scallops_expanded') || '[]')); }}
  catch (e) {{ return new Set(); }}
}}
function saveExpanded(set) {{
  localStorage.setItem('scallops_expanded', JSON.stringify([...set]));
}}
function toggleSlot(id) {{
  var el = document.querySelector("[data-slot-id='" + id + "'] .timeslot-body");
  if (!el) return;
  var now = getComputedStyle(el).display === 'block' ? 'none' : 'block';
  el.style.display = now;
  var s = expandedSlots();
  if (now === 'block') s.add(id); else s.delete(id);
  saveExpanded(s);
}}
// On page load, restore expanded state
(function() {{
  var s = expandedSlots();
  s.forEach(function(id) {{
    var el = document.querySelector("[data-slot-id='" + id + "'] .timeslot-body");
    if (el) el.style.display = 'block';
  }});
}})();
// Pause auto-refresh while user is interacting (no scroll loss)
(function() {{
  var lastScroll = parseInt(sessionStorage.getItem('scallops_scroll') || '0', 10);
  window.scrollTo(0, lastScroll);
  window.addEventListener('scroll', function() {{
    sessionStorage.setItem('scallops_scroll', String(window.scrollY));
  }});
}})();
</script>
</body></html>"""
    return HTMLResponse(html)


if __name__ == "__main__":
    print("Scallops Viewer → http://localhost:7332")
    uvicorn.run(app, host="0.0.0.0", port=7332, log_level="warning")
