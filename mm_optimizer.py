#!/usr/bin/env python3
"""
mm_optimizer.py — Find optimal (SIDE, BUY_PRICE, SELL_PRICE) for Kalshi 15-min market making.

Simulates every (side, buy, sell) price combination on all labeled historical windows.
Three outcomes per window:
  1. Buy never fills      → $0
  2. Buy + sell fill      → SELL - BUY  (locked profit)
  3. Buy fills, sell not  → hold to settlement (+$(1-BUY) if our side wins, -$BUY if not)

Run:  python mm_optimizer.py
"""
import sqlite3
from pathlib import Path

import numpy as np
from rich.console import Console
from rich.table import Table

DB      = Path.home() / ".btc_windows.db"
SYMBOLS = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M", "XRP": "KXXRP15M"}

B_MIN      = 0.25
B_MAX      = 0.76   # exclusive
B_STEP     = 0.02
MIN_SPREAD = 0.04   # minimum sell - buy
S_MAX      = 0.82
TOP_N         = 10
LOOKBACKS     = [5, 10, 20, 40]   # lookback window sizes to compare

console = Console()


# ── Data loading ──────────────────────────────────────────────────────────────

def load_windows(con, prefix, n):
    rows = con.execute(
        "SELECT w.id, w.winner FROM windows w "
        "WHERE w.winner IS NOT NULL AND w.ticker LIKE ? "
        "AND (SELECT COUNT(*) FROM ticks WHERE window_id=w.id) > 100 "
        "ORDER BY w.window_start_ts DESC LIMIT ?",
        (prefix + "%", n)
    ).fetchall()
    return list(reversed(rows))  # restore chronological order


def load_arrays(con, wid):
    rows = con.execute(
        "SELECT elapsed_sec, yes_ask, no_ask FROM ticks "
        "WHERE window_id=? AND elapsed_sec < 900 ORDER BY elapsed_sec",
        (wid,)
    ).fetchall()

    yes_arr = np.full(900, np.nan)
    no_arr  = np.full(900, np.nan)
    for sec, ya, na in rows:
        if 0 <= sec < 900:
            if ya is not None: yes_arr[sec] = ya
            if na is not None: no_arr[sec]  = na

    # Forward-fill gaps
    last_y, last_n = 0.5, 0.5
    for i in range(900):
        if np.isnan(yes_arr[i]): yes_arr[i] = last_y
        else: last_y = yes_arr[i]
        if np.isnan(no_arr[i]):  no_arr[i] = last_n
        else: last_n = no_arr[i]

    return yes_arr, no_arr


# ── Simulation ────────────────────────────────────────────────────────────────

def simulate(side, b, s, yes_arr, no_arr, winner):
    """
    Returns (pnl, status) for one (side, b, s) on one window.
    Fill conditions:
      YES buy  @ b : yes_ask <= b
      YES sell @ s : no_ask  <= 1-s   (proxy for yes_bid >= s)
      NO  buy  @ b : no_ask  <= b     (equiv: yes_ask >= 1-b)
      NO  sell @ s : yes_ask <= 1-s   (proxy for no_bid >= s)
    """
    if side == "yes":
        buy_mask = yes_arr <= b
    else:
        buy_mask = no_arr <= b

    if not buy_mask.any():
        return 0.0, "no_fill"

    t_buy = int(np.argmax(buy_mask))

    # Sell: must be strictly after buy fills
    if t_buy + 1 < 900:
        if side == "yes":
            sell_mask = no_arr[t_buy + 1:] <= (1.0 - s)
        else:
            sell_mask = yes_arr[t_buy + 1:] <= (1.0 - s)
        if sell_mask.any():
            return round(s - b, 4), "roundtrip"

    # Settlement
    our_win = (winner == side)
    return round((1.0 - b) if our_win else -b, 4), "settlement"


# ── Grid search for one coin ──────────────────────────────────────────────────

def optimize_coin(windows, arrays):
    b_grid = np.round(np.arange(B_MIN, B_MAX, B_STEP), 4)
    n = len(windows)
    configs = []

    for side in ("yes", "no"):
        for b in b_grid:
            b = float(b)
            for s in np.round(np.arange(b + MIN_SPREAD, S_MAX, B_STEP), 4):
                s = float(s)
                if s >= 1.0:
                    continue

                pnls            = []
                n_fills         = 0
                n_rt            = 0
                n_settle        = 0
                n_settle_win    = 0

                for (ya, na, winner) in arrays:
                    pnl, status = simulate(side, b, s, ya, na, winner)
                    pnls.append(pnl)
                    if status == "no_fill":
                        continue
                    n_fills += 1
                    if status == "roundtrip":
                        n_rt += 1
                    else:
                        n_settle += 1
                        if pnl > 0:
                            n_settle_win += 1

                spread   = round(s - b, 4)
                rt_rate  = n_rt / n_fills if n_fills else 0.0
                # MM metric: expected profit from completed roundtrips only
                # settlement exposure is managed separately, not penalised here
                rt_pnl   = rt_rate * spread

                configs.append({
                    "side":      side,
                    "buy":       b,
                    "sell":      s,
                    "spread":    spread,
                    "rt_pnl":   round(rt_pnl, 4),   # primary sort key
                    "rt_rate":  round(rt_rate, 3),
                    "fill_rate": round(n_fills / n, 3),
                    "n_fills":   n_fills,
                    "n_rt":      n_rt,
                    "n_settle":  n_settle,
                })

    # Only keep configs where buy fills in every window (guaranteed entry)
    configs = [c for c in configs if c["fill_rate"] == 1.0]
    # Sort by roundtrip hit-rate × spread (MM P&L, no settlement penalty)
    configs.sort(key=lambda x: x["rt_pnl"], reverse=True)
    return configs


# ── Display ───────────────────────────────────────────────────────────────────

def print_coin_table(sym, configs, n_windows):
    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
    tbl.add_column("Side",    width=5)
    tbl.add_column("Buy",     width=5,  justify="right")
    tbl.add_column("Sell",    width=5,  justify="right")
    tbl.add_column("Spread",  width=7,  justify="right")
    tbl.add_column("RT P&L",  width=9,  justify="right")
    tbl.add_column("RT%",     width=6,  justify="right")
    tbl.add_column("Fill%",   width=6,  justify="right")
    tbl.add_column("N RT",    width=6,  justify="right")

    for cfg in configs[:TOP_N]:
        rt_str    = f"[green]{cfg['rt_pnl']:+.4f}[/green]"
        rt_color  = "green" if cfg["rt_rate"] >= 0.60 else "yellow" if cfg["rt_rate"] >= 0.40 else "red"
        fill_color = "yellow" if cfg["fill_rate"] < 0.20 else "white"
        tbl.add_row(
            cfg["side"].upper(),
            f"{cfg['buy']:.2f}",
            f"{cfg['sell']:.2f}",
            f"{cfg['spread']:.2f}",
            rt_str,
            f"[{rt_color}]{cfg['rt_rate']:.0%}[/{rt_color}]",
            f"[{fill_color}]{cfg['fill_rate']:.0%}[/{fill_color}]",
            str(cfg["n_rt"]),
        )

    console.print(tbl)

    best = configs[0]
    console.print(
        f"\n  [bold yellow]★ Best {sym}:[/bold yellow]"
        f"  SIDE={best['side'].upper()}  BUY={best['buy']:.2f}  SELL={best['sell']:.2f}"
        f"  RT P&L={best['rt_pnl']:+.4f}  hit={best['rt_rate']:.0%}  fill={best['fill_rate']:.0%}"
    )


# ── Heatmaps ──────────────────────────────────────────────────────────────────

def write_heatmaps(all_results):
    try:
        import plotly.graph_objects as go
    except ImportError:
        console.print("\n[dim]pip install plotly for HTML heatmaps[/dim]")
        return

    for sym, configs in all_results.items():
        for side in ("yes", "no"):
            cfgs = [c for c in configs if c["side"] == side]
            if not cfgs:
                continue

            b_vals = sorted(set(c["buy"]  for c in cfgs))
            s_vals = sorted(set(c["sell"] for c in cfgs))
            b_idx  = {b: i for i, b in enumerate(b_vals)}
            s_idx  = {s: i for i, s in enumerate(s_vals)}

            z = np.full((len(s_vals), len(b_vals)), np.nan)
            for c in cfgs:
                z[s_idx[c["sell"]]][b_idx[c["buy"]]] = c["e_pnl"]

            fig = go.Figure(data=go.Heatmap(
                z=z,
                x=[f"{b:.2f}" for b in b_vals],
                y=[f"{s:.2f}" for s in s_vals],
                colorscale="RdYlGn",
                zmid=0,
                colorbar=dict(title="E[P&L]"),
                hovertemplate="Buy=%{x}<br>Sell=%{y}<br>E[P&L]=%{z:.4f}<extra></extra>",
            ))
            fig.update_layout(
                title=f"{sym} {side.upper()} — Expected P&L per window",
                xaxis_title="BUY_PRICE",
                yaxis_title="SELL_PRICE",
                width=700, height=600,
            )
            out = Path(f"mm_heatmap_{sym}_{side}.html")
            fig.write_html(str(out))
            console.print(f"  [dim]  → {out}[/dim]")


# ── Walk-forward backtest ─────────────────────────────────────────────────────

def walkforward_backtest(con, sym, prefix, train_n, test_n):
    """
    For each of the last `test_n` windows:
      1. Train optimizer on the `train_n` windows immediately before it
      2. Pick best (side, buy, sell)
      3. Simulate that config on the test window
    Prints a table of results.
    """
    all_windows = con.execute(
        "SELECT w.id, w.winner FROM windows w "
        "WHERE w.winner IS NOT NULL AND w.ticker LIKE ? "
        "AND (SELECT COUNT(*) FROM ticks WHERE window_id=w.id) > 100 "
        "ORDER BY w.window_start_ts",
        (prefix + "%",)
    ).fetchall()

    needed = train_n + test_n
    if len(all_windows) < needed:
        console.print(f"  [yellow]Need {needed} labeled windows, only have {len(all_windows)}.[/yellow]")
        return

    # Preload all arrays we'll need
    first_idx = len(all_windows) - needed
    relevant  = all_windows[first_idx:]
    arrays    = {wid: (*(load_arrays(con, wid)), winner) for wid, winner in relevant}

    console.print(
        f"\n[bold cyan]══ Walk-Forward Backtest: {sym}  "
        f"train={train_n}  test={test_n} ══[/bold cyan]"
    )

    tbl = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
    tbl.add_column("#",        width=3,  justify="right")
    tbl.add_column("Side",     width=5)
    tbl.add_column("Buy",      width=5,  justify="right")
    tbl.add_column("Sell",     width=5,  justify="right")
    tbl.add_column("Outcome",  width=10)
    tbl.add_column("P&L",      width=8,  justify="right")
    tbl.add_column("Winner",   width=7)

    total_pnl  = 0.0
    wins, losses, nofills = 0, 0, 0

    for i in range(test_n):
        # training slice: train_n windows before this test window
        train_slice = relevant[i : i + train_n]
        test_wid, test_winner = relevant[i + train_n]

        train_arrays = [arrays[wid] for wid, _ in train_slice]
        configs = optimize_coin(train_slice, train_arrays)
        if not configs:
            continue

        best = configs[0]
        side, buy, sell = best["side"], best["buy"], best["sell"]

        ya, na, winner = arrays[test_wid]
        pnl, status = simulate(side, buy, sell, ya, na, winner)

        # MM view: only roundtrips count as P&L — settlement is a held position
        if status == "no_fill":
            nofills += 1
            mm_pnl      = 0.0
            outcome_str = "[dim]no fill[/dim]"
            pnl_str     = "[dim]—[/dim]"
        elif status == "roundtrip":
            wins += 1
            mm_pnl      = pnl
            outcome_str = "[green]roundtrip ✓[/green]"
            pnl_str     = f"[green]+${pnl:.2f}[/green]"
        else:
            losses += 1
            mm_pnl      = 0.0   # holding position, not a realised loss
            outcome_str = "[yellow]holding[/yellow]"
            pnl_str     = "[yellow]held[/yellow]"

        total_pnl += mm_pnl

        tbl.add_row(
            str(i + 1),
            side.upper(),
            f"{buy:.2f}",
            f"{sell:.2f}",
            outcome_str,
            pnl_str,
            winner.upper(),
        )

    console.print(tbl)

    pnl_color = "green" if total_pnl > 0 else "red"
    hit_rate  = wins / (wins + losses + nofills)
    console.print(
        f"\n  [{pnl_color}]Locked P&L (per contract): ${total_pnl:+.2f}[/{pnl_color}]"
        f"   hit rate={hit_rate:.0%}  roundtrips={wins}  holding={losses}  no_fills={nofills}"
        f"   ({test_n} windows)"
    )
    console.print(f"  [dim]Scaled to 100 contracts: ${total_pnl * 100:+.2f}[/dim]")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    console.print("[bold cyan]MM Price Optimizer — lookback comparison[/bold cyan]\n")
    con = sqlite3.connect(DB)

    # best[(sym, lookback)] = best config dict
    best: dict = {}

    for lookback in LOOKBACKS:
        console.print(f"[bold]Running lookback = last {lookback} windows…[/bold]")
        for sym, prefix in SYMBOLS.items():
            windows = load_windows(con, prefix, lookback)
            n = len(windows)
            if n < 3:
                continue
            arrays  = [(*(load_arrays(con, wid)), winner) for wid, winner in windows]
            configs = optimize_coin(windows, arrays)
            if configs:
                best[(sym, lookback)] = configs[0]

    # ── Summary table: one row per (sym, lookback) ────────────────────────────
    console.print("\n[bold cyan]══ BEST CONFIG BY COIN × LOOKBACK ══[/bold cyan]")
    tbl = Table(show_header=True, header_style="bold cyan", box=None, padding=(0, 1))
    tbl.add_column("Coin",     width=5)
    tbl.add_column("Last N",   width=7,  justify="right")
    tbl.add_column("Side",     width=5)
    tbl.add_column("Buy",      width=5,  justify="right")
    tbl.add_column("Sell",     width=5,  justify="right")
    tbl.add_column("Spread",   width=7,  justify="right")
    tbl.add_column("E[P&L]",   width=9,  justify="right")
    tbl.add_column("Fill%",    width=6,  justify="right")
    tbl.add_column("RT%",      width=5,  justify="right")

    prev_sym = None
    for sym in SYMBOLS:
        for lookback in LOOKBACKS:
            cfg = best.get((sym, lookback))
            if cfg is None:
                continue
            sym_label = f"[bold]{sym}[/bold]" if sym != prev_sym else ""
            prev_sym  = sym
            rt_color  = "green" if cfg["rt_rate"] >= 0.60 else "yellow" if cfg["rt_rate"] >= 0.40 else "red"
            tbl.add_row(
                sym_label,
                str(lookback),
                cfg["side"].upper(),
                f"{cfg['buy']:.2f}",
                f"{cfg['sell']:.2f}",
                f"{cfg['sell'] - cfg['buy']:.2f}",
                f"[green]{cfg['rt_pnl']:+.4f}[/green]",
                f"[{rt_color}]{cfg['rt_rate']:.0%}[/{rt_color}]",
                f"{cfg['fill_rate']:.0%}",
            )
        # blank separator row between coins
        tbl.add_row("", "", "", "", "", "", "", "", "")

    console.print(tbl)

    # ── Stable configs: same side across all lookbacks ────────────────────────
    console.print("[bold cyan]══ STABLE CONFIGS (side consistent across all lookbacks) ══[/bold cyan]")
    for sym in SYMBOLS:
        sides = [best[(sym, lb)]["side"] for lb in LOOKBACKS if (sym, lb) in best]
        if len(sides) == len(LOOKBACKS) and len(set(sides)) == 1:
            cfg = best[(sym, LOOKBACKS[-1])]  # use widest lookback as anchor
            console.print(
                f"  [bold green]{sym}[/bold green]  side={sides[0].upper()} consistent  "
                f"→ BUY={cfg['buy']:.2f}  SELL={cfg['sell']:.2f}  "
                f"RT P&L={cfg['rt_pnl']:+.4f}  hit={cfg['rt_rate']:.0%}  fill={cfg['fill_rate']:.0%}"
            )
        else:
            side_str = " / ".join(f"{lb}:{s.upper()}" for lb, s in zip(LOOKBACKS, sides) if (sym, lb) in best)
            console.print(f"  [yellow]{sym}[/yellow]  side varies by lookback: {side_str}")

    # Walk-forward backtest
    console.print("\n")
    for sym, prefix in SYMBOLS.items():
        walkforward_backtest(con, sym, prefix, train_n=10, test_n=10)

    con.close()


if __name__ == "__main__":
    main()
