#!/usr/bin/env python3
"""
hmm_backtest.py — backtest HMM divergence signals on last N labeled windows.

For each window, replays what the monitor would have seen at t=120,180,240,300,360,420s.
If an edge signal fired (Bearish+yes_ask>=0.55 or Bullish+yes_ask<=0.45),
records the implied bet and checks against the actual winner.

Run:  python hmm_backtest.py
"""
import sqlite3
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table
from scipy.special import logsumexp
from scipy.stats import multivariate_normal
from sklearn.cluster import KMeans

# ── Config ────────────────────────────────────────────────────────────────────

DB          = Path.home() / ".btc_windows.db"
SYMBOLS     = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "XRP": "KXXRP15M", "SOL": "KXSOL15M"}
N_STATES    = 3
MAX_ITER    = 15
TOL         = 1.0
STATE_NAMES = ["Bearish", "Uncertain", "Bullish"]
EDGE_HI     = 0.55      # Bearish + yes_ask >= this → signal NO
EDGE_LO     = 0.45      # Bullish + yes_ask <= this → signal YES
BACKTEST_N  = 20        # last N windows to test per coin
CHECK_TIMES = [120, 180, 240, 300, 360, 420]   # seconds to sample signal at
BET_SIZE    = 1.0       # $1 per bet (for P&L calc)

console = Console()

# ── HMM core (identical to hmm_monitor.py) ───────────────────────────────────

def make_features(arr, T):
    x    = arr[:T]
    x_sm = pd.Series(x).rolling(15, min_periods=1).mean().to_numpy()
    delta = np.diff(x_sm, prepend=x_sm[0])
    return np.column_stack([x_sm, delta, np.abs(delta)])

def log_emission(obs, means, covs):
    T, N = len(obs), len(means)
    log_b = np.full((T, N), -1e10)
    for k in range(N):
        try:
            rv = multivariate_normal(mean=means[k], cov=covs[k], allow_singular=True)
            log_b[:, k] = np.log(np.clip(rv.pdf(obs), 1e-300, None))
        except Exception:
            pass
    return log_b

def forward(log_pi, log_A, log_b):
    T, N = log_b.shape
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
    T, N = log_b.shape
    delta = np.full((T, N), -np.inf)
    psi   = np.zeros((T, N), dtype=int)
    delta[0] = log_pi + log_b[0]
    for t in range(1, T):
        trans    = delta[t-1][:, None] + log_A
        psi[t]   = trans.argmax(axis=0)
        delta[t] = trans.max(axis=0) + log_b[t]
    states = np.zeros(T, dtype=int)
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
    covs = np.array(covs)
    pi   = np.full(N_STATES, 1.0 / N_STATES)
    A    = np.full((N_STATES, N_STATES), 1.0 / N_STATES)

    for _ in range(MAX_ITER):
        log_pi = np.log(pi + 1e-300)
        log_A  = np.log(A  + 1e-300)
        sum_g0 = np.zeros(N_STATES)
        sum_xi = np.zeros((N_STATES, N_STATES))
        sum_g  = np.zeros(N_STATES)
        sum_go = np.zeros((N_STATES, D))
        sum_go2= np.zeros((N_STATES, D))
        for obs in obs_seqs:
            log_b = log_emission(obs, means, covs)
            alpha = forward(log_pi, log_A, log_b)
            beta  = backward(log_A, log_b)
            ll    = logsumexp(alpha[-1])
            log_gamma = alpha + beta - ll
            gamma     = np.exp(log_gamma)
            log_xi    = (alpha[:-1, :, None] + log_A[None] +
                         log_b[1:, None, :] + beta[1:, None, :] - ll)
            xi = np.exp(log_xi)
            sum_g0  += gamma[0]
            sum_xi  += xi.sum(axis=0)
            sum_g   += gamma.sum(axis=0)
            sum_go  += gamma.T @ obs
            sum_go2 += gamma.T @ (obs ** 2)
        pi    = sum_g0 / sum_g0.sum()
        A     = sum_xi / sum_xi.sum(axis=1, keepdims=True).clip(1e-300)
        means = sum_go / sum_g[:, None].clip(1e-300)
        var   = sum_go2 / sum_g[:, None].clip(1e-300) - means**2
        covs  = np.array([np.diag(np.maximum(var[k], 1e-6)) for k in range(N_STATES)])

    order = np.argsort(means[:, 0])
    return {"pi": pi[order], "A": A[np.ix_(order, order)],
            "means": means[order], "covs": covs[order]}

def load_ticks(con, wid):
    rows = con.execute(
        "SELECT elapsed_sec, yes_ask FROM ticks "
        "WHERE window_id=? AND yes_ask IS NOT NULL ORDER BY elapsed_sec", (wid,)
    ).fetchall()
    arr = np.full(900, np.nan)
    for sec, ya in rows:
        if 0 <= sec < 900:
            arr[sec] = ya
    last = 0.5
    for i in range(900):
        if np.isnan(arr[i]): arr[i] = last
        else: last = arr[i]
    return arr

# ── Backtest ──────────────────────────────────────────────────────────────────

def run_backtest():
    con = sqlite3.connect(DB)

    all_bets   = []   # every individual bet across coins/times
    sym_summary= {}

    for sym, prefix in SYMBOLS.items():
        console.print(f"\n[bold cyan]── {sym} ──────────────────────────────────────────[/bold cyan]")

        # All labeled windows for this coin, chronological
        all_rows = con.execute(
            "SELECT w.id, w.winner, w.window_start_ts FROM windows w "
            "WHERE w.winner IS NOT NULL AND w.ticker LIKE ? "
            "AND (SELECT COUNT(*) FROM ticks WHERE window_id=w.id) > 100 "
            "ORDER BY w.window_start_ts",
            (prefix + "%",)
        ).fetchall()

        if len(all_rows) < BACKTEST_N + 10:
            console.print(f"  Not enough windows ({len(all_rows)}), need {BACKTEST_N+10}")
            continue

        # Split: train on all except last BACKTEST_N, test on last BACKTEST_N
        train_rows = all_rows[:-BACKTEST_N]
        test_rows  = all_rows[-BACKTEST_N:]

        console.print(f"  Train: {len(train_rows)} windows  |  Test: {len(test_rows)} windows")

        # Train model (no lookahead — test windows excluded)
        obs_seqs = [make_features(load_ticks(con, wid), 900) for wid, _, _ in train_rows]
        model    = train_hmm(obs_seqs)

        # Win rates from training set
        log_pi = np.log(model["pi"] + 1e-300)
        log_A  = np.log(model["A"]  + 1e-300)
        state_yes = np.zeros(N_STATES)
        state_no  = np.zeros(N_STATES)
        for (wid, winner, _), obs in zip(train_rows, obs_seqs):
            log_b = log_emission(obs, model["means"], model["covs"])
            sts   = viterbi(log_pi, log_A, log_b)
            dom   = np.bincount(sts, minlength=N_STATES).argmax()
            if winner == "yes": state_yes[dom] += 1
            else:               state_no[dom]  += 1
        total      = state_yes + state_no
        win_rates  = np.where(total > 0, state_yes / total, 0.5)

        # Backtest each test window
        sym_bets = []
        for wid, winner, wts in test_rows:
            arr  = load_ticks(con, wid)
            wlabel = datetime.fromtimestamp(wts).strftime("%H:%M")

            for t in CHECK_TIMES:
                if t >= 890: continue
                feat  = make_features(arr, t)
                log_b = log_emission(feat, model["means"], model["covs"])
                sts   = viterbi(log_pi, log_A, log_b)
                cur_state = int(sts[-1])
                cur_ya    = float(arr[t-1])
                cur_na    = round(1.0 - cur_ya, 2)
                wr        = float(win_rates[cur_state])

                # Determine signal
                signal_side = None
                cost        = 0.0
                if cur_state == 0 and cur_ya >= EDGE_HI:   # Bearish + elevated → NO
                    signal_side = "NO"
                    cost        = cur_na
                elif cur_state == 2 and cur_ya <= EDGE_LO:  # Bullish + depressed → YES
                    signal_side = "YES"
                    cost        = cur_ya

                if signal_side is None:
                    continue

                # Outcome
                won = (signal_side == "NO"  and winner == "no") or \
                      (signal_side == "YES" and winner == "yes")
                pnl = round((1.0 - cost) * BET_SIZE if won else -cost * BET_SIZE, 4)

                bet = {
                    "sym":    sym,
                    "window": wlabel,
                    "t":      t,
                    "state":  STATE_NAMES[cur_state],
                    "side":   signal_side,
                    "cost":   cost,
                    "ya":     cur_ya,
                    "wr":     wr,
                    "winner": winner.upper(),
                    "won":    won,
                    "pnl":    pnl,
                }
                sym_bets.append(bet)
                all_bets.append(bet)

        if not sym_bets:
            console.print("  No edge signals fired in test windows.")
            continue

        # Per-coin summary table
        tbl = Table(show_header=True, header_style="bold", box=None, padding=(0,1))
        tbl.add_column("Window", width=6)
        tbl.add_column("t",      width=4,  justify="right")
        tbl.add_column("State",  width=10)
        tbl.add_column("Side",   width=4)
        tbl.add_column("Cost",   width=5,  justify="right")
        tbl.add_column("ya",     width=5,  justify="right")
        tbl.add_column("Model%", width=7,  justify="right")
        tbl.add_column("Result", width=7)
        tbl.add_column("P&L",    width=7,  justify="right")

        for b in sym_bets:
            won_str = "[green]WIN[/green]" if b["won"] else "[red]LOSS[/red]"
            pnl_str = f"[green]+{b['pnl']:.2f}[/green]" if b["won"] else f"[red]{b['pnl']:.2f}[/red]"
            tbl.add_row(
                b["window"], str(b["t"]), b["state"], b["side"],
                f"{b['cost']:.2f}", f"{b['ya']:.2f}",
                f"{b['wr']:.0%}", won_str, pnl_str,
            )
        console.print(tbl)

        wins  = sum(1 for b in sym_bets if b["won"])
        total_bets = len(sym_bets)
        total_pnl  = sum(b["pnl"] for b in sym_bets)
        console.print(f"  {sym}: {wins}/{total_bets} wins  "
                      f"win rate={wins/total_bets:.0%}  "
                      f"P&L=${total_pnl:+.2f}")
        sym_summary[sym] = (wins, total_bets, total_pnl)

    # ── Overall summary ───────────────────────────────────────────────────────
    if not all_bets:
        console.print("\n[yellow]No edge signals fired across any test window.[/yellow]")
        return

    console.print("\n[bold cyan]══ OVERALL SUMMARY ══[/bold cyan]")

    # By check time
    console.print("\n[bold]By signal time (t):[/bold]")
    for t in CHECK_TIMES:
        tb = [b for b in all_bets if b["t"] == t]
        if not tb: continue
        w = sum(1 for b in tb if b["won"])
        pnl = sum(b["pnl"] for b in tb)
        console.print(f"  t={t:3d}s  signals={len(tb):3d}  wins={w:3d}  "
                      f"win%={w/len(tb):.0%}  P&L=${pnl:+.2f}")

    # By state
    console.print("\n[bold]By state:[/bold]")
    for state in STATE_NAMES:
        sb = [b for b in all_bets if b["state"] == state]
        if not sb: continue
        w   = sum(1 for b in sb if b["won"])
        pnl = sum(b["pnl"] for b in sb)
        console.print(f"  {state:10s}  signals={len(sb):3d}  wins={w:3d}  "
                      f"win%={w/len(sb):.0%}  P&L=${pnl:+.2f}")

    # Grand total
    w   = sum(1 for b in all_bets if b["won"])
    n   = len(all_bets)
    pnl = sum(b["pnl"] for b in all_bets)
    console.print(f"\n[bold]Grand total:  {w}/{n} wins  "
                  f"win rate={w/n:.0%}  P&L=${pnl:+.2f}[/bold]")
    be_rate = sum(b["cost"] for b in all_bets) / n
    console.print(f"Break-even win rate needed: {be_rate:.0%}  "
                  f"({'[green]PROFITABLE[/green]' if w/n > be_rate else '[red]NOT PROFITABLE[/red]'})")

    con.close()


if __name__ == "__main__":
    console.print("[bold cyan]HMM Divergence Backtest[/bold cyan]\n")
    run_backtest()
