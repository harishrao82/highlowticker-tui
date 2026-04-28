#!/usr/bin/env python3
"""
hmm_monitor.py — continuous HMM divergence alert monitor.

Runs every 30 seconds. Alerts when the HMM state disagrees with the current
market price — that's where the edge is:

  Bearish + yes_ask >= 0.55  →  buy NO  (market overpricing YES)
  Bullish + yes_ask <= 0.45  →  buy YES (market underpricing YES)

Break-even win rate at 0.30 cost = 30%.  1-for-2 correct = +$0.40 per 2 bets.

Run:   python hmm_monitor.py
Stop:  Ctrl-C
"""
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table
from scipy.special import logsumexp
from scipy.stats import multivariate_normal
from sklearn.cluster import KMeans

# ── Config ────────────────────────────────────────────────────────────────────

DB           = Path.home() / ".btc_windows.db"
SYMBOLS      = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "XRP": "KXXRP15M", "SOL": "KXSOL15M"}
N_STATES     = 3
MAX_ITER     = 15
TOL          = 1.0
STATE_NAMES  = ["Bearish", "Uncertain", "Bullish"]
POLL_SECS    = 30          # how often to check
EDGE_HI      = 0.55        # Bearish + yes_ask >= this → buy NO alert
EDGE_LO      = 0.45        # Bullish + yes_ask <= this → buy YES alert
MIN_T        = 120         # don't alert before 2 min into window (too noisy)
MIN_WINDOWS  = 5

console = Console()
_HMM_CACHE: dict = {}

# ── HMM core (log-space, no underflow) ───────────────────────────────────────

def make_features(arr: np.ndarray, T: int) -> np.ndarray:
    x    = arr[:T]
    x_sm = pd.Series(x).rolling(15, min_periods=1).mean().to_numpy()
    delta = np.diff(x_sm, prepend=x_sm[0])
    return np.column_stack([x_sm, delta, np.abs(delta)])


def log_emission(obs: np.ndarray, means: np.ndarray, covs: np.ndarray) -> np.ndarray:
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


def train_hmm(obs_seqs: list) -> dict:
    X_all  = np.vstack(obs_seqs)
    D      = X_all.shape[1]
    km     = KMeans(n_clusters=N_STATES, n_init=5, random_state=42).fit(X_all)
    means  = km.cluster_centers_.copy()
    labels = km.labels_
    covs   = []
    for k in range(N_STATES):
        pts = X_all[labels == k]
        if len(pts) < 2:
            pts = X_all
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
        total_ll = 0.0

        for obs in obs_seqs:
            log_b = log_emission(obs, means, covs)
            alpha = forward(log_pi, log_A, log_b)
            beta  = backward(log_A, log_b)
            ll    = logsumexp(alpha[-1])
            total_ll += ll
            log_gamma = alpha + beta - ll
            gamma     = np.exp(log_gamma)
            T_        = len(obs)
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

    # Relabel: sort by yes_ask mean (col 0) → state 0=Bearish, 2=Bullish
    order  = np.argsort(means[:, 0])
    means  = means[order]
    covs   = covs[order]
    pi     = pi[order]
    A      = A[np.ix_(order, order)]

    return {"pi": pi, "A": A, "means": means, "covs": covs}


def load_ticks(con: sqlite3.Connection, wid: int) -> np.ndarray:
    rows = con.execute(
        "SELECT elapsed_sec, yes_ask FROM ticks "
        "WHERE window_id=? AND yes_ask IS NOT NULL ORDER BY elapsed_sec", (wid,)
    ).fetchall()
    arr = np.full(900, np.nan)
    for sec, ya in rows:
        if 0 <= sec < 900:
            arr[sec] = ya
    # forward-fill
    last = 0.5
    for i in range(900):
        if np.isnan(arr[i]):
            arr[i] = last
        else:
            last = arr[i]
    return arr


def get_or_train(con: sqlite3.Connection, sym: str, prefix: str) -> dict | None:
    rows = con.execute(
        "SELECT w.id, w.winner FROM windows w "
        "WHERE w.winner IS NOT NULL AND w.ticker LIKE ? "
        "AND (SELECT COUNT(*) FROM ticks WHERE window_id=w.id) > 100 "
        "ORDER BY w.window_start_ts",
        (prefix + "%",)
    ).fetchall()
    n = len(rows)
    if n < MIN_WINDOWS:
        return None

    cached = _HMM_CACHE.get(sym)
    if cached and cached["n_windows"] == n:
        return cached

    console.print(f"  [dim cyan]Training {sym} HMM on {n} windows…[/dim cyan]")
    obs_seqs = [make_features(load_ticks(con, wid), 900) for wid, _ in rows]
    model    = train_hmm(obs_seqs)

    log_pi = np.log(model["pi"] + 1e-300)
    log_A  = np.log(model["A"]  + 1e-300)
    state_yes = np.zeros(N_STATES)
    state_no  = np.zeros(N_STATES)
    for (wid, winner), obs in zip(rows, obs_seqs):
        log_b = log_emission(obs, model["means"], model["covs"])
        sts   = viterbi(log_pi, log_A, log_b)
        dom   = np.bincount(sts, minlength=N_STATES).argmax()
        if winner == "yes":
            state_yes[dom] += 1
        else:
            state_no[dom] += 1

    total       = state_yes + state_no
    win_rates   = np.where(total > 0, state_yes / total, 0.5)
    model["win_rates"] = win_rates
    model["n_windows"] = n
    _HMM_CACHE[sym]    = model
    return model


# ── Single scan ───────────────────────────────────────────────────────────────

def scan(con: sqlite3.Connection) -> list[dict]:
    latest_ts = con.execute(
        "SELECT MAX(window_start_ts) FROM windows"
    ).fetchone()[0]
    if not latest_ts:
        return []

    now = int(datetime.now(timezone.utc).timestamp())
    T   = min(now - latest_ts, 897)

    if T < MIN_T:
        return []

    latest_wid = {}
    for sym, prefix in SYMBOLS.items():
        row = con.execute(
            "SELECT id FROM windows WHERE ticker LIKE ? ORDER BY window_start_ts DESC LIMIT 1",
            (prefix + "%",)
        ).fetchone()
        if row:
            latest_wid[sym] = row[0]

    alerts = []
    for sym, prefix in SYMBOLS.items():
        model = get_or_train(con, sym, prefix)
        if model is None:
            continue

        wid = latest_wid.get(sym)
        if not wid:
            continue

        arr   = load_ticks(con, wid)
        feat  = make_features(arr, T)
        log_pi = np.log(model["pi"] + 1e-300)
        log_A  = np.log(model["A"]  + 1e-300)
        log_b  = log_emission(feat, model["means"], model["covs"])
        states = viterbi(log_pi, log_A, log_b)

        cur_state  = int(states[-1])
        win_rate   = float(model["win_rates"][cur_state])
        cur_ya     = float(arr[T-1])
        cur_na     = round(1.0 - cur_ya, 2)

        edge = None
        if cur_state == 0 and cur_ya >= EDGE_HI:        # Bearish + elevated price → buy NO
            edge = {
                "side": "NO",
                "cost": cur_na,
                "payout": round(1.0 / cur_na, 2) if cur_na > 0 else 0,
                "model_no_rate": round(1 - win_rate, 2),
            }
        elif cur_state == 2 and cur_ya <= EDGE_LO:      # Bullish + depressed price → buy YES
            edge = {
                "side": "YES",
                "cost": cur_ya,
                "payout": round(1.0 / cur_ya, 2) if cur_ya > 0 else 0,
                "model_yes_rate": round(win_rate, 2),
            }

        alerts.append({
            "sym":       sym,
            "t":         T,
            "state":     STATE_NAMES[cur_state],
            "win_rate":  win_rate,
            "yes_ask":   cur_ya,
            "no_ask":    cur_na,
            "edge":      edge,
            "window_ts": latest_ts,
        })

    return alerts


# ── Display ───────────────────────────────────────────────────────────────────

def print_status(alerts: list[dict]) -> None:
    if not alerts:
        return

    t_sample   = alerts[0]["t"]
    win_et     = datetime.fromtimestamp(alerts[0]["window_ts"]).strftime("%H:%M")
    time_left  = max(0, 900 - t_sample)
    now_str    = datetime.now().strftime("%H:%M:%S")

    console.print(f"\n[bold]── {now_str}  window={win_et} ET  t={t_sample}s  {time_left}s left ──[/bold]")

    tbl = Table(show_header=True, header_style="bold cyan", box=None, padding=(0, 2))
    tbl.add_column("Coin", width=5)
    tbl.add_column("State",     width=10)
    tbl.add_column("YES rate",  width=9, justify="right")
    tbl.add_column("yes_ask",   width=8, justify="right")
    tbl.add_column("no_ask",    width=7, justify="right")
    tbl.add_column("Signal",    width=40)

    for a in alerts:
        edge = a["edge"]
        if edge:
            side    = edge["side"]
            cost    = edge["cost"]
            payout  = edge["payout"]
            prob    = edge.get("model_no_rate") or edge.get("model_yes_rate")
            signal  = (f"[bold yellow]*** BUY {side} @ {cost:.2f}  "
                       f"payout {payout:.1f}x  model={prob:.0%}[/bold yellow]")
        else:
            signal = "[dim]–[/dim]"

        state_color = {"Bearish": "red", "Uncertain": "yellow", "Bullish": "green"}[a["state"]]
        tbl.add_row(
            a["sym"],
            f"[{state_color}]{a['state']}[/{state_color}]",
            f"{a['win_rate']:.0%}",
            f"{a['yes_ask']:.2f}",
            f"{a['no_ask']:.2f}",
            signal,
        )

    console.print(tbl)

    edge_coins = [a for a in alerts if a["edge"]]
    if edge_coins:
        console.print()
        for a in edge_coins:
            e = a["edge"]
            console.print(
                f"  [bold yellow]EDGE → {a['sym']}[/bold yellow]  "
                f"buy {e['side']} @ {e['cost']:.2f}  "
                f"break-even win rate: {e['cost']:.0%}  "
                f"model confidence: {e.get('model_no_rate') or e.get('model_yes_rate'):.0%}"
            )


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    console.print("[bold cyan]HMM Divergence Monitor[/bold cyan]")
    console.print(f"Scanning every {POLL_SECS}s  |  "
                  f"Alert when Bearish+yes_ask≥{EDGE_HI} or Bullish+yes_ask≤{EDGE_LO}\n")

    con = sqlite3.connect(DB, check_same_thread=False)

    # Train on startup
    console.print("[dim]Training models on startup…[/dim]")
    for sym, prefix in SYMBOLS.items():
        get_or_train(con, sym, prefix)
    console.print("[dim]Ready.[/dim]")

    while True:
        try:
            alerts = scan(con)
            print_status(alerts)
        except Exception as e:
            console.print(f"[yellow]scan error: {e}[/yellow]")
        time.sleep(POLL_SECS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[dim]Monitor stopped.[/dim]")
