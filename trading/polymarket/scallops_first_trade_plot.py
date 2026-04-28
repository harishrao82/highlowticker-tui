#!/usr/bin/env python3
"""
scallops_first_trade_plot.py — interactive Plotly scatter of Scallops'
first trade per coin-window.

3 views per coin: delta→price, delta→elapsed, elapsed→price.
Hover any point for full details.

Output: ~/scallops_first_trade_plot.html (open in browser)
"""
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError:
    import subprocess
    subprocess.check_call(["pip", "install", "plotly"])
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

TRADE_LOG = Path.home() / ".scallops_live_trades.jsonl"
OUT = Path.home() / "scallops_first_trade_plot.html"
ET = timezone(timedelta(hours=-4))

COINS = ["BTC", "ETH", "SOL", "XRP"]


def main():
    trades = []
    with open(TRADE_LOG) as f:
        for line in f:
            try:
                t = json.loads(line)
                if (t.get("side") == "BUY" and
                    "-15m-" in t.get("slug", "") and
                    t.get("delta_pct") is not None):
                    trades.append(t)
            except Exception:
                continue
    print(f"Loaded {len(trades):,} 15m BUY trades")

    firsts = {}
    for t in trades:
        key = (t["coin"], t["window_start_ts"])
        if key not in firsts or t["elapsed_in_window"] < firsts[key]["elapsed_in_window"]:
            firsts[key] = t
    print(f"Unique coin-windows: {len(firsts)}")

    # 4 rows × 3 cols
    fig = make_subplots(
        rows=4, cols=3,
        subplot_titles=[
            f"{c}  Δ → price" for c in COINS
        ] + [f"{c}  Δ → elapsed" for c in COINS] + [f"{c}  elapsed → price" for c in COINS],
        horizontal_spacing=0.08, vertical_spacing=0.07,
    )
    # Flatten subplot_titles: subplot_titles expects them in row-major order
    # Rebuild properly
    titles = []
    for c in COINS:
        titles += [f"{c}  Δ → price", f"{c}  Δ → elapsed", f"{c}  elapsed → price"]
    fig = make_subplots(
        rows=4, cols=3, subplot_titles=titles,
        horizontal_spacing=0.08, vertical_spacing=0.07,
    )

    for i, coin in enumerate(COINS):
        r = i + 1
        coin_firsts = [t for t in firsts.values() if t["coin"] == coin]
        up = [t for t in coin_firsts if t["outcome"] == "Up"]
        dn = [t for t in coin_firsts if t["outcome"] == "Down"]

        def hover_text(t):
            dt = datetime.fromtimestamp(t["window_start_ts"], tz=ET)
            return (f"{coin} {t['outcome']}<br>"
                    f"Window: {dt.strftime('%m/%d %H:%M ET')}<br>"
                    f"T={t['elapsed_in_window']}s<br>"
                    f"Price: {t['price']:.3f}<br>"
                    f"Δ: {t['delta_pct']:+.4f}%<br>"
                    f"Size: ${t['notional']:.2f}")

        # Column 1: Δ → price
        if up:
            fig.add_trace(go.Scatter(
                x=[t["delta_pct"] for t in up],
                y=[t["price"] for t in up],
                mode="markers", name=f"Up ({coin})",
                marker=dict(color="#4ade80", size=7, opacity=0.6),
                text=[hover_text(t) for t in up], hoverinfo="text",
                legendgroup=coin, showlegend=(i == 0),
            ), row=r, col=1)
        if dn:
            fig.add_trace(go.Scatter(
                x=[t["delta_pct"] for t in dn],
                y=[t["price"] for t in dn],
                mode="markers", name=f"Down ({coin})",
                marker=dict(color="#f87171", size=7, opacity=0.6),
                text=[hover_text(t) for t in dn], hoverinfo="text",
                legendgroup=coin, showlegend=(i == 0),
            ), row=r, col=1)

        # Column 2: Δ → elapsed
        if up:
            fig.add_trace(go.Scatter(
                x=[t["delta_pct"] for t in up],
                y=[t["elapsed_in_window"] for t in up],
                mode="markers",
                marker=dict(color="#4ade80", size=7, opacity=0.6),
                text=[hover_text(t) for t in up], hoverinfo="text",
                legendgroup=coin, showlegend=False,
            ), row=r, col=2)
        if dn:
            fig.add_trace(go.Scatter(
                x=[t["delta_pct"] for t in dn],
                y=[t["elapsed_in_window"] for t in dn],
                mode="markers",
                marker=dict(color="#f87171", size=7, opacity=0.6),
                text=[hover_text(t) for t in dn], hoverinfo="text",
                legendgroup=coin, showlegend=False,
            ), row=r, col=2)

        # Column 3: elapsed → price
        if up:
            fig.add_trace(go.Scatter(
                x=[t["elapsed_in_window"] for t in up],
                y=[t["price"] for t in up],
                mode="markers",
                marker=dict(color="#4ade80", size=7, opacity=0.6),
                text=[hover_text(t) for t in up], hoverinfo="text",
                legendgroup=coin, showlegend=False,
            ), row=r, col=3)
        if dn:
            fig.add_trace(go.Scatter(
                x=[t["elapsed_in_window"] for t in dn],
                y=[t["price"] for t in dn],
                mode="markers",
                marker=dict(color="#f87171", size=7, opacity=0.6),
                text=[hover_text(t) for t in dn], hoverinfo="text",
                legendgroup=coin, showlegend=False,
            ), row=r, col=3)

        # Axes
        fig.update_xaxes(title_text="Δ %", row=r, col=1)
        fig.update_yaxes(title_text="price", range=[0, 1], row=r, col=1)
        fig.update_xaxes(title_text="Δ %", row=r, col=2)
        fig.update_yaxes(title_text="elapsed (s)", range=[0, 900], row=r, col=2)
        fig.update_xaxes(title_text="elapsed (s)", range=[0, 900], row=r, col=3)
        fig.update_yaxes(title_text="price", range=[0, 1], row=r, col=3)

    fig.update_layout(
        title=dict(text="Scallops FIRST trade per coin-window (15m markets)",
                   font=dict(color="#60a5fa", size=18)),
        template="plotly_dark",
        paper_bgcolor="#0a0a0f", plot_bgcolor="#0a0a0f",
        height=1400, width=1500,
        hovermode="closest",
        legend=dict(bgcolor="#111128", bordercolor="#333"),
    )
    fig.update_annotations(font_color="#fbbf24", font_size=12)

    fig.write_html(str(OUT), include_plotlyjs="cdn")
    print(f"Saved: {OUT}")
    print(f"Open: open {OUT}")


if __name__ == "__main__":
    main()
