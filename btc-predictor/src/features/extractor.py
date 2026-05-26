"""Feature extractor — shared by training (offline tick replay) and live inference.

Produces a 21-dimensional feature vector representing the state of the current
15-minute candle at a given moment. Group C (order book) is deferred to v2 —
order book data isn't available in Binance daily trade zips, so v1 ships
trades-only features.

The math is pure numpy. The same `compute_features` function is called from:
  • training/dataset.py — at predefined sample seconds within each historical window
  • model/predictor.py  — on every tick from the Coinbase live WS

Both code paths pass identical inputs and get identical outputs. Don't fork it.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

FEATURE_NAMES = [
    # Group A — Position (7) — all derived from TWAP-60 smoothed prices
    "body_pct", "time_fraction", "body_time_product",
    "running_range_pct", "price_position_in_range",
    "upper_wick_pct", "lower_wick_pct",
    # Group B — Volume & Flow (5) — tick-based, NOT TWAP
    "volume_pace",
    "flow_imbalance_30s", "flow_imbalance_60s", "flow_imbalance_300s",
    "trade_rate_ratio",
    # Group D — 1-min sequence (3) — tick-based (sub-minute O/C)
    "consecutive_1min_run", "green_ratio", "avg_1min_body",
    # Group E — Context (6) — non-price
    "session_bucket", "day_of_week", "is_weekend",
    "prior_direction", "prior_body_pct", "prior_range_pct",
]
NUM_FEATURES = len(FEATURE_NAMES)  # 21

WIN_SECS = 900            # length of the 15-minute window
TWAP_WINDOW_SECS = 60     # TWAP lookback


def compute_twap_per_sec(
    ts_i8: np.ndarray,
    prices: np.ndarray,
    window_open_ns: int,
    window_secs: int = WIN_SECS,
    twap_window_secs: int = TWAP_WINDOW_SECS,
) -> np.ndarray:
    """1-second TWAP-60 series across a 15-min window.

    Returns an array of length window_secs+1 (= 901). twap[s] is the mean of
    all tick prices in the half-open interval
        (window_open + s − twap_window_secs,  window_open + s]
    For s < twap_window_secs (the first minute) the lookback is truncated to
    start at window_open because we don't have prior-window ticks here.
    NaN where the lookback contains no ticks.

    Vectorised with cumsum + searchsorted — runs in microseconds even for
    50k-tick windows.
    """
    n = window_secs + 1
    twap_window_ns = twap_window_secs * 1_000_000_000
    query_ns = window_open_ns + np.arange(n, dtype=np.int64) * 1_000_000_000

    # cumsum[i] = sum of prices[:i]  (so sum of prices[lo:hi] = cumsum[hi] − cumsum[lo])
    cumsum = np.empty(len(prices) + 1, dtype=np.float64)
    cumsum[0] = 0.0
    np.cumsum(prices.astype(np.float64), out=cumsum[1:])

    lo_idx = np.searchsorted(ts_i8, query_ns - twap_window_ns, side="left")
    hi_idx = np.searchsorted(ts_i8, query_ns,                   side="right")

    counts = hi_idx - lo_idx
    sums   = cumsum[hi_idx] - cumsum[lo_idx]

    out = np.full(n, np.nan, dtype=np.float64)
    valid = counts > 0
    out[valid] = sums[valid] / counts[valid]
    return out


def get_session_bucket(hour_utc: int) -> int:
    """Map UTC hour → session bucket 0–4.

    0 = Asia Dead Zone  (21:00–24:00)
    1 = Asia Session    (00:00–08:00)
    2 = London Open     (08:00–13:00)   — extends through London lunch
    3 = US Session      (13:00–17:00)
    4 = US Afternoon    (17:00–21:00)
    """
    if hour_utc >= 21:
        return 0
    if hour_utc < 8:
        return 1
    if hour_utc < 13:
        return 2
    if hour_utc < 17:
        return 3
    return 4


def _signed_direction(open_p: float, close_p: float) -> int:
    if close_p > open_p:
        return 1
    if close_p < open_p:
        return -1
    return 0


def compute_features(
    ts_i8: np.ndarray,           # int64 ns timestamps for ticks in current window
    prices: np.ndarray,          # float64  (raw tick prices)
    qtys: np.ndarray,            # float64
    signed_qtys: np.ndarray,     # float64 (+ buy aggressor, − sell aggressor)
    upto: int,                   # number of ticks to use (slice [:upto])
    window_open_ns: int,         # ns timestamp of 15-min window open
    sample_sec: int,             # seconds since window open
    completed_1min_candles: list[dict],  # [{minute_idx, open, close}, ...]
    prior_candle: dict | None,   # {open, high, low, close} of previous 15-min window
    baselines: dict,             # {avg_15min_volume, avg_trade_rate_per_sec}
    twap_per_sec: np.ndarray | None = None,  # optional 901-element TWAP-60 series
) -> dict:
    """Compute the 21-dim feature vector at a given moment.

    If `twap_per_sec` is provided, all position features use the TWAP-60
    smoothed price series. If None, position features fall back to raw
    tick prices (legacy raw-trained model path). Volume/flow features and
    sub-minute candles always stay tick-based.
    """
    if upto <= 0:
        raise ValueError("upto must be > 0; no ticks available")
    use_twap = twap_per_sec is not None
    # No sample_sec gate — engine retains prior 60s of ticks across window
    # rolls, so TWAP-60 is computable from sample_sec = 0 of a new window.
    # (Caller in current_features() still enforces sample_sec >= 1.)

    q = qtys[:upto]
    s = signed_qtys[:upto]
    t = ts_i8[:upto]

    if use_twap:
        # TWAP-based price references — Kalshi-aligned convention.
        # twap_per_sec[0] is the STRIKE = TWAP of [ws − 60, ws] (the 60
        # seconds BEFORE window open). twap_per_sec[sample_sec] is the
        # current TWAP of [t − 60, t].
        candle_open = float(twap_per_sec[0])
        cur_price   = float(twap_per_sec[sample_sec])
        _twap_slice = twap_per_sec[0 : sample_sec + 1]
        running_high = float(np.nanmax(_twap_slice))
        running_low  = float(np.nanmin(_twap_slice))
    else:
        # Legacy raw-tick path (raw-trained models).
        p = prices[:upto]
        candle_open = float(p[0])
        cur_price   = float(p[-1])
        running_high = float(p.max())
        running_low  = float(p.min())
    sample_ns = window_open_ns + sample_sec * 1_000_000_000

    # === Group A — Position ===
    body_pct = (cur_price - candle_open) / candle_open * 100.0
    time_fraction = sample_sec / 900.0
    body_time_product = body_pct * time_fraction
    running_range_pct = (running_high - running_low) / candle_open * 100.0
    rng = running_high - running_low
    price_position_in_range = (cur_price - running_low) / rng if rng > 1e-10 else 0.5
    upper_wick_pct = (running_high - max(candle_open, cur_price)) / candle_open * 100.0
    lower_wick_pct = (min(candle_open, cur_price) - running_low) / candle_open * 100.0

    # === Group B — Volume & Flow ===
    total_volume = float(q.sum())
    expected_volume = baselines["avg_15min_volume"] * time_fraction
    volume_pace = total_volume / max(expected_volume, 1e-10)

    def _flow_imbalance(window_secs: float) -> float:
        cutoff = sample_ns - int(window_secs * 1_000_000_000)
        start = int(np.searchsorted(t, cutoff, side="left"))
        if start >= upto:
            return 0.0
        sub_s = s[start:upto].sum()
        sub_a = q[start:upto].sum()
        return float(sub_s / sub_a) if sub_a > 1e-10 else 0.0

    flow_30 = _flow_imbalance(30)
    flow_60 = _flow_imbalance(60)
    flow_300 = _flow_imbalance(300)

    cutoff_30 = sample_ns - 30 * 1_000_000_000
    start_30 = int(np.searchsorted(t, cutoff_30, side="left"))
    trade_rate_30s = (upto - start_30) / 30.0
    trade_rate_ratio = trade_rate_30s / max(baselines["avg_trade_rate_per_sec"], 1e-10)

    # === Group D — 1-min sequence ===
    current_minute_idx = sample_sec // 60
    completed = [c for c in completed_1min_candles if c["minute_idx"] < current_minute_idx]
    if completed:
        directions = [_signed_direction(c["open"], c["close"]) for c in completed]
        last = directions[-1]
        consec = 0
        for d in reversed(directions):
            if d == last:
                consec += 1
            else:
                break
        consecutive_1min_run = consec * last  # signed
        greens = sum(1 for d in directions if d > 0)
        green_ratio = greens / len(directions)
        bodies = [abs(c["close"] - c["open"]) / c["open"] for c in completed]
        avg_1min_body = float(np.mean(bodies))
    else:
        consecutive_1min_run = 0
        green_ratio = 0.5
        avg_1min_body = 0.0

    # === Group E — Context ===
    window_dt = pd.Timestamp(window_open_ns, unit="ns", tz="UTC")
    hour = window_dt.hour
    session_bucket = get_session_bucket(hour)
    day_of_week = window_dt.dayofweek
    is_weekend = int(day_of_week >= 5)

    if prior_candle is not None:
        po = prior_candle["open"]; pc = prior_candle["close"]
        ph = prior_candle["high"]; pl = prior_candle["low"]
        prior_direction = _signed_direction(po, pc)
        prior_body_pct = (pc - po) / po * 100.0
        prior_range_pct = (ph - pl) / po * 100.0
    else:
        prior_direction = 0
        prior_body_pct = 0.0
        prior_range_pct = 0.0

    return {
        "body_pct": body_pct,
        "time_fraction": time_fraction,
        "body_time_product": body_time_product,
        "running_range_pct": running_range_pct,
        "price_position_in_range": price_position_in_range,
        "upper_wick_pct": upper_wick_pct,
        "lower_wick_pct": lower_wick_pct,
        "volume_pace": volume_pace,
        "flow_imbalance_30s": flow_30,
        "flow_imbalance_60s": flow_60,
        "flow_imbalance_300s": flow_300,
        "trade_rate_ratio": trade_rate_ratio,
        "consecutive_1min_run": consecutive_1min_run,
        "green_ratio": green_ratio,
        "avg_1min_body": avg_1min_body,
        "session_bucket": session_bucket,
        "day_of_week": day_of_week,
        "is_weekend": is_weekend,
        "prior_direction": prior_direction,
        "prior_body_pct": prior_body_pct,
        "prior_range_pct": prior_range_pct,
    }
