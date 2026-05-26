"""Live feature engine + predictor for realtime inference.

Mirrors the offline training pipeline exactly:
  • Same `compute_features` (21 base features)
  • Same %-unit → fraction conversion at predict time
  • Same `add_derived_features` (7 derived) and final column order

State machine:
  process_tick(ts_ns, price, qty, aggressor_buy)   # called on every Coinbase tick
  current_features()  → dict | None                # 21 base features at current moment
  predict_now(model, feature_cols) → dict | None   # full prediction payload
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd

from src.features.extractor import (
    TWAP_WINDOW_SECS,
    compute_features,
    compute_twap_per_sec,
)
from src.training.trainer import (
    ALL_FEATURE_NAMES,
    PERCENT_COLS,
    add_derived_features,
)

WIN_NS = 15 * 60 * 1_000_000_000
MIN_NS = 60 * 1_000_000_000


def parse_iso_to_ns(iso: str) -> int:
    """Coinbase ISO timestamp 'YYYY-MM-DDTHH:MM:SS.ffffffZ' → ns since epoch."""
    s = iso[:-1] + "+00:00" if iso.endswith("Z") else iso
    dt = datetime.fromisoformat(s)
    return int(dt.timestamp()) * 1_000_000_000 + dt.microsecond * 1_000


class LiveFeatureEngine:
    """Per-tick state machine that mirrors the training-time feature extractor."""

    def __init__(self, baselines: dict, use_twap_for_model: bool = False):
        self.baselines = baselines
        # When the currently-loaded model was trained on TWAP-60 features,
        # set True so position features fed to model.predict() are TWAP-based.
        # When False (raw-trained model), the model gets raw prices, but the
        # engine still ships TWAP-derived display values to the dashboard.
        self.use_twap_for_model = use_twap_for_model
        # Tick arrays for the CURRENT 15-min window only
        self.ts_ns: list[int] = []
        self.prices: list[float] = []
        self.qtys: list[float] = []
        self.signed_qtys: list[float] = []
        # Window state
        self.current_window_ns: Optional[int] = None
        self.window_open_px: Optional[float] = None
        self.window_high: Optional[float] = None
        self.window_low: Optional[float] = None
        self.started_mid_window: bool = False
        # Sub-minute state
        self.current_min_ns: Optional[int] = None
        self.current_min_ohlc: Optional[dict] = None
        self.completed_1min: list[dict] = []
        # Prior 15-min candle (set when a window closes cleanly)
        self.prior_candle: Optional[dict] = None

    def process_tick(self, ts_ns: int, price: float, qty: float, aggressor_buy: bool) -> None:
        win_ns = (ts_ns // WIN_NS) * WIN_NS
        min_ns = (ts_ns // MIN_NS) * MIN_NS

        if self.current_window_ns != win_ns:
            # Transition: either first-ever tick, or boundary cross
            if self.current_window_ns is not None and self.window_open_px is not None and self.prices:
                # Roll prior from the window we just left
                self.prior_candle = {
                    "open": self.window_open_px,
                    "high": self.window_high,
                    "low": self.window_low,
                    "close": self.prices[-1],
                }
                self.started_mid_window = False
            else:
                # First-ever tick — figure out if we caught the window cleanly
                late_by_sec = (ts_ns - win_ns) / 1e9
                self.started_mid_window = late_by_sec > 5.0

            # Retain the last 60s of the PREVIOUS window's ticks so the new
            # window's TWAP-60 strike (= TWAP of [win_ns − 60s, win_ns]) is
            # immediately computable from sec 0. Drop everything older.
            cutoff_ns = win_ns - 60 * 1_000_000_000
            keep_from = 0
            for i, t in enumerate(self.ts_ns):
                if t >= cutoff_ns:
                    keep_from = i
                    break
            else:
                keep_from = len(self.ts_ns)
            self.ts_ns = self.ts_ns[keep_from:]
            self.prices = self.prices[keep_from:]
            self.qtys = self.qtys[keep_from:]
            self.signed_qtys = self.signed_qtys[keep_from:]

            self.current_window_ns = win_ns
            self.completed_1min = []
            self.current_min_ns = None
            self.current_min_ohlc = None
            self.window_open_px = price
            self.window_high = price
            self.window_low = price

        # Minute roll (inside the same window)
        if self.current_min_ns is not None and self.current_min_ns != min_ns:
            idx = int((self.current_min_ns - self.current_window_ns) // MIN_NS)
            self.completed_1min.append({
                "minute_idx": idx,
                "open": self.current_min_ohlc["o"],
                "close": self.current_min_ohlc["c"],
            })

        if self.current_min_ns != min_ns:
            self.current_min_ns = min_ns
            self.current_min_ohlc = {"o": price, "h": price, "l": price, "c": price}
        else:
            self.current_min_ohlc["h"] = max(self.current_min_ohlc["h"], price)
            self.current_min_ohlc["l"] = min(self.current_min_ohlc["l"], price)
            self.current_min_ohlc["c"] = price

        # Append tick to window arrays
        self.ts_ns.append(ts_ns)
        self.prices.append(price)
        self.qtys.append(qty)
        sign = 1.0 if aggressor_buy else -1.0
        self.signed_qtys.append(qty * sign)

        if price > self.window_high:
            self.window_high = price
        if price < self.window_low:
            self.window_low = price

    def current_features(self) -> Optional[dict]:
        if self.current_window_ns is None or not self.prices:
            return None
        sample_ns = self.ts_ns[-1]
        sample_sec = int((sample_ns - self.current_window_ns) // 1_000_000_000)
        # Engine retains last 60s of prior-window ticks across boundaries, so
        # TWAP-60 is computable from sample_sec = 1 of the new window
        # (lookback spans prior 60s − sample_sec into new window). This means
        # the dashboard refreshes at the :00/:15/:30/:45 boundaries instead
        # of blacking out for the first minute. Predictions at sample_sec < 60
        # are slightly OOD (training only sampled at sec 60, 120, …) — they
        # tend toward 0.5 (WEAK) which is the correct "no signal" behavior.
        if sample_sec < 1:
            return None

        ts_arr = np.asarray(self.ts_ns, dtype=np.int64)
        p_arr = np.asarray(self.prices, dtype=np.float64)
        q_arr = np.asarray(self.qtys, dtype=np.float64)
        s_arr = np.asarray(self.signed_qtys, dtype=np.float64)

        # Always compute the per-second TWAP-60 series (for the dashboard).
        twap_per_sec = compute_twap_per_sec(ts_arr, p_arr, self.current_window_ns)

        feats = compute_features(
            ts_i8=ts_arr,
            prices=p_arr,
            qtys=q_arr,
            signed_qtys=s_arr,
            upto=len(p_arr),
            window_open_ns=self.current_window_ns,
            sample_sec=sample_sec,
            completed_1min_candles=self.completed_1min,
            prior_candle=self.prior_candle,
            baselines=self.baselines,
            # Only pass TWAP to the model when it was trained on TWAP features.
            twap_per_sec=twap_per_sec if self.use_twap_for_model else None,
        )
        feats["sample_sec"] = sample_sec
        # Always cache TWAP open/current for the dashboard payload — these
        # MUST match what the model uses internally so UP-bps on the page
        # equals the model's body_pct. cfb convention: twap[0] is the strike
        # (TWAP of [ws − 60, ws]), twap[sample_sec] is the current TWAP.
        feats["_twap_open"]    = float(twap_per_sec[0])
        feats["_twap_current"] = float(twap_per_sec[sample_sec])
        return feats

    def predict_now(self, model, feature_cols: list[str] = ALL_FEATURE_NAMES) -> Optional[dict]:
        feats = self.current_features()
        if feats is None:
            return None
        # Aggregate window stats so the browser doesn't need its own Coinbase WS.
        total_vol = float(sum(self.qtys))
        buy_vol  = float(sum(q for q in self.signed_qtys if q > 0))
        sell_vol = float(-sum(q for q in self.signed_qtys if q < 0))
        # Dashboard shows the TWAP-smoothed prices the model is conditioning
        # on, NOT the latest raw tick — so the UP-bps the user sees is the
        # exact body_pct the model is using as its #1 feature.
        return predict_from_features(
            model=model,
            feature_cols=feature_cols,
            base_features=feats,
            extra={
                "window_open_ns":     self.current_window_ns,
                "window_open_px":     feats.get("_twap_open", self.window_open_px),
                "window_high":        self.window_high,
                "window_low":         self.window_low,
                "current_price":      feats.get("_twap_current", self.prices[-1] if self.prices else None),
                "window_volume":      total_vol,
                "window_buy_vol":     buy_vol,
                "window_sell_vol":    sell_vol,
                "window_trade_count": len(self.prices),
                "stale":              self.started_mid_window,
            },
        )


FLOW_COLS = ("flow_imbalance_30s", "flow_imbalance_60s", "flow_imbalance_300s")


def predict_from_features(
    model,
    feature_cols: list[str],
    base_features: dict,
    extra: Optional[dict] = None,
) -> dict:
    """Apply training-time normalization, derive features, score, package output."""
    f = dict(base_features)
    # Internal TWAP refs are passed via base_features for the dashboard but
    # must not reach the model's feature vector.
    f.pop("_twap_open", None)
    f.pop("_twap_current", None)
    # % → fraction (same as load_and_prepare)
    for c in PERCENT_COLS:
        f[c] = f[c] / 100.0
    # Clipping (same as load_and_prepare)
    for c in FLOW_COLS:
        f[c] = max(-1.0, min(1.0, f[c]))
    f["volume_pace"] = max(0.0, min(5.0, f["volume_pace"]))
    f["trade_rate_ratio"] = max(0.0, min(5.0, f["trade_rate_ratio"]))

    # Derive 7 meta-features via the SAME function the trainer used
    df = pd.DataFrame([f])
    df = add_derived_features(df)

    X = df[feature_cols].to_numpy()
    p_green = float(model.predict(X)[0])
    p_red = 1.0 - p_green

    # SHAP-style per-feature log-odds contributions. LightGBM's pred_contrib
    # returns an array of shape (n_features + 1,) where the last element is
    # the bias term (the model's average log-odds prediction). Sum of all
    # contributions + bias ≈ logit(p_green). Positive contrib → pushes the
    # prediction toward UP; negative → toward DOWN.
    try:
        raw_contribs = model.predict(X, pred_contrib=True)[0]
        pairs = [(name, float(raw_contribs[i])) for i, name in enumerate(feature_cols)]
        pairs.sort(key=lambda kv: abs(kv[1]), reverse=True)
        top_drivers = []
        row0 = df.iloc[0]
        for name, shap_val in pairs[:8]:
            v = row0.get(name)
            if v is not None and isinstance(v, (int, float)) and not (isinstance(v, float) and v != v):
                vdisp: object = round(float(v), 6)
            else:
                vdisp = None
            top_drivers.append({"name": name, "shap": round(shap_val, 4), "value": vdisp})
        shap_out = {"bias": round(float(raw_contribs[-1]), 4), "top": top_drivers}
    except Exception:  # noqa: BLE001
        shap_out = None

    sample_sec = int(f["sample_sec"])
    # Signal strength: pure transform of P(YES). Confidence is now the model's
    # own probability, full stop — no time-weighting wrapper (that would
    # double-count time, which is already a feature the model sees).
    signal_strength = abs(p_green - 0.5) * 2.0  # 0 at 50/50, 1 at 0% or 100%

    # Strength buckets in terms of |P(YES) − 0.5|:
    #   ≥ 0.45 (P ≤ 5%   or ≥ 95%)  → MAX
    #   ≥ 0.30 (P ≤ 20%  or ≥ 80%)  → HIGH
    #   ≥ 0.15 (P ≤ 35%  or ≥ 65%)  → STRONG
    #   ≥ 0.05 (P ≤ 45%  or ≥ 55%)  → LEAN
    #   else                        → WEAK
    if   signal_strength >= 0.90: strength = "MAX"
    elif signal_strength >= 0.60: strength = "HIGH"
    elif signal_strength >= 0.30: strength = "STRONG"
    elif signal_strength >= 0.10: strength = "LEAN"
    else:                         strength = "WEAK"

    out = {
        "p_green": round(p_green, 4),
        "p_red": round(p_red, 4),
        "signal": "GREEN" if p_green > 0.5 else "RED",
        "signal_strength": round(signal_strength, 3),
        "strength": strength,
        "sample_sec": sample_sec,
        "body_pct": round(base_features["body_pct"], 4),  # in %-units for display
        # Per-candle feature snapshot — what the model is seeing right now.
        # All "_bps" fields are in basis points (1 bp = 0.01%) for display.
        # Flow imbalances stay in their native [-1, +1] range.
        "features": {
            # Position (TWAP-smoothed)
            "body_bps":                round(base_features["body_pct"] * 100, 2),
            "range_bps":               round(base_features["running_range_pct"] * 100, 2),
            "upper_wick_bps":          round(base_features["upper_wick_pct"] * 100, 2),
            "lower_wick_bps":          round(base_features["lower_wick_pct"] * 100, 2),
            "price_position_in_range": round(base_features["price_position_in_range"], 3),
            # Tick-based flow
            "flow_30s":                round(base_features["flow_imbalance_30s"], 3),
            "flow_60s":                round(base_features["flow_imbalance_60s"], 3),
            "flow_300s":               round(base_features["flow_imbalance_300s"], 3),
            "volume_pace":             round(base_features["volume_pace"], 2),
            "trade_rate_ratio":        round(base_features["trade_rate_ratio"], 2),
            # 1-min sub-candle sequence
            "consecutive_1min_run":    int(base_features["consecutive_1min_run"]),
            "green_ratio":             round(base_features["green_ratio"], 3),
            "avg_1min_body_bps":       round(base_features["avg_1min_body"] * 10000, 2),
            # Context (static for the window)
            "session_bucket":          int(base_features["session_bucket"]),
            "day_of_week":             int(base_features["day_of_week"]),
            "is_weekend":              int(base_features["is_weekend"]),
            "prior_direction":         int(base_features["prior_direction"]),
            "prior_body_bps":          round(base_features["prior_body_pct"] * 100, 2),
            "prior_range_bps":         round(base_features["prior_range_pct"] * 100, 2),
        },
    }
    if shap_out is not None:
        out["shap"] = shap_out
    if extra:
        for src_k, dst_k in (
            ("window_open_ns",     None),                # handled below
            ("window_open_px",     "window_open_px"),
            ("window_high",        "window_high"),
            ("window_low",         "window_low"),
            ("current_price",      "current_price"),
            ("window_volume",      "window_volume"),
            ("window_buy_vol",     "window_buy_vol"),
            ("window_sell_vol",    "window_sell_vol"),
            ("window_trade_count", "window_trade_count"),
        ):
            if dst_k is None:
                continue
            v = extra.get(src_k)
            if v is not None:
                out[dst_k] = float(v) if not isinstance(v, int) else int(v)
        if extra.get("window_open_ns") is not None:
            out["window_open_ts"] = extra["window_open_ns"] / 1e9
        if extra.get("stale"):
            out["stale_window"] = True
    return out
