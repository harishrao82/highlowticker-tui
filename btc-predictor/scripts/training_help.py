"""
BTC 15-Min Candle Predictor — Pre-Computed Feature Format
==========================================================

Adapted for training data where features are already computed
and sampled at 60-second intervals into each 15-min candle.

Input CSV schema:
    window_open       : UTC timestamp of candle open
    sample_sec        : seconds into candle when snapshot was taken (60–840)
    body_pct          : (current_price - open) / open
    time_fraction     : sample_sec / 900
    body_time_product : body_pct * time_fraction
    running_range_pct : (high - low so far) / open
    price_position_in_range : where current price sits in running range (0–1)
    upper_wick_pct    : upper wick as % of open
    lower_wick_pct    : lower wick as % of open
    volume_pace       : actual volume / expected volume at this time fraction
    flow_imbalance_30s  : buy-sell flow imbalance last 30s (-1 to +1)
    flow_imbalance_60s  : buy-sell flow imbalance last 60s
    flow_imbalance_300s : buy-sell flow imbalance last 300s
    trade_rate_ratio  : actual trade rate / baseline trade rate
    consecutive_1min_run : signed run of same-direction 1-min candles
    green_ratio       : fraction of completed 1-min candles that are green
    avg_1min_body     : average absolute body size of completed 1-min candles
    session_bucket    : 0=dead 1=asia 2=london 3=us 4=us_afternoon
    day_of_week       : 0=Monday … 6=Sunday
    is_weekend        : 1 if Saturday or Sunday
    prior_direction   : sign of prior 15-min candle body (1, -1, or 0)
    prior_body_pct    : prior candle body as % of its open
    prior_range_pct   : prior candle range as % of its open
    label             : 1 = candle closed green, 0 = closed red

Live prediction:
    At any point during a live candle, compute the same features
    for the current state and call predictor.predict(feature_dict).
    The model was trained on all sample_sec values simultaneously,
    so it handles any time-into-candle naturally via time_fraction.
"""

import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import joblib
from pathlib import Path
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE COLUMNS — single source of truth, used by training AND live inference
# ─────────────────────────────────────────────────────────────────────────────

FEATURE_COLS = [
    # Position group
    "body_pct",
    "time_fraction",
    "body_time_product",
    "running_range_pct",
    "price_position_in_range",
    "upper_wick_pct",
    "lower_wick_pct",
    # Volume & flow group
    "volume_pace",
    "flow_imbalance_30s",
    "flow_imbalance_60s",
    "flow_imbalance_300s",
    "trade_rate_ratio",
    # 1-min sequence group
    "consecutive_1min_run",
    "green_ratio",
    "avg_1min_body",
    # Context group
    "session_bucket",
    "day_of_week",
    "is_weekend",
    "prior_direction",
    "prior_body_pct",
    "prior_range_pct",
    # Derived features (computed during load — not in raw CSV)
    "flow_short_long_alignment",   # sign(30s) * sign(300s): are windows aligned?
    "flow_momentum",               # flow_imbalance_30s - flow_imbalance_300s: trending or reversing?
    "body_acceleration",           # body_pct / (time_fraction + 1e-10): pace of move
    "wick_asymmetry",              # upper_wick_pct - lower_wick_pct: directional bias in wicks
    "range_consumed_ratio",        # running_range_pct / (avg_1min_body * 15 + 1e-10)
    "late_candle_flag",            # 1 if sample_sec >= 600 (last 5 minutes)
    "early_candle_flag",           # 1 if sample_sec <= 180 (first 3 minutes)
]

TARGET_COL  = "label"
META_COLS   = ["window_open", "sample_sec"]   # kept for analysis, not used in model


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING & VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

def load_and_validate(filepath: str) -> pd.DataFrame:
    """
    Load the pre-computed feature CSV, validate integrity,
    and add derived features.
    """
    df = pd.read_csv(filepath, index_col=0)

    # ── Parse window_open ──────────────────────────────────────────────────
    df["window_open"] = pd.to_datetime(df["window_open"], utc=True)

    # ── Basic integrity checks ─────────────────────────────────────────────
    raw_cols = [c for c in FEATURE_COLS
                if c not in ("flow_short_long_alignment", "flow_momentum",
                             "body_acceleration", "wick_asymmetry",
                             "range_consumed_ratio", "late_candle_flag",
                             "early_candle_flag")]
    missing = [c for c in raw_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in data: {missing}")

    # sample_sec should be multiples of 60, between 60 and 840
    valid_secs = set(range(60, 900, 60))
    bad_secs = df[~df["sample_sec"].isin(valid_secs)]["sample_sec"].unique()
    if len(bad_secs) > 0:
        print(f"  Warning: unexpected sample_sec values: {bad_secs} — dropping")
        df = df[df["sample_sec"].isin(valid_secs)]

    # time_fraction should match sample_sec / 900
    expected_tf = df["sample_sec"] / 900
    tf_mismatch = (df["time_fraction"] - expected_tf).abs() > 0.001
    if tf_mismatch.sum() > 0:
        print(f"  Warning: {tf_mismatch.sum()} rows have time_fraction != sample_sec/900 — fixing")
        df.loc[tf_mismatch, "time_fraction"] = expected_tf[tf_mismatch]

    # body_time_product should match body_pct * time_fraction
    expected_btp = df["body_pct"] * df["time_fraction"]
    btp_mismatch = (df["body_time_product"] - expected_btp).abs() > 1e-6
    if btp_mismatch.sum() > 0:
        print(f"  Fixing {btp_mismatch.sum()} body_time_product values")
        df.loc[btp_mismatch, "body_time_product"] = expected_btp[btp_mismatch]

    # ── Clip extreme outliers ──────────────────────────────────────────────
    # Flow imbalances should be -1 to +1
    for col in ["flow_imbalance_30s", "flow_imbalance_60s", "flow_imbalance_300s"]:
        df[col] = df[col].clip(-1.0, 1.0)

    # volume_pace and trade_rate_ratio can spike — cap at 5x
    df["volume_pace"]       = df["volume_pace"].clip(0, 5)
    df["trade_rate_ratio"]  = df["trade_rate_ratio"].clip(0, 5)

    # ── Add derived meta-features ──────────────────────────────────────────
    eps = 1e-10

    # Are the short and long flow windows pointing the same direction?
    # +1 = aligned (momentum), -1 = opposed (reversal brewing), 0 = one is flat
    df["flow_short_long_alignment"] = (
        np.sign(df["flow_imbalance_30s"]) * np.sign(df["flow_imbalance_300s"])
    )

    # Is short-term flow stronger or weaker than the 300s background?
    # Positive = short-term accelerating in same direction (momentum)
    # Negative = short-term fading vs background (exhaustion)
    df["flow_momentum"] = df["flow_imbalance_30s"] - df["flow_imbalance_300s"]

    # How fast is the body building relative to elapsed time?
    # A body_pct of 0.5% at time_fraction 0.1 is much more aggressive
    # than the same body_pct at time_fraction 0.8
    df["body_acceleration"] = df["body_pct"] / (df["time_fraction"] + eps)

    # Wick asymmetry: positive = upper wick dominant (selling pressure above)
    # negative = lower wick dominant (buying pressure below)
    df["wick_asymmetry"] = df["upper_wick_pct"] - df["lower_wick_pct"]

    # How much of the "expected" range has already been consumed?
    # Expected range ≈ avg_1min_body * number of minutes
    # > 1 means unusually large range for this point in the candle
    expected_range = df["avg_1min_body"] * (df["sample_sec"] / 60) + eps
    df["range_consumed_ratio"] = (df["running_range_pct"] / expected_range).clip(0, 10)

    # Time position flags — model can learn different behavior late vs early
    df["late_candle_flag"]  = (df["sample_sec"] >= 600).astype(int)
    df["early_candle_flag"] = (df["sample_sec"] <= 180).astype(int)

    # ── Fill any NaNs ──────────────────────────────────────────────────────
    nan_counts = df[FEATURE_COLS].isna().sum()
    if nan_counts.sum() > 0:
        print(f"  NaN values found:\n{nan_counts[nan_counts > 0]}")
        df[FEATURE_COLS] = df[FEATURE_COLS].fillna(0.0)

    print(f"  Loaded {len(df):,} training rows")
    print(f"  Candles:  {df['window_open'].nunique():,} unique 15-min windows")
    print(f"  Samples:  {df['sample_sec'].value_counts().sort_index().to_dict()}")
    print(f"  Label:    {df[TARGET_COL].mean():.1%} green  "
          f"({df[TARGET_COL].sum():,} green / {(~df[TARGET_COL].astype(bool)).sum():,} red)")
    print(f"  Sessions: {df['session_bucket'].value_counts().sort_index().to_dict()}")
    print(f"  Weekends: {df['is_weekend'].mean():.1%} of rows")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# CHRONOLOGICAL TRAIN / VAL / TEST SPLIT
# ─────────────────────────────────────────────────────────────────────────────

def split_data(df: pd.DataFrame, train_frac=0.70, val_frac=0.15):
    """
    Split by candle open time — chronological, never random.
    All rows from the same candle stay in the same split.

    Why candle-level split (not row-level):
    Multiple rows from the same candle share the same label and
    highly correlated features. Splitting them across train/test
    would cause data leakage and overstate test accuracy.
    """
    candles = df["window_open"].sort_values().unique()
    n = len(candles)

    train_end = int(n * train_frac)
    val_end   = int(n * (train_frac + val_frac))

    train_candles = set(candles[:train_end])
    val_candles   = set(candles[train_end:val_end])
    test_candles  = set(candles[val_end:])

    train = df[df["window_open"].isin(train_candles)].copy()
    val   = df[df["window_open"].isin(val_candles)].copy()
    test  = df[df["window_open"].isin(test_candles)].copy()

    print(f"\n  Split (candle-level):")
    print(f"  Train: {len(train_candles):,} candles → {len(train):,} rows")
    print(f"  Val:   {len(val_candles):,} candles → {len(val):,} rows")
    print(f"  Test:  {len(test_candles):,} candles → {len(test):,} rows")

    return train, val, test


# ─────────────────────────────────────────────────────────────────────────────
# RECENCY WEIGHTING
# ─────────────────────────────────────────────────────────────────────────────

def recency_weights(window_open: pd.Series, half_life_days: float = 60.0) -> np.ndarray:
    """
    Exponential decay: data from `half_life_days` ago gets half the weight.
    Applied to training set only.
    """
    latest = window_open.max()
    days_old = (latest - window_open).dt.total_seconds() / 86400
    w = np.exp(-np.log(2) * days_old / half_life_days)
    return (w / w.mean()).values


# ─────────────────────────────────────────────────────────────────────────────
# TRAINING
# ─────────────────────────────────────────────────────────────────────────────

def train(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    half_life_days: float = 60.0,
):
    """
    Train LightGBM binary classifier.

    Key decisions:
    - sample_sec included as a feature so the model learns that
      confidence should naturally increase later in the candle
    - Recency weighting so recent regime matters more
    - Categorical features declared so LightGBM handles them correctly
    """
    import lightgbm as lgb

    # Include sample_sec as a feature — the model uses it to weight
    # signals appropriately at different points in the candle
    all_feature_cols = FEATURE_COLS + ["sample_sec"]

    X_train = train_df[all_feature_cols].values
    y_train = train_df[TARGET_COL].values
    X_val   = val_df[all_feature_cols].values
    y_val   = val_df[TARGET_COL].values

    w_train = recency_weights(train_df["window_open"], half_life_days)

    # Categorical feature indices for LightGBM
    cat_features = [
        all_feature_cols.index(c) for c in
        ["session_bucket", "day_of_week", "is_weekend",
         "prior_direction", "late_candle_flag", "early_candle_flag",
         "flow_short_long_alignment"]
        if c in all_feature_cols
    ]

    dtrain = lgb.Dataset(
        X_train, y_train,
        weight=w_train,
        feature_name=all_feature_cols,
        categorical_feature=cat_features,
        free_raw_data=False,
    )
    dval = lgb.Dataset(
        X_val, y_val,
        reference=dtrain,
        feature_name=all_feature_cols,
        free_raw_data=False,
    )

    params = {
        "objective":            "binary",
        "metric":               "binary_logloss",
        "boosting_type":        "gbdt",
        "num_leaves":           63,
        "learning_rate":        0.02,
        "feature_fraction":     0.8,
        "bagging_fraction":     0.8,
        "bagging_freq":         5,
        "min_child_samples":    20,
        "lambda_l1":            0.05,
        "lambda_l2":            0.1,
        "verbose":              -1,
        "seed":                 42,
    }

    callbacks = [
        lgb.early_stopping(stopping_rounds=75, verbose=False),
        lgb.log_evaluation(period=100),
    ]

    print("\n  Training LightGBM...")
    model = lgb.train(
        params,
        dtrain,
        num_boost_round=2000,
        valid_sets=[dval],
        callbacks=callbacks,
    )

    print(f"  Best iteration: {model.best_iteration}")
    return model, all_feature_cols


# ─────────────────────────────────────────────────────────────────────────────
# EVALUATION
# ─────────────────────────────────────────────────────────────────────────────

def evaluate(model, test_df: pd.DataFrame, all_feature_cols: list):
    """
    Full evaluation suite on the held-out test set.
    Breaks down accuracy by time-into-candle and session.
    """
    from sklearn.metrics import roc_auc_score, brier_score_loss, accuracy_score

    X_test     = test_df[all_feature_cols].values
    y_test     = test_df[TARGET_COL].values
    sample_sec = test_df["sample_sec"].values
    session    = test_df["session_bucket"].values

    prob  = model.predict(X_test)
    pred  = (prob > 0.5).astype(int)

    auc   = roc_auc_score(y_test, prob)
    brier = brier_score_loss(y_test, prob)
    acc   = accuracy_score(y_test, pred)

    print(f"\n  {'─'*54}")
    print(f"  OVERALL TEST RESULTS")
    print(f"  {'─'*54}")
    print(f"  Accuracy  : {acc:.4f}")
    print(f"  AUC-ROC   : {auc:.4f}")
    print(f"  Brier     : {brier:.4f}  (0.25 = no-skill baseline)")

    # ── By sample_sec (time into candle) ──────────────────────────────────
    print(f"\n  BY TIME INTO CANDLE:")
    print(f"  {'sample_sec':>12} | {'accuracy':>10} | {'mean_prob':>10} | {'n':>6}")
    print(f"  {'─'*46}")
    for sec in sorted(test_df["sample_sec"].unique()):
        mask = sample_sec == sec
        if mask.sum() < 10:
            continue
        sec_acc  = accuracy_score(y_test[mask], pred[mask])
        sec_prob = prob[mask].mean()
        print(f"  {int(sec):>10}s | {sec_acc:>10.4f} | {sec_prob:>10.4f} | {mask.sum():>6,}")

    # ── By session bucket ──────────────────────────────────────────────────
    session_names = {0: "Dead Zone", 1: "Asia", 2: "London", 3: "US", 4: "US Afternoon"}
    print(f"\n  BY SESSION:")
    print(f"  {'session':>14} | {'accuracy':>10} | {'n':>6}")
    print(f"  {'─'*36}")
    for s in sorted(test_df["session_bucket"].unique()):
        mask = session == s
        if mask.sum() < 10:
            continue
        s_acc = accuracy_score(y_test[mask], pred[mask])
        name  = session_names.get(s, str(s))
        print(f"  {name:>14} | {s_acc:>10.4f} | {mask.sum():>6,}")

    # ── High confidence filter ─────────────────────────────────────────────
    print(f"\n  HIGH-CONFIDENCE FILTER:")
    for threshold in [0.55, 0.60, 0.65, 0.70]:
        lo, hi = 1 - threshold, threshold
        mask = (prob < lo) | (prob > hi)
        if mask.sum() < 10:
            continue
        hc_acc = accuracy_score(y_test[mask], pred[mask])
        print(f"  p < {lo:.2f} or p > {hi:.2f}  →  "
              f"acc={hc_acc:.4f}  coverage={mask.mean():.1%}  n={mask.sum():,}")

    # ── Feature importance ─────────────────────────────────────────────────
    importance = pd.Series(
        model.feature_importance(importance_type="gain"),
        index=all_feature_cols,
    ).sort_values(ascending=False)

    print(f"\n  TOP FEATURES BY GAIN:")
    print(f"  {'feature':<35} {'gain':>10}")
    print(f"  {'─'*48}")
    for feat, score in importance.head(15).items():
        print(f"  {feat:<35} {score:>10,.0f}")

    return {"accuracy": acc, "auc": auc, "brier": brier}


# ─────────────────────────────────────────────────────────────────────────────
# LIVE FEATURE BUILDER
# Mirrors exactly what the training data generator computed.
# Call this in your WebSocket handler on every new 1-min candle close.
# ─────────────────────────────────────────────────────────────────────────────

def build_live_features(
    candle_open_price: float,
    current_price: float,
    running_high: float,
    running_low: float,
    elapsed_seconds: int,           # how many seconds into the 15-min candle
    volume_so_far: float,
    expected_volume: float,         # from historical baseline for this session
    flow_imbalance_30s: float,
    flow_imbalance_60s: float,
    flow_imbalance_300s: float,
    trade_rate_ratio: float,
    completed_1min_candles: list,   # list of dicts: {open, high, low, close, volume}
    session_bucket: int,
    day_of_week: int,
    prior_direction: int,           # -1, 0, or 1
    prior_body_pct: float,
    prior_range_pct: float,
) -> dict:
    """
    Compute the full feature vector for a live candle at any point.

    This function is the contract between the WebSocket handler and the model.
    Every feature here must match exactly what was computed during training.
    """
    eps = 1e-10

    open_px   = candle_open_price
    cur_px    = current_price
    tf        = min(elapsed_seconds / 900.0, 1.0)
    rng       = max(running_high - running_low, eps)

    body_pct             = (cur_px - open_px) / open_px
    time_fraction        = tf
    body_time_product    = body_pct * tf
    running_range_pct    = rng / open_px
    price_in_range       = (cur_px - running_low) / rng
    upper_wick_pct       = (running_high - max(open_px, cur_px)) / open_px
    lower_wick_pct       = (min(open_px, cur_px) - running_low) / open_px
    volume_pace          = min(volume_so_far / (expected_volume * tf + eps), 5.0)

    # 1-min sequence features from completed candles
    n = len(completed_1min_candles)
    if n > 0:
        directions     = [1 if c["close"] > c["open"] else -1 for c in completed_1min_candles]
        green_ratio    = sum(1 for d in directions if d == 1) / n
        avg_1min_body  = np.mean([abs(c["close"] - c["open"]) / c["open"]
                                  for c in completed_1min_candles])

        # Consecutive run in current direction
        run = 0
        for d in reversed(directions):
            if d == directions[-1]:
                run += 1
            else:
                break
        consecutive_1min_run = run * directions[-1]  # signed
    else:
        green_ratio          = 0.5
        avg_1min_body        = 0.0
        consecutive_1min_run = 0

    is_weekend = int(day_of_week >= 5)

    # Derived meta-features (must match load_and_validate)
    flow_short_long_alignment = int(
        np.sign(flow_imbalance_30s) * np.sign(flow_imbalance_300s)
    )
    flow_momentum      = flow_imbalance_30s - flow_imbalance_300s
    body_acceleration  = body_pct / (tf + eps)
    wick_asymmetry     = upper_wick_pct - lower_wick_pct
    expected_range     = avg_1min_body * (elapsed_seconds / 60) + eps
    range_consumed_ratio = min(running_range_pct / expected_range, 10.0)
    late_candle_flag   = int(elapsed_seconds >= 600)
    early_candle_flag  = int(elapsed_seconds <= 180)

    return {
        # Raw features
        "body_pct":                  body_pct,
        "time_fraction":             time_fraction,
        "body_time_product":         body_time_product,
        "running_range_pct":         running_range_pct,
        "price_position_in_range":   np.clip(price_in_range, 0, 1),
        "upper_wick_pct":            upper_wick_pct,
        "lower_wick_pct":            lower_wick_pct,
        "volume_pace":               volume_pace,
        "flow_imbalance_30s":        np.clip(flow_imbalance_30s, -1, 1),
        "flow_imbalance_60s":        np.clip(flow_imbalance_60s, -1, 1),
        "flow_imbalance_300s":       np.clip(flow_imbalance_300s, -1, 1),
        "trade_rate_ratio":          min(trade_rate_ratio, 5.0),
        "consecutive_1min_run":      consecutive_1min_run,
        "green_ratio":               green_ratio,
        "avg_1min_body":             avg_1min_body,
        "session_bucket":            session_bucket,
        "day_of_week":               day_of_week,
        "is_weekend":                is_weekend,
        "prior_direction":           prior_direction,
        "prior_body_pct":            prior_body_pct,
        "prior_range_pct":           prior_range_pct,
        # Derived meta-features
        "flow_short_long_alignment": flow_short_long_alignment,
        "flow_momentum":             flow_momentum,
        "body_acceleration":         np.clip(body_acceleration, -0.5, 0.5),
        "wick_asymmetry":            wick_asymmetry,
        "range_consumed_ratio":      range_consumed_ratio,
        "late_candle_flag":          late_candle_flag,
        "early_candle_flag":         early_candle_flag,
        # sample_sec is also a feature
        "sample_sec":                elapsed_seconds,
    }


# ─────────────────────────────────────────────────────────────────────────────
# PREDICTOR — wraps trained model for live inference
# ─────────────────────────────────────────────────────────────────────────────

class CandlePredictor:
    """
    Wraps the trained LightGBM model.
    Call .predict(feature_dict) from your WebSocket handler.
    """

    def __init__(self, model_path: str = "model.pkl"):
        artifact   = joblib.load(model_path)
        self.model = artifact["model"]
        self.cols  = artifact["feature_cols"]
        print(f"  Loaded model ({len(self.cols)} features, "
              f"best_iter={self.model.best_iteration})")

    def predict(self, feature_dict: dict) -> dict:
        """
        Takes a feature dict (from build_live_features) and returns prediction.

        Returns:
            p_green    : probability candle closes green (0–1)
            p_red      : 1 - p_green
            signal     : "GREEN" or "RED"
            confidence : how much to trust the signal (0–1)
            strength   : "WEAK" / "MODERATE" / "STRONG" / "HIGH"
        """
        X = np.array([[feature_dict.get(c, 0.0) for c in self.cols]])
        p_green = float(self.model.predict(X)[0])
        p_red   = 1.0 - p_green

        tf         = feature_dict.get("time_fraction", 0.0)
        signal_str = abs(p_green - 0.5) * 2           # 0 at 50/50, 1 at 0% or 100%
        time_conf  = min(tf * 1.5, 1.0)               # ramps up, saturates at ~67% of candle
        confidence = round(0.5 * time_conf + 0.5 * signal_str, 3)

        signal   = "GREEN" if p_green > 0.5 else "RED"
        strength = (
            "HIGH"     if confidence >= 0.70 else
            "STRONG"   if confidence >= 0.50 else
            "MODERATE" if confidence >= 0.30 else
            "WEAK"
        )

        return {
            "p_green":    round(p_green, 4),
            "p_red":      round(p_red, 4),
            "signal":     signal,
            "confidence": confidence,
            "strength":   strength,
            "elapsed_s":  feature_dict.get("sample_sec", 0),
        }


# ─────────────────────────────────────────────────────────────────────────────
# SYNTHETIC DATA GENERATOR (matches your CSV schema exactly)
# Replace with: df = load_and_validate("your_real_data.csv")
# ─────────────────────────────────────────────────────────────────────────────

def generate_synthetic_dataset(n_candles: int = 800) -> pd.DataFrame:
    """
    Generate synthetic data matching the exact CSV schema.
    Used for pipeline testing without real data.
    """
    rng   = np.random.default_rng(42)
    rows  = []
    t     = pd.Timestamp("2026-02-01", tz="UTC")
    DELTA = pd.Timedelta(minutes=15)

    prior_direction = 0
    prior_body_pct  = 0.0
    prior_range_pct = 0.0

    for _ in range(n_candles):
        # True label for this candle
        label     = int(rng.random() > 0.48)
        direction = 1 if label == 1 else -1

        # Session context
        h       = t.hour
        session = (1 if 0 <= h < 8 else 2 if 8 <= h < 12 else
                   3 if 13 <= h < 17 else 4 if 17 <= h < 21 else 0)
        dow     = t.dayofweek
        weekend = int(dow >= 5)

        # Candle-level stats
        final_body_pct  = direction * rng.lognormal(-4.5, 0.8)
        range_pct       = abs(final_body_pct) * rng.uniform(1.5, 3.5)
        total_volume    = rng.lognormal(5.0, 0.8)

        for sample_sec in range(60, 900, 60):
            tf = sample_sec / 900.0

            # Simulate partial body at this time fraction
            progress   = tf ** 0.7 + rng.normal(0, 0.05)
            body_pct   = final_body_pct * np.clip(progress, -0.5, 1.5)
            run_range  = range_pct * np.clip(tf * 1.3 + rng.normal(0, 0.1), 0.1, 1.5)

            upper_wick = max(run_range * rng.uniform(0.05, 0.3) -
                            max(0, body_pct) * rng.uniform(0, 0.5), 0)
            lower_wick = max(run_range * rng.uniform(0.05, 0.3) -
                            max(0, -body_pct) * rng.uniform(0, 0.5), 0)

            price_in_range = np.clip(0.5 + body_pct / (run_range + 1e-10) * 0.5
                                     + rng.normal(0, 0.1), 0, 1)

            vol_pace    = np.clip(rng.lognormal(0, 0.3), 0.1, 4.0)
            flow_bias   = direction * rng.uniform(0.0, 0.4)
            fi_30s      = np.clip(flow_bias + rng.normal(0, 0.3), -1, 1)
            fi_60s      = np.clip(flow_bias + rng.normal(0, 0.2), -1, 1)
            fi_300s     = np.clip(flow_bias + rng.normal(0, 0.15), -1, 1)
            tr_ratio    = np.clip(rng.lognormal(0, 0.4), 0.1, 4.0)

            n_1min      = int(sample_sec / 60)
            green_ratio = np.clip(0.5 + direction * rng.uniform(0, 0.3)
                                  + rng.normal(0, 0.1), 0, 1)
            run_dir     = direction if rng.random() > 0.4 else -direction
            consec_run  = int(run_dir * rng.integers(1, 4))
            avg_body    = abs(final_body_pct) / 15 * rng.uniform(0.5, 1.5)

            rows.append({
                "window_open":            t,
                "sample_sec":             sample_sec,
                "body_pct":               round(body_pct, 6),
                "time_fraction":          round(tf, 6),
                "body_time_product":      round(body_pct * tf, 6),
                "running_range_pct":      round(run_range, 6),
                "price_position_in_range":round(price_in_range, 6),
                "upper_wick_pct":         round(upper_wick, 6),
                "lower_wick_pct":         round(lower_wick, 6),
                "volume_pace":            round(vol_pace, 4),
                "flow_imbalance_30s":     round(fi_30s, 4),
                "flow_imbalance_60s":     round(fi_60s, 4),
                "flow_imbalance_300s":    round(fi_300s, 4),
                "trade_rate_ratio":       round(tr_ratio, 4),
                "consecutive_1min_run":   consec_run,
                "green_ratio":            round(green_ratio, 4),
                "avg_1min_body":          round(avg_body, 6),
                "session_bucket":         session,
                "day_of_week":            dow,
                "is_weekend":             weekend,
                "prior_direction":        prior_direction,
                "prior_body_pct":         round(prior_body_pct, 6),
                "prior_range_pct":        round(prior_range_pct, 6),
                "label":                  label,
            })

        prior_direction = direction
        prior_body_pct  = final_body_pct
        prior_range_pct = range_pct
        t += DELTA

    df = pd.DataFrame(rows)
    print(f"  Generated {len(df):,} rows across {n_candles} candles")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("BTC 15-Min Predictor — Pre-Computed Feature Pipeline")
    print("=" * 60)

    # ── 1. Load data ──────────────────────────────────────────────────────────
    print("\n[1/4] Loading data...")

    DATA_FILE = "training_data.csv"
    if Path(DATA_FILE).exists():
        print(f"  Using real data: {DATA_FILE}")
        df = load_and_validate(DATA_FILE)
    else:
        print(f"  {DATA_FILE} not found — using synthetic data")
        df = generate_synthetic_dataset(n_candles=800)
        df = load_and_validate.__wrapped__(df) if hasattr(load_and_validate, '__wrapped__') \
             else load_and_validate.__func__(df) if hasattr(load_and_validate, '__func__') \
             else _validate_synthetic(df)

    # ── 2. Split ──────────────────────────────────────────────────────────────
    print("\n[2/4] Splitting data...")
    train_df, val_df, test_df = split_data(df)

    # ── 3. Train ──────────────────────────────────────────────────────────────
    print("\n[3/4] Training...")
    model, all_feature_cols = train(train_df, val_df)

    joblib.dump({"model": model, "feature_cols": all_feature_cols}, "model.pkl")
    print(f"\n  Saved → model.pkl")

    # ── 4. Evaluate ───────────────────────────────────────────────────────────
    print("\n[4/4] Evaluating on test set...")
    metrics = evaluate(model, test_df, all_feature_cols)

    # ── 5. Live prediction demo ───────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("LIVE PREDICTION DEMO")
    print("Simulating a candle updating second by second")
    print("=" * 60)

    predictor = CandlePredictor("model.pkl")

    # Take one test candle and simulate live updates at each sample_sec
    test_candle_id = test_df["window_open"].iloc[0]
    candle_rows = test_df[test_df["window_open"] == test_candle_id].sort_values("sample_sec")
    true_label  = "GREEN" if candle_rows["label"].iloc[0] == 1 else "RED"

    print(f"\n  Candle: {test_candle_id}  |  True outcome: {true_label}")
    print(f"\n  {'sec':>5} | {'P(Green)':>9} | {'Signal':>7} | {'Conf':>6} | "
          f"{'Strength':>9} | flow_align | body_acc")
    print(f"  {'─'*72}")

    for _, row in candle_rows.iterrows():
        # Build feature dict from the row — in production this comes from build_live_features()
        feat = {col: row[col] for col in all_feature_cols if col in row.index}
        feat["sample_sec"] = int(row["sample_sec"])

        result = predictor.predict(feat)
        icon   = "🟢" if result["signal"] == "GREEN" else "🔴"
        fa     = f"{row.get('flow_short_long_alignment', 0):+.0f}"
        ba     = f"{row.get('body_acceleration', 0):+.4f}"

        print(
            f"  {int(row['sample_sec']):>5} | "
            f"{result['p_green']:>8.1%}  | "
            f"{icon}{result['signal']:>6} | "
            f"{result['confidence']:>5.2f}  | "
            f"{result['strength']:>9} | "
            f"{fa:>10} | {ba}"
        )

    print(f"\n  True close: {true_label}")
    print("\n" + "=" * 60)
    print("To use with real data:  df = load_and_validate('your_data.csv')")
    print("To predict live:        predictor.predict(build_live_features(...))")
    print("=" * 60)


# ── Shim so synthetic data goes through validation too ────────────────────────
def _validate_synthetic(df: pd.DataFrame) -> pd.DataFrame:
    """Apply validation and derived features to synthetic df (no file I/O)."""
    eps = 1e-10
    df["flow_short_long_alignment"] = (
        np.sign(df["flow_imbalance_30s"]) * np.sign(df["flow_imbalance_300s"])
    )
    df["flow_momentum"]      = df["flow_imbalance_30s"] - df["flow_imbalance_300s"]
    df["body_acceleration"]  = df["body_pct"] / (df["time_fraction"] + eps)
    df["wick_asymmetry"]     = df["upper_wick_pct"] - df["lower_wick_pct"]
    expected_range           = df["avg_1min_body"] * (df["sample_sec"] / 60) + eps
    df["range_consumed_ratio"] = (df["running_range_pct"] / expected_range).clip(0, 10)
    df["late_candle_flag"]   = (df["sample_sec"] >= 600).astype(int)
    df["early_candle_flag"]  = (df["sample_sec"] <= 180).astype(int)
    df[FEATURE_COLS] = df[FEATURE_COLS].fillna(0.0)
    return df