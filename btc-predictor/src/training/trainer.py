"""LightGBM trainer + evaluator for the 15-min candle predictor.

Design adapted from scripts/training_help.py:
  • Derived meta-features layered on top of the 21 base features.
  • Candle-level (not row-level) chronological split — all 14 sample rows
    of a window stay together so the model can't peek at adjacent samples.
  • Exponential recency weighting on the train set.
  • LightGBM with categorical features declared, early stopping on val.

Unit conversion: the extractor emits %-unit columns (body_pct = 0.5 means
0.5%). The derived-feature math and clip bounds below assume fractional
units, so load_and_prepare divides those columns by 100 at load time.
"""
from __future__ import annotations

from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, brier_score_loss, roc_auc_score

from src.features.extractor import FEATURE_NAMES

PERCENT_COLS = (
    "body_pct", "body_time_product",
    "running_range_pct", "upper_wick_pct", "lower_wick_pct",
    "prior_body_pct", "prior_range_pct",
)

DERIVED_FEATURE_NAMES = [
    "flow_short_long_alignment",
    "flow_momentum",
    "body_acceleration",
    "wick_asymmetry",
    "range_consumed_ratio",
    "late_candle_flag",
    "early_candle_flag",
]

# Feature order is the contract between training and inference. Don't reorder.
ALL_FEATURE_NAMES: list[str] = (
    list(FEATURE_NAMES) + DERIVED_FEATURE_NAMES + ["sample_sec"]
)

CATEGORICAL_FEATURES = [
    "session_bucket", "day_of_week", "is_weekend",
    "late_candle_flag", "early_candle_flag",
    # prior_direction and flow_short_long_alignment are -1/0/1 — LightGBM
    # categoricals must be non-negative, so we feed them as ordinal numeric.
]

LGBM_PARAMS = {
    "objective": "binary",
    "metric": "binary_logloss",
    "boosting_type": "gbdt",
    "num_leaves": 63,
    "learning_rate": 0.02,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "min_child_samples": 20,
    "lambda_l1": 0.05,
    "lambda_l2": 0.1,
    "verbose": -1,
    "seed": 42,
}


def add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute the 7 derived meta-features in place. Assumes fractional units."""
    eps = 1e-10
    out = df.copy()
    out["flow_short_long_alignment"] = (
        np.sign(out["flow_imbalance_30s"]) * np.sign(out["flow_imbalance_300s"])
    ).astype(int)
    out["flow_momentum"] = out["flow_imbalance_30s"] - out["flow_imbalance_300s"]
    out["body_acceleration"] = (
        out["body_pct"] / (out["time_fraction"] + eps)
    ).clip(-0.5, 0.5)
    out["wick_asymmetry"] = out["upper_wick_pct"] - out["lower_wick_pct"]
    expected_range = out["avg_1min_body"] * (out["sample_sec"] / 60) + eps
    out["range_consumed_ratio"] = (out["running_range_pct"] / expected_range).clip(0, 10)
    out["late_candle_flag"] = (out["sample_sec"] >= 600).astype(int)
    out["early_candle_flag"] = (out["sample_sec"] <= 180).astype(int)
    return out


def load_and_prepare(parquet_path: Path) -> pd.DataFrame:
    """Load the parquet, convert %-units → fractions, clip outliers, derive features."""
    df = pd.read_parquet(parquet_path)
    for c in PERCENT_COLS:
        df[c] = df[c] / 100.0
    for c in ("flow_imbalance_30s", "flow_imbalance_60s", "flow_imbalance_300s"):
        df[c] = df[c].clip(-1.0, 1.0)
    df["volume_pace"] = df["volume_pace"].clip(0, 5)
    df["trade_rate_ratio"] = df["trade_rate_ratio"].clip(0, 5)
    df = add_derived_features(df)
    # Guard against any unexpected NaNs
    nan_counts = df[ALL_FEATURE_NAMES].isna().sum()
    if nan_counts.sum() > 0:
        print("  NaN counts:")
        print(nan_counts[nan_counts > 0])
        df[ALL_FEATURE_NAMES] = df[ALL_FEATURE_NAMES].fillna(0.0)
    return df


def candle_level_split(
    df: pd.DataFrame,
    train_frac: float = 0.70,
    val_frac: float = 0.15,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Chronological split BY WINDOW. All 14 sample rows of a window stay together."""
    candles = df["window_open"].drop_duplicates().sort_values().to_numpy()
    n = len(candles)
    train_end = int(n * train_frac)
    val_end = int(n * (train_frac + val_frac))
    train_w = set(candles[:train_end])
    val_w = set(candles[train_end:val_end])
    test_w = set(candles[val_end:])
    return (
        df[df["window_open"].isin(train_w)].copy(),
        df[df["window_open"].isin(val_w)].copy(),
        df[df["window_open"].isin(test_w)].copy(),
    )


def recency_weights(window_open: pd.Series, half_life_days: float = 90.0) -> np.ndarray:
    """Exponential decay weights, normalized so mean = 1."""
    latest = window_open.max()
    days_old = (latest - window_open).dt.total_seconds() / 86400.0
    w = np.exp(-np.log(2) * days_old / half_life_days)
    return (w / w.mean()).to_numpy()


def train_model(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    half_life_days: float = 90.0,
    num_boost_round: int = 2000,
    early_stopping_rounds: int = 75,
) -> lgb.Booster:
    X_tr = train_df[ALL_FEATURE_NAMES].to_numpy()
    y_tr = train_df["label"].to_numpy()
    X_va = val_df[ALL_FEATURE_NAMES].to_numpy()
    y_va = val_df["label"].to_numpy()
    w_tr = recency_weights(train_df["window_open"], half_life_days)

    cat_idx = [ALL_FEATURE_NAMES.index(c) for c in CATEGORICAL_FEATURES]

    dtrain = lgb.Dataset(
        X_tr, y_tr, weight=w_tr,
        feature_name=ALL_FEATURE_NAMES,
        categorical_feature=cat_idx,
        free_raw_data=False,
    )
    dval = lgb.Dataset(
        X_va, y_va,
        reference=dtrain,
        feature_name=ALL_FEATURE_NAMES,
        free_raw_data=False,
    )

    callbacks = [
        lgb.early_stopping(stopping_rounds=early_stopping_rounds, verbose=False),
        lgb.log_evaluation(period=100),
    ]
    return lgb.train(
        LGBM_PARAMS, dtrain,
        num_boost_round=num_boost_round,
        valid_sets=[dval],
        callbacks=callbacks,
    )


def evaluate(model: lgb.Booster, test_df: pd.DataFrame) -> dict:
    X = test_df[ALL_FEATURE_NAMES].to_numpy()
    y = test_df["label"].to_numpy()
    prob = model.predict(X)
    pred = (prob > 0.5).astype(int)

    overall = {
        "accuracy": float(accuracy_score(y, pred)),
        "auc": float(roc_auc_score(y, prob)),
        "brier": float(brier_score_loss(y, prob)),
    }

    print(f"\n{'=' * 56}")
    print("TEST SET")
    print(f"{'=' * 56}")
    print(f"Accuracy : {overall['accuracy']:.4f}")
    print(f"AUC-ROC  : {overall['auc']:.4f}")
    print(f"Brier    : {overall['brier']:.4f}   (0.25 = no-skill)")

    print("\nBY TIME INTO CANDLE")
    print(f"{'sec':>6} | {'acc':>7} | {'mean_p':>7} | {'n':>7}")
    print("-" * 36)
    by_sec = []
    sample_sec = test_df["sample_sec"].to_numpy()
    for sec in sorted(test_df["sample_sec"].unique()):
        m = sample_sec == sec
        if m.sum() < 10:
            continue
        acc = float(accuracy_score(y[m], pred[m]))
        mp = float(prob[m].mean())
        by_sec.append({"sample_sec": int(sec), "accuracy": acc, "mean_prob": mp, "n": int(m.sum())})
        print(f"{int(sec):>4}s  | {acc:>7.4f} | {mp:>7.4f} | {int(m.sum()):>7,}")

    print("\nBY SESSION")
    session_names = {0: "Dead", 1: "Asia", 2: "London", 3: "US", 4: "USPM"}
    print(f"{'session':>8} | {'acc':>7} | {'n':>7}")
    print("-" * 30)
    by_session = []
    sess = test_df["session_bucket"].to_numpy()
    for s in sorted(test_df["session_bucket"].unique()):
        m = sess == s
        if m.sum() < 10:
            continue
        acc = float(accuracy_score(y[m], pred[m]))
        by_session.append({"session": int(s), "name": session_names.get(s, str(s)),
                           "accuracy": acc, "n": int(m.sum())})
        print(f"{session_names.get(s, s):>8} | {acc:>7.4f} | {int(m.sum()):>7,}")

    print("\nHIGH-CONFIDENCE FILTER")
    hc_results = []
    for thr in (0.55, 0.60, 0.65, 0.70):
        lo, hi = 1 - thr, thr
        m = (prob < lo) | (prob > hi)
        if m.sum() < 10:
            continue
        acc = float(accuracy_score(y[m], pred[m]))
        cov = float(m.mean())
        hc_results.append({"threshold": thr, "accuracy": acc, "coverage": cov, "n": int(m.sum())})
        print(f"  p<{lo:.2f} or p>{hi:.2f}  →  "
              f"acc={acc:.4f}  coverage={cov:.1%}  n={int(m.sum()):,}")

    imp = pd.Series(
        model.feature_importance(importance_type="gain"),
        index=ALL_FEATURE_NAMES,
    ).sort_values(ascending=False)
    print("\nTOP FEATURES BY GAIN")
    print(f"{'feature':<32} {'gain':>10}")
    print("-" * 44)
    for feat, score in imp.head(15).items():
        print(f"{feat:<32} {score:>10,.0f}")

    return {
        **overall,
        "by_sample_sec": by_sec,
        "by_session": by_session,
        "high_confidence": hc_results,
        "feature_importance": {k: float(v) for k, v in imp.items()},
    }
