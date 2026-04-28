#!/usr/bin/env python3
"""
scallops_ml_train.py — binary classifier: will Scallops trade BTC in next 60s?

Simpler target than multiclass. Output probability [0,1].
Good predictions = we've learned his timing rules.

Usage: python3 scallops_ml_train.py
"""
import numpy as np
import pandas as pd
from pathlib import Path

try:
    import xgboost as xgb
    from sklearn.metrics import (classification_report, roc_auc_score,
                                  precision_recall_curve, confusion_matrix)
except ImportError:
    import subprocess
    subprocess.check_call(["pip", "install", "xgboost", "scikit-learn"])
    import xgboost as xgb
    from sklearn.metrics import (classification_report, roc_auc_score,
                                  precision_recall_curve, confusion_matrix)

CSV = Path.home() / ".scallops_ml_btc.csv"

FEATURES = [
    "sec",
    "delta_BTC", "delta_ETH", "delta_SOL", "delta_XRP",
    "BTC_yes_bid", "BTC_yes_ask", "BTC_no_bid", "BTC_no_ask",
    "ETH_yes_bid", "ETH_yes_ask", "ETH_no_bid", "ETH_no_ask",
    "SOL_yes_bid", "SOL_yes_ask", "SOL_no_bid", "SOL_no_ask",
    "XRP_yes_bid", "XRP_yes_ask", "XRP_no_bid", "XRP_no_ask",
    "btc_up_shares", "btc_dn_shares",
    "btc_up_avg", "btc_dn_avg",
    "btc_total_cost",
    "btc_if_up", "btc_if_dn",
    "btc_net_exposure", "btc_hedge_ratio",
    "btc_edge_if_up_frac", "btc_edge_if_dn_frac",
    "target_profit_up_at_bid", "target_profit_dn_at_bid",
]


def main():
    df = pd.read_csv(CSV)
    print(f"Loaded {len(df):,} rows, target='will_trade'")
    print(f"Positive rate: {df['will_trade'].mean()*100:.1f}% "
          f"({df['will_trade'].sum():,} of {len(df):,})\n")

    # Chronological split by wts
    wts_sorted = sorted(df["wts"].unique())
    split_idx = int(len(wts_sorted) * 0.8)
    train_wts = set(wts_sorted[:split_idx])
    test_wts = set(wts_sorted[split_idx:])

    train = df[df["wts"].isin(train_wts)].dropna(subset=FEATURES)
    test = df[df["wts"].isin(test_wts)].dropna(subset=FEATURES)
    print(f"Train: {len(train):,} rows / {len(train_wts)} windows "
          f"(positive: {train['will_trade'].mean()*100:.1f}%)")
    print(f"Test:  {len(test):,} rows / {len(test_wts)} windows "
          f"(positive: {test['will_trade'].mean()*100:.1f}%)\n")

    X_train = train[FEATURES].values
    y_train = train["will_trade"].values.astype(int)
    X_test = test[FEATURES].values
    y_test = test["will_trade"].values.astype(int)

    # Class weight — balance the positives
    pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
    print(f"scale_pos_weight = {pos_weight:.2f}\n")

    clf = xgb.XGBClassifier(
        n_estimators=500,
        max_depth=5,
        learning_rate=0.05,
        objective="binary:logistic",
        eval_metric="auc",
        scale_pos_weight=pos_weight,
        random_state=42,
        n_jobs=-1,
    )
    clf.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    proba = clf.predict_proba(X_test)[:, 1]
    preds = (proba >= 0.5).astype(int)

    # ── Metrics ──────────────────────────────────────────────────────
    print("=" * 60)
    print("BINARY CLASSIFIER — 'will trade in next 60s'")
    print("=" * 60)
    print(f"\nTest ROC-AUC: {roc_auc_score(y_test, proba):.3f}  "
          "(0.5 = random, 1.0 = perfect)")
    print(f"Test accuracy @0.5: {(preds == y_test).mean():.3f}")

    print("\nClassification report @ threshold 0.5:")
    print(classification_report(y_test, preds, target_names=["no_trade", "will_trade"]))

    print("Confusion matrix (rows=true, cols=pred):")
    cm = confusion_matrix(y_test, preds)
    print(f"               no_trade  will_trade")
    print(f"  no_trade    {cm[0][0]:>8}  {cm[0][1]:>10}")
    print(f"  will_trade  {cm[1][0]:>8}  {cm[1][1]:>10}")

    # Precision-recall at different thresholds
    print("\nPrecision / recall at different probability thresholds:")
    print(f"{'thr':>5}  {'TP':>5}  {'FP':>5}  {'FN':>5}  {'Prec':>6}  {'Recall':>7}  {'Support':>8}")
    for thr in [0.3, 0.4, 0.5, 0.6, 0.7, 0.8]:
        pr = (proba >= thr).astype(int)
        tp = int(((pr == 1) & (y_test == 1)).sum())
        fp = int(((pr == 1) & (y_test == 0)).sum())
        fn = int(((pr == 0) & (y_test == 1)).sum())
        prec = tp / (tp + fp) if (tp + fp) else 0
        rec = tp / (tp + fn) if (tp + fn) else 0
        support = int(pr.sum())
        print(f"{thr:>5.2f}  {tp:>5}  {fp:>5}  {fn:>5}  {prec:>5.1%}  {rec:>6.1%}  {support:>8}")

    # Feature importance
    print("\nTop 15 most important features:")
    importances = clf.feature_importances_
    for name, imp in sorted(zip(FEATURES, importances), key=lambda x: -x[1])[:15]:
        print(f"  {name:>30}  {imp:.4f}")

    # Baseline comparison: just predicting majority class
    baseline = 1 - y_test.mean() if y_test.mean() < 0.5 else y_test.mean()
    print(f"\nBaseline (majority-class accuracy): {baseline:.3f}")
    print(f"Model uplift: {((preds == y_test).mean() - baseline) * 100:+.1f} pp")


if __name__ == "__main__":
    main()
