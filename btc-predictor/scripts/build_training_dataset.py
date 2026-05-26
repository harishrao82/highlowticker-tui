#!/usr/bin/env python3
"""Build the training dataset from Binance daily tick zips."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.collector.historical import list_zips  # noqa: E402
from src.training.dataset import (  # noqa: E402
    DEFAULT_SAMPLE_SECONDS,
    build_training_dataset,
    compute_baselines,
    print_dataset_summary,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--zip-dir",
        type=Path,
        default=Path.home() / "binance_ticks" / "BTCUSDT",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "data" / "processed" / "training_rows.parquet",
    )
    parser.add_argument(
        "--baselines-out",
        type=Path,
        default=REPO_ROOT / "data" / "processed" / "baselines.json",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="If set, process only the first N zips (smoke testing).",
    )
    args = parser.parse_args()

    zips = list_zips(args.zip_dir)
    if args.limit:
        zips = zips[: args.limit]
    print(f"Found {len(zips)} zips: {zips[0].name} → {zips[-1].name}")

    t0 = time.time()
    print("\nPass 1/2 — computing baselines (training 70% of range)...")
    baselines = compute_baselines(zips)
    print(f"  avg_15min_volume       = {baselines['avg_15min_volume']:.3f}")
    print(f"  avg_trade_rate_per_sec = {baselines['avg_trade_rate_per_sec']:.2f}")
    args.baselines_out.parent.mkdir(parents=True, exist_ok=True)
    args.baselines_out.write_text(json.dumps(baselines, indent=2))
    print(f"  saved → {args.baselines_out}")

    print("\nPass 2/2 — building training rows...")
    df = build_training_dataset(
        zip_paths=zips,
        baselines=baselines,
        output_path=args.output,
        sample_seconds=DEFAULT_SAMPLE_SECONDS,
    )
    print_dataset_summary(df, baselines)
    print(f"\nWrote {args.output} ({args.output.stat().st_size / 1e6:.1f} MB) "
          f"in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
