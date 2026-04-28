"""Sector rotation classifier.

Classifies the cross-sector picture into one of:
  RISK-ON, RISK-OFF, FLIGHT-TO-QUALITY, DISPERSION, ALIGNED, QUIET.

Distinct from the regime classifier (which only sees the aggregate H/L spread):
this looks at *which* sectors are leading vs lagging, and labels the kind of
flow the tape is showing.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# Standard sector buckets — names must match keys in tickers/sectors.json
DEFENSIVE_SECTORS = {"Consumer Staples", "Utilities", "Healthcare", "Real Estate"}
CYCLICAL_SECTORS = {
    "Technology", "Consumer Disc", "Comm Services",
    "Financials", "Industrials", "Materials", "Energy",
}

ROTATION_STATES = (
    "RISK-ON", "RISK-OFF", "FLIGHT-TO-QUALITY",
    "DISPERSION", "ALIGNED", "QUIET",
)

ROTATION_COLORS = {
    "RISK-ON":           "rgb(34,197,94)",
    "RISK-OFF":          "rgb(239,68,68)",
    "FLIGHT-TO-QUALITY": "rgb(220,38,38)",
    "DISPERSION":        "rgb(147,197,253)",
    "ALIGNED":           "rgb(234,179,8)",
    "QUIET":             "rgb(120,120,120)",
}


@dataclass
class RotationResult:
    state: str
    leaders: list      # [(sector, score), ...] top 2 by avg_pct
    laggards: list     # [(sector, score), ...] bottom 2 by avg_pct
    separation: float  # avg(defensive) - avg(cyclical), in pct points (positive = defensives leading)
    dispersion: float  # top score - bottom score, in pct points


def classify_rotation(
    sector_stats: list,
    quiet_threshold: float = 0.30,
    flight_separation: float = 0.40,
    flight_dispersion: float = 0.80,
    risk_separation: float = 0.20,
    dispersion_threshold: float = 0.60,
) -> RotationResult:
    """sector_stats: list of (sector_name, avg_pct, h_count, l_count, n_symbols).

    All thresholds in percentage points (e.g. 0.30 = 0.30%).
    """
    usable = [(s, p) for s, p, _, _, n in sector_stats if n > 0 and s != "ETF"]
    if len(usable) < 4:
        return RotationResult(
            state="QUIET", leaders=[], laggards=[],
            separation=0.0, dispersion=0.0,
        )

    sorted_by_score = sorted(usable, key=lambda x: x[1], reverse=True)
    leaders = sorted_by_score[:2]
    laggards = sorted_by_score[-2:][::-1]  # most-negative first
    dispersion = sorted_by_score[0][1] - sorted_by_score[-1][1]

    def_scores = [p for s, p in usable if s in DEFENSIVE_SECTORS]
    cyc_scores = [p for s, p in usable if s in CYCLICAL_SECTORS]

    if not def_scores or not cyc_scores:
        separation = 0.0
    else:
        separation = (sum(def_scores) / len(def_scores)) - (sum(cyc_scores) / len(cyc_scores))

    if dispersion < quiet_threshold:
        state = "QUIET"
    elif separation > flight_separation and dispersion > flight_dispersion:
        state = "FLIGHT-TO-QUALITY"
    elif separation > risk_separation:
        state = "RISK-OFF"
    elif separation < -risk_separation:
        state = "RISK-ON"
    elif dispersion > dispersion_threshold:
        state = "DISPERSION"
    else:
        state = "ALIGNED"

    return RotationResult(
        state=state,
        leaders=leaders,
        laggards=laggards,
        separation=separation,
        dispersion=dispersion,
    )
