"""Breadth regime classifier.

Maps rolling new-high/new-low counts (1m / 5m / 20m windows) into one of
five regimes: THRUST UP, DRIFT UP, CHOP, DRIFT DOWN, CAPITULATION.

Inputs come from the same `highCounts` / `lowCounts` dicts that drive the
existing rate bars — the classifier is a pure function over that state plus
a small amount of internal debounce/transition tracking.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional


REGIMES = ("THRUST UP", "DRIFT UP", "CHOP", "DRIFT DOWN", "CAPITULATION")

REGIME_COLORS = {
    "THRUST UP":    "rgb(34,197,94)",
    "DRIFT UP":     "rgb(74,222,128)",
    "CHOP":         "rgb(234,179,8)",
    "DRIFT DOWN":   "rgb(239,68,68)",
    "CAPITULATION": "rgb(220,38,38)",
}


@dataclass
class RegimeResult:
    regime: str
    spread_1m: float       # (H-L)/(H+L) on 1m window, [-1, +1]
    spread_5m: float
    spread_20m: float
    intensity_1m: float    # (H+L)/N, fraction of universe hitting an extreme in 1m
    entered_at: float      # epoch seconds when current regime began
    seconds_in_regime: float
    transitioned_from: Optional[str]  # set on the tick the regime flipped, else None


class RegimeClassifier:
    """Stateful classifier with hysteresis + debounce.

    Why debounce: with 500+ symbols the 1m window flickers across thresholds
    constantly. We require a candidate regime to hold for `min_hold_secs`
    before officially transitioning. This trades a few seconds of latency
    for a much cleaner transition log.
    """

    def __init__(
        self,
        universe_size: int = 500,
        min_hold_secs: float = 15.0,
        thrust_spread: float = 0.55,
        thrust_intensity: float = 0.04,
        drift_spread: float = 0.20,
    ) -> None:
        self.universe_size = max(1, universe_size)
        self.min_hold_secs = min_hold_secs
        self.thrust_spread = thrust_spread
        self.thrust_intensity = thrust_intensity
        self.drift_spread = drift_spread

        self._current: str = "CHOP"
        self._entered_at: float = time.time()
        self._candidate: str = "CHOP"
        self._candidate_since: float = self._entered_at
        self._just_transitioned_from: Optional[str] = None

    def set_universe_size(self, n: int) -> None:
        if n > 0:
            self.universe_size = n

    @staticmethod
    def _spread(h: int, l: int) -> float:
        total = h + l
        if total <= 0:
            return 0.0
        return (h - l) / total

    def _raw_classify(self, s1: float, s5: float, intensity_1m: float) -> str:
        """Instantaneous classification without debounce."""
        if s1 >= self.thrust_spread and s5 >= self.drift_spread and intensity_1m >= self.thrust_intensity:
            return "THRUST UP"
        if s1 <= -self.thrust_spread and s5 <= -self.drift_spread and intensity_1m >= self.thrust_intensity:
            return "CAPITULATION"
        if s5 >= self.drift_spread and s1 >= 0.10:
            return "DRIFT UP"
        if s5 <= -self.drift_spread and s1 <= -0.10:
            return "DRIFT DOWN"
        return "CHOP"

    def classify(
        self,
        high_counts: dict,
        low_counts: dict,
        live_universe: Optional[int] = None,
        now: Optional[float] = None,
    ) -> RegimeResult:
        if now is None:
            now = time.time()
        if live_universe is not None:
            self.set_universe_size(live_universe)

        h1, l1 = high_counts.get("1m", 0),  low_counts.get("1m", 0)
        h5, l5 = high_counts.get("5m", 0),  low_counts.get("5m", 0)
        h20, l20 = high_counts.get("20m", 0), low_counts.get("20m", 0)

        s1  = self._spread(h1,  l1)
        s5  = self._spread(h5,  l5)
        s20 = self._spread(h20, l20)
        intensity_1m = (h1 + l1) / self.universe_size

        raw = self._raw_classify(s1, s5, intensity_1m)

        transitioned_from: Optional[str] = None
        if raw == self._current:
            self._candidate = raw
            self._candidate_since = now
        else:
            if raw != self._candidate:
                self._candidate = raw
                self._candidate_since = now
            elif now - self._candidate_since >= self.min_hold_secs:
                transitioned_from = self._current
                self._current = raw
                self._entered_at = now

        self._just_transitioned_from = transitioned_from

        return RegimeResult(
            regime=self._current,
            spread_1m=s1,
            spread_5m=s5,
            spread_20m=s20,
            intensity_1m=intensity_1m,
            entered_at=self._entered_at,
            seconds_in_regime=now - self._entered_at,
            transitioned_from=transitioned_from,
        )
