"""Rate-based spike detector for new-high / new-low surges.

Where the regime classifier looks at *level* (H/L spread averaged over a
window), this looks at *rate of change*: a sudden burst of new lows
(or highs) in the last 30s relative to the recent baseline. Fires the
moment the surge happens — the regime classifier confirms the new state
~15s later.

State labels:
  HIGH SURGE  — new highs spike well above recent baseline
  LOW SURGE   — new lows spike (capitulation tell)
  TWO-SIDED   — both sides spike together (volatility, no direction)
  NONE        — quiet
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional


SPIKE_STATES = ("HIGH SURGE", "LOW SURGE", "TWO-SIDED", "NONE")

SPIKE_COLORS = {
    "HIGH SURGE": "rgb(34,197,94)",
    "LOW SURGE":  "rgb(220,38,38)",
    "TWO-SIDED":  "rgb(147,197,253)",
    "NONE":       "rgb(120,120,120)",
}


@dataclass
class SpikeResult:
    state: str
    h_30s: int
    l_30s: int
    h_baseline_mean: float
    l_baseline_mean: float
    h_z: float           # z-score of current 30s highs vs baseline
    l_z: float
    seconds_since_fire: Optional[float]  # None if no recent fire


class SpikeDetector:
    """Tracks 30s H/L counts and fires when the latest reading is a statistical outlier.

    Baseline = mean+std of the prior 4 minutes (samples older than 30s but newer
    than 4.5min). Latest sample (≤30s old) is compared against that baseline.
    Requires both an absolute floor (≥4) and z-score floor (≥2.5) to fire.
    """

    def __init__(
        self,
        z_threshold: float = 2.5,
        absolute_floor: int = 4,
        baseline_secs: float = 240.0,
        decay_secs: float = 20.0,
        min_samples: int = 6,
        flat_baseline_multiplier: float = 3.0,
    ) -> None:
        self.z_threshold = z_threshold
        self.absolute_floor = absolute_floor
        self.baseline_secs = baseline_secs
        self.decay_secs = decay_secs
        self.min_samples = min_samples
        # When baseline std is 0 (perfectly flat tape) z-score is undefined.
        # Fall back to: latest count must exceed mean * this multiplier and
        # absolute_floor. e.g. baseline mean of 2 + multiplier 3 → fire on ≥6.
        self.flat_baseline_multiplier = flat_baseline_multiplier

        self._history: deque = deque()  # (ts, h_30s, l_30s)
        self._last_fire_ts: Optional[float] = None
        self._last_fire_state: str = "NONE"

    @staticmethod
    def _mean_std(values: list) -> tuple[float, float]:
        n = len(values)
        if n == 0:
            return 0.0, 0.0
        mean = sum(values) / n
        if n == 1:
            return mean, 0.0
        var = sum((v - mean) ** 2 for v in values) / (n - 1)
        return mean, math.sqrt(max(var, 0.0))

    def update(
        self,
        high_counts: dict,
        low_counts: dict,
        now: Optional[float] = None,
    ) -> SpikeResult:
        if now is None:
            now = time.time()

        h_30s = high_counts.get("30s", 0)
        l_30s = low_counts.get("30s", 0)

        self._history.append((now, h_30s, l_30s))
        cutoff = now - (self.baseline_secs + 30)
        while self._history and self._history[0][0] < cutoff:
            self._history.popleft()

        baseline_h = [h for ts, h, _ in self._history if now - ts > 30]
        baseline_l = [l for ts, _, l in self._history if now - ts > 30]

        h_mean, h_std = self._mean_std(baseline_h)
        l_mean, l_std = self._mean_std(baseline_l)

        h_flat = l_flat = False
        if len(baseline_h) >= self.min_samples and h_std > 0:
            h_z = (h_30s - h_mean) / h_std
        elif len(baseline_h) >= self.min_samples and h_std == 0:
            h_z = 0.0
            h_flat = True
        else:
            h_z = 0.0
        if len(baseline_l) >= self.min_samples and l_std > 0:
            l_z = (l_30s - l_mean) / l_std
        elif len(baseline_l) >= self.min_samples and l_std == 0:
            l_z = 0.0
            l_flat = True
        else:
            l_z = 0.0

        h_fire = h_30s >= self.absolute_floor and (
            h_z >= self.z_threshold
            or (h_flat and h_30s >= max(self.absolute_floor, self.flat_baseline_multiplier * h_mean))
        )
        l_fire = l_30s >= self.absolute_floor and (
            l_z >= self.z_threshold
            or (l_flat and l_30s >= max(self.absolute_floor, self.flat_baseline_multiplier * l_mean))
        )

        if h_fire and l_fire:
            self._last_fire_ts = now
            self._last_fire_state = "TWO-SIDED"
        elif h_fire:
            self._last_fire_ts = now
            self._last_fire_state = "HIGH SURGE"
        elif l_fire:
            self._last_fire_ts = now
            self._last_fire_state = "LOW SURGE"

        if self._last_fire_ts is not None and (now - self._last_fire_ts) <= self.decay_secs:
            state = self._last_fire_state
            since = now - self._last_fire_ts
        else:
            state = "NONE"
            since = None

        return SpikeResult(
            state=state,
            h_30s=h_30s,
            l_30s=l_30s,
            h_baseline_mean=h_mean,
            l_baseline_mean=l_mean,
            h_z=h_z,
            l_z=l_z,
            seconds_since_fire=since,
        )
