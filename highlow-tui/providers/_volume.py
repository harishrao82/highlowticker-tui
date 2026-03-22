"""VolumeTracker — wall-clock-aligned rolling volume spike detection.

Tracks per-symbol volume in 1-minute windows (default) aligned to wall-clock
boundaries (same philosophy as wall_clock_counts in _subscription.py).

Usage:
    tracker = VolumeTracker()
    ratio = tracker.record(sym, volume, ts)
    # ratio is the raw current_window / rolling_avg ratio, or None if not enough data.
    # Callers compare ratio against their configured threshold.
"""
import time
from typing import Dict, List, Optional


class VolumeTracker:
    """Detects volume spikes using wall-clock-aligned windows.

    Returns the raw ratio (current window volume / rolling average) when enough
    windows have been completed, None during the warmup period. The caller
    applies the spike threshold — this class only computes the ratio.
    """

    def __init__(
        self,
        window_seconds: int = 60,
        min_windows: int = 3,
    ) -> None:
        self._window = window_seconds
        self._min_windows = min_windows
        self._completed: Dict[str, List[float]] = {}  # sym → completed window volumes
        self._cur_vol:   Dict[str, float] = {}         # sym → accumulated vol this window
        self._cur_start: Dict[str, float] = {}         # sym → current window start time

    def record(self, sym: str, volume: float, ts: Optional[float] = None) -> Optional[float]:
        """Record volume for sym. Returns current/avg ratio when enough data, else None.

        None means "not enough history yet" — callers should not change spike state.
        Any float (including < 1.0) means "definitive reading" — callers can
        compare against their threshold and update spike state accordingly.
        """
        if not volume or volume <= 0:
            return None
        ts = ts or time.time()
        window_start = ts - (ts % self._window)  # snap to wall-clock boundary

        if sym not in self._cur_start:
            self._cur_start[sym] = window_start
            self._cur_vol[sym]   = volume
            return None

        if window_start > self._cur_start[sym]:
            # Window rolled — archive the completed window
            completed = self._completed.setdefault(sym, [])
            completed.append(self._cur_vol[sym])
            if len(completed) > 20:   # keep 20 windows max (20 min at 1-min windows)
                del completed[0]
            self._cur_start[sym] = window_start
            self._cur_vol[sym]   = volume
        else:
            self._cur_vol[sym] += volume

        completed = self._completed.get(sym, [])
        if len(completed) < self._min_windows:
            return None  # still in warmup

        avg = sum(completed) / len(completed)
        if not avg:
            return None

        return self._cur_vol[sym] / avg
