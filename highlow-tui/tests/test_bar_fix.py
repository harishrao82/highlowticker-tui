import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import time
import pytest
from providers._subscription import wall_clock_counts


def test_30s_bar_snaps_to_boundary():
    """A timestamp from the previous 30s window must NOT be counted."""
    now = time.time()
    window_start = now - (now % 30)
    t_previous_window = window_start - 1  # 1 second before current window
    counts = wall_clock_counts([t_previous_window])
    assert counts["30s"] == 0


def test_30s_bar_counts_current_window():
    """A timestamp inside the current 30s window MUST be counted."""
    now = time.time()
    window_start = now - (now % 30)
    t_current = window_start + 0.5
    counts = wall_clock_counts([t_current])
    assert counts["30s"] == 1


def test_1m_bar_counts_since_minute_boundary():
    """A timestamp since the last :00 mark is in the 1m window."""
    now = time.time()
    t_since_minute = now - (now % 60) + 1
    counts = wall_clock_counts([t_since_minute])
    assert counts["1m"] == 1


def test_multiple_timestamps_counted_correctly():
    now = time.time()
    window_start_30s = now - (now % 30)
    timestamps = [
        window_start_30s + 1,   # in 30s window
        window_start_30s + 2,   # in 30s window
        window_start_30s - 5,   # NOT in 30s window
    ]
    counts = wall_clock_counts(timestamps)
    assert counts["30s"] == 2
