"""Persistent leaders: symbols that keep printing on the highs/lows tables.

A symbol with one new-high entry is noise; a symbol with five entries in
the most recent 25 prints is the real trend. This module surfaces the
repeaters separately from the firehose.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass


@dataclass
class PersistentLeaders:
    highs: list  # [(symbol, frequency), ...] sorted desc
    lows: list


def find_persistent(
    session_highs,
    session_lows,
    window: int = 25,
    min_count: int = 3,
    top_n: int = 4,
) -> PersistentLeaders:
    """Return symbols that appear at least `min_count` times in the most
    recent `window` entries of each table.

    session_highs / session_lows: iterables of dicts with a `symbol` key —
    matches the deque structure used by HighLowTUI.session_highs / .session_lows.
    """
    def _top(entries):
        recent = list(entries)[:window]
        counts = Counter(e["symbol"] for e in recent if "symbol" in e)
        ranked = [(sym, c) for sym, c in counts.items() if c >= min_count]
        ranked.sort(key=lambda x: x[1], reverse=True)
        return ranked[:top_n]

    return PersistentLeaders(
        highs=_top(session_highs),
        lows=_top(session_lows),
    )
