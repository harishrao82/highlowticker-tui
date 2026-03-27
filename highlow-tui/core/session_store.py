"""Persist and restore intraday session state across restarts.

Saved to ~/.highlowticker/session_state.json.
State is only restored if it is from the same calendar date (ET) and
was written after 4 AM ET (covers pre-market through close).
"""
import json
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")
_STATE_PATH = Path.home() / ".highlowticker" / "session_state.json"


def save(state: dict) -> None:
    """Write state to disk.  Adds _saved_at and _date fields automatically."""
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    state["_saved_at"] = time.time()
    state["_date"]     = datetime.now(_ET).date().isoformat()
    try:
        with open(_STATE_PATH, "w") as f:
            json.dump(state, f)
    except Exception:
        pass


def load() -> dict | None:
    """Return saved state if it is valid for today's session, else None."""
    if not _STATE_PATH.exists():
        return None
    try:
        with open(_STATE_PATH) as f:
            state = json.load(f)
    except Exception:
        return None

    today = datetime.now(_ET).date().isoformat()
    if state.get("_date") != today:
        return None

    # Reject anything saved before 4 AM ET (stale from yesterday's session)
    saved_at  = state.get("_saved_at", 0)
    today_4am = datetime.now(_ET).replace(
        hour=4, minute=0, second=0, microsecond=0
    ).timestamp()
    if saved_at < today_4am:
        return None

    return state
