import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.rotation_classifier import classify_rotation
from core.spike_detector import SpikeDetector
from core.persistent_leaders import find_persistent


def _stats(by_sector):
    """Helper: list of (sector, avg_pct, h, l, n) — n stubbed to 10 unless given."""
    out = []
    for s, p in by_sector.items():
        out.append((s, p, 0, 0, 10))
    return out


def test_rotation_quiet_when_dispersion_low():
    res = classify_rotation(_stats({
        "Technology": 0.05, "Financials": 0.06, "Healthcare": 0.04,
        "Consumer Disc": 0.05, "Comm Services": 0.05, "Industrials": 0.06,
        "Consumer Staples": 0.05, "Energy": 0.05, "Materials": 0.05,
        "Real Estate": 0.05, "Utilities": 0.05,
    }))
    assert res.state == "QUIET"


def test_rotation_risk_off():
    """Defensives mildly up, cyclicals mildly down — RISK-OFF (not FLIGHT)."""
    res = classify_rotation(_stats({
        "Technology": -0.15, "Financials": -0.10, "Consumer Disc": -0.20,
        "Comm Services": -0.10, "Industrials": -0.05, "Materials": -0.15,
        "Energy": -0.10, "Healthcare": 0.20, "Consumer Staples": 0.25,
        "Utilities": 0.15, "Real Estate": 0.20,
    }))
    assert res.state == "RISK-OFF"
    leader_names = [s for s, _ in res.leaders]
    assert any(n in {"Consumer Staples", "Utilities", "Healthcare", "Real Estate"} for n in leader_names)


def test_rotation_risk_on():
    res = classify_rotation(_stats({
        "Technology": 0.8, "Financials": 0.5, "Consumer Disc": 0.7,
        "Comm Services": 0.6, "Industrials": 0.4, "Materials": 0.5,
        "Energy": 0.4, "Healthcare": -0.2, "Consumer Staples": -0.3,
        "Utilities": -0.4, "Real Estate": -0.2,
    }))
    assert res.state == "RISK-ON"


def test_rotation_flight_to_quality():
    """Big separation + high dispersion => FLIGHT."""
    res = classify_rotation(_stats({
        "Technology": -1.5, "Financials": -1.2, "Consumer Disc": -1.4,
        "Comm Services": -1.3, "Industrials": -1.0, "Materials": -1.1,
        "Energy": -1.2, "Healthcare": 0.6, "Consumer Staples": 0.8,
        "Utilities": 0.7, "Real Estate": 0.5,
    }))
    assert res.state == "FLIGHT-TO-QUALITY"


def test_rotation_dispersion_no_clean_axis():
    """Sectors split but not along def/cyc lines — DISPERSION."""
    res = classify_rotation(_stats({
        "Technology": 0.7, "Financials": -0.7, "Consumer Disc": 0.6,
        "Comm Services": -0.6, "Industrials": 0.3, "Materials": -0.3,
        "Energy": 0.4, "Healthcare": 0.2, "Consumer Staples": -0.2,
        "Utilities": 0.1, "Real Estate": -0.1,
    }))
    assert res.state in {"DISPERSION", "ALIGNED"}


def test_spike_no_fire_without_baseline():
    sd = SpikeDetector(min_samples=6)
    res = sd.update({"30s": 20}, {"30s": 0}, now=0.0)
    assert res.state == "NONE"


def test_spike_low_surge_fires():
    """Variable baseline so z-score is well-defined."""
    sd = SpikeDetector(z_threshold=2.0, min_samples=4)
    for i, l in enumerate([1, 2, 1, 3, 2, 1, 2, 1]):
        sd.update({"30s": 1}, {"30s": l}, now=i * 30.0)
    res = sd.update({"30s": 1}, {"30s": 25}, now=8 * 30.0 + 1)
    assert res.state == "LOW SURGE"
    assert res.l_z >= 2.0


def test_spike_flat_baseline_fires_on_burst():
    """When baseline is perfectly flat, fall back to absolute multiplier."""
    sd = SpikeDetector(z_threshold=2.0, min_samples=4, flat_baseline_multiplier=3.0)
    for i in range(8):
        sd.update({"30s": 1}, {"30s": 1}, now=i * 30.0)
    res = sd.update({"30s": 1}, {"30s": 25}, now=8 * 30.0 + 1)
    assert res.state == "LOW SURGE"


def test_spike_decays_to_none():
    sd = SpikeDetector(z_threshold=2.0, min_samples=4, decay_secs=10.0)
    for i in range(8):
        sd.update({"30s": 1}, {"30s": 1}, now=i * 30.0)
    fire_t = 8 * 30.0 + 1
    sd.update({"30s": 1}, {"30s": 25}, now=fire_t)
    res = sd.update({"30s": 1}, {"30s": 1}, now=fire_t + 30)
    assert res.state == "NONE"


def test_persistent_leaders_basic():
    highs = [{"symbol": "AAA"}] * 5 + [{"symbol": "BBB"}] * 4 + [{"symbol": "CCC"}] * 1
    lows = []
    res = find_persistent(highs, lows, window=25, min_count=3)
    sym_counts = dict(res.highs)
    assert sym_counts["AAA"] == 5
    assert sym_counts["BBB"] == 4
    assert "CCC" not in sym_counts


def test_persistent_leaders_window_truncates():
    highs = [{"symbol": "OLD"}] * 10 + [{"symbol": "NEW"}] * 10
    res = find_persistent(highs, [], window=10, min_count=3)
    sym_counts = dict(res.highs)
    assert "OLD" in sym_counts
    assert "NEW" not in sym_counts
