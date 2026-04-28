import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.regime_classifier import RegimeClassifier, REGIMES


def _step(rc, h, l, t):
    counts_h = {"1m": h, "5m": h, "20m": h}
    counts_l = {"1m": l, "5m": l, "20m": l}
    return rc.classify(counts_h, counts_l, now=t)


def test_default_regime_is_chop():
    rc = RegimeClassifier(universe_size=500)
    res = _step(rc, 0, 0, 1000.0)
    assert res.regime == "CHOP"
    assert res.transitioned_from is None


def test_thrust_up_requires_debounce():
    rc = RegimeClassifier(universe_size=500, min_hold_secs=15.0)
    res = _step(rc, 50, 5, 1000.0)
    assert res.regime == "CHOP"

    res = _step(rc, 50, 5, 1010.0)
    assert res.regime == "CHOP"

    res = _step(rc, 50, 5, 1020.0)
    assert res.regime == "THRUST UP"
    assert res.transitioned_from == "CHOP"


def test_capitulation_path():
    rc = RegimeClassifier(universe_size=500, min_hold_secs=10.0)
    _step(rc, 5, 80, 0.0)
    _step(rc, 5, 80, 5.0)
    res = _step(rc, 5, 80, 11.0)
    assert res.regime == "CAPITULATION"


def test_drift_up_when_intensity_low():
    rc = RegimeClassifier(universe_size=500, min_hold_secs=5.0)
    counts_h = {"1m": 8,  "5m": 30, "20m": 60}
    counts_l = {"1m": 2,  "5m": 10, "20m": 25}
    rc.classify(counts_h, counts_l, now=0.0)
    res = rc.classify(counts_h, counts_l, now=10.0)
    assert res.regime == "DRIFT UP"


def test_flicker_does_not_flip():
    """Candidate that doesn't hold should not transition the regime."""
    rc = RegimeClassifier(universe_size=500, min_hold_secs=15.0)
    _step(rc, 50, 5, 0.0)
    _step(rc, 50, 5, 5.0)
    res = _step(rc, 0, 0, 10.0)
    assert res.regime == "CHOP"
    assert res.transitioned_from is None


def test_transition_only_reported_once():
    rc = RegimeClassifier(universe_size=500, min_hold_secs=10.0)
    _step(rc, 50, 5, 0.0)
    _step(rc, 50, 5, 5.0)
    res1 = _step(rc, 50, 5, 11.0)
    assert res1.transitioned_from == "CHOP"

    res2 = _step(rc, 50, 5, 12.0)
    assert res2.regime == "THRUST UP"
    assert res2.transitioned_from is None


def test_seconds_in_regime_grows():
    rc = RegimeClassifier(universe_size=500, min_hold_secs=5.0)
    _step(rc, 50, 5, 0.0)
    _step(rc, 50, 5, 6.0)
    res_a = _step(rc, 50, 5, 30.0)
    res_b = _step(rc, 50, 5, 60.0)
    assert res_b.seconds_in_regime > res_a.seconds_in_regime


def test_all_regime_names_unique_and_known():
    assert len(set(REGIMES)) == 5
    assert "CHOP" in REGIMES
    assert "THRUST UP" in REGIMES
    assert "CAPITULATION" in REGIMES
