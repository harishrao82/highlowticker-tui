import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
import json
import time
import core.license as lic   # import as module, not from — keeps monkeypatch working


@pytest.fixture(autouse=True)
def isolated_paths(tmp_path, monkeypatch):
    """Redirect CACHE_PATH and CONFIG_PATH to temp dir for every test."""
    monkeypatch.setattr(lic, "CACHE_PATH", tmp_path / "license_cache.json")
    monkeypatch.setattr(lic, "CONFIG_PATH", tmp_path / "config.json")


def test_no_key_returns_free():
    result = lic.validate_license(key=None)
    assert result.tier == "free"
    assert result.valid is False
    assert "free mode" in result.message.lower()


def test_valid_key_from_server_returns_pro():
    from unittest.mock import MagicMock, patch
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"valid": True, "tier": "pro"}
    with patch("httpx.post", return_value=mock_resp):
        result = lic.validate_license(key="good-key")
    assert result.valid is True
    assert result.tier == "pro"


def test_valid_key_writes_cache(tmp_path, monkeypatch):
    from unittest.mock import MagicMock, patch
    monkeypatch.setattr(lic, "CACHE_PATH", tmp_path / "license_cache.json")
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"valid": True, "tier": "pro"}
    with patch("httpx.post", return_value=mock_resp):
        lic.validate_license(key="good-key")
    assert (tmp_path / "license_cache.json").exists()


def test_cached_key_skips_server_call():
    """Second call within 24h must not call the server."""
    from unittest.mock import patch
    cache = {"key": "cached-key", "valid": True, "tier": "pro",
             "cached_at": time.time()}
    lic.CACHE_PATH.write_text(json.dumps(cache))
    with patch("httpx.post", side_effect=AssertionError("must not call server")):
        result = lic.validate_license(key="cached-key")
    assert result.valid is True


def test_server_down_uses_cache():
    from unittest.mock import patch
    cache = {"key": "offline-key", "valid": True, "tier": "pro",
             "cached_at": time.time()}
    lic.CACHE_PATH.write_text(json.dumps(cache))
    with patch("httpx.post", side_effect=Exception("network error")):
        result = lic.validate_license(key="offline-key")
    assert result.valid is True


def test_server_down_no_cache_returns_free():
    from unittest.mock import patch
    with patch("httpx.post", side_effect=Exception("network error")):
        result = lic.validate_license(key="some-key")
    assert result.tier == "free"
    assert "offline" in result.message.lower()


def test_invalid_key_returns_free():
    from unittest.mock import MagicMock, patch
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"valid": False, "tier": "free"}
    with patch("httpx.post", return_value=mock_resp):
        result = lic.validate_license(key="bad-key")
    assert result.valid is False
    assert result.tier == "free"


def test_get_license_key_returns_none_when_no_config():
    # CONFIG_PATH doesn't exist (isolated_paths fixture redirects to tmp_path)
    result = lic.get_license_key()
    assert result is None


def test_get_license_key_returns_none_when_key_missing():
    import json
    lic.CONFIG_PATH.write_text(json.dumps({"other": "field"}))
    result = lic.get_license_key()
    assert result is None


def test_get_license_key_returns_key_when_present():
    import json
    lic.CONFIG_PATH.write_text(json.dumps({"license_key": "my-license-key"}))
    result = lic.get_license_key()
    assert result == "my-license-key"
