"""Instantiate the correct DataProvider from config + env vars."""
import os
from typing import Dict, List

DOCS_URL = "https://highlowtick.com/#brokers"


class ProviderLoadError(RuntimeError):
    pass


def _require_env(*names: str, broker: str, docs_url: str) -> Dict[str, str]:
    """Return env var values or raise ProviderLoadError if any are missing."""
    result = {}
    for name in names:
        val = os.getenv(name, "").strip()
        if not val:
            raise ProviderLoadError(
                f"[HighlowTicker] {broker} requires env var {name}.\n"
                f"  Setup guide: {docs_url}"
            )
        result[name] = val
    return result


def load_equity_provider(broker: str, symbols: List[str]):
    """Return a DataProvider instance for the given equity broker.

    Equity providers are available in HighlowTicker Pro.
    See https://highlowtick.com to upgrade.
    """
    raise ProviderLoadError(
        f"[HighlowTicker] Equity broker '{broker}' requires HighlowTicker Pro.\n"
        f"  Upgrade at: https://highlowtick.com\n"
        f"  (Crypto via Coinbase is free — set [crypto] broker = \"coinbase\" in config.toml)"
    )


def load_crypto_provider(broker: str, symbols: List[str]):
    """Return a DataProvider instance for the given crypto broker."""
    if broker == "coinbase":
        creds = _require_env(
            "COINBASE_API_KEY_USERNAME", "COINBASE_API_PRIVATE_KEY",
            broker=broker, docs_url=DOCS_URL,
        )
        from providers.coinbase_provider import CoinbaseProvider
        return CoinbaseProvider(
            creds["COINBASE_API_KEY_USERNAME"],
            creds["COINBASE_API_PRIVATE_KEY"],
            symbols,
        )

    raise ProviderLoadError(f"Unknown crypto broker: {broker}")
