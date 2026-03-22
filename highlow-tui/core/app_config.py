"""Load and validate ~/.highlowticker/config.toml."""
from pathlib import Path
from typing import Optional

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

CONFIG_PATH = Path.home() / ".highlowticker" / "config.toml"

EQUITY_BROKERS: set = set()  # Equity brokers require HighlowTicker Pro — https://highlowtick.com
CRYPTO_BROKERS = {"coinbase"}


class ConfigError(ValueError):
    pass


def load_config() -> dict:
    """Return parsed config dict. Empty dict if no config file exists."""
    if not CONFIG_PATH.exists():
        return {}
    with open(CONFIG_PATH, "rb") as f:
        return tomllib.load(f)


def get_equity_broker(cfg: dict) -> Optional[str]:
    """Return equity broker name or None if not configured.

    Equity brokers are only available in HighlowTicker Pro.
    See https://highlowtick.com to upgrade.
    """
    broker = cfg.get("equity", {}).get("broker")
    if broker is None:
        return None
    raise ConfigError(
        f"Equity broker '{broker}' requires HighlowTicker Pro.\n"
        f"  Upgrade at: https://highlowtick.com"
    )


def get_crypto_broker(cfg: dict) -> Optional[str]:
    """Return crypto broker name or None if not configured."""
    broker = cfg.get("crypto", {}).get("broker")
    if broker is None:
        return None
    if broker not in CRYPTO_BROKERS:
        raise ConfigError(
            f"Unknown crypto broker '{broker}'. Valid options: {sorted(CRYPTO_BROKERS)}\n"
            f"  See setup guide: https://highlowtick.com/#brokers"
        )
    return broker
