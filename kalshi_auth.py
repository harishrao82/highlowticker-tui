"""
kalshi_auth.py — Shared Kalshi API authentication.
Import from any script or notebook:
    from kalshi_auth import api_key, private_key, sign_headers
"""
import base64
import os
import time

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from dotenv import load_dotenv

load_dotenv()

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS   = "wss://api.elections.kalshi.com/trade-api/ws/v2"

api_key = os.environ["KALSHI_API_KEY"]

# Handle PEM stored as single line with literal \n (common in .env files)
_raw = os.environ["KALSHI_API_SECRET"]
if "\\n" in _raw:
    _raw = _raw.replace("\\n", "\n")
private_key = serialization.load_pem_private_key(_raw.encode(), password=None)


def sign_headers(method: str, path: str) -> dict:
    """Return signed auth headers for a Kalshi API request."""
    ts  = str(round(time.time() * 1000))
    msg = ts + method.upper() + path
    sig = private_key.sign(
        msg.encode(),
        asym_padding.PSS(mgf=asym_padding.MGF1(hashes.SHA256()),
                         salt_length=asym_padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "Content-Type":            "application/json",
    }
