"""Local license key validation — RSA-SHA256, fully offline.

Key format:  v{ver}.{base64url(JSON payload)}.{base64url(RSA-SHA256 signature)}
Payload:     {ver, mid, uid, pid, iat, seat}

Activation:  python app.py --activate <key>
  Sends key + machine fingerprint to https://highlowtick.com/activate,
  receives a machine-bound key, and saves it to config.toml.
"""
import base64
import hashlib
import json
import platform
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# Embedded public key — do not modify
_PUBLIC_KEY_PEM = b"""-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5CaSZHKt9bI3WY2C+0kw
bz3EImJ9z0XaKs+X4MJaRQWZOseYF5C3oRswBuL8cJzWlEayJC9KSptkxJnJQkgC
d0omCr8eyU87ss2UQQX73H+Mgfe1wxYlPfaqgaHijwcyn9i7kel2Ry0YdGiNFdJY
YLUooHFyCXB+bUUegayRWTxW4A7UZBZytT2apCUWoqU60xH9n48Fj9fdNtL8terp
8kU2yiLYpxj9UiJ4d3imLCNmnajNPx+aVgzYl+zvsjMTi7B7PlYSSgVoBgGFusxS
0ITrjR/KAznNrQXbo5geSaGtGZrU+jattv5eddri8gJRKNsyDh/ecTyseZhLxeVE
+wIDAQAB
-----END PUBLIC KEY-----"""

CONFIG_PATH  = Path.home() / ".highlowticker" / "config.toml"
ACTIVATE_URL = "https://highlowtick.com/activate"


@dataclass
class LicenseResult:
    valid:         bool
    version:       str   # "1" | "2" | ""
    machine_bound: bool
    machine_match: bool  # only meaningful when machine_bound is True
    message:       str   # user-visible; empty string = no message


def machine_id() -> str:
    """Stable machine fingerprint — hostname + MAC address, SHA-256 truncated."""
    raw = f"{platform.node()}{hex(uuid.getnode())}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def get_license_key() -> Optional[str]:
    """Read key from ~/.highlowticker/config.toml [license] key."""
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib  # type: ignore[no-redef]
    try:
        cfg = tomllib.loads(CONFIG_PATH.read_text())
        return cfg.get("license", {}).get("key") or None
    except Exception:
        return None


def save_license_key(key: str) -> None:
    """Write or update [license] key = "..." in config.toml."""
    import re
    try:
        text = CONFIG_PATH.read_text() if CONFIG_PATH.exists() else ""
    except Exception:
        text = ""

    key_line = f'key = "{key}"'
    if re.search(r"^\[license\]", text, re.MULTILINE):
        text = re.sub(r'(^\[license\][^\[]*?)key\s*=\s*"[^"]*"',
                      rf'\g<1>{key_line}', text, flags=re.MULTILINE | re.DOTALL)
    else:
        text = text.rstrip() + f"\n\n[license]\n{key_line}\n"

    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(text)


def validate(key: Optional[str] = None) -> LicenseResult:
    """Validate key signature and machine binding. Fully offline, no network."""
    if not key:
        return LicenseResult(valid=False, version="", machine_bound=False,
                             machine_match=False, message="")

    parts = key.split(".")
    if len(parts) != 3 or not parts[0].startswith("v"):
        return LicenseResult(valid=False, version="", machine_bound=False,
                             machine_match=False, message="Invalid key format.")

    version_tag, payload_b64, sig_b64 = parts
    version = version_tag[1:]

    # Verify RSA-SHA256 signature
    try:
        pub = serialization.load_pem_public_key(_PUBLIC_KEY_PEM)
        sig = base64.urlsafe_b64decode(sig_b64 + "==")
        pub.verify(sig, payload_b64.encode(), padding.PKCS1v15(), hashes.SHA256())
    except Exception:
        return LicenseResult(valid=False, version="", machine_bound=False,
                             machine_match=False,
                             message="Key verification failed — invalid signature.")

    # Decode payload
    try:
        payload = json.loads(base64.urlsafe_b64decode(payload_b64 + "=="))
    except Exception:
        return LicenseResult(valid=False, version="", machine_bound=False,
                             machine_match=False, message="Key payload decode failed.")

    mid           = payload.get("mid", "")
    machine_bound = mid != ""
    machine_match = (mid == machine_id()) if machine_bound else True

    if machine_bound and not machine_match:
        return LicenseResult(valid=True, version=version, machine_bound=True,
                             machine_match=False,
                             message="Warning: key is bound to a different machine.")

    return LicenseResult(valid=True, version=version, machine_bound=machine_bound,
                         machine_match=machine_match, message="")


def activate(key: str) -> str:
    """Bind an unbound key to this machine via ACTIVATE_URL.

    Returns the new machine-bound key. Raises RuntimeError on failure.
    """
    import urllib.request
    import urllib.error

    body = json.dumps({"key": key, "machine_id": machine_id()}).encode()
    req  = urllib.request.Request(
        ACTIVATE_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())["key"]
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Activation failed ({e.code}): {e.read().decode()}")
    except Exception as e:
        raise RuntimeError(f"Activation failed: {e}")
