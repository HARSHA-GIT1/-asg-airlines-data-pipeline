"""
ASG Airlines — PII Masking Library
====================================
Reusable masking functions for Silver and Gold layers.
All functions are deterministic given the same salt, so joins on
hashed IDs (e.g. passenger_id lookups) remain consistent across runs.

Masking operations:
  - hash_pii(salt, value)          : SHA-256(salt + value), returns 64-char hex string
  - mask_email(email)              : first_char + "***@" + domain
  - mask_phone(phone)              : keep country code + last 2 digits, mask middle
  - mask_name(name)                : first char + "***"
"""

import hashlib


def hash_pii(salt: str, value) -> str | None:
    """Return SHA-256(salt + str(value)) as a 64-char hex string, or None if value is null."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    combined = (salt + raw).encode("utf-8")
    return hashlib.sha256(combined).hexdigest()


def mask_email(email) -> str | None:
    """
    Partial mask: {first_char}***@{domain}
    e.g.  "john.doe@gmail.com"  ->  "j***@gmail.com"
    """
    if email is None:
        return None
    s = str(email).strip()
    if "@" not in s:
        return "***"
    local, domain = s.split("@", 1)
    first = local[0] if local else "*"
    return f"{first}***@{domain}"


def mask_phone(phone) -> str | None:
    """
    Keep country code prefix (up to '-') + last 2 digits; mask middle with 'X'.
    Expected format: +CC-XXXXXXXXXX  (e.g. "+91-6896233790")
    Output          : +CC-XXXXXX90
    Falls back gracefully for unexpected formats.
    """
    if phone is None:
        return None
    s = str(phone).strip()
    if "-" in s:
        prefix, number = s.split("-", 1)
        if len(number) >= 2:
            masked_mid = "X" * (len(number) - 2)
            return f"{prefix}-{masked_mid}{number[-2:]}"
    # Fallback: keep first 3 and last 2 characters only
    if len(s) >= 5:
        return s[:3] + "X" * (len(s) - 5) + s[-2:]
    return "***"


def mask_name(name) -> str | None:
    """
    First character + "***"
    e.g.  "John Smith"  ->  "J***"
    """
    if name is None:
        return None
    s = str(name).strip()
    if not s:
        return "***"
    return f"{s[0]}***"
