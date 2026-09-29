"""PHI/PII safeguards.

- member_token(): keyed HMAC of member_id. Stable (so analysts can join and
  count members), but not reversible without PHI_HASH_KEY, and not guessable
  the way a plain md5(member_id) would be.
- redact(): strips direct identifiers from free text (CHW notes) before it is
  written to logs or sent anywhere outside the trust boundary.
"""
import hashlib
import hmac
import re

from pipeline import config

# (pattern, replacement) pairs, applied in order. Order matters: SSNs are
# replaced before phone numbers so a 9-digit SSN isn't half-matched as a phone.
_PATTERNS = [
    (re.compile(r"\bMEM\w+\b"), "[MEMBER_ID]"),                                  # member ids like MEM10042
    (re.compile(r"\b\d{3}[-.\s]?\d{2}[-.\s]?\d{4}\b"), "[SSN]"),                  # 123-45-6789
    (re.compile(r"\(?\b\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"), "[PHONE]"),          # (718) 555-0142
    (re.compile(r"\b\d{4}-\d{2}-\d{2}\b|\b\d{1,2}/\d{1,2}/\d{4}\b"), "[DATE]"),   # 1948-03-14, 3/14/1948
    (re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+"), "[EMAIL]"),                          # name@domain.com
]


def member_token(member_id: str, key: str | None = None) -> str:
    # Use the configured key unless a test passes its own.
    key = key if key is not None else config.PHI_HASH_KEY
    if not key:
        # An empty key would make tokens trivially reversible -- fail instead.
        raise RuntimeError("PHI_HASH_KEY is not set -- refusing to tokenize with an empty key.")
    # HMAC-SHA256(key, member_id) -> 64-char hex string. Same input + key
    # always gives the same token, so joins/counts still work in analytics.
    return hmac.new(key.encode(), member_id.encode(), hashlib.sha256).hexdigest()


def redact(text: str) -> str:
    # Run every pattern over the text, replacing matches with a placeholder.
    for pattern, replacement in _PATTERNS:
        text = pattern.sub(replacement, text)
    return text
