"""Shared HTTP session for API sources: bearer auth, timeouts, and retries
with exponential backoff on 429/5xx (respecting Retry-After)."""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline import config


def session(token: str | None = None, auth: bool = True) -> requests.Session:
    s = requests.Session()  # reuses TCP connections across paginated calls
    retry = Retry(
        total=4,                     # up to 4 retries per request
        backoff_factor=1,            # waits grow exponentially between tries (1s, 2s, 4s...)
        status_forcelist=(429, 500, 502, 503, 504),  # rate-limited or server errors = worth retrying
        allowed_methods=frozenset({"GET"}),          # only retry reads, which are safe to repeat
        respect_retry_after_header=True,             # if the API says "wait N seconds", obey it
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)      # apply the retry policy to every http:// URL...
    s.mount("https://", adapter)     # ...and every https:// URL
    if auth:  # public open-data APIs take no bearer token (auth=False)
        s.headers["Authorization"] = f"Bearer {token or config.MOCK_API_TOKEN}"
    return s


def get_json(s: requests.Session, url: str, params: dict | None = None) -> dict:
    resp = s.get(url, params=params, timeout=30)  # never hang forever on a slow API
    resp.raise_for_status()                       # 4xx/5xx (after retries) -> exception
    return resp.json()                            # parsed JSON body as a dict
