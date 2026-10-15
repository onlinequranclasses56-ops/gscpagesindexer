"""Bing Webmaster Tools URL Submission API + IndexNow client.

Bing SubmitUrlBatch: POST https://ssl.bing.com/webmaster/api.svc/json/SubmitUrlBatch?apikey=KEY
    body {"siteUrl": ..., "urlList": [...]}, max 500 URLs per batch.
    Success = HTTP 200 (body is {"d":null} or empty).
GetUrlSubmissionQuota: GET .../GetUrlSubmissionQuota?siteUrl=...&apikey=...
    -> {"d": {"DailyQuota": n, "MonthlyQuota": m}} (remaining quota).

IndexNow (Bing, Yandex, ...): POST https://api.indexnow.org/indexnow
    body {"host": ..., "key": ..., "keyLocation": ..., "urlList": [...]}, max 10000.
    The key must be hosted at https://<host>/<key>.txt containing the key.

Honest limits: Bing has NO per-URL "indexed / not indexed" inspection API
(GetUrlInfo returns crawl info only). So we track submissions, not Bing
index status. A submission is a crawl hint, never an indexing guarantee.

Env vars (set in Render -> Environment; never shown in the UI):
  BING_API_KEY   Bing Webmaster Tools API key
  INDEXNOW_KEY   IndexNow key (same key file must be hosted on every site
                 as https://<host>/<KEY>.txt)
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request

BING_API_BASE = "https://ssl.bing.com/webmaster/api.svc/json"
INDEXNOW_ENDPOINT = "https://api.indexnow.org/indexnow"
BING_BATCH_MAX = 500
INDEXNOW_BATCH_MAX = 10000
_UA = {"User-Agent": "GSC-Indexer/1.0 (+bing-indexnow-client)"}


class BingError(Exception):
    """Bing Webmaster API failure."""


class IndexNowError(Exception):
    """IndexNow submission failure."""


def bing_configured():
    return bool(os.environ.get("BING_API_KEY"))


def indexnow_configured():
    return bool(os.environ.get("INDEXNOW_KEY"))


def resolve_indexnow_key(db_value=""):
    """Effective IndexNow key: INDEXNOW_KEY env var wins, otherwise the
    key stored in the tool's settings. The key is public by design (it is
    hosted as https://<host>/<key>.txt), so DB storage is safe."""
    return os.environ.get("INDEXNOW_KEY", "") or (db_value or "")


def _post_json(url, payload, timeout=30):
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={**_UA, "Content-Type": "application/json; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:300]
        raise BingError(f"HTTP {e.code}: {body}") from e


def submit_url_batch(api_key, site_url, urls):
    """Submit up to 500 URLs to Bing. Returns the number submitted."""
    urls = list(urls)
    if not urls:
        return 0
    if len(urls) > BING_BATCH_MAX:
        raise ValueError(f"Bing accepts max {BING_BATCH_MAX} URLs per batch")
    status, _ = _post_json(
        f"{BING_API_BASE}/SubmitUrlBatch?apikey={api_key}",
        {"siteUrl": site_url, "urlList": urls},
    )
    if status != 200:
        raise BingError(f"Bing SubmitUrlBatch returned HTTP {status}")
    return len(urls)


def get_quota(api_key, site_url, timeout=20):
    """Return (daily_remaining, monthly_remaining) for this site."""
    url = (f"{BING_API_BASE}/GetUrlSubmissionQuota"
           f"?siteUrl={urllib.parse.quote(site_url, safe='')}&apikey={api_key}")
    req = urllib.request.Request(url, headers=_UA)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise BingError(f"HTTP {e.code}: {e.read().decode(errors='replace')[:200]}") from e
    d = data.get("d", {}) if isinstance(data, dict) else {}
    return int(d.get("DailyQuota", 0) or 0), int(d.get("MonthlyQuota", 0) or 0)


def indexnow_submit(key, host, urls, key_location=None):
    """Submit URLs via IndexNow (Bing, Yandex, ...). Up to 10000 per call.
    Returns the number submitted."""
    urls = list(urls)
    if not urls:
        return 0
    if len(urls) > INDEXNOW_BATCH_MAX:
        raise ValueError(f"IndexNow accepts max {INDEXNOW_BATCH_MAX} URLs per call")
    payload = {
        "host": host,
        "key": key,
        "keyLocation": key_location or f"https://{host}/{key}.txt",
        "urlList": urls,
    }
    try:
        status, body = _post_json(INDEXNOW_ENDPOINT, payload)
    except BingError as e:
        raise IndexNowError(str(e)) from e
    if status == 200:
        return len(urls)
    if status == 429:
        raise IndexNowError("HTTP 429: IndexNow rate limit — try again later")
    if status in (400, 422):
        raise IndexNowError(f"HTTP {status}: rejected — {body.decode(errors='replace')[:200]}")
    if status == 403:
        raise IndexNowError("HTTP 403: key not accepted — is https://"
                            f"{host}/{key}.txt hosted with the key as its content?")
    raise IndexNowError(f"IndexNow returned HTTP {status}")


def chunked(urls, n):
    """Yield successive n-sized chunks."""
    urls = list(urls)
    for i in range(0, len(urls), n):
        yield urls[i:i + n]
