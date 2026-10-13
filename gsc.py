"""Google Search Console + Indexing API helpers (with retries)."""
import random
import time

from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/webmasters"]


def is_quota_error(e):
    msg = str(e).lower()
    return any(
        k in msg
        for k in (
            "quota", "quotaexceeded", "ratelimit", "rate limit",
            "userratelimitexceeded", "429",
        )
    )


def _call_with_retry(fn, tries=3):
    """Retry transient failures; quota errors are raised immediately."""
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            last = e
            if is_quota_error(e):
                raise
            time.sleep(2 ** i + random.random())
    raise last


def get_credentials(key_path):
    return service_account.Credentials.from_service_account_file(
        key_path, scopes=SCOPES
    )


def searchconsole_service(key_path):
    return build("searchconsole", "v1", credentials=get_credentials(key_path))


def indexing_service(key_path):
    # NOTE: Google officially supports the Indexing API ONLY for pages with
    # JobPosting or BroadcastEvent (in VideoObject) structured data.
    return build("indexing", "v3", credentials=get_credentials(key_path))


def inspect_url(sc, site_url, inspection_url):
    """Check one URL's index status. Costs 1 of 2000/day/property."""
    resp = _call_with_retry(
        lambda: sc.urlInspection()
        .index()
        .inspect(body={"inspectionUrl": inspection_url, "siteUrl": site_url})
        .execute()
    )
    status = resp.get("inspectionResult", {}).get("indexStatusResult", {})
    return {
        "verdict": status.get("verdict"),
        "coverage": status.get("coverageState", ""),
        "last_crawl": status.get("lastCrawlTime"),
    }


def is_indexed(inspection):
    cov = (inspection.get("coverage") or "").lower()
    return "indexed" in cov and "not indexed" not in cov


def publish_url(idx, url, action="URL_UPDATED"):
    """Push one URL to the Indexing API. Costs 1 of 200/day/project."""
    return _call_with_retry(
        lambda: idx.urlNotifications()
        .publish(body={"url": url, "type": action})
        .execute()
    )


def publish_batch(idx, urls, action="URL_UPDATED"):
    """Push up to 100 URLs in one batch request."""
    results = {}

    def cb(request_id, response, exception):
        results[request_id] = exception or response

    batch = idx.new_batch_http_request(callback=cb)
    for u in urls[:100]:
        batch.add(
            idx.urlNotifications().publish(body={"url": u, "type": action}),
            request_id=u,
        )
    batch.execute()
    return results


def submit_sitemap(sc, site_url, feedpath):
    """Resubmit a sitemap — the official crawl hint for general pages."""
    return sc.sitemaps().submit(siteUrl=site_url, feedpath=feedpath).execute()
