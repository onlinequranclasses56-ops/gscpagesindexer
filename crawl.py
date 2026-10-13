#!/usr/bin/env python3
"""Deep site crawler — discovers every page of a site by following internal
links from the homepage (BFS), beyond what the sitemap lists.

Same-site only, polite (delay + connection reuse), bounded (max pages/depth),
HTML-only, with URL canonicalization (fragments dropped, tracking params
stripped, trailing-slash normalized) so the same page isn't counted twice.
"""
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import (parse_qsl, urlencode, urljoin, urlparse, urlunparse)

import requests

HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)

# tracking params that create duplicate URLs of the same page
TRACK_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "fbclid", "gclid", "gbraid", "wbraid", "msclkid", "mc_cid",
    "mc_eid", "_ga", "yclid",
}

# never crawl these as pages
SKIP_EXT = (
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico", ".bmp",
    ".css", ".js", ".map", ".pdf", ".zip", ".rar", ".gz", ".mp4", ".webm",
    ".mp3", ".wav", ".woff", ".woff2", ".ttf", ".otf", ".eot", ".xml",
    ".txt", ".csv", ".json", ".rss", ".atom",
)

UA = "gsc-indexer/1.0 (+site-crawler; polite)"


def _norm_netloc(netloc):
    """Lowercased netloc (host:port kept) with www. stripped for comparison."""
    nl = (netloc or "").lower()
    if nl.startswith("www."):
        nl = nl[4:]
    return nl


def _same_site(netloc, start_netloc):
    return _norm_netloc(netloc) == _norm_netloc(start_netloc)


def canonicalize(netloc, url):
    """Return the canonical in-site URL, or None if off-site/unusable."""
    try:
        p = urlparse(url)
    except Exception:
        return None
    if p.scheme not in ("http", "https"):
        return None
    if not _same_site(p.netloc, netloc):
        return None
    path = p.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    if path.lower().endswith(SKIP_EXT):
        return None
    try:
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
             if k.lower() not in TRACK_PARAMS]
    except Exception:
        q = []
    query = urlencode(q)
    return urlunparse((p.scheme, p.netloc.lower(), path, "", query, ""))


def extract_links(html, base_url):
    """Raw href values resolved against base_url."""
    links = []
    for m in HREF_RE.finditer(html or ""):
        href = m.group(1).strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        links.append(urljoin(base_url, href))
    return links


def _fetch(session, url, delay, max_bytes=2_000_000):
    """GET a page; return list of raw links (HTML only), else []."""
    try:
        time.sleep(delay)
        r = session.get(url, timeout=12, headers={"User-Agent": UA},
                        allow_redirects=True)
        ctype = r.headers.get("Content-Type", "")
        if r.status_code != 200 or "html" not in ctype.lower():
            r.close()
            return []
        # cap download size
        chunks, size = [], 0
        for chunk in r.iter_content(65536, decode_unicode=True):
            if isinstance(chunk, str):
                chunks.append(chunk)
                size += len(chunk)
            if size > max_bytes:
                break
        r.close()
        return extract_links("".join(chunks), r.url or url)
    except Exception:
        return []


def crawl_site(start_url, max_pages=2000, max_depth=8, workers=8, delay=0.25):
    """BFS crawl from start_url. Returns a set of canonical in-site URLs
    (including the start page). Bounded and polite."""
    p0 = urlparse(start_url)
    if p0.scheme not in ("http", "https") or not p0.netloc:
        return set()
    netloc = p0.netloc.lower()
    start = canonicalize(netloc, start_url)
    if not start:
        return set()

    session = requests.Session()
    seen = {start}
    found = {start}
    frontier = [(start, 0)]  # (url, depth)

    while frontier and len(found) < max_pages:
        depth = frontier[0][1]
        batch, rest = [], []
        for u, d in frontier:
            (batch if d == depth else rest).append((u, d))
        frontier = rest
        if not batch:
            break
        with ThreadPoolExecutor(max_workers=workers) as ex:
            results = list(ex.map(lambda u: _fetch(session, u, delay),
                                  [u for u, _ in batch]))
        for (u, d), links in zip(batch, results):
            if d >= max_depth:
                continue
            for link in links:
                cu = canonicalize(netloc, link)
                if cu and cu not in seen:
                    seen.add(cu)
                    if len(found) < max_pages:
                        found.add(cu)
                        frontier.append((cu, d + 1))
    try:
        session.close()
    except Exception:
        pass
    return found
