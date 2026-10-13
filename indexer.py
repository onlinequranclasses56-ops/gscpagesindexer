#!/usr/bin/env python3
"""Multi-property GSC indexing pipeline — tuned for 100+ properties.

Daily flow per property:
  1. Pull URLs from sitemap(s) — auto-discovered from robots.txt if not set.
  2. Check index status via URL Inspection API (parallel workers).
  3. Optionally push eligible URLs to the Indexing API, rotating across
     multiple GCP projects (200/day each) for scale.
  4. Resubmit sitemap(s) — the official crawl hint for general pages.
  5. Save a daily snapshot for history charts.

Run daily, e.g. via cron:
  0 9 * * * /usr/bin/python3 /path/to/indexer.py --config /path/to/config.yaml
"""
import argparse
import re
import threading
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
import yaml

import db as store
import gsc

INDEXING_DAILY_PROJECT_CAP = 200

_tls = threading.local()


class _QuotaStop(Exception):
    """Raised inside a worker when Google reports quota exhaustion."""


def _thread_sc(key_path):
    """One Search Console client per thread (googleapiclient isn't thread-safe)."""
    if getattr(_tls, "sc_key", None) != key_path:
        _tls.sc = gsc.searchconsole_service(key_path)
        _tls.sc_key = key_path
    return _tls.sc


def discover_sitemaps(site_url):
    """Find sitemaps via robots.txt; fall back to /sitemap.xml."""
    m = re.match(r"sc-domain:(.+)", site_url or "")
    base = f"https://{m.group(1)}" if m else (site_url or "").rstrip("/")
    found = []
    try:
        r = requests.get(
            base + "/robots.txt", timeout=15,
            headers={"User-Agent": "gsc-indexer/1.0"},
        )
        if r.ok:
            found = re.findall(r"(?im)^sitemap:\s*(\S+)", r.text)
    except Exception:  # noqa: BLE001
        pass
    return found or [base + "/sitemap.xml"]


def fetch_sitemap_urls(sitemap_url, _depth=0):
    if _depth > 3:
        return []
    try:
        r = requests.get(
            sitemap_url, timeout=30, headers={"User-Agent": "gsc-indexer/1.0"}
        )
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  ! sitemap fetch failed {sitemap_url}: {e}")
        return []
    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as e:
        print(f"  ! sitemap parse failed {sitemap_url}: {e}")
        return []
    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    for sm in root.findall("s:sitemap", ns):  # sitemap index file
        loc = sm.find("s:loc", ns)
        if loc is not None and loc.text:
            urls += fetch_sitemap_urls(loc.text.strip(), _depth + 1)
    for u in root.findall("s:url", ns):
        loc = u.find("s:loc", ns)
        if loc is not None and loc.text:
            urls.append(loc.text.strip())
    return urls


def extract_pasted_urls(text):
    """Smart extraction from pasted text. Returns (urls, was_xml).

    - Raw sitemap XML (<urlset>/<sitemapindex>) pasted from a browser:
      pulls every <loc>, recursing into nested sitemap files (bounded).
    - Plain URL list (one per line, commas ok): kept as-is.
    Deduped, order preserved.
    """
    low = text.lower()
    if "<loc" in low and ("<urlset" in low or "<sitemapindex" in low):
        locs = re.findall(r"<loc>\s*(https?://[^<\s]+)\s*</loc>",
                          text, re.IGNORECASE)
        pages = []
        if "<sitemapindex" in low:
            for sm in locs[:20]:
                pages += fetch_sitemap_urls(sm)
        else:
            for u in locs:
                if u.lower().split("?")[0].endswith(".xml"):
                    pages += fetch_sitemap_urls(u)  # nested ref, bounded inside
                else:
                    pages.append(u)
        return list(dict.fromkeys(pages)), True
    urls = [l.strip() for l in text.replace(",", "\n").splitlines()
            if l.strip().startswith("http")]
    return list(dict.fromkeys(urls)), False


def _inspect_one(key_path, db_path, site, url):
    """Returns (status, message). Never raises except _QuotaStop."""
    try:
        res = gsc.inspect_url(_thread_sc(key_path), site, url)
        c = store.connect(db_path)
        try:
            store.mark_checked(
                c, site, url, gsc.is_indexed(res), res.get("coverage", "")
            )
        finally:
            c.close()
        return ("ok", "")
    except Exception as e:  # noqa: BLE001
        if gsc.is_quota_error(e):
            raise _QuotaStop(str(e))
        return ("err", str(e)[:300])


def _http_status(url):
    """Lightweight liveness check (headers only). None on network failure."""
    try:
        with requests.get(url, timeout=8, stream=True,
                          headers={"User-Agent": "gsc-indexer/1.0"}) as r:
            return r.status_code
    except Exception:  # noqa: BLE001
        return None


def _check_dead_links(conn, site, limit=150):
    """HEAD-check URLs never liveness-checked before; records http_status.
    Bounded and best-effort — never breaks the pipeline."""
    try:
        rows = conn.execute(
            "SELECT url FROM urls WHERE property=? AND http_status IS NULL LIMIT ?",
            (site, limit),
        ).fetchall()
        urls = [r[0] for r in rows]
        if not urls:
            return 0
        with ThreadPoolExecutor(max_workers=10) as ex:
            results = list(ex.map(lambda u: (u, _http_status(u)), urls))
        n = 0
        for u, st in results:
            if st:
                store.set_http_status(conn, site, u, st)
                n += 1
        return n
    except Exception:  # noqa: BLE001
        return 0


def inspect_property_urls(cfg, db_path, site, key_path, workers,
                         push_only=False):
    """Run URL inspections for one property (parallel workers).

    Opens its own DB connection. Returns the number of URLs inspected.
    Raises RuntimeError when every inspection fails systematically
    (e.g. wrong property URL format), so failures surface loudly.
    """
    conn = store.connect(db_path)
    try:
        cap = int(cfg.get("daily_inspection_cap_per_property", 1500))
        stale = int(cfg.get("inspection_stale_hours", 72))
        todo = store.candidates_for_inspection(conn, site, cap, stale)
        if push_only:
            print(f"  push-only mode for {site}: skipping {len(todo)} inspections")
            return 0
        stop = threading.Event()
        inspected = 0

        def job(url):
            if stop.is_set():
                return ("skip", "")
            try:
                return _inspect_one(key_path, db_path, site, url)
            except _QuotaStop:
                stop.set()
                return ("quota", "")

        first_error = ""
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(job, u) for u in todo]
            for f in as_completed(futures):
                r, msg = f.result()
                if r == "ok":
                    inspected += 1
                elif r == "err" and not first_error:
                    first_error = msg
                elif r == "quota":
                    print(f"  ! inspection quota hit for {site}; will resume next run")
                    break
        if inspected == 0 and todo and first_error and not stop.is_set():
            raise RuntimeError(
                f"URL Inspection API failed for all {len(todo)} URLs. "
                f"First error: {first_error}"
            )
        return inspected
    finally:
        conn.close()


def run_property(cfg, db_path, prop, pushers, push_lock, key_path):
    """Run the full pipeline for one property. Thread-safe; returns a report."""
    site = prop["site_url"]
    workers = int(cfg.get("inspection_workers", 6))
    conn = store.connect(db_path)
    try:
        # 1. sitemap sync (auto-discover when not configured)
        sitemaps = prop.get("sitemaps") or discover_sitemaps(site)
        if sitemaps and not prop.get("sitemaps"):
            # persist auto-discovered sitemaps so the dashboard can show them
            try:
                store.set_sitemaps(conn, site, sitemaps)
            except Exception:  # noqa: BLE001 — never break the pipeline
                pass
        all_urls = []
        for sm in sitemaps:
            all_urls += fetch_sitemap_urls(sm)
        all_urls = list(dict.fromkeys(all_urls))  # dedupe, keep order
        added = store.upsert_new(conn, site, all_urls)

        # 1b. dead-link hygiene (bounded, best-effort)
        _check_dead_links(conn, site)

        # 2. index-status inspection (parallel workers; own DB connection)
        # Push-only mode (no GSC access): skip inspections, only push.
        push_only = bool(prop.get("push_only"))
        inspected = inspect_property_urls(cfg, db_path, site, key_path,
                                         workers, push_only)

        # 3. Indexing API pushes — sequential, shared multi-project budget
        pushed = 0
        if cfg.get("use_indexing_api") and pushers:
            with push_lock:
                total_budget = sum(p["budget"] for p in pushers)
            cool = int(cfg.get("submit_cooldown_hours", 48))
            subs = store.candidates_for_submission(conn, site, total_budget, cool)
            action = cfg.get("indexing_action", "URL_UPDATED")
            i = 0
            while i < len(subs):
                u = subs[i]
                with push_lock:
                    svc = next((p for p in pushers if p["budget"] > 0), None)
                    if svc:
                        svc["budget"] -= 1
                if svc is None:
                    break
                try:
                    gsc.publish_url(svc["svc"], u, action)
                    store.mark_submitted(conn, site, u, f"indexing-api:{action}")
                    pushed += 1
                    i += 1
                except Exception as e:  # noqa: BLE001
                    if gsc.is_quota_error(e):
                        # This GCP project is out of quota — park it and retry
                        # the SAME url on the next project's key.
                        with push_lock:
                            svc["budget"] = 0
                        continue
                    with push_lock:
                        svc["budget"] += 1  # refund on failure
                    break

        # 4. sitemap resubmit — official crawl hint for general pages
        sc = _thread_sc(key_path)
        for sm in sitemaps:
            try:
                gsc.submit_sitemap(sc, site, sm)
            except Exception:  # noqa: BLE001
                pass

        s = store.summary(conn, site)
        store.save_snapshot(conn, site, s["total"], s["indexed"])
        return {
            "site": site, "urls": len(all_urls), "new": added,
            "inspected": inspected, "pushed": pushed, **s,
        }
    finally:
        conn.close()


def build_pushers(service_account_keys, use_indexing_api):
    """One 200/day budget per GCP project (key)."""
    pushers = []
    if not use_indexing_api:
        return pushers
    for k in service_account_keys:
        try:
            pushers.append({
                "key": k,
                "svc": gsc.indexing_service(k),
                "budget": INDEXING_DAILY_PROJECT_CAP,
            })
        except Exception as e:  # noqa: BLE001
            print(f"  ! indexing service failed for {k}: {e}")
    return pushers


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    keys = cfg.get("service_account_keys") or [cfg.get("service_account_key")]
    keys = [k for k in keys if k]
    if not keys:
        raise SystemExit("No service account keys configured.")
    key_path = keys[0]
    db_path = cfg.get("db_path", "indexing.db")
    push_lock = threading.Lock()
    pushers = build_pushers(keys, cfg.get("use_indexing_api"))

    prop_workers = int(cfg.get("property_workers", 3))

    def work(prop):
        try:
            rep = run_property(cfg, db_path, prop, pushers, push_lock, key_path)
            print(f"== {rep['site']}: total={rep['total']} indexed={rep['indexed']} "
                  f"inspected={rep['inspected']} pushed={rep['pushed']}")
            return rep
        except Exception as e:  # noqa: BLE001 — one property never kills the run
            print(f"== {prop.get('site_url')}: FAILED ({e})")
            return {"site": prop.get("site_url"), "error": str(e)}

    props = cfg.get("properties", [])
    with ThreadPoolExecutor(max_workers=max(1, prop_workers)) as ex:
        list(as_completed([ex.submit(work, p) for p in props]))

    print("done", datetime.now().isoformat(timespec="seconds"))


if __name__ == "__main__":
    main()
