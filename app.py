#!/usr/bin/env python3
"""Web dashboard for the GSC multi-site indexing pipeline (100-site scale).

Run:  python app.py   ->  http://127.0.0.1:5000
Keep it on localhost (no login in this MVP).
"""
import csv
import hmac
import io
import json
import os
import re
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from flask import (Flask, flash, jsonify, redirect, render_template, request,
                   Response, session, url_for)

import ai
import bing
import crawl
import db as store
import gsc
import indexer
from indexer import INDEXING_DAILY_PROJECT_CAP

APP_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(APP_DIR, "config.yaml")

app = Flask(__name__)
app.secret_key = os.environ.get("GSC_INDEXER_SECRET", "local-only-change-me")


# ---------------- access protection ----------------
# Set GSC_INDEXER_PASSWORD env var (e.g. in Render dashboard) to require a
# login. When unset (local dev), the dashboard stays open.

def _auth_on():
    return bool(os.environ.get("GSC_INDEXER_PASSWORD"))


@app.before_request
def _gate():
    if not _auth_on():
        return None
    if request.endpoint in ("login", "static"):
        return None
    if session.get("authed"):
        return None
    return redirect(url_for("login", next=request.path))


@app.context_processor
def _inject_auth():
    return {"auth_on": _auth_on(), "authed": bool(session.get("authed"))}


@app.route("/login", methods=["GET", "POST"])
def login():
    if not _auth_on():
        return redirect(url_for("dashboard"))
    err = ""
    if request.method == "POST":
        want = os.environ.get("GSC_INDEXER_PASSWORD", "")
        if want and hmac.compare_digest(request.form.get("password", ""), want):
            session["authed"] = True
            nxt = request.args.get("next") or url_for("dashboard")
            return redirect(nxt)
        err = "Wrong password."
    return render_template("login.html", err=err, state=state)


@app.route("/logout")
def logout():
    session.pop("authed", None)
    return redirect(url_for("login"))

state = {"running": False, "last_run": None, "last_error": None,
         "last_summary": None}

DEFAULT_SETTINGS = {
    "service_account_key": "",      # legacy single key
    "service_account_keys": "",     # one key path per line (multi-project)
    "db_path": os.path.join(APP_DIR, "indexing.db"),
    "daily_inspection_cap_per_property": "1500",
    "inspection_stale_hours": "72",
    "inspection_workers": "6",
    "property_workers": "3",
    "use_indexing_api": "false",
    "indexing_action": "URL_UPDATED",
    "submit_cooldown_hours": "48",
    "manual_daily_cap": "10",
    "run_time": "09:00",
    "auto_ai_analysis": "false",   # auto-analyze troubled properties after sync
    "auto_ai_max_per_run": "5",    # cap AI analyses per pipeline run
    "ai_reanalyze_days": "7",      # don't re-analyze a property sooner than this
    "ai_system_prompt": "",        # custom expert instructions; empty = built-in
    "crawl_max_pages": "2000",     # deep-crawl page cap per property
    "use_bing_api": "false",       # Bing Webmaster URL Submission API
    "use_indexnow": "false",       # IndexNow (Bing, Yandex, ...) submissions
}

DB_PATH = os.environ.get("GSC_DB_PATH") or DEFAULT_SETTINGS["db_path"]
_cfg_file = {}
if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH) as f:
        _cfg_file = yaml.safe_load(f) or {}
    DB_PATH = os.environ.get("GSC_DB_PATH") or _cfg_file.get("db_path", DB_PATH)


def db():
    return store.connect(DB_PATH)


def parse_keys(raw):
    keys = [k.strip() for k in (raw or "").replace(",", "\n").splitlines()
            if k.strip()]
    return keys


def seed_from_config():
    """First run: import settings and properties from config.yaml."""
    conn = db()
    try:
        for key, default in DEFAULT_SETTINGS.items():
            if key == "db_path" or store.get_setting(conn, key):
                continue
            val = _cfg_file.get(key, default)
            if isinstance(val, list):
                val = "\n".join(val)
            if isinstance(val, bool):
                val = str(val).lower()
            store.set_setting(conn, key, str(val))
        for p in _cfg_file.get("properties", []) or []:
            site = p.get("site_url")
            if site:
                store.add_property(conn, site, p.get("sitemaps", []))
    finally:
        conn.close()


def current_config():
    conn = db()
    try:
        raw = {k: store.get_setting(conn, k, d)
               for k, d in DEFAULT_SETTINGS.items() if k != "db_path"}
        keys = parse_keys(raw["service_account_keys"]) or (
            [raw["service_account_key"]] if raw["service_account_key"] else [])
        return {
            "service_account_keys": keys,
            "daily_inspection_cap_per_property": int(raw["daily_inspection_cap_per_property"] or 1500),
            "inspection_stale_hours": int(raw["inspection_stale_hours"] or 72),
            "inspection_workers": int(raw["inspection_workers"] or 6),
            "property_workers": int(raw["property_workers"] or 3),
            "use_indexing_api": raw["use_indexing_api"] == "true",
            "indexing_action": raw["indexing_action"] or "URL_UPDATED",
            "submit_cooldown_hours": int(raw["submit_cooldown_hours"] or 48),
        }
    finally:
        conn.close()


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_site(line):
    line = (line or "").strip()
    if not line:
        return None
    if line.startswith("sc-domain:"):
        return line.lower()
    if line.startswith("http://") or line.startswith("https://"):
        # URL-prefix property: keep EXACTLY as in Search Console (it must
        # match character-for-character, or Google rejects every API call).
        return line.lower().rstrip("/") + "/"
    dom = re.sub(r"^https?://", "", line.lower()).split("/")[0].split(":")[0]
    return f"sc-domain:{dom}" if dom and "." in dom else None


# ---------------- background pipeline ----------------

def _auto_ai_pass(cfg):
    """After a sync, auto-analyze the most troubled properties with the AI.

    Only runs when enabled in Settings AND AI_API_KEY is set. Targets
    properties with stuck (>7d unindexed) or deindexed pages, skips
    push-only properties and ones analyzed recently. Bounded per run.
    """
    if cfg.get("auto_ai_analysis") != "true" or not ai.is_configured():
        return 0
    try:
        max_n = max(1, int(cfg.get("auto_ai_max_per_run", 5) or 5))
        redays = max(1, int(cfg.get("ai_reanalyze_days", 7) or 7))
    except (ValueError, TypeError):
        max_n, redays = 5, 7
    cutoff = (datetime.now(timezone.utc) - timedelta(days=redays)).isoformat()
    conn = db()
    try:
        cands = []
        for p in store.list_properties(conn, active_only=True):
            if p.get("push_only"):
                continue
            site = p["site_url"]
            stuck = store.stuck_count(conn, site)
            snaps = store.get_snapshots(conn, site, 2)
            trend = (snaps["indexed"][-1] - snaps["indexed"][-2]
                     if len(snaps["indexed"]) >= 2 else 0)
            if stuck == 0 and trend >= 0:
                continue  # healthy — nothing to analyze
            rep = store.get_ai_report(conn, site)
            if rep and rep.get("created_at", "") >= cutoff:
                continue  # analyzed recently
            cands.append((stuck + max(0, -trend) * 2, p["id"], site))
        cands.sort(reverse=True)
        n = 0
        for _, pid, site in cands[:max_n]:
            try:
                _ai_worker(pid, site)  # synchronous; pipeline runs in background
                n += 1
            except Exception:  # noqa: BLE001 — one failure never blocks the rest
                pass
        return n
    finally:
        conn.close()


def run_pipeline(property_ids=None):
    if state["running"]:
        return
    state["running"] = True
    state["last_error"] = None
    try:
        cfg = current_config()
        keys = cfg["service_account_keys"]
        if not keys or not os.path.exists(keys[0]):
            raise RuntimeError("Service account key not set — add it in Settings first.")
        key_path = keys[0]
        push_lock = threading.Lock()

        conn = store.connect(DB_PATH)
        try:
            pushers = indexer.build_pushers(keys, cfg["use_indexing_api"], conn)
            props = [p for p in store.list_properties(conn, active_only=True)
                     if not property_ids or p["id"] in property_ids]
        finally:
            conn.close()

        done, failed, pushed = 0, 0, 0

        def work(p):
            prop = {"site_url": p["site_url"],
                    "sitemaps": json.loads(p["sitemaps"] or "[]"),
                    "push_only": p.get("push_only", 0)}
            try:
                rep = indexer.run_property(cfg, DB_PATH, prop, pushers,
                                           push_lock, key_path)
                return ("ok", rep)
            except Exception as e:  # noqa: BLE001 — isolate property failures
                return ("err", {"site": p["site_url"], "error": str(e)})

        with ThreadPoolExecutor(
                max_workers=max(1, cfg["property_workers"])) as ex:
            for f in as_completed([ex.submit(work, p) for p in props]):
                status, rep = f.result()
                if status == "ok":
                    done += 1
                    pushed += rep.get("pushed", 0)
                else:
                    failed += 1

        state["last_run"] = utcnow()
        state["last_summary"] = (f"{done} properties synced, {failed} failed, "
                                 f"{pushed} URLs pushed")
        ai_n = _auto_ai_pass(cfg)
        if ai_n:
            state["last_summary"] += f", {ai_n} AI-analyzed"
    except Exception as e:  # noqa: BLE001
        state["last_error"] = str(e)
    finally:
        state["running"] = False


def run_async(property_ids=None):
    threading.Thread(target=run_pipeline, args=(property_ids,), daemon=True).start()


# ---------------- routes ----------------

@app.route("/")
def dashboard():
    q = request.args.get("q", "").strip().lower()
    sort = request.args.get("sort", "worst")
    try:
        page = max(1, int(request.args.get("page", "1") or 1))
    except ValueError:
        page = 1
    PER = 24
    conn = db()
    try:
        all_props = []
        for p in store.list_properties(conn):
            s = store.summary(conn, p["site_url"])
            snaps = store.get_snapshots(conn, p["site_url"], 2)
            trend = None
            if len(snaps["indexed"]) >= 2:
                trend = snaps["indexed"][-1] - snaps["indexed"][-2]
            all_props.append({
                "id": p["id"], "site_url": p["site_url"], "active": p["active"],
                **s,
                "stuck": store.stuck_count(conn, p["site_url"]),
                "trend": trend,
                "pct": round(100 * s["indexed"] / s["total"]) if s["total"] else 0,
                "sitemaps": json.loads(p["sitemaps"] or "[]"),
            })
        deindexed_total = sum(-t["trend"] for t in all_props
                              if t["trend"] and t["trend"] < 0)
        newly_indexed_total = sum(t["trend"] for t in all_props
                                  if t["trend"] and t["trend"] > 0)
        attention = sorted([p for p in all_props if p["stuck"] > 0],
                           key=lambda p: -p["stuck"])[:10]
        props = [p for p in all_props if not q or q in p["site_url"].lower()]
        if sort == "worst":
            props.sort(key=lambda p: (p["pct"], -p["stuck"]))
        elif sort == "stuck":
            props.sort(key=lambda p: -p["stuck"])
        elif sort == "biggest":
            props.sort(key=lambda p: -p["total"])
        else:
            props.sort(key=lambda p: p["site_url"])
        pages = max(1, (len(props) + PER - 1) // PER)
        page = min(page, pages)
        page_props = props[(page - 1) * PER:page * PER]
        totals = {"properties": sum(1 for p in all_props if p["active"]),
                  "urls": sum(p["total"] for p in all_props if p["active"]),
                  "indexed": sum(p["indexed"] for p in all_props if p["active"])}
        totals["pct"] = round(100 * totals["indexed"] / totals["urls"]) if totals["urls"] else 0
        return render_template("dashboard.html", props=page_props,
                               attention=attention, totals=totals, state=state,
                               q=request.args.get("q", ""), sort=sort,
                               page=page, pages=pages,
                               deindexed_total=deindexed_total,
                               newly_indexed_total=newly_indexed_total)
    finally:
        conn.close()


def _bing_quota_safe(site):
    """Bing daily/monthly remaining quota, or None (fast-fail, never blocks)."""
    if not bing.bing_configured():
        return None
    try:
        return bing.get_quota(os.environ["BING_API_KEY"], site, timeout=8)
    except Exception:  # noqa: BLE001
        return None


@app.route("/p/<int:pid>")
def property_page(pid):
    filt = request.args.get("f", "all")
    q = request.args.get("q", "").strip()
    conn = db()
    try:
        p = store.get_property(conn, pid)
        if not p:
            flash("Property not found.", "error")
            return redirect(url_for("dashboard"))
        site = p["site_url"]
        s = store.summary(conn, site)
        manual_cap = int(store.get_setting(conn, "manual_daily_cap", "10") or 10)
        manual_used = store.manual_slots_used_24h(conn, site)
        ctx = {
            "p": p, "site": site,
            "sitemaps": json.loads(p["sitemaps"] or "[]"),
            "summary": s,
            "pct": round(100 * s["indexed"] / s["total"]) if s["total"] else 0,
            "stuck": store.stuck_count(conn, site),
            "coverage": store.coverage_breakdown(conn, site),
            "manual_cap": manual_cap, "manual_used": manual_used,
            "manual_left": max(0, manual_cap - manual_used),
            "filt": filt, "q": q,
            "filters": ["all", "indexed", "unindexed", "unchecked",
                        "submitted", "unsubmitted", "stuck", "dead"],
            "dead": store.dead_count(conn, site),
            "count": store.count_urls(conn, site, filt, q),
            "urls": store.list_urls(conn, site, filt, q, limit=500),
            "indexing_enabled": store.get_setting(conn, "use_indexing_api") == "true",
            "state": state,
            "ai_report": store.get_ai_report(conn, site),
            "ai_running": pid in state.get("ai_running", set()),
            "ai_configured": ai.is_configured(),
            "bing_on": store.get_setting(conn, "use_bing_api") == "true" and bing.bing_configured(),
            "indexnow_on": store.get_setting(conn, "use_indexnow") == "true" and bing.indexnow_configured(),
            "bing_quota": _bing_quota_safe(site),
            "ai_error": state.get("ai_errors", {}).pop(pid, None),
            "crawling": pid in state.get("crawling", set()),
            "crawl_msg": state.get("crawl_done", {}).pop(pid, None),
            "sitemap_importing": pid in state.get("sitemap_importing", set()),
            "sitemap_msg": state.get("sitemap_done", {}).pop(pid, None),
        }
        return render_template("property.html", **ctx)
    finally:
        conn.close()


def _slug(site):
    return re.sub(r"[^a-z0-9]+", "-",
                  site.replace("https://", "").replace("http://", "")
                  .strip("/").lower()).strip("-") or "site"


def _url_status(u):
    if u.get("indexed"):
        return "indexed"
    if u.get("last_checked"):
        return "not indexed"
    return "unchecked"


@app.get("/p/<int:pid>/export")
def export_urls(pid):
    """Download the currently filtered URL list as CSV — GSC-style export
    for bulk technical-SEO work. Respects the active filter + search."""
    filt = request.args.get("f", "all")
    q = request.args.get("q", "").strip()
    conn = db()
    try:
        p = store.get_property(conn, pid)
        if not p:
            flash("Property not found.", "error")
            return redirect(url_for("dashboard"))
        site = p["site_url"]
        rows = store.list_urls(conn, site, filt, q, limit=100000)
    finally:
        conn.close()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["URL", "Status", "Coverage", "First seen", "Last checked",
                "Last submitted", "Submit attempts", "Bing submitted",
                "IndexNow submitted", "HTTP status",
                "Priority"])
    for u in rows:
        w.writerow([
            u["url"], _url_status(u), u.get("index_state") or "",
            (u.get("first_seen") or "")[:19],
            (u.get("last_checked") or "")[:19],
            (u.get("last_submitted") or "")[:19],
            u.get("submit_attempts") or 0,
            (u.get("last_bing_submitted") or "")[:19],
            (u.get("last_indexnow_submitted") or "")[:19],
            u.get("http_status") or "",
            "yes" if u.get("priority") else "no",
        ])
    fname = f"{_slug(site)}_{filt}_{datetime.now(timezone.utc):%Y-%m-%d}.csv"
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition":
                             f"attachment; filename={fname}"})


@app.get("/p/<int:pid>/ai-report.md")
def download_ai_report(pid):
    """Download the AI Index Analyst report as Markdown."""
    conn = db()
    try:
        p = store.get_property(conn, pid)
        if not p:
            flash("Property not found.", "error")
            return redirect(url_for("dashboard"))
        site = p["site_url"]
        rep = store.get_ai_report(conn, site)
        if not rep:
            flash("No AI report yet — run 'Analyze with AI' first.", "error")
            return redirect(url_for("property_page", pid=pid))
    finally:
        conn.close()
    md = (f"# AI Index Analysis — {site}\n\n"
          f"_Generated: {(rep['created_at'] or '')[:16]}_\n\n"
          f"{rep['report']}\n")
    fname = f"{_slug(site)}_ai-report_{(rep['created_at'] or '')[:10]}.md"
    return Response(md, mimetype="text/markdown",
                    headers={"Content-Disposition":
                             f"attachment; filename={fname}"})


@app.post("/p/<int:pid>/push-only")
def toggle_push_only(pid):
    """Toggle push-only mode: skip index inspections (for properties the
    service account can't access in GSC), only push to the Indexing API."""
    conn = db()
    try:
        p = store.get_property(conn, pid)
        if not p:
            flash("Property not found.", "error")
        else:
            new_val = not p.get("push_only")
            store.set_push_only(conn, pid, new_val)
            flash(("Push-only mode ON — inspections skipped, only Indexing API "
                   "pushes will run." if new_val else
                   "Push-only mode OFF — full inspection + push pipeline."),
                  "ok")
    finally:
        conn.close()
    return redirect(url_for("property_page", pid=pid))


@app.route("/p/<int:pid>/run", methods=["POST"])
def run_one(pid):
    run_async([pid])
    flash("Sync started in the background.", "ok")
    return redirect(url_for("property_page", pid=pid))


@app.route("/run-all", methods=["POST"])
def run_all():
    run_async()
    flash("Full sync started in the background.", "ok")
    return redirect(url_for("dashboard"))


@app.route("/p/<int:pid>/bulk", methods=["POST"])
def bulk_action(pid):
    urls = request.form.getlist("u")
    action = request.form.get("action")
    conn = db()
    try:
        p = store.get_property(conn, pid)
        if not p or not urls:
            flash("Nothing selected.", "error")
            return redirect(url_for("property_page", pid=pid))
        site = p["site_url"]
        if action == "priority":
            store.set_priority(conn, site, urls, 10)
            flash(f"{len(urls)} URLs marked high priority.", "ok")
        elif action == "unpriority":
            store.set_priority(conn, site, urls, 0)
            flash(f"{len(urls)} URLs set back to normal priority.", "ok")
        elif action == "delete":
            store.delete_urls(conn, site, urls)
            flash(f"Removed {len(urls)} URLs from tracking.", "ok")
        elif action == "push":
            if store.get_setting(conn, "use_indexing_api") != "true":
                flash("Indexing API is disabled — enable it in Settings first.", "error")
            else:
                keys = parse_keys(store.get_setting(conn, "service_account_keys")) or \
                       [store.get_setting(conn, "service_account_key")]
                pushers = indexer.build_pushers([k for k in keys if k], True, conn)
                if not pushers:
                    flash("No valid service account keys.", "error")
                else:
                    act = store.get_setting(conn, "indexing_action", "URL_UPDATED")
                    ok, lock = 0, threading.Lock()
                    urls = list(urls)
                    denied = []
                    i = 0
                    while i < len(urls):
                        u = urls[i]
                        with lock:
                            svc = next((x for x in pushers
                                        if x["budget"] > 0 and x["key"] not in denied),
                                       None)
                            if svc:
                                svc["budget"] -= 1
                        if svc is None:
                            if denied:
                                flash("Push stopped: URL ownership not verified for "
                                      f"{', '.join(denied)}. Add that key's service-account "
                                      "email (see Settings) as Owner in Search Console.",
                                      "error")
                            else:
                                flash("Indexing API daily budget exhausted.", "error")
                            break
                        try:
                            gsc.publish_url(svc["svc"], u, act)
                            store.mark_submitted(conn, site, u, f"manual-push:{act}")
                            store.log_push(conn, svc["key"])
                            ok += 1
                            i += 1
                        except Exception as e:  # noqa: BLE001
                            if gsc.is_quota_error(e):
                                # Fail over: retry the SAME url on the next
                                # project's key.
                                with lock:
                                    svc["budget"] = 0
                                continue
                            if gsc.is_ownership_error(e):
                                # This key's SA doesn't own this property —
                                # try the next key for the same URL.
                                denied.append(svc["key"])
                                with lock:
                                    svc["budget"] += 1  # refund
                                continue
                            with lock:
                                svc["budget"] += 1
                            flash(f"Push stopped ({svc['key']}): {e}", "error")
                            break
                    flash(f"Pushed {ok} URLs to the Indexing API.", "ok")
        elif action == "push-bing":
            if store.get_setting(conn, "use_bing_api") != "true" or not bing.bing_configured():
                flash("Bing submission is disabled or BING_API_KEY is missing — see Settings.", "error")
            else:
                api_key = os.environ["BING_API_KEY"]
                ok = 0
                try:
                    daily_left, _ = bing.get_quota(api_key, site)
                    todo = [u for u in urls[:max(0, daily_left)]]
                    for batch in bing.chunked(todo, bing.BING_BATCH_MAX):
                        bing.submit_url_batch(api_key, site, batch)
                        for u in batch:
                            store.mark_bing_submitted(conn, site, u)
                        ok += len(batch)
                    if len(urls) > len(todo):
                        flash(f"Bing daily quota left: {daily_left} — {len(urls) - len(todo)} URLs skipped.", "error")
                except Exception as e:  # noqa: BLE001
                    flash(f"Bing push stopped: {e}", "error")
                flash(f"Submitted {ok} URLs to Bing.", "ok")
        elif action == "push-indexnow":
            if store.get_setting(conn, "use_indexnow") != "true" or not bing.indexnow_configured():
                flash("IndexNow is disabled or INDEXNOW_KEY is missing — see Settings.", "error")
            else:
                in_key = os.environ["INDEXNOW_KEY"]
                host = urllib.parse.urlparse(site).netloc
                ok = 0
                try:
                    for batch in bing.chunked(urls, bing.INDEXNOW_BATCH_MAX):
                        bing.indexnow_submit(in_key, host, batch)
                        for u in batch:
                            store.mark_indexnow_submitted(conn, site, u)
                        ok += len(batch)
                except Exception as e:  # noqa: BLE001
                    flash(f"IndexNow stopped: {e}", "error")
                flash(f"Submitted {ok} URLs via IndexNow.", "ok")
        return redirect(url_for("property_page", pid=pid))
    finally:
        conn.close()


@app.route("/p/<int:pid>/import", methods=["POST"])
def import_urls(pid):
    raw = request.form.get("urls", "")
    # Smart: raw sitemap XML pasted from the browser is auto-parsed —
    # every <loc> becomes a tracked URL instantly.
    urls, was_xml = indexer.extract_pasted_urls(raw)
    conn = db()
    try:
        p = store.get_property(conn, pid)
        if not p:
            flash("Property not found.", "error")
            return redirect(url_for("dashboard"))
        added = store.upsert_new(conn, p["site_url"], urls)
        if was_xml:
            flash(f"Extracted {len(urls)} URLs from sitemap XML "
                  f"({added} new added to tracking).", "ok")
        else:
            flash(f"Imported {len(urls)} URLs ({added} new).", "ok")
        return redirect(url_for("property_page", pid=pid))
    finally:
        conn.close()


@app.route("/p/<int:pid>/manual-request", methods=["POST"])
def manual_request(pid):
    url = request.form.get("url", "").strip()
    conn = db()
    try:
        p = store.get_property(conn, pid)
        if not p or not url.startswith("http"):
            flash("Enter a valid URL.", "error")
            return redirect(url_for("property_page", pid=pid))
        cap = int(store.get_setting(conn, "manual_daily_cap", "10") or 10)
        used = store.manual_slots_used_24h(conn, p["site_url"])
        if used >= cap:
            flash("Manual quota used up for the last 24h — wait for slots to free.", "error")
        else:
            store.log_manual_slot(conn, p["site_url"], url)
            flash(f"Logged. Request '{url}' in GSC now — {cap - used - 1} slots left.", "ok")
        return redirect(url_for("property_page", pid=pid))
    finally:
        conn.close()


@app.route("/p/<int:pid>/export")
def export_csv(pid):
    conn = db()
    try:
        p = store.get_property(conn, pid)
        if not p:
            return redirect(url_for("dashboard"))
        rows = store.list_urls(conn, p["site_url"], "all", "", limit=100000)
        lines = ["url,first_seen,last_checked,indexed,index_state,"
                 "last_submitted,attempts,priority"]
        for r in rows:
            lines.append(",".join([
                r["url"], str(r["first_seen"]), str(r["last_checked"] or ""),
                str(r["indexed"] if r["indexed"] is not None else ""),
                f"\"{r['index_state'] or ''}\"", str(r["last_submitted"] or ""),
                str(r["submit_attempts"]), str(r["priority"]),
            ]))
        return Response("\n".join(lines), mimetype="text/csv",
                        headers={"Content-Disposition":
                                 f"attachment;filename=property-{pid}.csv"})
    finally:
        conn.close()


@app.route("/api/history/<int:pid>")
def api_history(pid):
    conn = db()
    try:
        p = store.get_property(conn, pid)
        if not p:
            return jsonify({"error": "not found"}), 404
        return jsonify(store.get_snapshots(conn, p["site_url"]))
    finally:
        conn.close()


@app.post("/properties/bulk-delete")
def bulk_delete_properties():
    pids = request.form.getlist("pids")
    if not pids:
        flash("Nothing selected.", "error")
        return redirect(url_for("settings"))
    conn = db()
    try:
        n = 0
        for pid in pids:
            try:
                store.delete_property(conn, int(pid))
                n += 1
            except (ValueError, TypeError):
                pass
        flash(f"Deleted {n} properties and their tracked data.", "ok" if n else "error")
    finally:
        conn.close()
    return redirect(url_for("settings"))


@app.route("/properties/bulk-import", methods=["POST"])
def bulk_import():
    lines = [l for l in request.form.get("sites", "").splitlines() if l.strip()]
    if not lines:
        flash("Paste at least one domain.", "error")
        return redirect(url_for("settings"))
    # Guard: deep page URLs are not properties — skip them with guidance.
    clean, skipped = [], 0
    for l in lines:
        if re.match(r"https?://[^/]+/.+", l.strip()):
            skipped += 1
        else:
            clean.append(l)
    lines = clean
    if skipped:
        flash(f"Skipped {skipped} lines that look like page URLs, not domains. "
              "Bulk import is for DOMAINS — add ONE property for the site, then "
              "paste page URLs into that property's Import URLs box.", "error")
    if not lines:
        return redirect(url_for("settings"))

    def job():
        conn = store.connect(DB_PATH)
        try:
            for line in lines:
                site = normalize_site(line)
                if not site:
                    continue
                try:
                    sms = indexer.discover_sitemaps(site)
                except Exception:  # noqa: BLE001
                    sms = []
                store.add_property(conn, site, sms)
        finally:
            conn.close()

    threading.Thread(target=job, daemon=True).start()
    flash(f"Importing {len(lines)} sites in the background "
          "(sitemaps auto-discovered from robots.txt)…", "ok")
    return redirect(url_for("settings"))


@app.route("/demo/seed", methods=["POST"])
def demo_seed():
    import demo
    conn = db()
    try:
        n = demo.seed(conn)
    finally:
        conn.close()
    flash(f"Demo data loaded ({n} demo sites). Click around freely — "
          "reset it anytime from Settings.", "ok")
    return redirect(url_for("dashboard"))


@app.route("/demo/reset", methods=["POST"])
def demo_reset():
    import demo
    conn = db()
    try:
        n = demo.reset(conn)
    finally:
        conn.close()
    flash(f"Removed {n} demo properties.", "ok")
    return redirect(url_for("settings"))


def _sa_emails(key_paths):
    """Extract service-account client_email from each key file (for copy-paste
    into Search Console user management). Never exposes the private key."""
    out = []
    for p in key_paths or []:
        email = ""
        try:
            with open(p) as f:
                email = json.load(f).get("client_email", "")
        except Exception:
            pass
        out.append((p, email))
    return out


@app.route("/settings", methods=["GET", "POST"])
def settings():
    conn = db()
    try:
        if request.method == "POST":
            form = request.form
            if "add_site" in form:
                site = normalize_site(form.get("site_url", ""))
                sms = [s.strip() for s in form.get("sitemaps", "")
                       .replace(",", "\n").splitlines() if s.strip()]
                if site:
                    if not sms:
                        sms = indexer.discover_sitemaps(site)
                    store.add_property(conn, site, sms)
                    flash(f"Added {site}.", "ok")
                else:
                    flash("Enter a valid domain.", "error")
            elif "delete_pid" in form:
                store.delete_property(conn, int(form["delete_pid"]))
                flash("Property deleted.", "ok")
            elif "toggle_pid" in form:
                p = store.get_property(conn, int(form["toggle_pid"]))
                if p:
                    store.set_property_active(conn, p["id"], not p["active"])
            else:
                for key in DEFAULT_SETTINGS:
                    if key in ("db_path", "service_account_key"):
                        continue
                    if key in ("use_indexing_api", "auto_ai_analysis",
                               "use_bing_api", "use_indexnow"):
                        store.set_setting(conn, key,
                                          "true" if form.get(key) else "false")
                    elif key in form:
                        store.set_setting(conn, key, form.get(key, "").strip())
                flash("Settings saved.", "ok")
                schedule_daily()
            return redirect(url_for("settings"))
        cfg = {k: store.get_setting(conn, k, d)
               for k, d in DEFAULT_SETTINGS.items()}
        props = store.list_properties(conn)
        sa_info = _sa_emails(parse_keys(cfg.get("service_account_keys")))
        usage = store.push_usage_today(conn)
        quota_info = [
            {"path": p, "email": e or f"(key unreadable: {p})",
             "used": usage.get(p, 0),
             "remaining": max(0, 200 - usage.get(p, 0))}
            for p, e in sa_info
        ]
        return render_template("settings.html", cfg=cfg, props=props, state=state,
                               sa_info=sa_info, quota_info=quota_info,
                               quota_day=store.quota_day(),
                               bing_key=bing.bing_configured(),
                               indexnow_key=bing.indexnow_configured(),
                               discovered=state.get("discovered"))
    finally:
        conn.close()


# ---------------- GSC property discovery ----------------

@app.post("/settings/discover-sites")
def discover_sites():
    """One click: list every Search Console property the service account(s)
    can access, so the user can import them all without typing domains."""
    conn = db()
    try:
        keys = parse_keys(store.get_setting(conn, "service_account_keys"))
        found = []
        errors = []
        for k in keys:
            try:
                found += gsc.list_sites(gsc.searchconsole_service(k))
            except Exception as e:  # noqa: BLE001
                errors.append(f"{k}: {e}"[:160])
        found = sorted(set(found))
        existing = {p["site_url"] for p in store.list_properties(conn)}
        state["discovered"] = [s for s in found if s not in existing]
        if errors:
            flash("Discovery errors: " + " | ".join(errors), "error")
        flash(f"Found {len(found)} properties in Google, "
              f"{len(state['discovered'])} new.", "ok")
    finally:
        conn.close()
    return redirect(url_for("settings"))


@app.post("/settings/import-discovered")
def import_discovered():
    conn = db()
    try:
        sites = state.pop("discovered", [])
        n = 0
        for site in sites:
            try:
                store.add_property(conn, site, indexer.discover_sitemaps(site))
                n += 1
            except Exception:  # noqa: BLE001
                pass
        flash(f"Imported {n} properties. Run a sync to discover their URLs.",
              "ok")
    finally:
        conn.close()
    return redirect(url_for("settings"))


# ---------------- AI index analyst ----------------

def _ai_worker(pid, site):
    conn = db()
    try:
        s = store.summary(conn, site)
        snaps = store.get_snapshots(conn, site, 2)
        trend = (snaps["indexed"][-1] - snaps["indexed"][-2]
                 if len(snaps["indexed"]) >= 2 else None)
        data = {
            "site_url": site,
            "total": s["total"], "indexed": s["indexed"],
            "pct": round(100 * s["indexed"] / s["total"]) if s["total"] else 0,
            "coverage": store.coverage_breakdown(conn, site),
            "stuck": store.stuck_count(conn, site),
            "dead": store.dead_count(conn, site),
            "trend": trend,
            "submitted": s["submitted"],
            "unindexed_sample": [r["url"] for r in
                                 store.list_urls(conn, site, "unindexed",
                                                 limit=30)],
        }
        custom = (store.get_setting(conn, "ai_system_prompt") or "").strip()
        ok, text = ai.analyze(data, system_prompt=custom or None)
        if ok:
            store.save_ai_report(conn, site, text)
        else:
            state.setdefault("ai_errors", {})[pid] = text
    finally:
        state.get("ai_running", set()).discard(pid)
        conn.close()


@app.post("/p/<int:pid>/analyze")
def analyze_property(pid):
    conn = db()
    try:
        p = store.get_property(conn, pid)
        if not p:
            flash("Property not found.", "error")
            return redirect(url_for("dashboard"))
        if not ai.is_configured():
            flash("AI not configured — set AI_API_KEY in Render → "
                  "Environment → Variables.", "error")
            return redirect(url_for("property_page", pid=pid))
        running = state.setdefault("ai_running", set())
        if pid in running:
            flash("Analysis already running — refresh in a bit.", "ok")
        else:
            running.add(pid)
            threading.Thread(target=_ai_worker,
                             args=(pid, p["site_url"]),
                             daemon=True).start()
            flash("AI analysis started — refresh this page in a minute.", "ok")
    finally:
        conn.close()
    return redirect(url_for("property_page", pid=pid))


# ---------------- sitemap import + analyze ----------------

def _sitemap_import_worker(pid, site, sitemaps, cfg, key_path, workers,
                           push_only):
    conn = db()
    try:
        state.setdefault("sitemap_importing", set()).add(pid)
        total, added = 0, 0
        for sm in sitemaps:
            urls = indexer.fetch_sitemap_urls(sm)
            total += len(urls)
            added += store.upsert_new(conn, site, urls)
        # analyze immediately: inspect so URLs land in indexed/non-indexed
        inspected = indexer.inspect_property_urls(cfg, DB_PATH, site,
                                                 key_path, workers,
                                                 push_only)
        state.setdefault("sitemap_done", {})[pid] = (
            f"Sitemap import done: {total} URLs found ({added} new), "
            f"{inspected} pages analyzed — use the filters below to browse "
            f"Indexed / Not indexed / etc.")
    except Exception as e:  # noqa: BLE001
        state.setdefault("sitemap_done", {})[pid] = f"Import failed: {e}"[:250]
    finally:
        state.get("sitemap_importing", set()).discard(pid)
        conn.close()


@app.post("/p/<int:pid>/import-sitemap")
def import_sitemap(pid):
    """Import every page from the property's sitemap(s), then analyze
    (inspect) them so they categorize into indexed / non-indexed / etc."""
    conn = db()
    try:
        p = store.get_property(conn, pid)
        if not p:
            flash("Property not found.", "error")
            return redirect(url_for("dashboard"))
        if pid in state.get("sitemap_importing", set()):
            flash("Sitemap import already running…", "ok")
            return redirect(url_for("property_page", pid=pid))
        sm = request.form.get("sm", "all")
        sitemaps = json.loads(p["sitemaps"] or "[]") or \
            indexer.discover_sitemaps(p["site_url"])
        if sm != "all":
            sitemaps = [sm]
        if not sitemaps:
            flash("No sitemaps found for this property.", "error")
            return redirect(url_for("property_page", pid=pid))
        cfg = current_config()
        keys = cfg["service_account_keys"]
        if not keys or not os.path.exists(keys[0]):
            flash("Service account key not set — add it in Settings first.",
                  "error")
            return redirect(url_for("property_page", pid=pid))
        workers = int(cfg.get("inspection_workers", 6))
        threading.Thread(
            target=_sitemap_import_worker,
            args=(pid, p["site_url"], sitemaps, cfg, keys[0], workers,
                  bool(p.get("push_only", 0))),
            daemon=True).start()
        flash("Importing all pages from sitemap(s) in the background, then "
              "analyzing each page's index status…", "ok")
    finally:
        conn.close()
    return redirect(url_for("property_page", pid=pid))


# ---------------- deep crawl ----------------

def _crawl_worker(pid, site, start_url, max_pages):
    conn = db()
    try:
        state.setdefault("crawling", set()).add(pid)
        urls = crawl.crawl_site(start_url, max_pages=max_pages)
        added = store.upsert_new(conn, site, sorted(urls))
        state.setdefault("crawl_done", {})[pid] = (
            f"Deep crawl finished: {len(urls)} pages found, {added} new "
            f"added to tracking.")
    except Exception as e:  # noqa: BLE001
        state.setdefault("crawl_done", {})[pid] = f"Crawl failed: {e}"[:200]
    finally:
        state.get("crawling", set()).discard(pid)
        conn.close()


@app.post("/p/<int:pid>/crawl")
def deep_crawl(pid):
    """BFS crawl from the site's homepage; imports every page found."""
    conn = db()
    try:
        p = store.get_property(conn, pid)
        if not p:
            flash("Property not found.", "error")
            return redirect(url_for("dashboard"))
        if pid in state.get("crawling", set()):
            flash("Crawl already running…", "ok")
            return redirect(url_for("property_page", pid=pid))
        site = p["site_url"]
        if site.startswith("http"):
            start = site
        else:
            start = "https://" + site.replace("sc-domain:", "", 1)
        try:
            max_pages = max(50, int(store.get_setting(conn, "crawl_max_pages",
                                                      "2000") or 2000))
        except (ValueError, TypeError):
            max_pages = 2000
        threading.Thread(target=_crawl_worker,
                         args=(pid, site, start, max_pages),
                         daemon=True).start()
        flash("Deep crawl started in the background — it walks every internal "
              "link from your homepage. This can take several minutes.",
              "ok")
    finally:
        conn.close()
    return redirect(url_for("property_page", pid=pid))


# ---------------- scheduler ----------------

scheduler = BackgroundScheduler()


def schedule_daily():
    conn = db()
    try:
        rt = store.get_setting(conn, "run_time", "09:00") or "09:00"
        h, m = (int(x) for x in rt.split(":"))
        scheduler.remove_all_jobs()
        scheduler.add_job(run_pipeline, "cron", hour=h, minute=m, id="daily")
    except Exception:  # noqa: BLE001
        pass
    finally:
        conn.close()


# Start the daily scheduler even under gunicorn (which imports this module but
# never runs __main__). render.yaml pins --workers 1 so it runs exactly once.
try:
    schedule_daily()
    if not scheduler.running:
        scheduler.start()
except Exception:  # noqa: BLE001
    pass


def _find_free_port(start=5000, tries=10):
    import socket
    for p in range(start, start + tries):
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", p))
                return p
            except OSError:
                continue
    return start


if __name__ == "__main__":
    seed_from_config()
    schedule_daily()
    if not scheduler.running:
        scheduler.start()
    port = _find_free_port(5000)
    print("=" * 56)
    print("  GSC Indexer is running!")
    print(f"  Open this address in your browser:  http://127.0.0.1:{port}")
    print("  Keep this window open while you use it.")
    print("=" * 56)
    app.run(host="127.0.0.1", port=port, use_reloader=False)
