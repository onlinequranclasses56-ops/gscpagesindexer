#!/usr/bin/env python3
"""Web dashboard for the GSC multi-site indexing pipeline (100-site scale).

Run:  python app.py   ->  http://127.0.0.1:5000
Keep it on localhost (no login in this MVP).
"""
import hmac
import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from flask import (Flask, flash, jsonify, redirect, render_template, request,
                   Response, session, url_for)

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
        pushers = indexer.build_pushers(keys, cfg["use_indexing_api"])

        conn = store.connect(DB_PATH)
        try:
            props = [p for p in store.list_properties(conn, active_only=True)
                     if not property_ids or p["id"] in property_ids]
        finally:
            conn.close()

        done, failed, pushed = 0, 0, 0

        def work(p):
            prop = {"site_url": p["site_url"],
                    "sitemaps": json.loads(p["sitemaps"] or "[]")}
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
                        "submitted", "stuck", "dead"],
            "dead": store.dead_count(conn, site),
            "count": store.count_urls(conn, site, filt, q),
            "urls": store.list_urls(conn, site, filt, q, limit=500),
            "indexing_enabled": store.get_setting(conn, "use_indexing_api") == "true",
            "state": state,
        }
        return render_template("property.html", **ctx)
    finally:
        conn.close()


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
                pushers = indexer.build_pushers([k for k in keys if k], True)
                if not pushers:
                    flash("No valid service account keys.", "error")
                else:
                    act = store.get_setting(conn, "indexing_action", "URL_UPDATED")
                    ok, lock = 0, threading.Lock()
                    for u in urls:
                        with lock:
                            svc = next((x for x in pushers if x["budget"] > 0), None)
                            if svc:
                                svc["budget"] -= 1
                        if svc is None:
                            flash("Indexing API daily budget exhausted.", "error")
                            break
                        try:
                            gsc.publish_url(svc["svc"], u, act)
                            store.mark_submitted(conn, site, u, f"manual-push:{act}")
                            ok += 1
                        except Exception as e:  # noqa: BLE001
                            with lock:
                                svc["budget"] += 1
                            flash(f"Push stopped: {e}", "error")
                            break
                    flash(f"Pushed {ok} URLs to the Indexing API.", "ok")
        return redirect(url_for("property_page", pid=pid))
    finally:
        conn.close()


@app.route("/p/<int:pid>/import", methods=["POST"])
def import_urls(pid):
    raw = request.form.get("urls", "")
    urls = [l.strip() for l in raw.replace(",", "\n").splitlines()
            if l.strip().startswith("http")]
    conn = db()
    try:
        p = store.get_property(conn, pid)
        if not p:
            flash("Property not found.", "error")
            return redirect(url_for("dashboard"))
        added = store.upsert_new(conn, p["site_url"], urls)
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


@app.route("/properties/bulk-import", methods=["POST"])
def bulk_import():
    lines = [l for l in request.form.get("sites", "").splitlines() if l.strip()]
    if not lines:
        flash("Paste at least one domain.", "error")
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
                    if key == "use_indexing_api":
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
        return render_template("settings.html", cfg=cfg, props=props, state=state,
                               sa_info=sa_info)
    finally:
        conn.close()


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
