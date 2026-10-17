"""SQLite tracking for the GSC indexer (CLI + web dashboard)."""
import json
import sqlite3
import urllib.parse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

SCHEMA_URLS = """
CREATE TABLE IF NOT EXISTS urls (
    property TEXT NOT NULL,
    url TEXT NOT NULL,
    first_seen TEXT NOT NULL,
    last_checked TEXT,
    indexed INTEGER,
    index_state TEXT,
    last_submitted TEXT,
    submit_attempts INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 0,
    notes TEXT,
    PRIMARY KEY (property, url)
);
"""

SCHEMA_PROPS = """
CREATE TABLE IF NOT EXISTS properties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_url TEXT UNIQUE NOT NULL,
    sitemaps TEXT NOT NULL DEFAULT '[]',
    active INTEGER NOT NULL DEFAULT 1,
    added_at TEXT NOT NULL
);
"""

SCHEMA_SETTINGS = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
);
"""

SCHEMA_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS snapshots (
    day TEXT NOT NULL,
    property TEXT NOT NULL,
    total INTEGER NOT NULL,
    indexed INTEGER NOT NULL,
    PRIMARY KEY (day, property)
);
"""

SCHEMA_SLOTS = """
CREATE TABLE IF NOT EXISTS manual_slots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property TEXT NOT NULL,
    url TEXT NOT NULL,
    used_at TEXT NOT NULL
);
"""

SCHEMA_AI = """
CREATE TABLE IF NOT EXISTS ai_reports (
    property TEXT PRIMARY KEY,
    report TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""

SCHEMA_PUSH_USAGE = """
CREATE TABLE IF NOT EXISTS push_usage (
    day TEXT NOT NULL,
    key TEXT NOT NULL,
    used INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, key)
);
"""

SCHEMA_QUEUE = """
CREATE TABLE IF NOT EXISTS push_queue (
    id INTEGER PRIMARY KEY,
    property TEXT NOT NULL,
    url TEXT NOT NULL,
    action TEXT NOT NULL DEFAULT 'URL_UPDATED',
    enqueued_at TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    UNIQUE(property, url)
);
"""


def quota_day():
    """Google's Indexing API quota window resets at midnight Pacific Time."""
    return datetime.now(ZoneInfo("America/Los_Angeles")).date().isoformat()


def log_push(conn, key):
    """Record one successful Indexing API publish against today's PT quota day."""
    conn.execute(
        "INSERT INTO push_usage(day, key, used) VALUES(?,?,1) "
        "ON CONFLICT(day, key) DO UPDATE SET used=used+1",
        (quota_day(), key),
    )
    conn.commit()


def push_used(conn, key, day=None):
    r = conn.execute(
        "SELECT used FROM push_usage WHERE day=? AND key=?",
        (day or quota_day(), key),
    ).fetchone()
    return r[0] if r else 0


def push_usage_today(conn):
    return {r["key"]: r["used"] for r in
            conn.execute("SELECT key, used FROM push_usage WHERE day=?",
                         (quota_day(),))}


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def today():
    return datetime.now(timezone.utc).date().isoformat()


def connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(SCHEMA_URLS)
    conn.execute(SCHEMA_PROPS)
    conn.execute(SCHEMA_SETTINGS)
    conn.execute(SCHEMA_SNAPSHOTS)
    conn.execute(SCHEMA_SLOTS)
    conn.execute(SCHEMA_AI)
    conn.execute(SCHEMA_PUSH_USAGE)
    conn.execute(SCHEMA_QUEUE)
    # migrate: priority column for DBs created by older versions
    cols = [r[1] for r in conn.execute("PRAGMA table_info(urls)").fetchall()]
    if "priority" not in cols:
        conn.execute("ALTER TABLE urls ADD COLUMN priority INTEGER NOT NULL DEFAULT 0")
    # migrate: http_status for dead-link detection
    if "http_status" not in cols:
        conn.execute("ALTER TABLE urls ADD COLUMN http_status INTEGER")
    # migrate: Bing + IndexNow submission tracking
    if "last_bing_submitted" not in cols:
        conn.execute("ALTER TABLE urls ADD COLUMN last_bing_submitted TEXT")
    if "bing_attempts" not in cols:
        conn.execute("ALTER TABLE urls ADD COLUMN bing_attempts INTEGER NOT NULL DEFAULT 0")
    if "last_indexnow_submitted" not in cols:
        conn.execute("ALTER TABLE urls ADD COLUMN last_indexnow_submitted TEXT")
    if "indexnow_attempts" not in cols:
        conn.execute("ALTER TABLE urls ADD COLUMN indexnow_attempts INTEGER NOT NULL DEFAULT 0")
    # migrate: push_only mode for properties without GSC access
    pcols = [r[1] for r in conn.execute("PRAGMA table_info(properties)").fetchall()]
    if "push_only" not in pcols:
        conn.execute("ALTER TABLE properties ADD COLUMN push_only INTEGER NOT NULL DEFAULT 0")
    # migrate: fast-lane re-check timestamp (re-inspect soon after a push)
    if "recheck_after" not in cols:
        conn.execute("ALTER TABLE urls ADD COLUMN recheck_after TEXT")
    # indexes for 100-property scale
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_urls_lookup "
        "ON urls(property, indexed, last_checked)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_urls_priority ON urls(property, priority)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_slots_prop ON manual_slots(property, used_at)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_prop ON snapshots(property, day)")
    conn.commit()
    return conn


# ---------------- URL tracking (used by CLI + dashboard) ----------------

def upsert_new(conn, prop, urls):
    """Track new URLs, storing each in its ORIGINAL form.

    Canonicalization is used ONLY to detect variants of an already-tracked
    page (/a, /a/, /a?utm_x=1 stay ONE row) — it never rewrites the stored
    URL, because Google's verdict belongs to the exact URL form.
    """
    now = utcnow()
    cur = conn.cursor()
    seen_canon = {canonicalize_url(r[0]) for r in
                  cur.execute("SELECT url FROM urls WHERE property=?", (prop,)).fetchall()}
    added = 0
    for u in urls:
        u = (u or "").strip()
        if not u:
            continue
        cu = canonicalize_url(u)
        if cu in seen_canon:
            continue
        cur.execute(
            "INSERT OR IGNORE INTO urls (property, url, first_seen) VALUES (?, ?, ?)",
            (prop, u, now),
        )
        if cur.rowcount:
            added += 1
            seen_canon.add(cu)
    conn.commit()
    return added


# Params that never change the page — stripped for dedup purposes.
_TRACK_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "fbclid", "gclid", "msclkid", "igshid", "mc_cid", "mc_eid",
    "dclid", "wbraid", "gbraid",
})


def canonicalize_url(url):
    """Canonical form of a URL for dedup: lowercase scheme/host, default
    ports dropped, fragment dropped, trailing slash stripped (except root),
    tracking params removed, remaining query sorted. Path case is kept."""
    try:
        p = urllib.parse.urlparse((url or "").strip())
    except Exception:  # noqa: BLE001
        return url
    if p.scheme.lower() not in ("http", "https") or not p.netloc:
        return url
    scheme = p.scheme.lower()
    host = p.netloc.lower()
    if (scheme == "http" and host.endswith(":80")) or \
       (scheme == "https" and host.endswith(":443")):
        host = host.rsplit(":", 1)[0]
    path = p.path or "/"
    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    try:
        q = sorted(
            (k, v) for k, v in urllib.parse.parse_qsl(p.query, keep_blank_values=True)
            if k.lower() not in _TRACK_PARAMS
        )
    except Exception:  # noqa: BLE001
        q = []
    return urllib.parse.urlunparse(
        (scheme, host, path, "", urllib.parse.urlencode(q), ""))


def merge_duplicate_urls(conn, prop):
    """Merge rows that canonicalize to the same URL. Keeps one survivor per
    page (indexed wins, then earliest seen), folding timestamps/attempts.
    The survivor keeps its ORIGINAL stored URL — canonicalization is only
    for grouping and must never rewrite a tracked URL (rewriting broke
    Google verdicts on 2026-10-16: stripped trailing slashes made Google
    report 'URL is unknown to Google' for indexed pages).
    Returns the number of removed duplicate rows."""
    rows = [dict(r) for r in
            conn.execute("SELECT * FROM urls WHERE property=?", (prop,)).fetchall()]
    groups = {}
    for r in rows:
        groups.setdefault(canonicalize_url(r["url"]), []).append(r)
    merged = 0
    for canon, members in groups.items():
        if len(members) < 2:
            continue  # never rewrite a lone row's URL
        members.sort(key=lambda r: (0 if r["indexed"] else 1, r["first_seen"] or ""))
        keep = members[0]
        freshest = max(members, key=lambda r: r["last_checked"] or "")
        drop = [r["url"] for r in members[1:] if r["url"] != keep["url"]]
        # Delete losers FIRST to avoid UNIQUE collisions.
        for du in drop:
            conn.execute("DELETE FROM urls WHERE property=? AND url=?", (prop, du))
        conn.execute(
            """UPDATE urls SET first_seen=?, last_checked=?, indexed=?, index_state=?,
               last_submitted=?, submit_attempts=?, priority=?, http_status=?,
               last_bing_submitted=?, bing_attempts=?,
               last_indexnow_submitted=?, indexnow_attempts=?
               WHERE property=? AND url=?""",
            (min(r["first_seen"] or "" for r in members) or None,
             max([r["last_checked"] or "" for r in members]) or None,
             max(r["indexed"] or 0 for r in members),
             freshest["index_state"],
             max([r["last_submitted"] or "" for r in members]) or None,
             sum(r["submit_attempts"] or 0 for r in members),
             max(r["priority"] or 0 for r in members),
             freshest["http_status"],
             max([r["last_bing_submitted"] or "" for r in members]) or None,
             sum(r["bing_attempts"] or 0 for r in members),
             max([r["last_indexnow_submitted"] or "" for r in members]) or None,
             sum(r["indexnow_attempts"] or 0 for r in members),
             prop, keep["url"]))
        merged += len(drop)
    conn.commit()
    return merged


def restore_url_forms(conn, prop, true_urls):
    """Restore tracked URLs to their true (e.g. sitemap) forms.

    One-time repair for the 2026-10-16 canonicalization rewrite, which
    stripped trailing slashes from stored URLs and caused Google to report
    'URL is unknown to Google' for pages that are actually indexed.
    Only touches rows whose canonical form matches a true URL but whose
    stored form differs; resets last_checked so they are re-inspected
    promptly. Idempotent. Returns the number of rows restored.
    """
    fixed = 0
    for t in dict.fromkeys((u or "").strip() for u in true_urls or ()):
        if not t:
            continue
        canon = canonicalize_url(t)
        if canon == t:
            continue
        cur = conn.execute("SELECT * FROM urls WHERE property=? AND url=?",
                           (prop, canon))
        r = cur.fetchone()
        if not r:
            continue
        r = dict(r)
        trow = conn.execute("SELECT * FROM urls WHERE property=? AND url=?",
                            (prop, t)).fetchone()
        if trow:
            trow = dict(trow)
            conn.execute(
                """UPDATE urls SET first_seen=?, last_checked=NULL, indexed=?,
                   index_state=?, last_submitted=?, submit_attempts=?, priority=?,
                   http_status=?, last_bing_submitted=?, bing_attempts=?,
                   last_indexnow_submitted=?, indexnow_attempts=?
                   WHERE property=? AND url=?""",
                (min(trow.get("first_seen") or "", r.get("first_seen") or "") or None,
                 max(trow.get("indexed") or 0, r.get("indexed") or 0),
                 trow.get("index_state") or r.get("index_state"),
                 max(trow.get("last_submitted") or "", r.get("last_submitted") or "") or None,
                 (trow.get("submit_attempts") or 0) + (r.get("submit_attempts") or 0),
                 max(trow.get("priority") or 0, r.get("priority") or 0),
                 trow.get("http_status") or r.get("http_status"),
                 max(trow.get("last_bing_submitted") or "", r.get("last_bing_submitted") or "") or None,
                 (trow.get("bing_attempts") or 0) + (r.get("bing_attempts") or 0),
                 max(trow.get("last_indexnow_submitted") or "", r.get("last_indexnow_submitted") or "") or None,
                 (trow.get("indexnow_attempts") or 0) + (r.get("indexnow_attempts") or 0),
                 prop, t))
            conn.execute("DELETE FROM urls WHERE property=? AND url=?", (prop, canon))
        else:
            conn.execute(
                "UPDATE urls SET url=?, last_checked=NULL WHERE property=? AND url=?",
                (t, prop, canon))
        fixed += 1
    conn.commit()
    return fixed


def mark_checked(conn, prop, url, indexed, index_state):
    conn.execute(
        "UPDATE urls SET last_checked=?, indexed=?, index_state=?, "
        "recheck_after=NULL WHERE property=? AND url=?",
        (utcnow(), 1 if indexed else 0, index_state, prop, url),
    )
    conn.commit()


def schedule_recheck(conn, prop, url, hours=2):
    """Fast-lane: re-inspect this URL soon after a push to surface the
    result quickly (instead of waiting for the stale threshold)."""
    try:
        minutes = max(1, int(float(hours) * 60))
    except (TypeError, ValueError):
        minutes = 120
    conn.execute(
        "UPDATE urls SET recheck_after=datetime('now', ?) WHERE property=? AND url=?",
        (f"+{minutes} minutes", prop, url),
    )
    conn.commit()


def enqueue_push(conn, prop, urls, action="URL_UPDATED"):
    """Queue URLs for the background instant-push worker. Dedups; returns
    the number of newly queued URLs."""
    now = utcnow()
    added = 0
    for u in dict.fromkeys((x or "").strip() for x in urls or ()):
        if not u:
            continue
        cur = conn.execute(
            "INSERT OR IGNORE INTO push_queue(property, url, action, enqueued_at)"
            " VALUES(?,?,?,?)",
            (prop, u, action or "URL_UPDATED", now),
        )
        added += cur.rowcount
    conn.commit()
    return added


def queue_depth(conn):
    cur = conn.execute("SELECT COUNT(*) FROM push_queue")
    return cur.fetchone()[0]


def mark_submitted(conn, prop, url, note=""):
    conn.execute(
        "UPDATE urls SET last_submitted=?, submit_attempts=submit_attempts+1, notes=? "
        "WHERE property=? AND url=?",
        (utcnow(), note, prop, url),
    )
    conn.commit()


def mark_bing_submitted(conn, prop, url):
    """Record a successful Bing SubmitUrlBatch acceptance."""
    conn.execute(
        "UPDATE urls SET last_bing_submitted=?, bing_attempts=bing_attempts+1 "
        "WHERE property=? AND url=?",
        (utcnow(), prop, url),
    )
    conn.commit()


def mark_indexnow_submitted(conn, prop, url):
    """Record a successful IndexNow submission."""
    conn.execute(
        "UPDATE urls SET last_indexnow_submitted=?, indexnow_attempts=indexnow_attempts+1 "
        "WHERE property=? AND url=?",
        (utcnow(), prop, url),
    )
    conn.commit()


def candidates_for_bing(conn, prop, limit):
    """Non-indexed URLs never submitted to Bing, priority first."""
    cur = conn.execute(
        """SELECT url FROM urls WHERE property=? AND (indexed=0 OR indexed IS NULL)
           AND last_bing_submitted IS NULL
           ORDER BY priority DESC, first_seen LIMIT ?""",
        (prop, limit),
    )
    return [r[0] for r in cur.fetchall()]


def candidates_for_indexnow(conn, prop, limit):
    """Non-indexed URLs never submitted via IndexNow, priority first."""
    cur = conn.execute(
        """SELECT url FROM urls WHERE property=? AND (indexed=0 OR indexed IS NULL)
           AND last_indexnow_submitted IS NULL
           ORDER BY priority DESC, first_seen LIMIT ?""",
        (prop, limit),
    )
    return [r[0] for r in cur.fetchall()]


def candidates_for_inspection(conn, prop, limit, stale_hours=72):
    """URLs needing (re-)inspection: never checked, stale, or due for a
    fast-lane re-check after a push. Fast-lane URLs jump the queue."""
    cur = conn.execute(
        """SELECT url FROM urls WHERE property=?
           AND (last_checked IS NULL OR last_checked < datetime('now', ?)
                OR (recheck_after IS NOT NULL AND recheck_after <= datetime('now')))
           ORDER BY (recheck_after IS NOT NULL
                     AND recheck_after <= datetime('now')) DESC,
                    priority DESC, last_checked NULLS FIRST LIMIT ?""",
        (prop, f"-{stale_hours} hours", limit),
    )
    return [r[0] for r in cur.fetchall()]


def candidates_for_submission(conn, prop, limit, cooldown_hours=48):
    cur = conn.execute(
        """SELECT url FROM urls WHERE property=? AND (indexed=0 OR indexed IS NULL)
           AND (last_submitted IS NULL OR last_submitted < datetime('now', ?))
           ORDER BY priority DESC, first_seen LIMIT ?""",
        (prop, f"-{cooldown_hours} hours", limit),
    )
    return [r[0] for r in cur.fetchall()]


def summary(conn, prop):
    cur = conn.execute(
        """SELECT COUNT(*),
                  COALESCE(SUM(indexed),0),
                  SUM(CASE WHEN last_submitted IS NOT NULL THEN 1 ELSE 0 END),
                  SUM(CASE WHEN last_checked IS NULL THEN 1 ELSE 0 END)
           FROM urls WHERE property=?""",
        (prop,),
    )
    total, indexed, submitted, unchecked = cur.fetchone()
    return {
        "total": total or 0,
        "indexed": indexed or 0,
        "submitted": submitted or 0,
        "unchecked": unchecked or 0,
    }


# ---------------- properties ----------------

def set_sitemaps(conn, site_url, sitemaps):
    conn.execute("UPDATE properties SET sitemaps=? WHERE site_url=?",
                 (json.dumps(sitemaps), site_url))
    conn.commit()


def add_property(conn, site_url, sitemaps):
    conn.execute(
        "INSERT OR IGNORE INTO properties(site_url, sitemaps, added_at) VALUES(?,?,?)",
        (site_url, json.dumps(sitemaps), utcnow()),
    )
    conn.commit()


def list_properties(conn, active_only=False):
    q = "SELECT * FROM properties"
    if active_only:
        q += " WHERE active=1"
    q += " ORDER BY site_url"
    return [dict(r) for r in conn.execute(q).fetchall()]


def get_property(conn, pid):
    r = conn.execute("SELECT * FROM properties WHERE id=?", (pid,)).fetchone()
    return dict(r) if r else None


def set_property_active(conn, pid, active):
    conn.execute("UPDATE properties SET active=? WHERE id=?", (1 if active else 0, pid))
    conn.commit()


def set_push_only(conn, pid, push_only):
    """Push-only mode: skip index inspections (no GSC access), only push URLs
    to the Indexing API. For properties the service account can't access."""
    conn.execute("UPDATE properties SET push_only=? WHERE id=?",
                 (1 if push_only else 0, pid))
    conn.commit()


def delete_property(conn, pid):
    p = get_property(conn, pid)
    if not p:
        return
    conn.execute("DELETE FROM urls WHERE property=?", (p["site_url"],))
    conn.execute("DELETE FROM snapshots WHERE property=?", (p["site_url"],))
    conn.execute("DELETE FROM manual_slots WHERE property=?", (p["site_url"],))
    conn.execute("DELETE FROM ai_reports WHERE property=?", (p["site_url"],))
    conn.execute("DELETE FROM properties WHERE id=?", (pid,))
    conn.commit()


# ---------------- settings ----------------

def get_setting(conn, key, default=""):
    r = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return r[0] if r else default


def set_setting(conn, key, value):
    conn.execute(
        "INSERT INTO settings(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()


# ---------------- snapshots / history ----------------

def save_snapshot(conn, prop, total, indexed):
    conn.execute(
        "INSERT INTO snapshots(day,property,total,indexed) VALUES(?,?,?,?) "
        "ON CONFLICT(day,property) DO UPDATE SET total=excluded.total, indexed=excluded.indexed",
        (today(), prop, total, indexed),
    )
    conn.commit()


def get_snapshots(conn, prop, days=30):
    rows = conn.execute(
        "SELECT day,total,indexed FROM snapshots WHERE property=? "
        "ORDER BY day DESC LIMIT ?",
        (prop, days),
    ).fetchall()
    rows = list(reversed(rows))
    return {
        "labels": [r[0] for r in rows],
        "total": [r[1] for r in rows],
        "indexed": [r[2] for r in rows],
    }


# ---------------- manual "Request Indexing" slots ----------------

def log_manual_slot(conn, prop, url):
    conn.execute(
        "INSERT INTO manual_slots(property,url,used_at) VALUES(?,?,?)",
        (prop, url, utcnow()),
    )
    conn.commit()


def manual_slots_used_24h(conn, prop):
    r = conn.execute(
        "SELECT COUNT(*) FROM manual_slots WHERE property=? "
        "AND used_at > datetime('now','-24 hours')",
        (prop,),
    ).fetchone()
    return r[0] if r else 0


# ---------------- dashboard queries ----------------

def stuck_count(conn, prop, days=7):
    r = conn.execute(
        """SELECT COUNT(*) FROM urls WHERE property=?
           AND (indexed=0 OR indexed IS NULL)
           AND first_seen < datetime('now', ?)""",
        (prop, f"-{days} days"),
    ).fetchone()
    return r[0] if r else 0


def coverage_breakdown(conn, prop):
    rows = conn.execute(
        """SELECT COALESCE(NULLIF(index_state,''),'Not checked yet') AS state, COUNT(*)
           FROM urls WHERE property=? GROUP BY state ORDER BY 2 DESC""",
        (prop,),
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


FILTERS = {
    "all": "",
    "indexed": "AND indexed=1",
    "unindexed": "AND (indexed=0 OR indexed IS NULL) AND last_checked IS NOT NULL",
    "unchecked": "AND last_checked IS NULL",
    "submitted": "AND last_submitted IS NOT NULL",
    "unsubmitted": "AND (indexed=0 OR indexed IS NULL) AND last_submitted IS NULL",
    "stuck": "AND (indexed=0 OR indexed IS NULL) AND first_seen < datetime('now','-7 days')",
    "dead": "AND http_status >= 400",
}


def count_urls(conn, prop, filt="all", search=""):
    where = FILTERS.get(filt, "")
    params = [prop]
    if search:
        where += " AND url LIKE ?"
        params.append(f"%{search}%")
    r = conn.execute(
        f"SELECT COUNT(*) FROM urls WHERE property=? {where}", params
    ).fetchone()
    return r[0] if r else 0


def list_urls(conn, prop, filt="all", search="", limit=500, offset=0):
    where = FILTERS.get(filt, "")
    params = [prop]
    if search:
        where += " AND url LIKE ?"
        params.append(f"%{search}%")
    params += [limit, offset]
    rows = conn.execute(
        f"""SELECT url, first_seen, last_checked, indexed, index_state,
                   last_submitted, submit_attempts, priority, http_status,
                   last_bing_submitted, bing_attempts,
                   last_indexnow_submitted, indexnow_attempts
            FROM urls WHERE property=? {where}
            ORDER BY priority DESC, first_seen DESC LIMIT ? OFFSET ?""",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def save_ai_report(conn, prop, report):
    conn.execute(
        "INSERT INTO ai_reports(property, report, created_at) VALUES(?,?,?) "
        "ON CONFLICT(property) DO UPDATE SET report=excluded.report, "
        "created_at=excluded.created_at",
        (prop, report, utcnow()),
    )
    conn.commit()


def get_ai_report(conn, prop):
    r = conn.execute(
        "SELECT report, created_at FROM ai_reports WHERE property=?",
        (prop,),
    ).fetchone()
    return dict(r) if r else None


def set_http_status(conn, prop, url, status):
    """Record the HTTP status seen when fetching a URL (dead-link hygiene)."""
    conn.execute(
        "UPDATE urls SET http_status=? WHERE property=? AND url=?",
        (status, prop, url),
    )
    conn.commit()


def dead_count(conn, prop):
    r = conn.execute(
        "SELECT COUNT(*) FROM urls WHERE property=? AND http_status >= 400",
        (prop,),
    ).fetchone()
    return r[0] if r else 0


def set_priority(conn, prop, urls, priority):
    conn.executemany(
        "UPDATE urls SET priority=? WHERE property=? AND url=?",
        [(priority, prop, u) for u in urls],
    )
    conn.commit()


def delete_urls(conn, prop, urls):
    """Remove URLs from tracking (e.g. junk like sitemap.xml itself)."""
    conn.executemany(
        "DELETE FROM urls WHERE property=? AND url=?",
        [(prop, u) for u in urls],
    )
    conn.commit()
