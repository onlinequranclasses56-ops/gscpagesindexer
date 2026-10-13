"""SQLite tracking for the GSC indexer (CLI + web dashboard)."""
import json
import sqlite3
from datetime import datetime, timezone

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
    # migrate: priority column for DBs created by older versions
    cols = [r[1] for r in conn.execute("PRAGMA table_info(urls)").fetchall()]
    if "priority" not in cols:
        conn.execute("ALTER TABLE urls ADD COLUMN priority INTEGER NOT NULL DEFAULT 0")
    # migrate: http_status for dead-link detection
    if "http_status" not in cols:
        conn.execute("ALTER TABLE urls ADD COLUMN http_status INTEGER")
    # migrate: push_only mode for properties without GSC access
    pcols = [r[1] for r in conn.execute("PRAGMA table_info(properties)").fetchall()]
    if "push_only" not in pcols:
        conn.execute("ALTER TABLE properties ADD COLUMN push_only INTEGER NOT NULL DEFAULT 0")
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
    now = utcnow()
    cur = conn.cursor()
    added = 0
    for u in urls:
        cur.execute(
            "INSERT OR IGNORE INTO urls (property, url, first_seen) VALUES (?, ?, ?)",
            (prop, u, now),
        )
        added += cur.rowcount
    conn.commit()
    return added


def mark_checked(conn, prop, url, indexed, index_state):
    conn.execute(
        "UPDATE urls SET last_checked=?, indexed=?, index_state=? "
        "WHERE property=? AND url=?",
        (utcnow(), 1 if indexed else 0, index_state, prop, url),
    )
    conn.commit()


def mark_submitted(conn, prop, url, note=""):
    conn.execute(
        "UPDATE urls SET last_submitted=?, submit_attempts=submit_attempts+1, notes=? "
        "WHERE property=? AND url=?",
        (utcnow(), note, prop, url),
    )
    conn.commit()


def candidates_for_inspection(conn, prop, limit, stale_hours=72):
    cur = conn.execute(
        """SELECT url FROM urls WHERE property=?
           AND (last_checked IS NULL OR last_checked < datetime('now', ?))
           ORDER BY priority DESC, last_checked NULLS FIRST LIMIT ?""",
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
                   last_submitted, submit_attempts, priority, http_status
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
