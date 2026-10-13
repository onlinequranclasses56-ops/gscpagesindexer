#!/usr/bin/env python3
"""Demo mode: seed realistic fake data so you can try the dashboard
without any Google credentials.

  python demo.py            # seed into indexing.db
  python demo.py --reset    # remove demo data again
"""
import random
import sys
from datetime import datetime, timedelta, timezone

import db as store

DEMO_PREFIX = "sc-domain:demo"

SERVICES = ["emergency-plumber", "drain-cleaning", "water-heater-repair",
            "leak-detection", "sewer-line-repair", "toilet-repair",
            "faucet-installation", "gas-line-services"]
LOCATIONS = ["downtown", "north-austin", "round-rock", "cedar-park",
             "georgetown", "pflugerville", "lakeway", "dripping-springs",
             "bee-cave", "west-lake"]
POSTS = ["how-much-does-drain-cleaning-cost", "signs-you-need-water-heater-repair",
         "prevent-frozen-pipes-winter", "tank-vs-tankless-water-heater",
         "why-is-my-water-bill-so-high", "sewer-smell-in-bathroom-fix",
         "how-long-do-water-heaters-last", "diy-vs-pro-plumbing-repairs"]

SITES = [
    ("sc-domain:demoplumbing-austin.com",
     "https://demoplumbing-austin.com/sitemap.xml"),
    ("sc-domain:demodental-dallas.com",
     "https://demodental-dallas.com/sitemap.xml"),
    ("sc-domain:demoroofing-houston.com",
     "https://demoroofing-houston.com/sitemap.xml"),
]

COVERAGE_INDEXED = ["Submitted and indexed", "Indexed, not submitted in sitemap"]
COVERAGE_NOT = ["Crawled - currently not indexed",
                "Discovered - currently not indexed",
                "Alternate page with proper canonical tag",
                "Duplicate without user-selected canonical"]


def _days_ago(n):
    return (datetime.now(timezone.utc) - timedelta(days=n)).isoformat(timespec="seconds")


def seed(conn, rng_seed=7):
    rng = random.Random(rng_seed)
    for site, sm in SITES:
        store.add_property(conn, site, [sm])
        domain = site.replace("sc-domain:", "https://")
        paths = ["/"]
        paths += [f"/services/{s}/" for s in SERVICES]
        paths += [f"/locations/{loc}/" for loc in LOCATIONS]
        paths += [f"/blog/{p}/" for p in POSTS]
        urls = [domain + p for p in paths]
        store.upsert_new(conn, site, urls)
        for u in urls:
            first_seen = _days_ago(rng.randint(1, 25))
            roll = rng.random()
            if roll < 0.62:  # indexed
                indexed, cov = 1, rng.choice(COVERAGE_INDEXED)
                checked = _days_ago(rng.randint(0, 3))
            elif roll < 0.85:  # not indexed (some stuck)
                indexed, cov = 0, rng.choice(COVERAGE_NOT)
                checked = _days_ago(rng.randint(0, 3))
                if rng.random() < 0.4:  # stuck > 7 days
                    first_seen = _days_ago(rng.randint(8, 22))
            else:  # never checked
                indexed, cov, checked = None, "", None
            submitted = None
            attempts = 0
            if indexed == 0 and rng.random() < 0.5:
                submitted = _days_ago(rng.randint(1, 4))
                attempts = rng.randint(1, 3)
            priority = 10 if ("/services/" in u and rng.random() < 0.3) else 0
            conn.execute(
                """UPDATE urls SET first_seen=?, last_checked=?, indexed=?,
                          index_state=?, last_submitted=?, submit_attempts=?,
                          priority=?, notes=? WHERE property=? AND url=?""",
                (first_seen, checked, indexed, cov, submitted, attempts,
                 priority, "demo data", site, u),
            )
        # 14-day history: indexed count grows over time
        total = len(urls)
        base = int(total * 0.35)
        for d in range(14, 0, -1):
            day = (datetime.now(timezone.utc) - timedelta(days=d)).date().isoformat()
            indexed_n = min(total, base + int((14 - d) * total * 0.02) + rng.randint(-2, 2))
            conn.execute(
                "INSERT INTO snapshots(day,property,total,indexed) VALUES(?,?,?,?) "
                "ON CONFLICT(day,property) DO UPDATE SET total=excluded.total,"
                " indexed=excluded.indexed",
                (day, site, total, max(0, indexed_n)),
            )
        conn.commit()
    return len(SITES)


def reset(conn):
    cur = conn.execute(
        "SELECT id FROM properties WHERE site_url LIKE ?", (DEMO_PREFIX + "%",))
    ids = [r[0] for r in cur.fetchall()]
    for pid in ids:
        store.delete_property(conn, pid)
    return len(ids)


if __name__ == "__main__":
    import os
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "indexing.db")
    conn = store.connect(db_path)
    try:
        if "--reset" in sys.argv:
            print("removed", reset(conn), "demo properties")
        else:
            print("seeded", seed(conn), "demo properties ->", db_path)
    finally:
        conn.close()
