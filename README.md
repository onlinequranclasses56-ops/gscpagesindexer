# GSC Multi-Site Indexing Pipeline

A small, honest tool for local SEO workflows: it tracks index status across
multiple Search Console properties and pushes crawl signals the **official**
way. No fake "force index" claims — Google always makes the final call.

## Try it in 2 minutes (no Google account needed)

```bash
pip install -r requirements.txt
python app.py
# open http://127.0.0.1:5000 -> click "Load demo data"
```

You get 3 fake local-SEO sites with realistic index states (indexed,
crawled-not-indexed, stuck >7 days, unchecked), 14-day history charts,
quota trackers and all filters — every screen works. Reset anytime from
Settings → Demo mode. When you're ready for real data, add your service
account key in Settings and import your sites.

## What it does (daily)

1. Pulls URLs from each property's sitemap(s) into a local SQLite DB.
2. Checks index status via the **URL Inspection API** (2000/day/property).
3. Optionally pushes eligible URLs to the **Indexing API** (200/day/project).
4. Resubmits sitemaps — the official crawl hint for general pages.
5. Prints a per-property summary (total / indexed / submitted).

## What it does NOT do

- It cannot *force* indexing. The Indexing API is a crawl request; Google
  decides whether to index based on quality signals.
- There is no API for the "Request Indexing" button in Search Console.
  That stays manual (~10–13 URLs/day/property in the UI).

## The Indexing API rule (important)

Google officially supports the Indexing API **only** for pages with
`JobPosting` or `BroadcastEvent` (in `VideoObject`) structured data.
`use_indexing_api` is `false` by default — only enable it if your pages
actually carry that markup. Using it for general pages works in practice
for many tools but is outside documented terms and can be throttled.

## Option A — Web dashboard (recommended)

```bash
pip install -r requirements.txt
cp config.example.yaml config.yaml   # optional: pre-fill key path + sites
python app.py
# open http://127.0.0.1:5000
```

- **Dashboard**: all properties with indexed %, "stuck >7 days" alerts.
- **Property page**: filters (indexed / not indexed / unchecked / stuck),
  search, coverage breakdown, bulk actions (priority, Indexing API push),
  URL import, CSV export, index-history chart.
- **Manual "Request indexing" tracker**: logs each GSC button click so you
  never burn your ~10/day/property UI quota blindly (no API exists for it).
- **Scheduler**: runs the full pipeline daily at the time set in Settings.
- First run imports key path, quotas and properties from `config.yaml`
  if it exists; afterwards everything is managed in the UI.
- Keep it on localhost — this MVP has no login.

## Option B — CLI only

1. **Google Cloud project** — create one at console.cloud.google.com.
2. **Enable APIs**: "Google Search Console API" and (if needed)
   "Web Search Indexing API".
3. **Service account** — create one, download the JSON key.
4. **Grant access** — in Search Console, add the service account email as
   an **Owner** of every property (Settings → Users and permissions).
5. Install deps and configure:

```bash
pip install -r requirements.txt
cp config.example.yaml config.yaml
# edit config.yaml: key path + your properties
python indexer.py --config config.yaml
```

6. **Schedule daily** (cron example, runs 9:00 AM):

```
0 9 * * * /usr/bin/python3 /path/to/gsc-indexer/indexer.py --config /path/to/gsc-indexer/config.yaml >> /tmp/gsc-indexer.log 2>&1
```

## Quotas (Google's, not ours)

| API | Limit |
|---|---|
| URL Inspection | 2,000/day **per property** |
| Indexing API | 200/day **per GCP project** (auto-rotated across your keys) |
| "Request Indexing" button | no API; ~10–13/day/property via UI |

The tool tracks `last_checked` / `last_submitted` per URL so daily runs
rotate through the backlog instead of burning quota on repeats.

## Scaling to 100 sites

- **Onboarding**: Settings → Bulk import — paste 100 domains, sitemaps are
  auto-discovered from each site's `robots.txt` in the background.
- **Indexing API budget**: 200/day/project is shared, so add one service
  account key **per GCP project** in Settings (5 projects = 1,000 pushes/day).
  The pipeline rotates through them automatically.
- **Speed**: inspections run on parallel workers per property, and multiple
  properties sync in parallel (`inspection_workers` / `property_workers`
  in Settings). API calls retry transient failures; quota hits pause that
  property and resume next run — one failing site never kills the run.
- **Triage at scale**: the dashboard sorts worst-indexed first, has search
  + pagination, and a "Needs attention" panel for sites with pages stuck
  unindexed >7 days.
