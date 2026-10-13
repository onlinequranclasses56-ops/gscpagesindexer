#!/usr/bin/env python3
"""AI Index Analyst — grounded SEO analysis via an OpenAI-compatible API.

Config (env vars):
  AI_API_KEY   required — your provider key (never shown in the UI)
  AI_BASE_URL  optional — default https://api.openai.com/v1 (any
               OpenAI-compatible endpoint works)
  AI_MODEL     optional — default gpt-4o-mini

Only stdlib (urllib) is used, so no new dependencies. The model only ever
sees the property's real coverage data, and is instructed never to invent
facts or guarantee indexing.
"""
import json
import os
import urllib.error
import urllib.request

DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_MODEL = "gpt-4o-mini"

# Master-trained multi-discipline expert persona for the AI Index Analyst.
# Users can override it from Settings ("AI expert instructions").
EXPERT_SYSTEM_PROMPT = """You are the master SEO consultant for a local SEO agency managing 100+
local service business websites — locksmiths, car detailing, cleaning companies,
car lifts, taxis across the UK, UAE and USA. You have 15 years of battle-tested
experience and you think in revenue impact, not vanity metrics.

YOUR 5 MASTERIES:

1. LOCAL SEO MASTER — Location and service-area pages are money pages. You
   instantly spot: missing service+city combinations, thin/duplicate location
   pages, weak internal linking to location pages, missing LocalBusiness schema,
   NAP inconsistencies. "Discovered - currently not indexed" on a location page
   is an emergency to you.
2. TECHNICAL SEO MASTER — Crawl budget, indexation, Core Web Vitals, XML
   sitemaps, robots.txt, canonicals, redirect chains, duplicate and thin
   content, site architecture. You diagnose WHY Google refuses to index a page
   from its coverage verdict alone.
3. AEO / AIO / GEO MASTER — You optimize for AI Overviews, featured snippets
   and LLM citations. You prescribe answer-first content blocks, FAQ and
   Speakable schema, crisp entity definitions, and quotable facts that AI
   engines love to cite.
4. NEXT.JS + TAILWIND MASTER DEVELOPER — You know exactly how rendering breaks
   indexing: client-side hydration hiding content from crawlers, ISR/SSG
   misconfiguration, missing metadata API, broken programmatic sitemaps. Every
   technical fix you suggest comes with a concrete implementation hint.
5. GSC INDEXING API MASTER OPERATOR — You read URL Inspection verdicts like a
   native. You know URL_UPDATED vs URL_DELETED, the 200/day/project quota and
   resubmit cooldowns — and you build push queues ruthlessly, money pages first.

HOW YOU ANALYZE EVERY PROPERTY:
1. PATTERNS — cluster the unindexed URLs by path segment, depth and page type.
   Name each cluster.
2. TOP 5 ACTIONS — ordered by expected revenue impact. Label each [Local SEO],
   [Technical], [AEO/GEO], [Next.js] or [Indexing API]. Concrete, with real
   URLs. Zero theory.
3. PUSH QUEUE — the exact 10 URLs to push first via the Indexing API, and why
   each earns its slot.
4. CLIENT-READY SUMMARY — 3 bullets in plain non-technical language, ready to
   paste into a client report.

RULES — non-negotiable:
- Analyze ONLY the data provided. Never invent URLs, numbers, or facts.
- Never promise or guarantee indexing — Google alone decides.
- Max 400 words. Direct, no fluff, no introductions."""


def is_configured():
    return bool(os.environ.get("AI_API_KEY"))


def _prompt(d):
    lines = [
        f"Property: {d['site_url']}",
        f"URLs tracked: {d['total']}, indexed: {d['indexed']} ({d['pct']}%)",
        "Coverage breakdown:",
    ]
    for name, cnt in d["coverage"]:
        lines.append(f"- {name}: {cnt}")
    lines += [
        f"Stuck (not indexed for >7 days): {d['stuck']}",
        f"Dead links (HTTP 4xx/5xx): {d['dead']}",
        ("Trend vs yesterday: %+d indexed" % d["trend"])
        if d.get("trend") is not None else "Trend vs yesterday: n/a",
        f"URLs ever submitted to Indexing API: {d['submitted']}",
    ]
    if d.get("unindexed_sample"):
        lines.append("Sample unindexed URLs:")
        lines += [f"- {u}" for u in d["unindexed_sample"][:30]]
    lines.append(
        "Task:\n"
        "1) PATTERNS — group the unindexed URLs: common path segments, depth, "
        "page types (location pages? guides? thin/duplicate clusters?).\n"
        "2) TOP 5 ACTIONS — prioritized by expected impact. Be concrete, "
        "reference actual URLs, and label each with its discipline: "
        "[Local SEO] [Technical] [AEO/GEO] [Next.js] [Indexing API].\n"
        "3) PUSH QUEUE — which 10 URLs to push first via the Indexing API "
        "and why.\n"
        "4) Where relevant add: local-SEO notes (location-page coverage gaps), "
        "a Next.js/Tailwind implementation hint (metadata API, sitemap "
        "generation, ISR, crawler-blocking hydration), and one AEO/GEO tweak "
        "(FAQ/Speakable schema, answer-first block) for AI citations.\n"
        "Max 400 words. Reference only the data above."
    )
    return "\n".join(lines)


def analyze(data, timeout=120, system_prompt=None):
    """Run the analysis. Returns (ok, text).

    system_prompt overrides the built-in expert persona (e.g. from the
    "AI expert instructions" setting) so the user can keep training it.
    """
    key = os.environ.get("AI_API_KEY", "")
    if not key:
        return (False,
                "AI_API_KEY is not set. Add it under Render → Environment → "
                "Variables (get a key from your AI provider).")
    base = os.environ.get("AI_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    model = os.environ.get("AI_MODEL", DEFAULT_MODEL)
    body = json.dumps({
        "model": model,
        "temperature": 0.3,
        "max_tokens": 2200,
        "messages": [
            {"role": "system",
             "content": system_prompt or EXPERT_SYSTEM_PROMPT},
            {"role": "user", "content": _prompt(data)},
        ],
    }).encode()
    try:
        req = urllib.request.Request(
            base + "/chat/completions", data=body,
            headers={"Authorization": "Bearer " + key,
                     "Content-Type": "application/json",
                     # Groq sits behind Cloudflare, which 403s Python's
                     # default "Python-urllib/x.y" user-agent before the
                     # request ever reaches auth. A real UA passes through.
                     "User-Agent": "gsc-indexer/1.0 (+ai-analyst)"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            out = json.load(r)
        text = out["choices"][0]["message"]["content"].strip()
        return (True, text)
    except urllib.error.HTTPError as e:
        # Surface the provider's error body so auth problems are
        # distinguishable from WAF/network blocks.
        try:
            detail = e.read().decode("utf-8", "replace")[:200]
        except Exception:  # noqa: BLE001
            detail = ""
        return (False,
                f"AI request failed: HTTP {e.code} {e.reason} {detail}"[:300])
    except Exception as e:  # noqa: BLE001
        return (False, f"AI request failed: {e}"[:300])
