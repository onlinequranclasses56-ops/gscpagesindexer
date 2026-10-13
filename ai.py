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
        "1) Identify patterns in the unindexed URLs (common path segments, "
        "depth, page types).\n"
        "2) Top 5 prioritized actions to get them indexed, ordered by expected "
        "impact — be concrete and reference actual URLs.\n"
        "3) Which 10 URLs should be pushed first via the Indexing API and why.\n"
        "Max 320 words. Reference only the data above."
    )
    return "\n".join(lines)


def analyze(data, timeout=120):
    """Run the analysis. Returns (ok, text)."""
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
        "max_tokens": 1600,
        "messages": [
            {"role": "system",
             "content": "You are a senior technical SEO analyst specializing "
                        "in Google indexation. Analyze ONLY the data provided. "
                        "Never invent URLs, numbers, or facts. Never promise "
                        "or guarantee indexing — Google alone decides."},
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
