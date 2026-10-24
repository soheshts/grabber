#!/usr/bin/env python3
"""Fetch RSS/Atom feeds listed in sources.json and merge into data/items.json."""
import calendar
import html
import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse

import feedparser
import requests

SOURCES = Path("sources.json")
OUT = Path("data/items.json")
UA = "Mozilla/5.0 (compatible; FeedGrabber/1.0)"
TIMEOUT = 20
MAX_PER_FEED = 50          # newest entries kept per feed per run
MAX_AGE_DAYS = 30          # drop items older than this
MAX_TOTAL = 3000           # hard cap on stored items
OG_FETCH_LIMIT = 20        # max og:image page fetches per run (slow)

session = requests.Session()
session.headers.update({"User-Agent": UA})


def clean_text(raw, limit=500):
    text = re.sub(r"<[^>]+>", " ", raw or "")
    text = re.sub(r"\s+", " ", html.unescape(text)).strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def entry_date(e):
    t = e.get("published_parsed") or e.get("updated_parsed")
    if not t:
        return None
    return datetime.fromtimestamp(calendar.timegm(t), tz=timezone.utc).isoformat()


def entry_author(e, feed):
    """Atom <author><name>, RSS <author>/<dc:creator>, then feed-level author."""
    names = [a.get("name") for a in e.get("authors", []) or [] if a.get("name")]
    if names:
        return ", ".join(dict.fromkeys(names))
    name = e.get("author") or (e.get("author_detail") or {}).get("name") or feed.get("author")
    return clean_text(name, 200) or None


def favicon(site_url):
    host = urlparse(site_url).netloc
    return f"https://www.google.com/s2/favicons?domain={host}&sz=64" if host else None


def feed_icon(feed, fallback_url):
    """RSS <image>, Atom <logo>/<icon>, then favicon fallback."""
    img = feed.get("image")
    if img and img.get("href"):
        return img["href"]
    for key in ("icon", "logo"):
        if feed.get(key):
            return feed[key]
    return favicon(feed.get("link") or fallback_url)


def entry_image(e, base):
    # media:thumbnail
    for t in e.get("media_thumbnail", []) or []:
        if t.get("url"):
            return urljoin(base, t["url"])
    # media:content
    for m in e.get("media_content", []) or []:
        if m.get("url") and (m.get("medium") == "image" or "image" in m.get("type", "")):
            return urljoin(base, m["url"])
    # enclosures / Atom links with image type
    for l in (e.get("enclosures", []) or []) + (e.get("links", []) or []):
        if "image" in (l.get("type") or "") and l.get("href"):
            return urljoin(base, l["href"])
    # first <img> in content or summary
    blobs = [c.get("value", "") for c in e.get("content", []) or []]
    blobs.append(e.get("summary", ""))
    for b in blobs:
        m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', b or "", re.I)
        if m:
            return urljoin(base, html.unescape(m.group(1)))
    return None


def og_image(url):
    try:
        r = session.get(url, timeout=TIMEOUT, stream=True)
        chunk = next(r.iter_content(150_000), b"").decode(r.encoding or "utf-8", "ignore")
        r.close()
        for pat in (
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)',
            r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
        ):
            m = re.search(pat, chunk, re.I)
            if m:
                return urljoin(url, html.unescape(m.group(1)))
    except Exception:
        pass
    return None


def fetch_source(src):
    r = session.get(src["url"], timeout=TIMEOUT)
    r.raise_for_status()
    parsed = feedparser.parse(r.content)  # handles RSS 0.9x/1.0/2.0 and Atom
    if parsed.bozo and not parsed.entries:
        raise ValueError(f"unparseable feed: {parsed.bozo_exception}")
    feed = parsed.feed
    icon = feed_icon(feed, src["url"])
    title = feed.get("title") or src["name"]
    items = []
    for e in parsed.entries[:MAX_PER_FEED]:
        link = e.get("link")
        uid = e.get("id") or link
        if not uid:
            continue
        content = (e.get("content") or [{}])[0].get("value", "")
        items.append({
            "id": uid,
            "title": clean_text(e.get("title"), 300),
            "published": entry_date(e),
            "author": entry_author(e, feed),
            "description": clean_text(e.get("summary") or content),
            "link": link,
            "image": entry_image(e, link or src["url"]),
            "feed_name": src["name"],
            "feed_title": title,
            "category": src.get("category"),
            "feed_url": src["url"],
            "feed_icon": icon,
        })
    return items


def main():
    sources = json.loads(SOURCES.read_text())
    existing = {}
    if OUT.exists():
        try:
            for it in json.loads(OUT.read_text()).get("items", []):
                existing[it["id"]] = it
        except json.JSONDecodeError:
            pass

    og_budget = OG_FETCH_LIMIT
    for src in sources:
        try:
            items = fetch_source(src)
        except Exception as ex:
            print(f"[skip] {src['name']}: {ex}")
            continue
        new = 0
        for it in items:
            old = existing.get(it["id"])
            if old:
                # refresh feed metadata, keep any image we already found
                it["image"] = it["image"] or old.get("image")
            else:
                new += 1
                if not it["image"] and it["link"] and og_budget > 0:
                    og_budget -= 1
                    it["image"] = og_image(it["link"])
            existing[it["id"]] = it
        print(f"[ok] {src['name']}: {len(items)} entries, {new} new")

    cutoff = (datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)).isoformat()
    merged = [i for i in existing.values() if not i["published"] or i["published"] >= cutoff]
    merged.sort(key=lambda i: i["published"] or "", reverse=True)
    merged = merged[:MAX_TOTAL]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(
        {"updated": datetime.now(timezone.utc).isoformat(), "items": merged},
        ensure_ascii=False, indent=2,
    ))
    print(f"Wrote {len(merged)} items")


if __name__ == "__main__":
    main()
