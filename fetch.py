#!/usr/bin/env python3
"""Fetch RSS/Atom feeds from sources.json and write one JSON file per category.

sources.json can be either:
  - a flat list:   [{"name": "...", "category": "tech", "url": "..."}]
  - grouped dict:  {"tech": [{"name": "...", "url": "..."}]}
Output: data/<category-slug>.json for each category, plus data/index.json.
"""
import calendar
import html
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse

import feedparser
import requests

SOURCES = Path("sources.json")
OUT_DIR = Path("data")
UA = "Mozilla/5.0 (compatible; FeedGrabber/1.0)"
TIMEOUT = 20
WORKERS = 16               # feeds fetched in parallel
MAX_PER_FEED = 20          # newest entries kept per feed per run
MAX_AGE_DAYS = 30          # drop items older than this
MAX_PER_CATEGORY = 1000    # hard cap on items stored per category file
OG_FETCH_LIMIT = 60        # max og:image page fetches per run (slow)

session = requests.Session()
session.headers.update({"User-Agent": UA})


def slugify(text):
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-") or "uncategorized"


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
    for t in e.get("media_thumbnail", []) or []:
        if t.get("url"):
            return urljoin(base, t["url"])
    for m in e.get("media_content", []) or []:
        if m.get("url") and (m.get("medium") == "image" or "image" in m.get("type", "")):
            return urljoin(base, m["url"])
    for l in (e.get("enclosures", []) or []) + (e.get("links", []) or []):
        if "image" in (l.get("type") or "") and l.get("href"):
            return urljoin(base, l["href"])
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


def load_sources():
    """Return {slug: {"name": display name, "sources": [source, ...]}}."""
    raw = json.loads(SOURCES.read_text(encoding="utf-8"))
    if isinstance(raw, dict):  # grouped format -> flat
        raw = [{**s, "category": cat} for cat, lst in raw.items() for s in lst]

    categories, seen = {}, set()
    for s in raw:
        url = (s.get("url") or "").strip()
        if not url:
            continue
        cat = (s.get("category") or "uncategorized").strip()
        slug = slugify(cat)
        if (slug, url) in seen:  # same feed listed twice in a category
            continue
        seen.add((slug, url))
        entry = categories.setdefault(slug, {"name": cat, "sources": []})
        entry["sources"].append({
            "name": (s.get("name") or url).strip(),
            "url": url,
            "category": entry["name"],
            "slug": slug,
        })
    return categories


def load_existing(slug):
    path = OUT_DIR / f"{slug}.json"
    if not path.exists():
        return {}
    try:
        return {it["id"]: it for it in json.loads(path.read_text(encoding="utf-8")).get("items", [])}
    except (json.JSONDecodeError, KeyError):
        return {}


def fetch_source(src):
    r = session.get(src["url"], timeout=TIMEOUT)
    r.raise_for_status()
    parsed = feedparser.parse(r.content)  # handles RSS and Atom
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
            "category": src["category"],
            "feed_url": src["url"],
            "feed_icon": icon,
        })
    return items


def main():
    categories = load_sources()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stores = {slug: load_existing(slug) for slug in categories}
    pending_og = []

    all_sources = [s for c in categories.values() for s in c["sources"]]
    print(f"{len(all_sources)} feeds in {len(categories)} categories")

    with ThreadPoolExecutor(WORKERS) as pool:
        futures = {pool.submit(fetch_source, s): s for s in all_sources}
        for fut in as_completed(futures):
            src = futures[fut]
            try:
                items = fut.result()
            except Exception as ex:
                print(f"[skip] {src['category']} / {src['name']}: {ex}")
                continue
            store = stores[src["slug"]]
            for it in items:
                old = store.get(it["id"])
                if old:
                    it["image"] = it["image"] or old.get("image")
                elif not it["image"] and it["link"]:
                    pending_og.append(it)
                store[it["id"]] = it

    # og:image fallback: newest new items first, limited per run
    pending_og.sort(key=lambda i: i["published"] or "", reverse=True)
    batch = pending_og[:OG_FETCH_LIMIT]
    with ThreadPoolExecutor(WORKERS) as pool:
        for it, img in zip(batch, pool.map(lambda i: og_image(i["link"]), batch)):
            it["image"] = img

    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(days=MAX_AGE_DAYS)).isoformat()
    index = []
    for slug, cat in categories.items():
        items = [i for i in stores[slug].values() if not i["published"] or i["published"] >= cutoff]
        items.sort(key=lambda i: i["published"] or "", reverse=True)
        items = items[:MAX_PER_CATEGORY]
        (OUT_DIR / f"{slug}.json").write_text(json.dumps(
            {"category": cat["name"], "updated": now.isoformat(), "count": len(items), "items": items},
            ensure_ascii=False, indent=2,
        ), encoding="utf-8")
        index.append({"name": cat["name"], "slug": slug, "file": f"{slug}.json",
                      "feeds": len(cat["sources"]), "count": len(items)})
        print(f"[ok] {slug}.json: {len(items)} items")

    (OUT_DIR / "index.json").write_text(
        json.dumps({"updated": now.isoformat(), "categories": index}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()