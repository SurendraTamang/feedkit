import os
import time
from datetime import date
from typing import Iterator

import httpx

from feedkit import DELAY, USER_AGENT

NAME = "hackernews"
REQUIRED_FIELDS = ["title", "score"]
API = "https://hacker-news.firebaseio.com/v0"


def fetch(run_date: date, stats) -> Iterator[dict]:
    limit = int(os.environ.get("FEEDKIT_HN_LIMIT", "100"))
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30) as client:
        r = client.get(f"{API}/topstories.json")
        r.raise_for_status()
        for item_id in r.json()[:limit]:
            resp = client.get(f"{API}/item/{item_id}.json")
            if resp.status_code in (403, 429):
                stats.blocked += 1
                continue
            resp.raise_for_status()
            item = resp.json()
            if not item or item.get("deleted") or item.get("dead"):
                continue
            yield {
                "external_id": str(item["id"]),
                "title": item.get("title"),
                "url": item.get("url"),
                "score": item.get("score"),
                "comments": item.get("descendants"),
            }
            time.sleep(DELAY)
