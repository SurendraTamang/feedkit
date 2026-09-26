import os
import time
from datetime import date
from typing import Iterator

import httpx
from parsel import Selector

from feedkit import DELAY, USER_AGENT

NAME = "books"
REQUIRED_FIELDS = ["title", "price"]
BASE = "https://books.toscrape.com/catalogue/"


def fetch(run_date: date, stats) -> Iterator[dict]:
    pages = int(os.environ.get("FEEDKIT_BOOKS_PAGES", "5"))
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30, follow_redirects=True) as client:
        for n in range(1, pages + 1):
            r = client.get(f"{BASE}page-{n}.html")
            if r.status_code in (403, 429):
                stats.blocked += 1
                continue
            r.raise_for_status()
            for card in Selector(r.text).css("article.product_pod"):
                href = card.css("h3 a::attr(href)").get()
                yield {
                    "external_id": href.split("/")[0],
                    "title": card.css("h3 a::attr(title)").get(),
                    "price": card.css("p.price_color::text").get("").lstrip("Â£"),
                    "availability": " ".join(
                        t.strip() for t in card.css("p.availability::text").getall() if t.strip()
                    ),
                    "url": BASE + href,
                }
            time.sleep(DELAY)
