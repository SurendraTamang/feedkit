# feedkit

Turn any scraper into a monitored data feed.

- **One row per record.** Deduplicated on `(source, external_id)`.
- **Full history.** SCD Type 2 versions (`valid_from` / `valid_to`), written only when content changes.
- **Every run recorded.** Rows seen, new, changed, blocked, status and error.
- **Safe reruns.** Same source + date twice changes nothing.
- **Scraper-agnostic.** A source is any function that yields dicts: Scrapy, httpx or Playwright inside.

## Quick start

```bash
docker compose up -d --build
docker compose run --rm app feedkit run --source books
docker compose run --rm app feedkit run --source hackernews
```

## Querying history

```sql
-- a record as it was on a given date
select data from record_versions
where source = 'hackernews' and external_id = '123'
  and valid_from <= '2026-09-30' and (valid_to > '2026-09-30' or valid_to is null);

-- last 10 runs
select source, run_date, status, rows_seen, rows_new, rows_changed, blocked
from runs order by run_id desc limit 10;
```

## Adding a source

Create `src/feedkit/sources/<name>.py` with `NAME`, `REQUIRED_FIELDS` and `fetch(run_date, stats)` yielding dicts that include `external_id`. Increment `stats.blocked` on 403/429/challenge pages. Register it in `src/feedkit/sources/__init__.py`.

## Development

```bash
uv sync
docker compose up -d db
export DATABASE_URL=postgresql://feedkit:feedkit@localhost:5433/feedkit_test
uv run pytest -v
```

Tests truncate tables, so they run against `feedkit_test`, a separate database from `feedkit`, to avoid wiping real data.

## Compliance

The demo sources are [books.toscrape.com](https://books.toscrape.com), a sandbox built for scraping practice, and the official [Hacker News API](https://github.com/HackerNews/API). No usernames are stored. Sources for real sites should respect the site's terms and rate limits and avoid collecting personal data that isn't needed.

## Roadmap

Checks and alerts (zero rows, drops, freshness, nulls, block rate) → Airflow DAG → TypeScript/Crawlee sources → AWS deploy → LLM extraction with accuracy checks.
