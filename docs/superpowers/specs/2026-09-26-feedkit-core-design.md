# feedkit core (weeks 1–3): design

## Purpose

A small, reusable kit that turns any scraper into a monitored data feed: every run is recorded, every record is stored once, and every change is kept as history. It is dropped into client projects to back the "monitored data feed" retainer. It is also the public portfolio repo.

This spec covers weeks 1–3 only: storage, run tracking, CLI, two demo sources, Docker. Checks and alerts (weeks 4–6) and Airflow (weeks 7–10) get their own specs.

## Principles

- **Scraper-agnostic.** A source is any Python function that yields dicts. It can use Scrapy, httpx or Playwright inside.
- **Idempotent runs.** Running the same source and date twice creates no duplicate rows and no new versions.
- **Standard patterns.** SCD Type 2 history, 12-factor config (`DATABASE_URL` from the environment), one command per source and date, so it drops into Airflow unchanged later.
- **Few dependencies.** Python 3.12, `psycopg[binary]` 3, `httpx`, `parsel`. `argparse` from stdlib. No ORM.

## Layout

```
feedkit/
  pyproject.toml
  schema.sql
  src/feedkit/
    __init__.py
    cli.py        # argparse: init-db, run
    store.py      # Postgres: runs, upsert, SCD2 versions
    runner.py     # executes one source for one date, fills the runs row
    sources/
      __init__.py # registry: name -> module
      books.py    # books.toscrape.com
      hackernews.py  # official HN Firebase API
  tests/test_store.py
  docker-compose.yml
  Dockerfile
  crontab
  README.md
```

## Source interface

```python
# sources/<name>.py
NAME = "books"
REQUIRED_FIELDS = ["title", "price"]      # used by week 4–6 checks

def fetch(run_date: date, stats: Stats) -> Iterator[dict]:
    """Yield records. Each dict must contain 'external_id' (str)."""
```

`Stats` is a small dataclass the source can increment: `stats.blocked += 1` when it sees a 403, a 429 or a challenge page. The runner counts rows itself.

Demo sources:

- **books**: books.toscrape.com (a site built for scraping practice). Catalogue pages 1..`FEEDKIT_BOOKS_PAGES` (default 5). Fields: `external_id` (product slug), `title`, `price` (Decimal as string), `availability`, `url`. Content never changes, so it's good for tests and the idempotency demo.
- **hackernews**: `https://hacker-news.firebaseio.com/v0/topstories.json`, first 100 IDs, then `/item/<id>.json`. Fields: `external_id`, `title`, `url`, `score`, `comments` (from `descendants`). No usernames stored. Scores change hourly, so history fills up with real changes.

Both send an identifying User-Agent and wait between requests (0.5 s default).

## Database

```sql
create table if not exists runs (
    run_id      bigserial primary key,
    source      text not null,
    run_date    date not null,
    started_at  timestamptz not null default now(),
    finished_at timestamptz,
    status      text not null default 'running',   -- running | ok | failed
    rows_seen   int not null default 0,
    rows_new    int not null default 0,
    rows_changed int not null default 0,
    blocked     int not null default 0,
    error       text
);

create table if not exists records (
    source       text not null,
    external_id  text not null,
    data         jsonb not null,
    content_hash text not null,
    first_seen   timestamptz not null,
    last_seen    timestamptz not null,
    last_run_id  bigint not null references runs,
    primary key (source, external_id)
);

create table if not exists record_versions (
    source       text not null,
    external_id  text not null,
    data         jsonb not null,
    content_hash text not null,
    valid_from   timestamptz not null,
    valid_to     timestamptz,                         -- null = current version
    run_id       bigint not null references runs,
    primary key (source, external_id, valid_from)
);

create unique index if not exists record_versions_current
    on record_versions (source, external_id) where valid_to is null;
```

**Why SCD Type 2.** It's the standard warehouse pattern for history (Kimball). Analysts and dbt users already know how to query it ("as of date X": `valid_from <= X and (valid_to > X or valid_to is null)`). It loads into Snowflake or BigQuery unchanged. The partial unique index guarantees exactly one current version per record at the database level, not just in app code.

**Why `jsonb` for `data`.** The same three tables work for every client and source. Typed views per source can be added on top when a client wants columns.

**Content hash.** `sha256` of the record's JSON with sorted keys, excluding `external_id`. A new version is written only when the hash changes. That makes reruns idempotent.

## Write path (per record, one transaction per run)

1. Compute `content_hash`.
2. `select content_hash from records where source = %s and external_id = %s for update`.
3. Not found (new): insert into `records` and the first `record_versions` row (`valid_to` null), `rows_new += 1`.
4. Hash differs (changed): update `records` (`data`, `content_hash`, `last_seen`, `last_run_id`), close the current version (`valid_to = now()`), insert the new one, `rows_changed += 1`.
5. Hash equal (unchanged): update only `last_seen` and `last_run_id`.

`ponytail:` one select per record. That's fine to ~100K records per run; switch to a staging table + set-based merge if a source gets bigger. It assumes one run per source at a time, which supercronic and Airflow enforce.

The whole run commits once at the end. A crash mid-run rolls back the records and versions. The `runs` row is written in its own transaction so a failure is still recorded, with `status='failed'` and `error`.

`valid_from` is the observation time (`now()` at write). Out-of-order backfills of old dates don't rewrite history. `ponytail:` observation-time history; switch to run_date-based validity if a client needs true backfilled history.

## CLI

```
feedkit init-db                                   # applies schema.sql (idempotent)
feedkit run --source books [--date 2026-09-30]    # default date: today (UTC)
```

Exit code 0 on success, 1 on failure, so cron, supercronic and Airflow all see failures.

## Docker

- `docker-compose.yml`: `db` (postgres:16, named volume, healthcheck), `app` (built from `Dockerfile`, `DATABASE_URL` env, depends on healthy `db`), `scheduler` (same image, runs `supercronic /app/crontab`).
- `crontab`: `books` daily, `hackernews` hourly.
- **Why supercronic, not cron.** Container cron loses environment variables and logs to files. Supercronic is the common container-native choice: it logs to stdout, reads the env, and by default skips a job if the previous run is still going (the same protection `flock` gives on a server).

## Testing

`tests/test_store.py` runs against the compose Postgres (`DATABASE_URL`). It uses an in-memory fake source, no network.

1. First run of 3 records → 3 rows in `records`, 3 versions, `rows_new = 3`.
2. Same run again → no new rows or versions, `rows_new = rows_changed = 0`.
3. One record's field changes → 1 new version, the previous version gets `valid_to`, `rows_changed = 1`.

## Out of scope (later specs)

Checks and alerts, the Airflow DAG, TypeScript/Crawlee sources, AWS deploy, LLM extraction, detecting deleted records, typed per-source views, migrations tooling (Alembic).

## Compliance

The README states that the demo sources are a scraping sandbox and an official public API. It also notes that client sources must respect site terms and rate limits and avoid personal data they don't need.
