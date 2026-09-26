# feedkit core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A reusable kit that runs any scraper source for a date, stores each record once in Postgres, keeps SCD Type 2 history, and records every run, runnable via CLI and on a schedule in Docker.

**Architecture:** A source is a `fetch(run_date, stats)` generator of dicts. `runner.run_source` feeds records to `store`, which dedups by `(source, external_id)`, detects change by content hash, and writes `records` + `record_versions` in one transaction per run. The `runs` row is committed separately so failures are always recorded. `cli.py` wraps it; supercronic schedules it in Docker.

**Tech Stack:** Python 3.12, uv, psycopg 3, httpx, parsel, pytest, Postgres 16, Docker Compose, supercronic.

Spec: `docs/superpowers/specs/2026-09-26-feedkit-core-design.md`. One deviation: `schema.sql` lives in `src/feedkit/` (not repo root) so it ships inside the package and `init-db` can load it with `importlib.resources`.

Commit rule for this repo: short messages, **no Claude signature / Co-Authored-By lines**.

---

## File map

| File | Responsibility |
|---|---|
| `pyproject.toml` | Package metadata, deps, `feedkit` console script |
| `.python-version` | Pins Python 3.12 for uv (matches Docker) |
| `.gitignore` | venv, caches |
| `docker-compose.yml` | `db`, `app` (init-db), `scheduler` (supercronic) |
| `Dockerfile` | App image with uv + supercronic |
| `crontab` | Schedules for the demo sources |
| `src/feedkit/__init__.py` | `USER_AGENT`, `DELAY` shared by sources |
| `src/feedkit/schema.sql` | Tables and the partial unique index |
| `src/feedkit/store.py` | `connect`, `init_db`, `content_hash`, `start_run`, `save_record`, `finish_run` |
| `src/feedkit/runner.py` | `Stats`, `run_source` |
| `src/feedkit/sources/__init__.py` | `SOURCES` registry |
| `src/feedkit/sources/books.py` | books.toscrape.com source |
| `src/feedkit/sources/hackernews.py` | HN API source |
| `src/feedkit/cli.py` | `init-db`, `run` |
| `tests/test_store.py` | Hash, idempotency, versions, failure tests |
| `tests/test_cli.py` | CLI init-db test |
| `README.md` | What it is, how to run, compliance note |

---

### Task 1: Project scaffold and local Postgres

**Files:**
- Create: `pyproject.toml`, `.python-version`, `.gitignore`, `src/feedkit/__init__.py`, `docker-compose.yml`

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[project]
name = "feedkit"
version = "0.1.0"
description = "Turn any scraper into a monitored data feed: dedup, SCD2 history, run tracking."
requires-python = ">=3.12"
dependencies = [
    "psycopg[binary]>=3.2",
    "httpx>=0.27",
    "parsel>=1.9",
]

[project.scripts]
feedkit = "feedkit.cli:main"

[dependency-groups]
dev = ["pytest>=8"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

- [ ] **Step 2: Write `.python-version`, `.gitignore`, package init**

`.python-version`:
```
3.12
```

`.gitignore`:
```
.venv/
__pycache__/
.pytest_cache/
*.egg-info/
.env
```

`src/feedkit/__init__.py`:
```python
import os

USER_AGENT = "feedkit/0.1 (+https://github.com/SurendraTamang/feedkit)"
DELAY = float(os.environ.get("FEEDKIT_DELAY", "0.5"))
```

- [ ] **Step 3: Write `docker-compose.yml` (db only for now)**

```yaml
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_USER: feedkit
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-feedkit}
      POSTGRES_DB: feedkit
    ports:
      - "5433:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U feedkit"]
      interval: 5s
      retries: 10

volumes:
  pgdata: {}
```

Port 5433 on the host avoids clashing with a local Postgres on 5432.

- [ ] **Step 4: Install and start the database**

Run:
```bash
cd ~/Desktop/projects/feedkit
uv sync
docker compose up -d db
docker compose ps
uv run python -c "import feedkit, psycopg, httpx, parsel; print('ok')"
```
Expected: `db` shows `healthy` (may take ~10 s), and the last command prints `ok`.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock .python-version .gitignore src/feedkit/__init__.py docker-compose.yml
git commit -m "Scaffold feedkit package and local Postgres"
```

---

### Task 2: Schema, connection, content hash

**Files:**
- Create: `src/feedkit/schema.sql`, `src/feedkit/store.py`
- Test: `tests/test_store.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_store.py`:
```python
import os

import pytest

from feedkit import store

needs_db = pytest.mark.skipif("DATABASE_URL" not in os.environ, reason="needs Postgres")


def test_content_hash_ignores_key_order_and_external_id():
    a = {"external_id": "1", "title": "A", "price": "1.00"}
    b = {"price": "1.00", "title": "A", "external_id": "999"}
    assert store.content_hash(a) == store.content_hash(b)
    assert store.content_hash(a) != store.content_hash({**a, "price": "2.00"})


@pytest.fixture
def conn():
    with store.connect() as c:
        store.init_db(c)
        c.execute("truncate record_versions, records, runs restart identity")
        c.commit()
        yield c


@needs_db
def test_init_db_is_idempotent(conn):
    store.init_db(conn)
    tables = {r[0] for r in conn.execute(
        "select tablename from pg_tables where schemaname = 'public'"
    )}
    assert {"runs", "records", "record_versions"} <= tables
```

- [ ] **Step 2: Run to verify failure**

Run:
```bash
export DATABASE_URL=postgresql://feedkit:feedkit@localhost:5433/feedkit
uv run pytest tests/test_store.py -v
```
Expected: FAIL/ERROR with `AttributeError: module 'feedkit.store' has no attribute ...` or `ModuleNotFoundError: No module named 'feedkit.store'`.

- [ ] **Step 3: Write `src/feedkit/schema.sql`**

```sql
create table if not exists runs (
    run_id       bigserial primary key,
    source       text not null,
    run_date     date not null,
    started_at   timestamptz not null default now(),
    finished_at  timestamptz,
    status       text not null default 'running',
    rows_seen    int not null default 0,
    rows_new     int not null default 0,
    rows_changed int not null default 0,
    blocked      int not null default 0,
    error        text
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
    valid_to     timestamptz,
    run_id       bigint not null references runs,
    primary key (source, external_id, valid_from)
);

create unique index if not exists record_versions_current
    on record_versions (source, external_id) where valid_to is null;
```

- [ ] **Step 4: Write `src/feedkit/store.py` (first part)**

```python
import hashlib
import json
import os
from importlib.resources import files

import psycopg


def connect(url: str | None = None) -> psycopg.Connection:
    return psycopg.connect(url or os.environ["DATABASE_URL"])


def init_db(conn: psycopg.Connection) -> None:
    conn.execute(files("feedkit").joinpath("schema.sql").read_text())
    conn.commit()


def _data(record: dict) -> dict:
    return {k: v for k, v in record.items() if k != "external_id"}


def content_hash(record: dict) -> str:
    payload = json.dumps(_data(record), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_store.py -v`
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add src/feedkit/schema.sql src/feedkit/store.py tests/test_store.py
git commit -m "Add schema, connection and content hash"
```

---

### Task 3: Runs, record writes and the runner

**Files:**
- Modify: `src/feedkit/store.py` (append functions)
- Create: `src/feedkit/runner.py`
- Test: `tests/test_store.py` (append tests)

- [ ] **Step 1: Append the failing tests to `tests/test_store.py`**

```python
from datetime import date

from feedkit.runner import run_source

D = date(2026, 9, 30)
BOOKS = [
    {"external_id": "a", "title": "A", "price": "1.00"},
    {"external_id": "b", "title": "B", "price": "2.00"},
    {"external_id": "c", "title": "C", "price": "3.00"},
]


def fake(records):
    def fetch(run_date, stats):
        yield from records
    return fetch


def counts(conn):
    return [conn.execute(f"select count(*) from {t}").fetchone()[0] for t in ("records", "record_versions")]


@needs_db
def test_first_run_inserts_everything_and_skips_duplicates(conn):
    s = run_source(conn, "fake", fake(BOOKS + [BOOKS[0]]), D)
    assert (s.rows_seen, s.rows_new, s.rows_changed) == (3, 3, 0)
    assert counts(conn) == [3, 3]
    assert conn.execute("select status, rows_new from runs").fetchone() == ("ok", 3)


@needs_db
def test_rerun_is_idempotent(conn):
    run_source(conn, "fake", fake(BOOKS), D)
    s = run_source(conn, "fake", fake(BOOKS), D)
    assert (s.rows_new, s.rows_changed) == (0, 0)
    assert counts(conn) == [3, 3]


@needs_db
def test_changed_record_gets_new_version(conn):
    run_source(conn, "fake", fake(BOOKS), D)
    changed = [{**BOOKS[0], "price": "9.00"}, *BOOKS[1:]]
    s = run_source(conn, "fake", fake(changed), D)
    assert (s.rows_new, s.rows_changed) == (0, 1)
    assert counts(conn) == [3, 4]
    rows = conn.execute(
        "select data->>'price', valid_to is null from record_versions "
        "where external_id = 'a' order by valid_from"
    ).fetchall()
    assert rows == [("1.00", False), ("9.00", True)]


@needs_db
def test_failed_run_is_recorded_and_rolled_back(conn):
    def boom(run_date, stats):
        yield BOOKS[0]
        raise RuntimeError("layout changed")

    with pytest.raises(RuntimeError):
        run_source(conn, "fake", boom, D)
    assert counts(conn) == [0, 0]
    status, error = conn.execute("select status, error from runs").fetchone()
    assert status == "failed" and "layout changed" in error
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_store.py -v`
Expected: collection ERROR `ModuleNotFoundError: No module named 'feedkit.runner'`.

- [ ] **Step 3: Append to `src/feedkit/store.py`**

```python
from datetime import date

from psycopg.types.json import Jsonb


def start_run(conn: psycopg.Connection, source: str, run_date: date) -> int:
    run_id = conn.execute(
        "insert into runs (source, run_date) values (%s, %s) returning run_id",
        (source, run_date),
    ).fetchone()[0]
    conn.commit()
    return run_id


def save_record(conn: psycopg.Connection, source: str, record: dict, run_id: int) -> str:
    """Write one record. Returns 'new', 'changed' or 'unchanged'. Caller commits."""
    # ponytail: one select per record, fine to ~100K rows/run; staging table + set-based merge if bigger
    ext = str(record["external_id"])
    data, h = Jsonb(_data(record)), content_hash(record)
    key = (source, ext)
    row = conn.execute(
        "select content_hash from records where source = %s and external_id = %s for update", key
    ).fetchone()
    if row is None:
        conn.execute(
            "insert into records (source, external_id, data, content_hash, first_seen, last_seen, last_run_id) "
            "values (%s, %s, %s, %s, now(), now(), %s)",
            (*key, data, h, run_id),
        )
        conn.execute(
            "insert into record_versions (source, external_id, data, content_hash, valid_from, run_id) "
            "values (%s, %s, %s, %s, now(), %s)",
            (*key, data, h, run_id),
        )
        return "new"
    if row[0] == h:
        conn.execute(
            "update records set last_seen = now(), last_run_id = %s where source = %s and external_id = %s",
            (run_id, *key),
        )
        return "unchanged"
    conn.execute(
        "update records set data = %s, content_hash = %s, last_seen = now(), last_run_id = %s "
        "where source = %s and external_id = %s",
        (data, h, run_id, *key),
    )
    conn.execute(
        "update record_versions set valid_to = now() "
        "where source = %s and external_id = %s and valid_to is null",
        key,
    )
    conn.execute(
        "insert into record_versions (source, external_id, data, content_hash, valid_from, run_id) "
        "values (%s, %s, %s, %s, now(), %s)",
        (*key, data, h, run_id),
    )
    return "changed"


def finish_run(conn: psycopg.Connection, run_id: int, status: str, stats, error: str | None = None) -> None:
    conn.execute(
        "update runs set finished_at = now(), status = %s, rows_seen = %s, rows_new = %s, "
        "rows_changed = %s, blocked = %s, error = %s where run_id = %s",
        (status, stats.rows_seen, stats.rows_new, stats.rows_changed, stats.blocked, error, run_id),
    )
    conn.commit()
```

Move the two new imports (`from datetime import date`, `from psycopg.types.json import Jsonb`) up to the import block at the top of the file.

Why `now()` works here: in Postgres, `now()` is the transaction start time. Closing the old version and opening the new one in the same transaction gives them the exact same timestamp, so history has no gaps. Duplicate IDs inside one run are skipped by the runner, so the `(source, external_id, valid_from)` primary key can't collide.

- [ ] **Step 4: Write `src/feedkit/runner.py`**

```python
from dataclasses import dataclass
from datetime import date
from typing import Callable, Iterator

import psycopg

from feedkit import store


@dataclass
class Stats:
    rows_seen: int = 0
    rows_new: int = 0
    rows_changed: int = 0
    blocked: int = 0


Fetch = Callable[[date, Stats], Iterator[dict]]


def run_source(conn: psycopg.Connection, source: str, fetch: Fetch, run_date: date) -> Stats:
    stats = Stats()
    run_id = store.start_run(conn, source, run_date)
    seen: set[str] = set()
    try:
        for record in fetch(run_date, stats):
            ext = str(record["external_id"])
            if ext in seen:
                continue
            seen.add(ext)
            stats.rows_seen += 1
            result = store.save_record(conn, source, record, run_id)
            if result == "new":
                stats.rows_new += 1
            elif result == "changed":
                stats.rows_changed += 1
        conn.commit()
    except Exception as e:
        conn.rollback()
        store.finish_run(conn, run_id, "failed", stats, repr(e))
        raise
    store.finish_run(conn, run_id, "ok", stats)
    return stats
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_store.py -v`
Expected: 6 passed.

- [ ] **Step 6: Commit**

```bash
git add src/feedkit/store.py src/feedkit/runner.py tests/test_store.py
git commit -m "Add run tracking, dedup and SCD2 versioning"
```

---

### Task 4: books.toscrape.com source

**Files:**
- Create: `src/feedkit/sources/books.py`

- [ ] **Step 1: Write `src/feedkit/sources/books.py`**

```python
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
```

- [ ] **Step 2: Smoke-test against the live sandbox**

Run:
```bash
FEEDKIT_BOOKS_PAGES=1 uv run python -c "
from datetime import date
from feedkit.runner import Stats
from feedkit.sources import books
rows = list(books.fetch(date.today(), Stats()))
print(len(rows)); print(rows[0])"
```
Expected: `20`, then a dict like `{'external_id': 'a-light-in-the-attic_1000', 'title': 'A Light in the Attic', 'price': '51.77', 'availability': 'In stock', 'url': 'https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html'}`.

Note: `src/feedkit/sources/__init__.py` doesn't exist yet. Python 3 treats `sources/` as a namespace package, so this import works. Task 6 adds the real `__init__.py`.

- [ ] **Step 3: Commit**

```bash
git add src/feedkit/sources/books.py
git commit -m "Add books.toscrape.com source"
```

---

### Task 5: Hacker News API source

**Files:**
- Create: `src/feedkit/sources/hackernews.py`

- [ ] **Step 1: Write `src/feedkit/sources/hackernews.py`**

```python
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
```

Usernames (`by`) are deliberately not stored: no personal data we don't need.

- [ ] **Step 2: Smoke-test against the live API**

Run:
```bash
FEEDKIT_HN_LIMIT=3 uv run python -c "
from datetime import date
from feedkit.runner import Stats
from feedkit.sources import hackernews
rows = list(hackernews.fetch(date.today(), Stats()))
print(len(rows)); print(rows[0])"
```
Expected: `3`, then a dict with `external_id`, `title`, `url`, `score`, `comments`.

- [ ] **Step 3: Commit**

```bash
git add src/feedkit/sources/hackernews.py
git commit -m "Add Hacker News API source"
```

---

### Task 6: Source registry and CLI

**Files:**
- Create: `src/feedkit/sources/__init__.py`, `src/feedkit/cli.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the failing test**

`tests/test_cli.py`:
```python
import os

import pytest

from feedkit.cli import main


@pytest.mark.skipif("DATABASE_URL" not in os.environ, reason="needs Postgres")
def test_init_db_command(capsys):
    assert main(["init-db"]) == 0
    assert "schema applied" in capsys.readouterr().out


def test_unknown_source_is_rejected():
    with pytest.raises(SystemExit):
        main(["run", "--source", "nope"])
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_cli.py -v`
Expected: collection ERROR `ModuleNotFoundError: No module named 'feedkit.cli'`.

- [ ] **Step 3: Write `src/feedkit/sources/__init__.py`**

```python
from feedkit.sources import books, hackernews

SOURCES = {m.NAME: m for m in (books, hackernews)}
```

- [ ] **Step 4: Write `src/feedkit/cli.py`**

```python
import argparse
import sys
from datetime import date, datetime, timezone

from feedkit import store
from feedkit.runner import run_source
from feedkit.sources import SOURCES


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="feedkit")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-db", help="create tables (safe to rerun)")
    run = sub.add_parser("run", help="run one source for one date")
    run.add_argument("--source", required=True, choices=sorted(SOURCES))
    run.add_argument("--date", type=date.fromisoformat, help="YYYY-MM-DD, default today (UTC)")
    args = parser.parse_args(argv)

    with store.connect() as conn:
        if args.cmd == "init-db":
            store.init_db(conn)
            print("schema applied")
            return 0
        run_date = args.date or datetime.now(timezone.utc).date()
        try:
            s = run_source(conn, args.source, SOURCES[args.source].fetch, run_date)
        except Exception as e:
            print(f"{args.source} {run_date} failed: {e!r}", file=sys.stderr)
            return 1
        print(f"{args.source} {run_date}: seen={s.rows_seen} new={s.rows_new} changed={s.rows_changed} blocked={s.blocked}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
```

`argparse` rejects an unknown `--source` before any DB connection, which is why the second test needs no database.

- [ ] **Step 5: Run all tests**

Run: `uv run pytest -v`
Expected: 8 passed.

- [ ] **Step 6: End-to-end run from the CLI**

Run:
```bash
FEEDKIT_BOOKS_PAGES=1 uv run feedkit run --source books
FEEDKIT_BOOKS_PAGES=1 uv run feedkit run --source books
psql "$DATABASE_URL" -c "select run_id, source, status, rows_seen, rows_new, rows_changed from runs order by run_id"
```
Expected: first run prints `seen=20 new=20 changed=0`; second prints `seen=20 new=0 changed=0`; `psql` shows both runs with `status = ok`.

- [ ] **Step 7: Commit**

```bash
git add src/feedkit/sources/__init__.py src/feedkit/cli.py tests/test_cli.py
git commit -m "Add source registry and CLI"
```

---

### Task 7: Docker image, scheduler and crontab

**Files:**
- Create: `Dockerfile`, `crontab`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Look up the latest supercronic release and its checksums**

Open https://github.com/aptible/supercronic/releases/latest. The release notes list a `SUPERCRONIC_SHA1SUM` for each binary. Note the version tag and the SHA1 for `supercronic-linux-amd64` and `supercronic-linux-arm64`. Use them in Step 2 as `SUPERCRONIC_VERSION`, `SHA1_AMD64` and `SHA1_ARM64`. Verifying the published checksum protects the image from a tampered download.

- [ ] **Step 2: Write `Dockerfile`**

```dockerfile
FROM python:3.12-slim

ARG TARGETARCH
ARG SUPERCRONIC_VERSION=<version tag from Step 1, e.g. v0.2.x>
ARG SHA1_AMD64=<sha1 from Step 1>
ARG SHA1_ARM64=<sha1 from Step 1>

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && SHA1=$([ "$TARGETARCH" = "arm64" ] && echo "$SHA1_ARM64" || echo "$SHA1_AMD64") \
    && curl -fsSLo /usr/local/bin/supercronic \
       "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-${TARGETARCH}" \
    && echo "${SHA1}  /usr/local/bin/supercronic" | sha1sum -c - \
    && chmod +x /usr/local/bin/supercronic \
    && apt-get purge -y curl && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY src ./src
RUN uv sync --frozen --no-dev
COPY crontab ./crontab

ENV PATH="/app/.venv/bin:$PATH"
CMD ["feedkit", "--help"]
```

The `<...>` values are filled from Step 1 while doing this task. They're the only values that can't be known when writing the plan.

- [ ] **Step 3: Write `crontab`**

```
# books: daily at 02:15 UTC. hackernews: hourly at :05.
15 2 * * * feedkit run --source books
5 * * * * feedkit run --source hackernews
```

- [ ] **Step 4: Replace `docker-compose.yml` with the full stack**

```yaml
x-app: &app
  build: .
  environment:
    DATABASE_URL: postgresql://feedkit:${POSTGRES_PASSWORD:-feedkit}@db:5432/feedkit
  depends_on:
    db:
      condition: service_healthy

services:
  db:
    image: postgres:16
    environment:
      POSTGRES_USER: feedkit
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-feedkit}
      POSTGRES_DB: feedkit
    ports:
      - "5433:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U feedkit"]
      interval: 5s
      retries: 10

  app:
    <<: *app
    command: ["feedkit", "init-db"]

  scheduler:
    <<: *app
    command: ["supercronic", "/app/crontab"]
    depends_on:
      db:
        condition: service_healthy
      app:
        condition: service_completed_successfully

volumes:
  pgdata: {}
```

`app` runs `init-db` once and exits. `scheduler` starts only after it succeeds.

- [ ] **Step 5: Build and verify**

Run:
```bash
docker compose up -d --build
docker compose logs app
docker compose run --rm -e FEEDKIT_HN_LIMIT=5 app feedkit run --source hackernews
docker compose logs scheduler | tail -5
```
Expected: `app` logs `schema applied`. The manual run prints `hackernews <today>: seen=5 new=5 ...` (or `new=0 changed=N` if already run). The scheduler logs show supercronic has read the crontab and is waiting for the next job.

- [ ] **Step 6: Commit**

```bash
git add Dockerfile crontab docker-compose.yml
git commit -m "Add Docker image and supercronic scheduler"
```

---

### Task 8: README

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write `README.md`**

````markdown
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
export DATABASE_URL=postgresql://feedkit:feedkit@localhost:5433/feedkit
uv run pytest -v
```

## Compliance

The demo sources are [books.toscrape.com](https://books.toscrape.com), a sandbox built for scraping practice, and the official [Hacker News API](https://github.com/HackerNews/API). No usernames are stored. Sources for real sites should respect the site's terms and rate limits and avoid collecting personal data that isn't needed.

## Roadmap

Checks and alerts (zero rows, drops, freshness, nulls, block rate) → Airflow DAG → TypeScript/Crawlee sources → AWS deploy → LLM extraction with accuracy checks.
````

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "Add README"
```
