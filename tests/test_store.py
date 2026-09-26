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
