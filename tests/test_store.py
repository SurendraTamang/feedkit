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
