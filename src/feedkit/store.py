import hashlib
import json
import os
from datetime import date
from importlib.resources import files

import psycopg
from psycopg.types.json import Jsonb


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
