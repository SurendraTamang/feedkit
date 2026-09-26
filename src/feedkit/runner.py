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
