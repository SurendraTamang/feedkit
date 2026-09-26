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
