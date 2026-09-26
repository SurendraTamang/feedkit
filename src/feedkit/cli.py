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
