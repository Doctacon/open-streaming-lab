"""Run example DuckDB analytics queries for Open Streaming Lab."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

import duckdb

from streaming_tutorial.duckdb_sink import DEFAULT_DUCKDB_PATH

SUMMARY_SQL = """
SELECT
    page_url,
    sum(pageview_count) AS pageviews,
    count(DISTINCT session_id) AS sessions,
    count(DISTINCT user_id) AS users,
    min(source_timestamp) AS first_seen,
    max(source_timestamp) AS last_seen
FROM pageviews_by_url
GROUP BY page_url
ORDER BY pageviews DESC, page_url
"""

REFERRER_SQL = """
SELECT
    coalesce(referrer, '<null>') AS referrer,
    sum(pageview_count) AS pageviews
FROM pageviews_by_url
GROUP BY referrer
ORDER BY pageviews DESC, referrer
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="duckdb-analytics",
        description="Run example SQL over the local Open Streaming Lab DuckDB database.",
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DUCKDB_PATH,
        help=f"DuckDB database path. Default: {DEFAULT_DUCKDB_PATH}",
    )
    parser.add_argument(
        "--query",
        choices=("summary", "referrers"),
        default="summary",
        help="Example query to run. Default: summary",
    )
    return parser


def run_query(*, db_path: Path, query: str) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB database does not exist yet: {db_path}. Run kafka-sink-duckdb first.")
    sql = SUMMARY_SQL if query == "summary" else REFERRER_SQL
    with duckdb.connect(str(db_path), read_only=True) as connection:
        return connection.sql(sql).show(max_rows=50)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        run_query(db_path=Path(args.db_path), query=args.query)
        return 0
    except (FileNotFoundError, duckdb.Error) as exc:
        print(exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
