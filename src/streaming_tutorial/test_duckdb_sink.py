from __future__ import annotations

from pathlib import Path

from streaming_tutorial.duckdb_sink import ensure_database, insert_pageview


PAGEVIEW = {
    "event_type": "pageview_by_url",
    "page_url": "https://example.com/contact",
    "source_timestamp": "2026-06-16T00:04:02.404Z",
    "session_id": "81daebdf-fd8e-4fb7-a49c-6d0fe2a2d654",
    "user_id": "2eb2a94f-44ac-4997-ac74-135f949ac3b7",
    "referrer": "https://google.com",
    "pageview_count": 1,
    "processed_at": "2026-06-16T00:04:07.197196Z",
}


def test_insert_pageview_is_idempotent_by_kafka_coordinates(tmp_path: Path) -> None:
    db_path = tmp_path / "analytics.duckdb"
    connection = ensure_database(db_path)

    insert_pageview(
        connection,
        topic="mockingbird.pageviews.by_url",
        partition=1,
        offset=42,
        payload=PAGEVIEW,
    )
    insert_pageview(
        connection,
        topic="mockingbird.pageviews.by_url",
        partition=1,
        offset=42,
        payload=PAGEVIEW,
    )

    rows = connection.execute(
        "SELECT page_url, session_id, user_id, pageview_count, count(*) OVER () AS total_rows "
        "FROM pageviews_by_url"
    ).fetchall()
    connection.close()

    assert rows == [
        (
            "https://example.com/contact",
            "81daebdf-fd8e-4fb7-a49c-6d0fe2a2d654",
            "2eb2a94f-44ac-4997-ac74-135f949ac3b7",
            1,
            1,
        )
    ]
