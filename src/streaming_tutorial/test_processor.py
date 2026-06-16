from __future__ import annotations

from datetime import UTC, datetime

from streaming_tutorial.processor import build_pageview_by_url_event, pageview_key


WEB_ANALYTICS_EVENT = {
    "timestamp": "2026-06-15T23:32:14.243Z",
    "session_id": "fc7fb8e4-58f2-46d9-a74d-605729432c7d",
    "user_id": "ea876f36-e768-4897-80bd-9e1bdb641a46",
    "page_url": "https://example.com/contact",
    "referrer": "https://facebook.com",
}


def test_build_pageview_by_url_event_preserves_traceable_fields() -> None:
    derived = build_pageview_by_url_event(
        WEB_ANALYTICS_EVENT,
        processed_at=datetime(2026, 6, 16, 0, 5, tzinfo=UTC),
    )

    assert derived == {
        "event_type": "pageview_by_url",
        "page_url": "https://example.com/contact",
        "source_timestamp": "2026-06-15T23:32:14.243Z",
        "session_id": "fc7fb8e4-58f2-46d9-a74d-605729432c7d",
        "user_id": "ea876f36-e768-4897-80bd-9e1bdb641a46",
        "referrer": "https://facebook.com",
        "pageview_count": 1,
        "processed_at": "2026-06-16T00:05:00Z",
    }


def test_pageview_key_uses_page_url_for_partition_affinity() -> None:
    assert pageview_key(WEB_ANALYTICS_EVENT) == "https://example.com/contact"
