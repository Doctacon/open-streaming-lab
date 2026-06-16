from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from streaming_tutorial.registry import artifact_url, register_schema


def test_artifact_url_builds_v3_collection_and_content_urls() -> None:
    assert (
        artifact_url(
            registry_url="http://127.0.0.1:8081/",
            group_id="open-streaming-lab",
            artifact_id="web_analytics_v1",
        )
        == "http://127.0.0.1:8081/apis/registry/v3/groups/open-streaming-lab/artifacts"
    )
    assert (
        artifact_url(
            registry_url="http://127.0.0.1:8081",
            group_id="open streaming lab",
            artifact_id="web/analytics",
            version="1",
        )
        == "http://127.0.0.1:8081/apis/registry/v3/groups/open%20streaming%20lab/"
        "artifacts/web%2Fanalytics/versions/1/content"
    )


def test_register_schema_posts_apicurio_json_artifact(monkeypatch: Any, tmp_path: Path) -> None:
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "x-schema-name": "test_schema",
        "x-schema-version": "1",
        "properties": {"id": {"type": "string"}},
    }
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps(schema))
    captured: dict[str, Any] = {}

    def fake_request_json(request: Any, *, timeout: float) -> dict[str, Any]:
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        captured["content_type"] = request.headers["Content-type"]
        captured["body"] = json.loads(request.data.decode("utf-8"))
        captured["timeout"] = timeout
        return {"artifact": {"artifactId": "test_schema"}}

    monkeypatch.setattr("streaming_tutorial.registry._request_json", fake_request_json)

    result = register_schema(
        registry_url="http://registry:8080",
        group_id="open-streaming-lab",
        artifact_id="test_schema",
        version="1",
        schema_file=schema_file,
        timeout=3,
    )

    assert result == {"artifact": {"artifactId": "test_schema"}}
    assert captured["url"] == "http://registry:8080/apis/registry/v3/groups/open-streaming-lab/artifacts"
    assert captured["method"] == "POST"
    assert captured["content_type"] == "application/json"
    assert captured["timeout"] == 3
    assert captured["body"]["artifactId"] == "test_schema"
    assert captured["body"]["artifactType"] == "JSON"
    assert captured["body"]["firstVersion"]["version"] == "1"
    assert json.loads(captured["body"]["firstVersion"]["content"]["content"]) == schema
    assert captured["body"]["firstVersion"]["content"]["contentType"] == "application/json"
