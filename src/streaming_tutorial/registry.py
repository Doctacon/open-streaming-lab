"""Small Apicurio Registry helper CLI for Open Streaming Lab."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from streaming_tutorial.config import defaults_from_env
from streaming_tutorial.validation import DEFAULT_SCHEMA_ID, build_event_schema, project_root

JSON_SCHEMA_ARTIFACT_TYPE = "JSON"
JSON_CONTENT_TYPE = "application/json"


def build_parser() -> argparse.ArgumentParser:
    defaults = defaults_from_env()
    parser = argparse.ArgumentParser(
        prog="schema-registry",
        description="Register and fetch Open Streaming Lab schemas from local Apicurio Registry.",
    )
    parser.add_argument(
        "--registry-url",
        default=defaults.registry_url,
        help=f"Apicurio Registry base URL. Default: {defaults.registry_url}",
    )
    parser.add_argument(
        "--group-id",
        default=defaults.registry_group,
        help=f"Apicurio group id. Default: {defaults.registry_group}",
    )
    parser.add_argument(
        "--artifact-id",
        default=DEFAULT_SCHEMA_ID,
        help=f"Schema artifact id. Default: {DEFAULT_SCHEMA_ID}",
    )
    parser.add_argument(
        "--version",
        default=defaults.registry_version,
        help=f"Artifact version to fetch/register. Default: {defaults.registry_version}",
    )
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds. Default: 10")

    subparsers = parser.add_subparsers(dest="command", required=True)

    register = subparsers.add_parser("register", help="Register a local JSON Schema artifact.")
    register.add_argument(
        "--schema-file",
        default=str(project_root() / "schemas" / f"{DEFAULT_SCHEMA_ID}.json"),
        help="Path to the local JSON Schema file to register.",
    )

    fetch = subparsers.add_parser("fetch", help="Fetch a schema artifact version from Apicurio.")
    fetch.add_argument(
        "--output",
        default="-",
        help="Output path for fetched schema JSON, or '-' for stdout. Default: '-'",
    )
    fetch.add_argument(
        "--validate-schema",
        action="store_true",
        help="Validate the fetched JSON as a JSON Schema before writing it.",
    )

    return parser


def artifact_url(*, registry_url: str, group_id: str, artifact_id: str, version: str | None = None) -> str:
    base = registry_url.rstrip("/")
    group = quote(group_id, safe="")
    artifact = quote(artifact_id, safe="")
    if version is None:
        return f"{base}/apis/registry/v3/groups/{group}/artifacts"
    return f"{base}/apis/registry/v3/groups/{group}/artifacts/{artifact}/versions/{quote(version, safe='')}/content"


def _request_json(request: Request, *, timeout: float) -> Any:
    with urlopen(request, timeout=timeout) as response:  # noqa: S310 - local tutorial URL, no secrets
        body = response.read().decode("utf-8")
        if not body:
            return None
        return json.loads(body)


def fetch_schema(
    *,
    registry_url: str,
    group_id: str,
    artifact_id: str,
    version: str,
    timeout: float,
) -> dict[str, Any]:
    """Fetch a JSON Schema artifact version from Apicurio Registry."""

    url = artifact_url(registry_url=registry_url, group_id=group_id, artifact_id=artifact_id, version=version)
    request = Request(url, method="GET", headers={"Accept": JSON_CONTENT_TYPE})
    try:
        payload = _request_json(request, timeout=timeout)
    except HTTPError as exc:
        raise RuntimeError(f"Could not fetch schema {artifact_id!r} version {version!r}: HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"Could not connect to Apicurio Registry at {registry_url}: {exc.reason}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"Registry returned {type(payload).__name__}, expected JSON object schema")
    return payload


def register_schema(
    *,
    registry_url: str,
    group_id: str,
    artifact_id: str,
    version: str,
    schema_file: Path,
    timeout: float,
) -> dict[str, Any] | None:
    """Register a local JSON Schema file in Apicurio Registry.

    Returns the Apicurio response body, or None if the artifact already exists.
    """

    raw_schema = json.loads(schema_file.read_text())
    build_event_schema(schema_id=artifact_id, raw_schema=raw_schema)
    body = {
        "artifactId": artifact_id,
        "artifactType": JSON_SCHEMA_ARTIFACT_TYPE,
        "firstVersion": {
            "version": version,
            "content": {
                "content": json.dumps(raw_schema, separators=(",", ":")),
                "contentType": JSON_CONTENT_TYPE,
            },
        },
    }
    encoded = json.dumps(body).encode("utf-8")
    request = Request(
        artifact_url(registry_url=registry_url, group_id=group_id, artifact_id=artifact_id),
        data=encoded,
        method="POST",
        headers={"Content-Type": JSON_CONTENT_TYPE, "Accept": JSON_CONTENT_TYPE},
    )
    try:
        response = _request_json(request, timeout=timeout)
    except HTTPError as exc:
        if exc.code == 409:
            return None
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Could not register schema {artifact_id!r}: HTTP {exc.code} {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"Could not connect to Apicurio Registry at {registry_url}: {exc.reason}") from exc

    if isinstance(response, dict):
        return response
    return {"response": response}


def _write_schema(raw_schema: dict[str, Any], *, output: str) -> None:
    rendered = json.dumps(raw_schema, indent=2, sort_keys=True) + "\n"
    if output == "-":
        print(rendered, end="")
        return
    Path(output).write_text(rendered)
    print(f"Wrote schema to {output}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "register":
            result = register_schema(
                registry_url=args.registry_url,
                group_id=args.group_id,
                artifact_id=args.artifact_id,
                version=args.version,
                schema_file=Path(args.schema_file),
                timeout=args.timeout,
            )
            if result is None:
                print(
                    f"Schema artifact already exists: group={args.group_id} "
                    f"artifact={args.artifact_id} version={args.version}"
                )
            else:
                print(json.dumps(result, indent=2, sort_keys=True, default=str))
            return 0

        if args.command == "fetch":
            raw_schema = fetch_schema(
                registry_url=args.registry_url,
                group_id=args.group_id,
                artifact_id=args.artifact_id,
                version=args.version,
                timeout=args.timeout,
            )
            if args.validate_schema:
                build_event_schema(schema_id=args.artifact_id, raw_schema=raw_schema)
            _write_schema(raw_schema, output=args.output)
            return 0

        parser.error(f"Unknown command: {args.command}")
        return 2
    except (OSError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
