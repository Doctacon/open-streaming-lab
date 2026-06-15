# Python Kafka Tutorial Scaffold Validation

ID: evidence:20260613-python-kafka-tutorial-validation
Type: Evidence Dossier
Status: recorded
Created: 2026-06-13
Updated: 2026-06-13
Observed: 2026-06-13

## Summary

Validation observations for `ticket:20260613-python-kafka-mockingbird-tutorial`. Static checks, dependency installation, entrypoint checks, unit tests, Docker Compose configuration validation, Mockingbird JSONL generation, Colima/Docker connectivity repair, and a live Kafka produce/consume smoke test succeeded. Two environment/scaffold issues were found and addressed: Docker config referenced Docker Desktop's missing credential helper, and the Kafka data volume caused a container write-permission failure.

## Observations

- Observation: Python source compiled successfully.
  - Procedure/source: Ran `python3 -m compileall src` from `/Users/crlough/Code/personal/streaming-tutorial` after scaffold creation.
  - Actual result: Python compiled `__init__.py`, `admin.py`, `config.py`, `consumer.py`, `producer.py`, and `test_producer.py` without syntax errors.

- Observation: Node Mockingbird helper syntax check succeeded.
  - Procedure/source: Ran `node --check tools/mockingbird-jsonl.mjs`.
  - Actual result: Command exited successfully with no syntax error output.

- Observation: Docker Compose configuration is syntactically valid.
  - Procedure/source: Ran `docker-compose config >/tmp/streaming-tutorial-compose-config.txt`.
  - Actual result: Command exited successfully (`docker-compose config OK`).

- Observation: Node dependencies installed and Mockingbird helper generated JSONL.
  - Procedure/source: Ran `npm install`, `npm run mockingbird:templates | head -20`, and `npm run mockingbird:sample`.
  - Actual result: `npm install` added 242 packages and produced `package-lock.json`; template listing included `Content Tracking`, `Flight Bookings`, `Log Analytics Starter Kit`, `Simple Example`, `Sportsbetting`, `Stock Prices`, `Web Analytics Starter Kit`, and `eCommerce Transactions`; sample command printed 5 JSON Lines with web analytics fields including `timestamp`, `session_id`, `user_id`, `page_url`, and `referrer`.

- Observation: npm audit reported moderate dependency vulnerabilities in Mockingbird's transitive dependency tree.
  - Procedure/source: Ran `npm audit --omit=dev --audit-level=high` after `npm install`.
  - Actual result: Audit reported 8 moderate vulnerabilities involving `uuid`, `gaxios`, `google-gax`, `@google-cloud/spanner`, and related transitive packages under `@tinybirdco/mockingbird`; suggested `npm audit fix --force` would install `@tinybirdco/mockingbird@1.3.0` and is a breaking downgrade, so it was not applied.

- Observation: Python dependencies installed with `uv`, and unit tests passed.
  - Procedure/source: Ran `uv sync --extra dev`, then `uv run pytest`, then `make test`.
  - Actual result: `uv sync --extra dev` created `.venv`, resolved dependencies, installed `confluent-kafka==2.14.2`, `pytest==9.0.3`, and the local package; `uv run pytest` collected 4 tests and all passed; `make test` reran tests, Node syntax check, and `docker-compose config` successfully.

- Observation: Python console entrypoints are present and parse help successfully.
  - Procedure/source: Ran `uv run kafka-admin --help`, `uv run kafka-produce --help`, and `uv run kafka-consume --help`, redirecting output to temp files.
  - Actual result: All three commands exited successfully (`entrypoint help OK`).

- Observation: Initial Docker connectivity failed because the active Docker context pointed at a stopped Colima default profile.
  - Procedure/source: Ran `colima ls`, `colima status`, `docker version`, `docker context ls`, and socket checks.
  - Actual result: `colima ls` reported `default` as `Stopped`; Docker context `colima` pointed to `unix:///Users/crlough/.config/colima/default/docker.sock`; the socket did not exist, so `docker info` could not connect.

- Observation: Starting Colima restored Docker Engine connectivity.
  - Procedure/source: Ran `colima start`, then `colima ls` and `docker info`.
  - Actual result: Colima default profile became `Running`; Docker server reported `Server Version: 29.2.1` on the `colima` context.

- Observation: Initial image pull failed because Docker config still referenced Docker Desktop's credential helper.
  - Procedure/source: Ran `docker-compose up -d`; inspected `~/.docker/config.json`.
  - Actual result: Compose failed with `error getting credentials - err: exec: "docker-credential-desktop": executable file not found in $PATH`; `~/.docker/config.json` contained `"credsStore": "desktop"`. The config was backed up to `~/.docker/config.json.bak-streaming-tutorial-20260613`, `credsStore` was removed, and `currentContext` remained `colima`.

- Observation: Initial Kafka container start failed because the named volume mounted at `/tmp/kraft-combined-logs` was not writable by the image user.
  - Procedure/source: Ran `docker-compose up -d` and `docker-compose logs kafka --tail=200`.
  - Actual result: Kafka exited with `java.nio.file.AccessDeniedException: /tmp/kraft-combined-logs/bootstrap.checkpoint.tmp`. `compose.yaml` was changed to avoid mounting a named volume at the log directory for this beginner tutorial.

- Observation: Full Kafka runtime smoke test succeeded after the Docker config and Compose fixes.
  - Procedure/source: Ran `docker-compose down -v`, `docker-compose up -d`, `uv run kafka-admin create-topic --topic mockingbird.events --partitions 3`, `uv run kafka-produce --topic mockingbird.events --template "Web Analytics Starter Kit" --eps 5 --limit 5`, and `uv run kafka-consume --topic mockingbird.events --group-id smoke-test-$(date +%s) --from-beginning --max-messages 5`.
  - Actual result: Kafka became healthy, the topic was created with 3 partitions, the producer queued and delivered 5 Mockingbird events, and the consumer read 5 JSON messages from Kafka. `compose.yaml` and Python defaults were changed from `localhost:9092` to `127.0.0.1:9092` to avoid noisy IPv6 `::1` connection attempts observed during the first successful smoke run.

## Artifacts

- `/tmp/streaming-tutorial-compose-config.txt` - generated normalized Compose configuration from the successful `docker-compose config` run; temp artifact may not persist across machines.
- `/tmp/kafka-admin-help.txt`, `/tmp/kafka-produce-help.txt`, `/tmp/kafka-consume-help.txt` - CLI help output from successful entrypoint checks; temp artifacts may not persist across machines.
- `~/.docker/config.json.bak-streaming-tutorial-20260613` - local backup of Docker config before removing the stale Docker Desktop credential helper reference.
- Excerpt from `uv run pytest`:

```text
collected 4 items
src/streaming_tutorial/test_producer.py ....                             [100%]
4 passed in 0.36s
```

- Excerpt from `npm run mockingbird:sample`:

```text
{"timestamp":"2026-06-13T13:28:00.697Z","session_id":"2be11fdc-34a4-488e-a13f-9614391622f1","user_id":"1e66a22f-0ddf-4d26-bef7-84f4e46a8b0d","page_url":"https://example.com/contact","referrer":"https://google.com"}
```

## What This Shows

- `ticket:20260613-python-kafka-mockingbird-tutorial#ACC-001` - supports - `compose.yaml` is accepted by `docker-compose config`; the local Apache Kafka broker became healthy under Colima; Kafbat UI started; no managed/cloud Kafka dependency is present.
- `ticket:20260613-python-kafka-mockingbird-tutorial#ACC-002` - supports - `uv sync --extra dev`, entrypoint help checks, compile checks, tests, topic creation, and live admin/producer/consumer commands succeeded.
- `ticket:20260613-python-kafka-mockingbird-tutorial#ACC-003` - supports - the live smoke test produced 5 Kafka messages from Mockingbird-generated `Web Analytics Starter Kit` JSONL events.
- `ticket:20260613-python-kafka-mockingbird-tutorial#ACC-004` - supports - README now includes the Colima/Docker Desktop credential-helper troubleshooting found during validation, the 127.0.0.1 listener note, and the hands-on produce/consume flow.
- `ticket:20260613-python-kafka-mockingbird-tutorial#ACC-005` - supports - validation commands, failures, fixes, and limitations are recorded here.

## What This Does Not Show

- Does not resolve the 8 moderate npm audit findings in Mockingbird's transitive dependencies; the scaffold keeps Mockingbird because it is the operator-selected data source and avoids using the affected Google Spanner destination path.
- Does not constitute an independent Ralph audit; previous subagent audit attempts timed out.
- Does not prove behavior on Docker Desktop, another Colima profile, or Linux hosts.
- Recheck if `compose.yaml`, Python source, Node helper, dependency lockfiles, Docker config/context, or Docker/Node/Python versions materially change.

## Related Records

- `ticket:20260613-python-kafka-mockingbird-tutorial` - consuming ticket whose acceptance criteria this evidence supports.
- `research:20260613-local-kafka-mockingbird-tutorial` - source-backed rationale for the local/open-source architecture.
