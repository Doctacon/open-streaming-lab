COMPOSE ?= docker-compose
TOPIC ?= mockingbird.events
DLQ_TOPIC ?= mockingbird.events.dlq
PARTITIONS ?= 3
DLQ_PARTITIONS ?= 1
TEMPLATE ?= Web Analytics Starter Kit
EPS ?= 2
LIMIT ?= 20
INVALID_EVERY ?= 0
GROUP ?= mockingbird-python-learners
REGISTRY_URL ?= http://127.0.0.1:8081
REGISTRY_GROUP ?= open-streaming-lab
REGISTRY_ARTIFACT ?= web_analytics_v1
REGISTRY_VERSION ?= 1
PAGEVIEWS_TOPIC ?= mockingbird.pageviews.by_url
PROCESSOR_GROUP ?= open-streaming-lab-pageviews-processor
DUCKDB_PATH ?= data/open_streaming_lab.duckdb
SINK_GROUP ?= open-streaming-lab-duckdb-sink

.PHONY: install up down reset logs topic dlq-topic topics derived-topic describe describe-dlq describe-derived registry-register registry-fetch produce produce-invalid produce-registry process sink-duckdb analytics consume consume-dlq consume-derived test ui registry-ui

install:
	uv sync --extra dev
	npm install

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f kafka

topic:
	uv run kafka-admin create-topic --topic "$(TOPIC)" --partitions $(PARTITIONS)

dlq-topic:
	uv run kafka-admin create-topic --topic "$(DLQ_TOPIC)" --partitions $(DLQ_PARTITIONS)

topics:
	uv run kafka-admin create-topics --topic "$(TOPIC)" --dlq-topic "$(DLQ_TOPIC)" --partitions $(PARTITIONS) --dlq-partitions $(DLQ_PARTITIONS)

derived-topic:
	uv run kafka-admin create-derived-topics --pageviews-topic "$(PAGEVIEWS_TOPIC)" --partitions $(PARTITIONS)

describe:
	uv run kafka-admin describe --topic "$(TOPIC)"

describe-dlq:
	uv run kafka-admin describe --topic "$(DLQ_TOPIC)"

describe-derived:
	uv run kafka-admin describe --topic "$(PAGEVIEWS_TOPIC)"

registry-register:
	uv run schema-registry --registry-url "$(REGISTRY_URL)" --group-id "$(REGISTRY_GROUP)" --artifact-id "$(REGISTRY_ARTIFACT)" --version "$(REGISTRY_VERSION)" register

registry-fetch:
	uv run schema-registry --registry-url "$(REGISTRY_URL)" --group-id "$(REGISTRY_GROUP)" --artifact-id "$(REGISTRY_ARTIFACT)" --version "$(REGISTRY_VERSION)" fetch --validate-schema

produce:
	uv run kafka-produce --topic "$(TOPIC)" --dlq-topic "$(DLQ_TOPIC)" --template "$(TEMPLATE)" --eps $(EPS) --limit $(LIMIT) --invalid-every $(INVALID_EVERY)

produce-invalid:
	$(MAKE) produce INVALID_EVERY=2 LIMIT=4

produce-registry:
	uv run kafka-produce --topic "$(TOPIC)" --dlq-topic "$(DLQ_TOPIC)" --template "$(TEMPLATE)" --eps $(EPS) --limit $(LIMIT) --invalid-every $(INVALID_EVERY) --schema-source registry --registry-url "$(REGISTRY_URL)" --registry-group "$(REGISTRY_GROUP)" --registry-version "$(REGISTRY_VERSION)"

process:
	uv run kafka-process --input-topic "$(TOPIC)" --output-topic "$(PAGEVIEWS_TOPIC)" --group-id "$(PROCESSOR_GROUP)" --from-beginning

sink-duckdb:
	uv run kafka-sink-duckdb --topic "$(PAGEVIEWS_TOPIC)" --group-id "$(SINK_GROUP)" --db-path "$(DUCKDB_PATH)" --from-beginning

analytics:
	uv run duckdb-analytics --db-path "$(DUCKDB_PATH)"

consume:
	uv run kafka-consume --topic "$(TOPIC)" --group-id "$(GROUP)" --from-beginning

consume-dlq:
	uv run kafka-consume --topic "$(DLQ_TOPIC)" --group-id "$(GROUP)-dlq" --from-beginning

consume-derived:
	uv run kafka-consume --topic "$(PAGEVIEWS_TOPIC)" --group-id "$(GROUP)-derived" --from-beginning

test:
	uv run pytest
	node --check tools/mockingbird-jsonl.mjs
	$(COMPOSE) config >/dev/null

ui:
	@echo "Open Kafka UI: http://localhost:8080"

registry-ui:
	@echo "Open Apicurio Registry: http://localhost:8081"
