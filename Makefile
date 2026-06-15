COMPOSE ?= docker-compose
TOPIC ?= mockingbird.events
PARTITIONS ?= 3
TEMPLATE ?= Web Analytics Starter Kit
EPS ?= 2
LIMIT ?= 20
GROUP ?= mockingbird-python-learners

.PHONY: install up down reset logs topic describe produce consume test ui

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

describe:
	uv run kafka-admin describe --topic "$(TOPIC)"

produce:
	uv run kafka-produce --topic "$(TOPIC)" --template "$(TEMPLATE)" --eps $(EPS) --limit $(LIMIT)

consume:
	uv run kafka-consume --topic "$(TOPIC)" --group-id "$(GROUP)" --from-beginning

test:
	uv run pytest
	node --check tools/mockingbird-jsonl.mjs
	$(COMPOSE) config >/dev/null

ui:
	@echo "Open http://localhost:8080"
