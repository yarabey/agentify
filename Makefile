# Makefile монорепо agentify.
#
# Цели соответствуют AGENTS.md §7. В тикете 0.1 реально работают `lint` и
# `test`; `tools` ставит инструментарий; `generate`, `bdd`, `docs-check`,
# `run-local` — заглушки, реализуемые в тикетах 0.2 / 11.2 / 0.7 / 0.3.

# --- Зафиксированные версии инструментов (тикет 0.1) ---
GOLANGCI_LINT_VERSION ?= v2.5.0
SQLC_VERSION          ?= v1.27.0
GOOSE_VERSION         ?= v3.24.1
OAPI_CODEGEN_VERSION  ?= v2.4.1
GORELEASER_VERSION    ?= v2.5.1

# Каталог, куда go install кладёт бинари; добавляем его в PATH для целей.
GOBIN := $(shell go env GOPATH)/bin
export PATH := $(GOBIN):$(PATH)

# Системный golangci-lint фиксированной версии (тикет 0.7): docs-check должен
# использовать именно его, не полагаясь на GOPATH/bin. Переопределяемо извне.
SYSTEM_GOLANGCI_LINT ?= /usr/local/bin/golangci-lint

# Инструменты кодогенерации (тикет 0.2). Берём из GOBIN (ставит `make tools`),
# но позволяем переопределить, если бинарь лежит в другом месте.
OAPI_CODEGEN ?= $(GOBIN)/oapi-codegen
SQLC         ?= $(GOBIN)/sqlc

.DEFAULT_GOAL := help

.PHONY: help tools tools-generate tools-generate-go generate generate-go generate-ts generate-sql generate-check lint test bdd docs-check run-local run-local-down smoke backup restore-check

# Compose-файл локального стека (тикет 0.3).
COMPOSE_FILE ?= deploy/docker-compose.yml
DOCKER_COMPOSE ?= docker compose -f $(COMPOSE_FILE)

# Оверрай сервиса бэкапа Postgres (тикет 11.5); добавляется к базовому файлу.
BACKUP_COMPOSE_FILE ?= deploy/docker-compose.backup.yml

help: ## Показать список целей.
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

tools: ## Установить инструментарий (sqlc, goose, oapi-codegen, golangci-lint, goreleaser) фиксированных версий.
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)
	go install github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION)
	go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION)
	go install github.com/goreleaser/goreleaser/v2@$(GORELEASER_VERSION)
	npm --prefix web ci
	@echo "tools installed into $(GOBIN) и web/node_modules (openapi-typescript)"

tools-generate-go: ## Установить только Go-инструменты кодогенерации (sqlc, oapi-codegen) — без npm (см. tools-generate).
	go install github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)
	go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION)
	@echo "tools-generate-go: sqlc+oapi-codegen installed into $(GOBIN)"

tools-generate: tools-generate-go ## Установить только инструменты кодогенерации (sqlc, oapi-codegen) + npm-зависимости web — используется generate-check в CI (без golangci-lint/goose/goreleaser, они там не нужны).
	npm --prefix web ci
	@echo "tools-generate: sqlc+oapi-codegen installed into $(GOBIN) и web/node_modules (openapi-typescript)"

generate: generate-go generate-ts generate-sql ## Кодогенерация из openapi.yaml (oapi-codegen, openapi-typescript) и схемы БД (sqlc).
	@echo "generate: done (go types+server, ts schema, sqlc models)"

generate-go: ## Сгенерировать Go-типы и chi-серверный интерфейс из api/openapi.yaml (oapi-codegen).
	$(OAPI_CODEGEN) --config api/oapi-codegen.types.yaml api/openapi.yaml
	$(OAPI_CODEGEN) --config api/oapi-codegen.server.yaml api/openapi.yaml

generate-ts: ## Сгенерировать TS-типы web из api/openapi.yaml (openapi-typescript).
	npm --prefix web run gen:api

generate-sql: ## Сгенерировать Go-модели БД из миграций (sqlc); запросы добавит тикет 1.1.
	$(SQLC) generate

generate-check: generate ## Проверить, что закоммиченная генерёнка совпадает с контрактом (CI, тикет 0.4).
	@git diff --exit-code -- orchestrator/internal/api orchestrator/internal/db web/src/api/schema.ts \
		|| { echo "generate-check: сгенерированный код разошёлся с контрактом — запусти 'make generate' и закоммить"; exit 1; }
	@echo "generate-check: генерёнка синхронна с контрактом"

lint: ## Запустить golangci-lint (вкл. линтеры документации revive/godot).
	golangci-lint run ./...

test: ## Запустить unit-тесты Go.
	go test ./...

bdd: ## [ЗАГЛУШКА — тикет 11.2] Прогон Gherkin-сценариев (godog).
	@echo "bdd: implemented in ticket 11.2"

docs-check: ## Проверка документации: godoc на экспортируемых символах, маркеры задач с номером, согласованность openapi.yaml (тикет 0.7).
	GOLANGCI_LINT=$(SYSTEM_GOLANGCI_LINT) deploy/scripts/docs-check.sh

run-local: ## Поднять весь стек локально (Postgres+Redpanda+orchestrator+bot+caddy+web), собрав образы.
	$(DOCKER_COMPOSE) up -d --build

run-local-down: ## Погасить локальный стек и удалить тома (postgres/redpanda data).
	$(DOCKER_COMPOSE) down -v

smoke: ## Smoke-проверка локального стека: /healthz всех сервисов отвечает 200.
	deploy/scripts/smoke.sh

backup: ## Снять разовый бэкап Postgres в том pg_backups (тикет 11.5; postgres должен быть запущен).
	$(DOCKER_COMPOSE) -f $(BACKUP_COMPOSE_FILE) run --rm backup /usr/local/bin/backup.sh

restore-check: ## Проверка восстановления последнего дампа на чистый инстанс (FR I2, тикет 11.5).
	deploy/backup/restore-check.sh
