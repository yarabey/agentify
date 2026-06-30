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

.DEFAULT_GOAL := help

.PHONY: help tools generate lint test bdd docs-check run-local

help: ## Показать список целей.
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

tools: ## Установить инструментарий (sqlc, goose, oapi-codegen, golangci-lint, goreleaser) фиксированных версий.
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)
	go install github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION)
	go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION)
	go install github.com/goreleaser/goreleaser/v2@$(GORELEASER_VERSION)
	@echo "tools installed into $(GOBIN)"

generate: ## [ЗАГЛУШКА — тикет 0.2] Кодогенерация из openapi.yaml и sqlc.
	@echo "generate: implemented in ticket 0.2"

lint: ## Запустить golangci-lint (вкл. линтеры документации revive/godot).
	golangci-lint run ./...

test: ## Запустить unit-тесты Go.
	go test ./...

bdd: ## [ЗАГЛУШКА — тикет 11.2] Прогон Gherkin-сценариев (godog).
	@echo "bdd: implemented in ticket 11.2"

docs-check: ## Проверка документации: godoc на экспортируемых символах, маркеры задач с номером, согласованность openapi.yaml (тикет 0.7).
	GOLANGCI_LINT=$(SYSTEM_GOLANGCI_LINT) deploy/scripts/docs-check.sh

run-local: ## [ЗАГЛУШКА — тикет 0.3] Поднять стек в docker compose локально.
	@echo "run-local: implemented in ticket 0.3"
