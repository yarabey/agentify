# agentify

[![CI](https://github.com/yarabey/agentify/actions/workflows/ci.yml/badge.svg)](https://github.com/yarabey/agentify/actions/workflows/ci.yml)

Монорепозиторий системы оркестрации агентов: оркестратор, Telegram-бот, агент-демон и web PWA с общим контрактом OpenAPI.

> Архитектура и обоснование решений — в [`docs/01_tech_stack_and_architecture.md`](docs/01_tech_stack_and_architecture.md).
> Правила работы и дисциплина документации (Definition of Done) — в [`AGENTS.md`](AGENTS.md).

## Зафиксированные версии

| Инструмент | Версия |
|---|---|
| Go | **1.24** |
| Node.js | **22** |

Версии инструментов кодогенерации/линта зафиксированы в [`Makefile`](Makefile)
(`golangci-lint`, `sqlc`, `goose`, `oapi-codegen`, `goreleaser`).

## Карта репозитория (01_tech_stack §5)

```
/orchestrator        # Go: REST+WS, FSM, auth, БД, мост к Redpanda
  /internal/...       # внутренние пакеты сервиса
  /migrations         # goose-миграции (SQL)
  /queries            # sqlc .sql (наполняется в 0.2)
  Dockerfile
/bot                 # Go: Telegram-адаптер (webhook + доставка уведомлений)
  Dockerfile
/agent               # Go: демон на машину пользователя (GoReleaser, не в compose)
/web                 # React PWA (Vite)
  Dockerfile          # multi-stage → статика, которую раздаёт Caddy
/api                 # openapi.yaml (источник правды) + сгенерированные клиенты
/deploy
  docker-compose.yml  # заглушка → наполняется в 0.3
  Caddyfile           # заглушка → 0.3/0.5
  /scripts            # deploy.sh (ssh), bootstrap
/install             # install.sh (одна команда для установки агента)
/docs                # документы проекта, ADR
.github/workflows    # ci.yml, deploy.yml, release-agent.yml (реализуются в 0.4/0.5)
Makefile             # tools, generate, lint, test, bdd, docs-check, run-local
```

Один корневой Go-модуль: `github.com/yarabey/agentify`.

## Команды (`make`)

| Цель | Назначение | Статус (тикет 0.1) |
|---|---|---|
| `make tools` | установить sqlc, goose, oapi-codegen, golangci-lint, goreleaser | работает |
| `make lint` | golangci-lint (вкл. линтеры документации revive/godot) | работает |
| `make test` | unit-тесты Go | работает |
| `make generate` | кодогенерация из openapi.yaml и sqlc | заглушка → тикет 0.2 |
| `make run-local` | поднять стек в docker compose | заглушка → тикет 0.3 |
| `make docs-check` | проверка документации | заглушка → тикет 0.7 |
| `make bdd` | прогон Gherkin-сценариев (godog), см. [`orchestrator/features/README.md`](orchestrator/features/README.md) | работает (тикет 11.2) |

### Быстрый старт для разработчика

```bash
make tools   # один раз: установить инструментарий нужных версий
make lint    # линт всего модуля
make test    # unit-тесты
```

## CI (что блокирует мёрж)

CI описан в [`.github/workflows/ci.yml`](.github/workflows/ci.yml) (тикет 0.4) и
запускается на каждый `pull_request` и `push` в ветки разработки/`main`. Команды
джоб — ровно те, что в `make` (AGENTS.md §7), чтобы CI совпадал с локальным
прогоном.

| Джоба | Команда | Назначение |
|---|---|---|
| `lint` | `make lint` (golangci-lint **v2.5.0**) | стиль + линтеры документации (revive/godot) |
| `build` | `go build ./...` | модуль собирается |
| `test` | `make test` | unit-тесты Go |
| `generate-check` | `make generate-check` | контракт-генерёнка (oapi-codegen/sqlc/openapi-typescript) синхронна |
| `docs-check` | `make docs-check` | godoc на экспортируемых символах, TODO с номером, согласованность openapi |
| `bdd` | `make bdd` | Gherkin-сценарии §1–§10 (godog, orchestrator/features) на testcontainers-Postgres — тикет 11.2, см. [`orchestrator/features/README.md`](orchestrator/features/README.md) |
| `integration` | `go test -tags=integration ./...` | testcontainers Postgres+Redpanda — каркас для 1.1/3.2/11.3 (тестов с тегом пока нет ⇒ no-op) |
| `agent-release-build` | `goreleaser check` + `goreleaser build --snapshot --clean` | тикет 4.1: CI собирает все 4 кросс-таргета агента (darwin/linux × amd64/arm64) — без публикации; публикует релизы отдельный [`release-agent.yml`](.github/workflows/release-agent.yml) по git-тегу `v*` |
| `agent-install-test` | `goreleaser release --snapshot ...` + `install/test-install.sh` в `docker run ubuntu:24.04` | тикет 4.2: `install/install.sh` (скачивание бинаря + sha256 + установка + Node.js) прогоняется целиком в чистом Ubuntu без предустановленного Node |
| `agent-service-test` | `agentify-agent setup` + `sudo agentify-agent service-install` + `kill -9` MainPID | тикет 4.4: демонизация (FR C5) — в отличие от `agent-install-test` запускается напрямую на хосте раннера `ubuntu-latest` (реальный systemd как PID 1), проверяет, что после `kill` процесс поднимается заново (systemd `Restart=on-failure`) |
| `web` | `npm ci` + `npm run gen:api` + `tsc` | TS-схема из контракта типизируется (без полного Vite — 9.1) |

**Path-фильтры** (`dorny/paths-filter`, джоба `changes`): один корневой Go-модуль,
поэтому изменения в любом Go-коде/контракте/тулинге гоняют все Go-джобы, а
изменения `web/**` (и `api/openapi.yaml`) — джобу `web`. Изменения только в
markdown-документации не держат Go-гейты.

**Версия golangci-lint** в CI зафиксирована на `v2.5.0` (env
`GOLANGCI_LINT_VERSION`, совпадает с `Makefile` и системным бинарём локально):
ставится v2-модуль `go install .../golangci-lint/v2@v2.5.0`, что гарантирует
v2-схему `.golangci.yml`. Джоба `docs-check` передаёт путь к нему через
`SYSTEM_GOLANGCI_LINT`.

### Branch protection (ручной шаг — `MANUAL_STEPS.md`)

В коде protection не настраивается. Для блокировки мёржа при красном CI включите в
GitHub → Settings → Branches → Branch protection (для `main` и ветки разработки)
required status checks по именам джоб:

```
lint, build, test, generate-check, docs-check, bdd, integration, agent-release-build, agent-install-test, agent-service-test, web
```

> Замечание: джобы `lint/build/test/generate-check/docs-check/bdd/integration` и
> `web` пропускаются path-фильтром, когда соответствующих изменений нет. Если ваша
> политика protection требует «жёстко обязательных» статусов даже на skip, делайте
> required только те, которые применимы к вашим правкам, либо переведите фильтры на
> схему «всегда запускать с быстрым no-op» — для MVP оставлено гибкое поведение.

## Линтеры документации

Согласно [`AGENTS.md` §3](AGENTS.md), документация — часть Definition of Done.
[`.golangci.yml`](.golangci.yml) написан по **схеме golangci-lint v2** (ключ
`version: "2"`), под системный бинарь `v2.5.0`. Включены:

- **revive** — требует godoc на экспортируемых символах (`exported`) и
  комментарии пакетов (`package-comments`);
- **godot** — комментарии должны заканчиваться точкой.

> Отклонение тикета 0.1: отдельный линтер `godoclint`, упомянутый в AGENTS.md §3
> и тексте тикета, в `golangci-lint` **отсутствует** в реестре линтеров
> (проверено в `v1.64.8` и `v2.5.0`). Покрытие godoc обеспечивается связкой
> `revive` (`exported` + `package-comments`) и `godot`. Полную проверку
> документации возьмёт на себя `make docs-check` (тикет 0.7); при появлении
> `godoclint` он будет добавлен в конфиг.

Дополнительно: [`.editorconfig`](.editorconfig) (единый стиль файлов) и
[`.pre-commit-config.yaml`](.pre-commit-config.yaml) (gofmt/vet/golangci-lint
перед коммитом — `pip install pre-commit && pre-commit install`).

## Лицензия

Проект распространяется под лицензией
[FSL-1.1-ALv2 (Functional Source License)](LICENSE.md): код открыт, его можно
свободно читать, использовать, модифицировать и распространять для любых целей,
**кроме создания конкурирующего коммерческого продукта или сервиса**. Каждая
версия кода автоматически перелицензируется под Apache-2.0 через два года после
публикации.
