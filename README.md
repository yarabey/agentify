# agentify

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
| `make bdd` | прогон Gherkin-сценариев (godog) | заглушка → тикет 11.2 |

### Быстрый старт для разработчика

```bash
make tools   # один раз: установить инструментарий нужных версий
make lint    # линт всего модуля
make test    # unit-тесты
```

## Линтеры документации

Согласно [`AGENTS.md` §3](AGENTS.md), документация — часть Definition of Done.
В [`.golangci.yml`](.golangci.yml) включены:

- **revive** — требует godoc на экспортируемых символах (`exported`) и
  комментарии пакетов (`package-comments`);
- **godot** — комментарии должны заканчиваться точкой.

> Отклонение тикета 0.1: отдельный линтер `godoclint`, упомянутый в AGENTS.md §3
> и тексте тикета, в `golangci-lint v1.64.8` **отсутствует** в реестре линтеров.
> Покрытие godoc обеспечивается связкой `revive` (`exported` + `package-comments`)
> и `godot`. Полную проверку документации возьмёт на себя `make docs-check`
> (тикет 0.7); при появлении `godoclint` он будет добавлен в конфиг.

Дополнительно: [`.editorconfig`](.editorconfig) (единый стиль файлов) и
[`.pre-commit-config.yaml`](.pre-commit-config.yaml) (gofmt/vet/golangci-lint
перед коммитом — `pip install pre-commit && pre-commit install`).
