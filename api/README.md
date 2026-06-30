# api/ — контракт и кодогенерация

`api/openapi.yaml` — **единственный источник правды** для HTTP-API оркестратора
(AGENTS.md §0 п.5, §2). Все каналы (web PWA, Telegram-бот) и серверная реализация
обязаны соответствовать этому контракту; типы и интерфейсы не пишутся руками, а
**генерятся** из него. Менять API = править `openapi.yaml` и перегенерировать
(`make generate`) в том же PR (правило одного PR, §4).

## Что и куда генерится (`make generate`)

| Что | Инструмент (версия) | Конфиг | Выход |
|---|---|---|---|
| Go-модели/типы DTO | oapi-codegen v2.4.1 | `api/oapi-codegen.types.yaml` | `orchestrator/internal/api/types.gen.go` |
| Go chi-серверный интерфейс | oapi-codegen v2.4.1 | `api/oapi-codegen.server.yaml` | `orchestrator/internal/api/server.gen.go` |
| TS-типы web | openapi-typescript 7.13.0 | `web/package.json` (скрипт `gen:api`) | `web/src/api/schema.ts` |
| Go-модели БД | sqlc v1.27.0 | `sqlc.yaml` | `orchestrator/internal/db/*.go` |

Все Go/TS-файлы помечены заголовком `Code generated … DO NOT EDIT.` (или
аналогом TS) — golangci-lint v2 исключает их из revive/godot, поэтому
сгенерированные символы вручную **не документируются**. Правится контракт, а не
генерёнка.

## Команды

```bash
make tools          # поставить oapi-codegen, sqlc, golangci-lint + web/node_modules
make generate       # перегенерировать всё из контракта (идемпотентно)
make generate-check # generate + git diff --exit-code: проверка синхронности (CI, 0.4)
```

Конфиги oapi-codegen задают пути относительно **корня** репозитория — запускать
через `make generate` из корня, не из каталога `api/`.

## Заметки по тикету 0.2

- **sqlc на пустых запросах.** sqlc v1.27.0 падает с «no queries contained», если
  `orchestrator/queries` пуст. Бизнес-запросы добавит тикет 1.1; до тех пор есть
  один затравочный `orchestrator/queries/bootstrap.sql` (`SELECT 1`), благодаря
  которому sqlc генерит `models.go` из схемы миграций. Файл удалится/заменится в 1.1.
- **embedded-spec отключён** у oapi-codegen намеренно: он тянет kin-openapi,
  чьи актуальные версии требуют go ≥ 1.25 и конфликтуют с системным
  golangci-lint v2.5.0 (собран под go 1.24). Спека и так лежит в `openapi.yaml`.
- **OpenAPI 3.1.** oapi-codegen v2.4.1 печатает WARNING про 3.1.x, но генерит
  корректный код для используемых здесь конструкций; контракт не понижался до 3.0.
