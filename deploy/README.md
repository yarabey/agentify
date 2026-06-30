# deploy — локальный запуск стека agentify

Этот каталог содержит всё для локального поднятия системы в Docker Compose
(тикет 0.3): инфраструктуру (Postgres, Redpanda) и наши сервисы (orchestrator,
bot, web за фронтовым Caddy). Прод-деплой через GHCR/SSH — тикет 0.5.

## Состав

| Файл | Назначение |
|---|---|
| `docker-compose.yml` | описание сервисов, healthcheck'ов, томов, зависимостей |
| `Caddyfile` | фронтовый reverse proxy: `/api/*`→orchestrator, `/bot/*`→bot, остальное→web |
| `caddy.Dockerfile` | образ фронтового Caddy с запечённым `Caddyfile` |
| `.env.example` | шаблон секретов/настроек (скопировать в `.env`) |
| `scripts/smoke.sh` | smoke-проверка: `/healthz` всех сервисов отвечает 200 |

Статика web и её внутренний `/healthz` собираются из `web/Dockerfile` +
`web/Caddyfile` (в 0.3 — заглушка `index.html`; полноценный Vite — тикет 9.1).

## Предусловия

- Docker + Docker Compose v2.
- Скопировать окружение и задать пароль Postgres:
  ```bash
  cp deploy/.env.example deploy/.env
  # отредактировать POSTGRES_PASSWORD (без него compose не стартует)
  ```
  Реальный `deploy/.env` **не коммитится** (см. `.gitignore`).

## Запуск

Из корня репозитория:

```bash
make run-local          # docker compose up -d --build
```

Дождаться, пока зависимости станут healthy (Postgres/Redpanda), затем проверить:

```bash
make smoke              # или: deploy/scripts/smoke.sh
```

Ожидаемый вывод — `OK` по каждому сервису и `smoke: УСПЕХ`.

## Порты и URL

Наружу публикуется **только фронтовый Caddy** (порт `HTTP_PORT`, по умолчанию
`8080`). Внутренние сервисы доступны через него:

| URL (локально) | Куда ведёт |
|---|---|
| `http://127.0.0.1:8080/` | статика web |
| `http://127.0.0.1:8080/healthz` | liveness фронтового Caddy |
| `http://127.0.0.1:8080/api/healthz` | `/healthz` оркестратора |
| `http://127.0.0.1:8080/bot/healthz` | `/healthz` бота |

Postgres (`5432`) и Redpanda (`9092`) намеренно **не публикуются** на хост —
доступ к ним только внутри compose-сети. Поменять внешний порт фронта:
`HTTP_PORT=9000 make run-local`.

## Миграции на старте

Оркестратор при запуске сам применяет goose-миграции схемы к Postgres и только
потом начинает отвечать готовым: SQL-файлы `orchestrator/migrations/*.sql`
встроены в бинарь (`embed.FS`, пакет `orchestrator/migrations`), прогон —
`orchestrator/internal/migrate` поверх библиотеки `pressly/goose`. Отдельного
сервиса/образа миграций нет; повторный старт идемпотентен (применяются только
новые версии). DSN берётся из `ORCH_DATABASE_URL` (собирается в compose из
`POSTGRES_*`).

## Логи

```bash
docker compose -f deploy/docker-compose.yml logs -f                # все сервисы
docker compose -f deploy/docker-compose.yml logs -f orchestrator   # один сервис
docker compose -f deploy/docker-compose.yml ps                     # статус + health
```

В логах оркестратора при старте видно применение миграций (`goose: ...`,
`migrate: миграции применены`).

## CI: интеграционные тесты на testcontainers

CI-workflow ([`.github/workflows/ci.yml`](../.github/workflows/ci.yml), тикет 0.4)
содержит джобу `integration` — каркас под интеграционные тесты на
**testcontainers-go** (реальные Postgres + Redpanda), которые добавят тикеты
1.1 / 3.2 / 11.3.

Решение по реализации сейчас (тестов ещё нет):

- Интеграционные тесты будут помечены build-тегом `//go:build integration`.
- Джоба гоняет `go test -tags=integration ./...`. На пустом множестве
  файлов с этим тегом команда — no-op (`exit 0`), поэтому джоба зелёная и не
  держит мёрж до появления тестов.
- Инфраструктура готова: в `ubuntu-latest` Docker доступен по умолчанию, поэтому
  testcontainers-go сможет поднимать контейнеры без дополнительной настройки
  (джоба проверяет это шагом `docker version`).
- Когда появятся файлы с тегом `integration`, та же команда автоматически начнёт
  их выполнять — правок workflow не потребуется.

> Локальный `make test` запускает только unit-тесты (`go test ./...`, без тега);
> интеграционные включаются явным тегом, чтобы не требовать Docker на каждой
> машине разработчика.

## Гашение

```bash
make run-local-down     # docker compose down -v — гасит и удаляет тома данных
```

`-v` удаляет тома `postgres_data` и `redpanda_data` (чистый старт в следующий
раз). Без удаления данных: `docker compose -f deploy/docker-compose.yml down`.

## Переменные окружения

Все из `deploy/.env.example`. Имена сервисных переменных совпадают с
`internal/platform.Config` (префиксы `ORCH_`/`BOT_`): `ENV`, `LOG_LEVEL`,
`LOG_FORMAT`; `HEALTH_ADDR` фиксируется в compose как `:8080`. Секреты — только
через `.env`, никогда в репозитории.
