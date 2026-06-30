# orchestrator — ядро системы agentify

## Назначение и бизнес-контекст

Оркестратор — центральный сервис системы. Он реализует **единый REST+WebSocket
API**, в который ходят и web PWA, и Telegram-бот (принцип «единый API», см.
[`docs/01_tech_stack_and_architecture.md` §1](../docs/01_tech_stack_and_architecture.md)).
Здесь живёт вся бизнес-логика, которой нет в адаптерах каналов:

- аутентификация и изоляция данных пользователей (FR A1–A4);
- интеграции и выдача UUID машин (FR B1–B6);
- конечный автомат задачи (FSM) и история (FR E1–E7, H1–H3);
- вопрос-ответ и согласование команд агента (FR F1–F5);
- мост к шине Redpanda и адресация команд конкретной машине (§6 архитектуры).

Оркестратор деплоится на наш VPS в составе `docker compose` за Caddy.

## Что реализовано сейчас

**Каркас HTTP-API + регистрация по токену (тикет 1.2).** Поверх общего
операционного каркаса из пакета [`internal/platform`](../internal/platform)
(конфиг из env, slog, graceful shutdown) оркестратор теперь поднимает
сгенерированный из [`api/openapi.yaml`](../api/openapi.yaml) chi-роутер
([`internal/api`](internal/api)) и подключается к Postgres через `pgxpool`
(`*db.Queries` из sqlc).

- `GET /healthz` — `200`, тело `{"status":"ok","service":"orchestrator"}`
  (нужно `docker compose` и Caddy для проверки живости, приёмка тикета 0.3).
- **`POST /auth/register`** — регистрация по секретному токену (FR A1,
  Gherkin §1). Доступ закрытый: аккаунт создаётся только при предъявлении
  действующего токена регистрации.
  - тело `RegisterRequest` (`username`, `password`, `registration_token`);
  - нет/неверный токен → `403` (Gherkin §1 «Регистрация без токена запрещена»);
  - пароль хэшируется argon2id (не plaintext, FR I1) и кладётся в `users`;
  - занятый `username` → `409`; успех → `201`.
- Остальные операции контракта (логин/refresh/logout — 1.3, middleware — 1.4,
  интеграции/задачи — позже) пока отвечают `501 Not Implemented` (встроенная
  заглушка `api.Unimplemented`) и реализуются в своих тикетах.

Маршруты монтируются от корня (`/auth/register`, `/healthz`): Caddy в compose
роутит `/api/*` → orchestrator со стрипом префикса.

Если `ORCH_DATABASE_URL` не задан (каркасные прогоны без БД), API не
поднимается — остаётся только `/healthz` из `platform`.

## Конфигурация (env)

Все переменные — с префиксом `ORCH_`. Дефолты рассчитаны на запуск в dev без
единой переменной.

| Переменная | Значение по умолчанию | Назначение |
|---|---|---|
| `ORCH_ENV` | `dev` | Режим выполнения: `dev` или `prod`. Влияет на формат логов по умолчанию. |
| `ORCH_LOG_LEVEL` | `info` | Уровень логирования: `debug`/`info`/`warn`/`error`. |
| `ORCH_LOG_FORMAT` | по `ENV` (`json` в prod, `text` в dev) | Формат slog: `json` или `text`. |
| `ORCH_HEALTH_ADDR` | `:8080` | Адрес HTTP-сервера (`/healthz` + API). |
| `ORCH_SHUTDOWN_TIMEOUT` | `10s` | Крайний срок graceful-остановки HTTP-сервера. |
| `ORCH_DATABASE_URL` | — (пусто) | DSN Postgres (`postgres://…`). Без него миграции и API не поднимаются. |

Специфичные для оркестратора поля (Redpanda, JWT-ключи) добавляются под тем же
префиксом `ORCH_` в соответствующих тикетах.

## Запуск локально

```bash
go run ./orchestrator           # dev: text-лог, только /healthz (без БД)
curl -s localhost:8080/healthz  # {"status":"ok","service":"orchestrator"}

# С БД — поднимается API (миграции на старте + POST /auth/register):
ORCH_DATABASE_URL='postgres://app:app@localhost:5432/app?sslmode=disable' \
  go run ./orchestrator
curl -s -X POST localhost:8080/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"s3cr3t","registration_token":"<активный токен>"}'
# 201 — аккаунт создан; 403 — нет/неверный токен; 409 — username занят.
```

Активный токен регистрации создаётся bootstrap-командой (тикет 1.7) или
вручную в таблице `registration_tokens`.

Остановка — `Ctrl+C` (SIGINT) или `kill -TERM <pid>`: сервис гасится gracefully.
