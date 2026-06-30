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

Только для подкоманды `orchestrator bootstrap` (тикет 1.7, см. ниже) — без
дефолтов, обязательны для самого bootstrap'а:

| Переменная | Назначение |
|---|---|
| `ORCH_BOOTSTRAP_ADMIN_USERNAME` | Username первого администратора. |
| `ORCH_BOOTSTRAP_ADMIN_PASSWORD` | Пароль первого администратора (хэшируется argon2id, в БД не попадает plaintext). |
| `ORCH_INITIAL_REGISTRATION_TOKEN` | Значение стартового токена регистрации (FR A2). Имя секрета зафиксировано в [`docs/MANUAL_STEPS.md`](../docs/MANUAL_STEPS.md) (`openssl rand -hex 16`), читается под общим префиксом `ORCH_`. |

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

## Bootstrap первого администратора (тикет 1.7)

Доступ в систему закрытый (FR A1): обычная регистрация (`POST /auth/register`)
сама требует активного токена регистрации, поэтому самый первый аккаунт и сам
этот токен заводит отдельная подкоманда того же бинаря — `orchestrator
bootstrap` (без нового CLI-фреймворка, простой разбор `os.Args[1]`; без
аргументов бинарь по-прежнему стартует как обычный сервис).

```bash
ORCH_DATABASE_URL='postgres://app:app@localhost:5432/app?sslmode=disable' \
ORCH_BOOTSTRAP_ADMIN_USERNAME='admin' \
ORCH_BOOTSTRAP_ADMIN_PASSWORD='<сгенерированный пароль>' \
ORCH_INITIAL_REGISTRATION_TOKEN='<значение из docs/MANUAL_STEPS.md, openssl rand -hex 16>' \
  go run ./orchestrator bootstrap
```

Команда сама приводит схему БД к актуальной версии (как и обычный запуск
сервиса), затем идемпотентно:

- создаёт первого администратора (`is_admin=true`, пароль — argon2id-хэш, FR I1)
  с username/паролем из `ORCH_BOOTSTRAP_ADMIN_USERNAME`/`ORCH_BOOTSTRAP_ADMIN_PASSWORD`,
  либо, если пользователь с этим username уже существует (например, заведён
  обычной регистрацией), доводит его до администратора, не создавая дубликат;
- создаёт активный токен регистрации со значением `ORCH_INITIAL_REGISTRATION_TOKEN`
  (FR A2), либо — если активный токен уже есть (неважно с каким значением,
  ротация токена вне MVP) — пропускает создание.

Повторные запуски с теми же переменными окружения (типичный сценарий —
bootstrap-шаг при каждом деплое) **идемпотентны**: они не падают с ошибкой
уникальности и не создают второго администратора или второго активного
токена — каждый шаг no-op, если уже выполнен. Подробности — godoc
[`orchestrator/internal/bootstrap`](internal/bootstrap).
