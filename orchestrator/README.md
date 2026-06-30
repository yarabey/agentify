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

## Что реализовано сейчас (тикет 0.6)

Только **общий операционный каркас** из пакета
[`internal/platform`](../internal/platform): загрузка конфига из env, slog,
эндпоинт `GET /healthz`, graceful shutdown по SIGTERM/SIGINT. Бизнес-логика
добавляется в последующих тикетах EPIC 1/2/3/5/6/8.

`GET /healthz` отвечает `200` и телом `{"status":"ok","service":"orchestrator"}`
— это нужно `docker compose` и Caddy для проверки живости (приёмка тикета 0.3).

## Конфигурация (env)

Все переменные — с префиксом `ORCH_`. Дефолты рассчитаны на запуск в dev без
единой переменной.

| Переменная | Значение по умолчанию | Назначение |
|---|---|---|
| `ORCH_ENV` | `dev` | Режим выполнения: `dev` или `prod`. Влияет на формат логов по умолчанию. |
| `ORCH_LOG_LEVEL` | `info` | Уровень логирования: `debug`/`info`/`warn`/`error`. |
| `ORCH_LOG_FORMAT` | по `ENV` (`json` в prod, `text` в dev) | Формат slog: `json` или `text`. |
| `ORCH_HEALTH_ADDR` | `:8080` | Адрес HTTP-сервера с `/healthz`. |
| `ORCH_SHUTDOWN_TIMEOUT` | `10s` | Крайний срок graceful-остановки HTTP-сервера. |

Специфичные для оркестратора поля (БД, Redpanda, JWT-ключи) добавляются под тем
же префиксом `ORCH_` в соответствующих тикетах.

## Запуск локально

```bash
go run ./orchestrator           # dev: text-лог, /healthz на :8080
ORCH_ENV=prod go run ./orchestrator   # prod: JSON-лог
curl -s localhost:8080/healthz  # {"status":"ok","service":"orchestrator"}
```

Остановка — `Ctrl+C` (SIGINT) или `kill -TERM <pid>`: сервис гасится gracefully.
