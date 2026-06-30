# bot — адаптер канала Telegram

## Назначение и бизнес-контекст

Telegram-бот — **тонкий адаптер канала**, а не отдельная бизнес-логика. Он
владеет всем Telegram-I/O (входящие апдейты по webhook и доставка уведомлений),
а за любой бизнес-операцией ходит в **тот же REST API оркестратора**, что и web
(принцип «единый API», [`docs/01_tech_stack_and_architecture.md` §1 и РЕШЕНИЕ 3](../docs/01_tech_stack_and_architecture.md)).
Так токен бота принадлежит ровно одному сервису, а оркестратор остаётся
Telegram-агностиком (FR D1, D4, G2).

Роль бота в системе:

- привязка Telegram-аккаунта к пользователю по deep-link коду (FR D3, EPIC 10.2);
- постановка/отмена задачи и ответ на вопрос агента из Telegram (EPIC 10.3);
- доставка уведомлений: consumer топика `notifications.telegram` → Bot API
  (FR G1, EPIC 10.4).

Бот деплоится на наш VPS в составе `docker compose` за Caddy (`bot.<домен>`).

## Что реализовано сейчас (тикет 0.6)

Только **общий операционный каркас** из пакета
[`internal/platform`](../internal/platform): загрузка конфига из env, slog,
эндпоинт `GET /healthz`, graceful shutdown по SIGTERM/SIGINT. Webhook (telebot
v3) и consumer уведомлений добавляются в EPIC 10.

`GET /healthz` отвечает `200` и телом `{"status":"ok","service":"bot"}` — нужно
`docker compose` и Caddy для проверки живости (приёмка тикета 0.3).

## Конфигурация (env)

Все переменные — с префиксом `BOT_`. Дефолты рассчитаны на запуск в dev без
единой переменной.

| Переменная | Значение по умолчанию | Назначение |
|---|---|---|
| `BOT_ENV` | `dev` | Режим выполнения: `dev` или `prod`. Влияет на формат логов по умолчанию. |
| `BOT_LOG_LEVEL` | `info` | Уровень логирования: `debug`/`info`/`warn`/`error`. |
| `BOT_LOG_FORMAT` | по `ENV` (`json` в prod, `text` в dev) | Формат slog: `json` или `text`. |
| `BOT_HEALTH_ADDR` | `:8080` | Адрес HTTP-сервера с `/healthz`. |
| `BOT_SHUTDOWN_TIMEOUT` | `10s` | Крайний срок graceful-остановки HTTP-сервера. |

Специфичные для бота поля (токен Telegram, webhook-URL, адрес оркестратора)
добавляются под тем же префиксом `BOT_` в EPIC 10.

## Запуск локально

```bash
go run ./bot                    # dev: text-лог, /healthz на :8080
BOT_ENV=prod go run ./bot       # prod: JSON-лог
curl -s localhost:8080/healthz  # {"status":"ok","service":"bot"}
```

Остановка — `Ctrl+C` (SIGINT) или `kill -TERM <pid>`: сервис гасится gracefully.
