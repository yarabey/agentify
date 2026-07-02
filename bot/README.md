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

## Что реализовано сейчас (тикеты 0.6, 10.1)

**Общий операционный каркас** из пакета
[`internal/platform`](../internal/platform): загрузка конфига из env, slog,
эндпоинт `GET /healthz`, graceful shutdown по SIGTERM/SIGINT (тикет 0.6).

**Каркас приёма апдейтов Telegram по webhook** (тикет 10.1, FR D1): библиотека
[telebot v3](https://pkg.go.dev/gopkg.in/telebot.v3), приём апдейтов **через
webhook** (не long-polling) за Caddy на `bot.<домен>`. HTTP-обвязка — в
[`bot/internal/webhook`](internal/webhook): она монтируется на тот же
chi-роутер, что и `/healthz`, по **секретному пути** вида `/webhook/<secret>`,
сверяет заголовок `X-Telegram-Bot-Api-Secret-Token`, декодирует апдейт, логирует
факт приёма и передаёт его в маршрутизатор telebot (`bot.ProcessUpdate`). При
старте, если задан публичный URL, webhook регистрируется в Telegram
(`SetWebhook`). Команды `/start`, привязка аккаунта и действия — тикеты
10.2/10.3; consumer уведомлений — тикет 10.4. Пока ни одного хендлера не
зарегистрировано: апдейт логируется и маршрутизация — no-op.

`GET /healthz` отвечает `200` и телом `{"status":"ok","service":"bot"}` — нужно
`docker compose` и Caddy для проверки живости (приёмка тикета 0.3).

## Конфигурация (env)

Все переменные — с префиксом `BOT_`. Дефолты рассчитаны на запуск в dev без
единой переменной: без токена бот поднимает только `/healthz` (каркас 0.6) и не
падает.

| Переменная | Значение по умолчанию | Назначение |
|---|---|---|
| `BOT_ENV` | `dev` | Режим выполнения: `dev` или `prod`. Влияет на формат логов по умолчанию. |
| `BOT_LOG_LEVEL` | `info` | Уровень логирования: `debug`/`info`/`warn`/`error`. |
| `BOT_LOG_FORMAT` | по `ENV` (`json` в prod, `text` в dev) | Формат slog: `json` или `text`. |
| `BOT_HEALTH_ADDR` | `:8080` | Адрес HTTP-сервера с `/healthz` и webhook. |
| `BOT_SHUTDOWN_TIMEOUT` | `10s` | Крайний срок graceful-остановки HTTP-сервера. |
| `BOT_TOKEN` | *(пусто)* | **Секрет.** Токен бота от @BotFather (FR D1). Пусто → Telegram-часть отключена, только `/healthz`. НЕ коммитить. |
| `BOT_WEBHOOK_SECRET` | *(пусто)* | **Секрет.** Секрет в пути `/webhook/<secret>` и как `secret_token` Telegram. Обязателен при заданном `BOT_PUBLIC_URL` (иначе фатальная ошибка старта). НЕ коммитить. |
| `BOT_PUBLIC_URL` | *(пусто)* | Внешний базовый URL бота за Caddy (например `https://bot.<домен>`). Задан → webhook регистрируется в Telegram (`SetWebhook`). Пусто → webhook не регистрируется (dev). |

> **Секреты** (`BOT_TOKEN`, `BOT_WEBHOOK_SECRET`) в репозиторий не коммитятся:
> задаются через окружение хоста / GitHub Secrets. `BOT_TOKEN` — это
> `TELEGRAM_BOT_TOKEN` из [`docs/MANUAL_STEPS.md`](../docs/MANUAL_STEPS.md) §3;
> `BOT_WEBHOOK_SECRET` генерируется разово (см. MANUAL_STEPS §2). Полный
> публичный адрес webhook, который выставляется в Telegram, —
> `https://bot.<домен>/webhook/<BOT_WEBHOOK_SECRET>`.

## Запуск локально

```bash
go run ./bot                    # dev: text-лог, /healthz на :8080, без webhook
BOT_ENV=prod go run ./bot       # prod: JSON-лог
curl -s localhost:8080/healthz  # {"status":"ok","service":"bot"}

# С реальным ботом (dev, без публичного URL — webhook только монтируется, не регистрируется):
BOT_TOKEN=123:ABC BOT_WEBHOOK_SECRET=devsecret go run ./bot
# Локально можно постучать в webhook (эмулируя Telegram):
curl -s -XPOST localhost:8080/webhook/devsecret \
  -H 'X-Telegram-Bot-Api-Secret-Token: devsecret' \
  -d '{"update_id":1,"message":{"message_id":1,"chat":{"id":1,"type":"private"},"text":"/start"}}'
```

Остановка — `Ctrl+C` (SIGINT) или `kill -TERM <pid>`: сервис гасится gracefully.
