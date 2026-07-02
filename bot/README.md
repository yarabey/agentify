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

## Что реализовано сейчас (тикеты 0.6, 10.1, 10.2)

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
(`SetWebhook`).

**Привязка Telegram-аккаунта — `/start <code>`** (тикет 10.2, FR D3
«привязка канала к аккаунту описана явно», Gherkin §6 «Уведомления»,
предусловие сценария «Уведомление в Telegram») — первый реальный хендлер, зарегистрированный
через `bot.Handle("/start", ...)` (см. [`bot/start.go`](start.go)). Пользователь
генерирует одноразовый код привязки на экране «Настройки» в web (тикет 9.6,
`POST /channels/telegram/link-code`, реализован в оркестраторе — см.
[`orchestrator/README.md`](../orchestrator/README.md); для локальной проверки
БЕЗ поднятого web можно вставить код напрямую в БД, см. «Запуск локально»
ниже) и пересылает его боту как deep-link (`t.me/<bot>?start=<code>`) либо вручную
(`/start <code>`) — Telegram доставляет оба варианта одинаково текстом команды
с payload после пробела. Обработчик вызывает единый API оркестратора —
[`bot/internal/orchestrator`](internal/orchestrator).`Client.LinkTelegram` →
`POST /channels/telegram/link` (без Bearer: код — единственное доказательство
права на привязку, та же модель доверия, что у `registration_token` в `POST
/auth/register`) — и отвечает пользователю понятным текстом: успех, либо
конкретная причина отказа (код не найден / истёк / уже использован /
telegram-аккаунт уже привязан к другому пользователю). Вся бизнес-логика
(проверка кода, атомарная запись `channel_links`) — в оркестраторе
(`orchestrator/internal/channel.Linker`, см. `orchestrator/README.md`), бот
только адаптирует HTTP↔Telegram (принцип «единый API»).

Постановка/отмена задачи и ответ на вопрос агента из Telegram — тикет 10.3;
consumer уведомлений — тикет 10.4. Хендлеры для них пока не зарегистрированы.

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
| `BOT_ORCHESTRATOR_URL` | *(пусто)* | Базовый URL API оркестратора (тикет 10.2, FR D3), нужен `/start <code>` для обмена кода привязки. Внутри `docker compose` — `http://orchestrator:8080` (внутренний DNS, напрямую, минуя Caddy). Пусто → `/start <code>` отвечает «функция недоступна» вместо обращения к оркестратору (dev/CI без БД). |

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

# С привязкой аккаунта (тикет 10.2) — нужен ещё и запущенный оркестратор:
BOT_TOKEN=123:ABC BOT_WEBHOOK_SECRET=devsecret BOT_ORCHESTRATOR_URL=http://localhost:8081 go run ./bot
# Код привязки в обычном флоу получают на экране «Настройки» в web
# (POST /channels/telegram/link-code — тикет 9.6). Без запущенного web для
# локальной проверки можно вставить код напрямую в БД:
#   INSERT INTO channel_link_codes (code, user_id, channel, expires_at)
#   VALUES ('devcode', '<user_id>', 'telegram', now() + interval '1 hour');
curl -s -XPOST localhost:8080/webhook/devsecret \
  -H 'X-Telegram-Bot-Api-Secret-Token: devsecret' \
  -d '{"update_id":2,"message":{"message_id":2,"from":{"id":999,"first_name":"U"},"chat":{"id":999,"type":"private"},"text":"/start devcode"}}'
```

Остановка — `Ctrl+C` (SIGINT) или `kill -TERM <pid>`: сервис гасится gracefully.
