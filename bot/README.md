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

## Что реализовано сейчас (тикеты 0.6, 10.1, 10.2, 10.3, 10.4)

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

**Действия из Telegram — постановка/отмена задачи, ответ на вопрос** (тикет
10.3, FR D1, §4 «Постановка задачи из канала», Примеры: канал=telegram) — см.
[`bot/task.go`](task.go). Пользователь, уже привязавший аккаунт (`/start
<code>`), может:
  - написать боту **обычный текст** (не команду) — бот ставит задачу
    подключённой машине пользователя (`GET /integrations` → если ровно одна —
    сразу `POST /tasks`; если ни одной — подсказка подключить машину в web;
    если несколько — бот НЕ угадывает и просит уточнить машину явно через
    `/task`, см. ниже — выбор из нескольких машин кнопками/списком — вне
    объёма этого тикета);
  - `/task <id машины> <текст задачи>` — та же постановка с ЯВНЫМ выбором
    машины (нужно, когда подключено больше одной);
  - `/cancel <id задачи>` — отменяет задачу (FR E6, тикет 8.4);
  - `/answer <id задачи> <id вопроса> <текст ответа>` — отвечает на вопрос
    агента (FR F1/F2, тикет 6.1). Доставка САМОГО вопроса пользователю в
    Telegram (см. «Доставка уведомлений» ниже) уже включает `id задачи`/
    `id вопроса` прямо в тексте уведомления — их не нужно искать отдельно в
    web.

**Архитектурное решение — как бот действует от имени пользователя без
пользовательского JWT.** Все три действия идут через ТОТ ЖЕ REST API
оркестратора, что и web (принцип «единый API»), но REST API аутентифицирует
защищённые операции по access-JWT пользователя (`Authorization: Bearer`), а у
бота такого токена нет — Telegram передаёт боту только `telegram_user_id`
отправителя апдейта. Решение: перед каждым действием бот вызывает
[`bot/internal/orchestrator`](internal/orchestrator).`Client.GetActingToken` →
`POST /channels/telegram/token` — служебный (НЕ пользовательский) эндпоинт,
аутентифицированный ОБЩИМ СЕРВИСНЫМ СЕКРЕТОМ между ботом и оркестратором
(`BOT_SERVICE_SECRET`/`ORCH_BOT_SERVICE_SECRET`, заголовок
`X-Bot-Service-Secret`) — доверенным ровно тем же способом, что и
`BOT_ORCHESTRATOR_URL` (тикет 10.2): прямой вызов внутри `docker compose`-сети,
минуя Caddy/публичный интернет. Оркестратор резолвит `telegram_user_id` в
`user_id` через `channel_links` (та же привязка, что и `/start <code>`) и
выпускает ОБЫЧНЫЙ access-JWT (тот же формат/TTL, что и `POST /auth/login`).
Дальше `Client.ListIntegrations`/`CreateTask`/`AnswerTask`/`CancelTask`
вызывают `GET /integrations`/`POST /tasks`/`POST /tasks/{id}/answer`/
`POST /tasks/{id}/cancel` с этим токеном как Bearer — РОВНО как это делал бы
web-клиент; в оркестраторе НЕТ отдельной, Telegram-специфичной логики
постановки/отмены задачи (`tasks.go` этим тикетом не меняется вовсе). Полное
обоснование выбора — годок `PostChannelsTelegramToken` в
[`orchestrator/internal/api/channels.go`](../orchestrator/internal/api/channels.go)
и [`orchestrator/README.md`](../orchestrator/README.md).

**Доставка уведомлений — consumer `notifications.telegram`** (тикет 10.4,
deps: 10.1, 7.3; FR G1, Gherkin §6 «Уведомление в Telegram») — см.
[`bot/notify.go`](notify.go). Оркестратор (тикет 7.3,
[`orchestrator/internal/notify/telegram`](../orchestrator/internal/notify/telegram))
резолвит привязку канала пользователя (`channel_links`, тикет 10.2) и, если
она есть, публикует в топик `notifications.telegram` (ADR 0001, consumer
group `telegram-bot`) сообщение с УЖЕ готовым `telegram_chat_id` и УЖЕ
отформатированным человекочитаемым текстом — этот файл лишь читает топик
(`bus.Consumer`, тот же commit-after-success цикл, что и у presence-consumer'а
оркестратора) и пересылает текст в чат через `*tele.Bot.Send`. Бот НЕ ходит в
БД оркестратора и не разбирает бизнес-смысл уведомления (принцип «единый
API» — оркестратор единственный владелец связи `user_id` ↔
`telegram_user_id`). Требует ОБА: непустой `BOT_TOKEN` (нужен `*tele.Bot` для
отправки) и `BOT_REDPANDA_SEEDS` — отсутствие любого тихо отключает только
эту фичу (webhook/привязка/действия продолжают работать как раньше).
Транзиентная ошибка отправки (сеть/Bot API) не коммитит offset записи
(at-least-once, ADR 0001) — запись будет перечитана после перезапуска
процесса; повреждённые данные (не тот тип/невалидный JSON/нулевой
`chat_id`/пустой текст) пропускаются с предупреждением в лог, не блокируя
партию.

`GET /healthz` отвечает `200` и телом `{"status":"ok","service":"bot"}` — нужно
`docker compose` и Caddy для проверки живости (приёмка тикета 0.3).

## Наблюдаемость (тикет 11.4, ТЗ «эксплуатация»)

`GET /metrics` (Prometheus text exposition format) смонтирован безусловно тем
же общим каркасом `internal/platform` (см. `orchestrator/README.md` →
«Наблюдаемость» — подробное описание формата/лейблов, не дублируется здесь):
счётчик `agentify_http_requests_total`/гистограмма
`agentify_http_request_duration_seconds` по (`method`, `path`, `service=bot`)
плюс стандартные Go/process-метрики. Специфичных для бота бизнес-метрик
(в отличие от оркестратора — переходы FSM/WS-соединения) в MVP-объёме этого
тикета не заведено: бот — тонкий адаптер без собственного домена (см.
«Назначение и бизнес-контекст» выше), HTTP-метрик достаточно, чтобы увидеть
проблему (растущая латентность webhook, всплеск 5xx).

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
| `BOT_ORCHESTRATOR_URL` | *(пусто)* | Базовый URL API оркестратора (тикеты 10.2/10.3, FR D3/D1), нужен `/start <code>` (обмен кода привязки) и действиям из Telegram (постановка/отмена задачи, ответ на вопрос). Внутри `docker compose` — `http://orchestrator:8080` (внутренний DNS, напрямую, минуя Caddy). Пусто → эти хендлеры отвечают «функция недоступна» вместо обращения к оркестратору (dev/CI без БД). |
| `BOT_SERVICE_SECRET` | *(пусто)* | **Секрет.** Общий сервисный секрет между ботом и оркестратором (тикет 10.3, FR D1) — отправляется как `X-Bot-Service-Secret` в `POST /channels/telegram/token` (`Client.GetActingToken`). ОБЯЗАН совпадать со значением `ORCH_BOT_SERVICE_SECRET` на стороне оркестратора. Пусто → действия из Telegram отключены (оркестратор всегда отвечает `401`, см. `orchestrator/README.md`). НЕ коммитить. |
| `BOT_REDPANDA_SEEDS` | *(пусто)* | Адреса брокеров Redpanda через запятую (тикет 10.4, FR G1, см. «Доставка уведомлений» выше). Нужен ТОЛЬКО доставке уведомлений; ни webhook, ни привязка, ни действия из Telegram от него не зависят. Пусто ИЛИ пустой `BOT_TOKEN` → доставка уведомлений отключена. |

> **Секреты** (`BOT_TOKEN`, `BOT_WEBHOOK_SECRET`, `BOT_SERVICE_SECRET`) в
> репозиторий не коммитятся: задаются через окружение хоста / GitHub Secrets.
> `BOT_TOKEN` — это `TELEGRAM_BOT_TOKEN` из
> [`docs/MANUAL_STEPS.md`](../docs/MANUAL_STEPS.md) §3; `BOT_WEBHOOK_SECRET` и
> `BOT_SERVICE_SECRET` генерируются разово (см. MANUAL_STEPS §2). Полный
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

# Действия из Telegram (тикет 10.3) — нужен ещё BOT_SERVICE_SECRET,
# совпадающий с ORCH_BOT_SERVICE_SECRET оркестратора:
BOT_TOKEN=123:ABC BOT_WEBHOOK_SECRET=devsecret \
  BOT_ORCHESTRATOR_URL=http://localhost:8081 BOT_SERVICE_SECRET=devbotsecret go run ./bot
# Постановка задачи обычным текстом (пользователь 999 уже привязан, см. выше):
curl -s -XPOST localhost:8080/webhook/devsecret \
  -H 'X-Telegram-Bot-Api-Secret-Token: devsecret' \
  -d '{"update_id":3,"message":{"message_id":3,"from":{"id":999,"first_name":"U"},"chat":{"id":999,"type":"private"},"text":"Собери проект и прогони тесты"}}'
# Отмена: /cancel <id задачи>; ответ на вопрос: /answer <id задачи> <id вопроса> <текст>.
```

Остановка — `Ctrl+C` (SIGINT) или `kill -TERM <pid>`: сервис гасится gracefully.
