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

**`POST /channels/telegram/link`** — обмен одноразового кода привязки на
привязку Telegram-аккаунта (тикет 10.2, FR D3 «привязка канала к аккаунту
описана явно», Gherkin §6 «Уведомления», см. годок
[`internal/api/channels.go`](internal/api/channels.go)).
Вызывается ботом при обработке `/start <code>` (см. `bot/README.md`).
Маршрут **без Bearer** (`security: []`) — в этой точке пользователь ещё не
аутентифицирован как web-клиент, единственное доказательство права на
привязку — сам одноразовый код (та же модель доверия, что у
`registration_token` в `POST /auth/register`). Тело `ChannelLinkRequest`
(`code`, `telegram_user_id`); вся проверка (не найден/истёк/уже использован) и
атомарная запись `channel_links` — в
[`internal/channel.Linker.Exchange`](internal/channel/link.go) (одна
транзакция: код помечается использованным ТОЛЬКО при успешной привязке).
Код привязки генерирует **`POST /channels/telegram/link-code`** (тикет 9.6, FR
A2, D3, экран «Настройки» в web) — таблицы `channel_link_codes`/`channel_links`
— миграция `00004_channels.sql`.
- успех → `200` + `ChannelLink`;
- код не найден → `404 link_code_not_found`;
- код истёк → `409 link_code_expired`;
- код уже использован → `409 link_code_used`;
- этот `telegram_user_id` уже привязан к другому пользователю →
  `409 channel_already_linked`.

**`POST /channels/telegram/link-code`** — генерация одноразового кода
привязки (тикет 9.6, FR A2, D3). В отличие от `/channels/telegram/link`
маршрут **защищён Bearer** (нет `security: []` в `api/openapi.yaml`) — код
выпускается СТРОГО для вызывающего пользователя (`user_id` из access-токена),
без параметра «для кого» в теле запроса. Бизнес-логика (генерация
случайного кода, вставка с TTL) — в
[`internal/channel.CodeIssuer.IssueLinkCode`](internal/channel/codegen.go);
HTTP-обёртка — `PostChannelsTelegramLinkCode` в тех же
[`internal/api/channels.go`](internal/api/channels.go). Успех → `201` +
`{code, expires_at}`. TTL — `ORCH_TELEGRAM_LINK_CODE_TTL` (дефолт `15m`,
продуктовое значение не зафиксировано, см.
[`docs/MANUAL_STEPS.md`](../docs/MANUAL_STEPS.md) §4). Код показывается
пользователю на экране «Настройки» (`web/src/pages/SettingsPage.tsx`) вместе
с инструкцией отправить его боту командой `/start <code>`.

**`POST /channels/telegram/token`** — действия из Telegram от имени
привязанного пользователя (тикет 10.3, FR D1, §4 «Постановка задачи из
канала», см. годок [`internal/api/channels.go`](internal/api/channels.go)).
Вызывается ботом ПЕРЕД каждым действием пользователя в Telegram (постановка
задачи, отмена, ответ на вопрос — см. `bot/README.md`), чтобы получить право
действовать от его имени. **Архитектурное решение** (аутентификация бота):
у бота нет пользовательского access-JWT (только `telegram_user_id` из
апдейта), поэтому этот эндпоинт **не** защищён Bearer — вместо этого
аутентифицируется общим **сервисным секретом** между ботом и оркестратором
(заголовок `X-Bot-Service-Secret`, сверяется constant-time с
`ORCH_BOT_SERVICE_SECRET`), доверенным ровно тем же способом, что и
`BOT_ORCHESTRATOR_URL` (тикет 10.2) — прямой вызов внутри compose-сети, минуя
Caddy/публичный интернет. При валидном секрете `telegram_user_id` резолвится
в `user_id` через `channel_links` (тот же механизм привязки, что и `/start
<code>`, тикет 10.2) и выпускается **обычный** access-JWT (`auth.IssueAccessToken`
— та же функция и TTL, что и `POST /auth/login`). Дальше бот действует как
ОБЫЧНЫЙ клиент контракта — `POST /tasks`, `POST /tasks/{id}/cancel`,
`POST /tasks/{id}/answer`, `GET /integrations` с этим токеном как Bearer —
принцип «единый API»: `tasks.go`/`integrations.go` этим тикетом НЕ меняются
вообще, никакой Telegram-специфичной бизнес-логики постановки задачи в
оркестраторе нет.
- тело `TelegramActingTokenRequest` (`telegram_user_id`);
- пустой/неверный `X-Bot-Service-Secret`, ЛИБО `ORCH_BOT_SERVICE_SECRET` не
  настроен на сервере (fail closed — эндпоинт тогда ВСЕГДА отвечает `401`,
  без исключений для пустого presented-заголовка) → `401`;
- `telegram_user_id` не привязан ни к одному аккаунту → `404 not_linked`;
- успех → `200` + `TelegramActingToken` (`access_token`, `expires_at`).

Маршруты монтируются от корня (`/auth/register`, `/healthz`): Caddy в compose
роутит `/api/*` → orchestrator со стрипом префикса.

Если `ORCH_DATABASE_URL` не задан (каркасные прогоны без БД), API не
поднимается — остаётся только `/healthz` из `platform`.

### Уведомления: web (тикет 7.2) и Telegram (тикет 7.3)

Доменное событие уведомления (`orchestrator/internal/notify.Notification`,
тикет 7.1) формируется при вопросе агента и важных сменах статуса задачи
(`agent_question`/`command_approval_request`/`agent_completed`,
`handleAgentQuestion` и соседние обработчики в
[`internal/api/machine_ws.go`](internal/api/machine_ws.go)) и рассылается по
всем активным каналам (FR G1, Gherkin §6 «Уведомления»):

- **web** — `ClientConnHub` (тикет 7.2, [`internal/api/client_ws.go`](internal/api/client_ws.go)):
  рассылает уведомление всем открытым `/ws`-соединениям браузера того же
  пользователя; всегда доступен, от Redpanda не зависит.
- **Telegram** — `telegram.Notifier` (тикет 7.3,
  [`internal/notify/telegram`](internal/notify/telegram)): резолвит активную
  привязку канала пользователя (`channel_links`, тикет 10.2) и, если она есть,
  публикует в топик `notifications.telegram` (ADR 0001) УЖЕ готовый текст и
  `telegram_chat_id` — бот (`bot/notify.go`, тикет 10.4) лишь пересылает его в
  Bot API, не имея доступа к БД оркестратора (принцип «единый API»). Требует
  `ORCH_REDPANDA_SEEDS`; без него регистрируется только web-канал.

`orchestrator/main.go` регистрирует один и тот же `Notifier` дважды: сразу
после `NewServer` — только `ClientConnHub` (баз., без Redpanda), и, если
`ORCH_REDPANDA_SEEDS` задан, — `multiNotifier{ClientConnHub, telegram.Notifier}`
(`orchestrator/notify_fanout.go`) поверх обоих.

### Маршрутизация между каналами (тикет 7.4, FR G2)

FR G2 требует, чтобы при одновременно активных нескольких каналах была
определена политика — "куда доставлять, дублировать ли". Принятое решение:
**дублировать** — `multiNotifier` безусловно вызывает `Notify` КАЖДОГО
зарегистрированного канала (ошибка одного канала логируется и не блокирует
остальные), не выбирая между ними. Политика вынесена именованной функцией
`routingPolicy`/`duplicateToAllChannels` (`orchestrator/notify_fanout.go`) —
это осознанное, а не временное решение; полное обоснование — в годоке этого
файла. Коротко:

- FR G1 описывает MVP-доставку как "в web ... **и** в Telegram" — базовый
  сценарий продукта уже подразумевает дублирование, а не выбор одного канала.
- Оба канала УЖЕ инкапсулируют свой критерий активности и молча не
  доставляют, если он не выполнен: `ClientConnHub.Notify` — нет открытого
  WS-соединения; `telegram.Notifier.Notify` — Telegram не привязан
  (`channel_links`). Поэтому безусловная рассылка на уровне `multiNotifier`
  автоматически сводится к "доставить в оба активных, доставить в
  единственный активный, не доставить, если ни одного" — ровно то, что
  требует G2, — без дублирования знания об "активности" канала в
  `multiNotifier`.
- Дублирование безопаснее эксклюзивного выбора: пользователь может пропустить
  web-уведомление (вкладка в фоне) или Telegram (уведомления выключены на
  телефоне) — цель G1/Gherkin §6 "не пропустить и не потерять задачу"
  достигается избыточностью. Эксклюзивный выбор канала добавил бы риск
  молчаливой потери уведомления без компенсирующей пользы для MVP.

Если в будущем понадобится более сложная маршрутизация (приоритеты,
пользовательские настройки "куда слать" — FR G3, вне MVP), её добавляют новой
`routingPolicy`-функцией, не трогая остальной `multiNotifier`.

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
| `ORCH_JWT_SIGNING_KEY` | — (пусто) | Секрет HMAC для подписи access-JWT (FR A3). При поднятом API обязателен — пустой ключ фатален на старте. |
| `ORCH_APP_ENCRYPTION_KEY` | — (пусто) | Мастер-ключ шифрования at-rest, base64 → ровно 32 байта (`openssl rand -base64 32`). При поднятом API обязателен и валидируется по длине. См. раздел «Шифрование at-rest» ниже. |
| `ORCH_TELEGRAM_LINK_CODE_TTL` | `15m` | Срок действия кода привязки Telegram (`POST /channels/telegram/link-code`, тикет 9.6, FR A2/D3). Продуктовое значение не зафиксировано, см. `docs/MANUAL_STEPS.md` §4. |
| `ORCH_BOT_SERVICE_SECRET` | — (пусто) | **Секрет.** Общий сервисный секрет между ботом и оркестратором (тикет 10.3, FR D1) для `POST /channels/telegram/token` (заголовок `X-Bot-Service-Secret`). Совпадает со значением `BOT_SERVICE_SECRET` у бота. Пусто (дефолт) → эндпоинт ВСЕГДА отвечает `401` (действия из Telegram отключены) — не фатально для старта, в отличие от `ORCH_JWT_SIGNING_KEY`/`ORCH_APP_ENCRYPTION_KEY`. НЕ коммитится. |
| `ORCH_REDPANDA_SEEDS` | — (пусто) | Адреса брокеров Redpanda через запятую (тикет 3.2+). Гейтит мост `machine.commands` → WS (3.4), presence/heartbeat (3.6) и Telegram-канал доставки уведомлений (тикет 7.3, см. «Уведомления» выше). Пусто → эти подсистемы не поднимаются, оркестратор продолжает обслуживать REST+WS-handshake и web-уведомления. |

Специфичные для оркестратора поля (Redpanda и др.) добавляются под тем же
префиксом `ORCH_` в соответствующих тикетах.

## Шифрование чувствительных данных at-rest (тикет 11.1, FR I1)

Чувствительные значения не лежат в БД в открытом виде — они шифруются
**AES-256-GCM** (AEAD) единым крипто-модулем [`internal/crypto`](../internal/crypto):

| Колонка | Что хранит | Кто пишет / кто читает |
|---|---|---|
| `integrations.uuid_enc` | UUID-секрет машины (для показа владельцу, FR B2) | `internal/api` (тикеты 2.2/2.3) |
| `tasks.text_enc` | текст задачи | шифрует и расшифровывает `internal/api` (`tasks.go`) |
| `task_events.payload_enc` | содержимое события (вопрос/ответ/смена статуса/…) | шифрует единственный писатель `internal/task.Transitioner`; расшифровывает `internal/api` при показе истории/сопоставлении |

Единственный источник ключа — `ORCH_APP_ENCRYPTION_KEY` (32 байта после
base64-декодирования). Мастер-ключ **никогда не используется в примитивах
напрямую**: из него через `crypto.DeriveKey(masterKey, purpose)`
(HKDF-подобный HMAC-SHA256) выводятся независимые подключи под каждое
применение (UUID-секрет, `text_enc`, `payload_enc`) — reuse одного ключа в
разных примитивах/колонках недопустим. `payload_enc` пишется в пакете `task`, а
читается в пакете `api`, поэтому оба выводят подключ под ОБЩИМ purpose
(`task.EventPayloadKeyPurpose`) из одного и того же мастер-ключа — иначе
GCM-тег не сойдётся; в проде это обеспечивает `main.go`, передавая один
`ORCH_APP_ENCRYPTION_KEY` и в `api.NewServer`, и в
`task.NewTransitioner(..., task.WithMasterKey(key))`.

Формат хранения — `nonce || ciphertext+tag` (стандарт для GCM), уникальный
nonce (`crypto/rand`) на каждое шифрование. **Внешнее поведение сохранено:** API
по-прежнему отдаёт открытый текст задачи и разобранный payload события —
шифрование прозрачно на границе БД. Смена мастер-ключа после первого запуска с
данными сделает ранее зашифрованные значения нечитаемыми (см. предупреждение в
[`docs/MANUAL_STEPS.md`](../docs/MANUAL_STEPS.md)). Обоснование выбора примитива
и вывода подключей — в [`internal/crypto/README.md`](../internal/crypto/README.md).

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

# Генерация кода привязки Telegram (тикет 9.6) — требует Bearer (см. POST
# /auth/login выше):
curl -s -X POST localhost:8080/channels/telegram/link-code \
  -H "Authorization: Bearer <access_token>"
# 201 — {"code":"...", "expires_at":"..."}

# Обмен кода на привязку (тикет 10.2) — без Bearer, код из ответа выше:
curl -s -X POST localhost:8080/channels/telegram/link \
  -H 'Content-Type: application/json' \
  -d '{"code":"<code из предыдущего ответа>","telegram_user_id":"999"}'
# 200 — привязано; 404 — код не найден; 409 — истёк/использован/telegram уже привязан.

# Действия из Telegram (тикет 10.3) — сначала acting-токен по сервисному
# секрету (ORCH_BOT_SERVICE_SECRET), затем ОБЫЧНЫЙ вызов защищённой операции
# контракта этим токеном как Bearer:
curl -s -X POST localhost:8080/channels/telegram/token \
  -H 'Content-Type: application/json' \
  -H 'X-Bot-Service-Secret: <ORCH_BOT_SERVICE_SECRET>' \
  -d '{"telegram_user_id":"999"}'
# 200 — {"access_token":"...", "expires_at":"..."}; 401 — неверный/не настроен
# секрет; 404 not_linked — telegram_user_id не привязан (см. /start <code> выше).
curl -s -X POST localhost:8080/tasks \
  -H "Authorization: Bearer <access_token из предыдущего ответа>" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: tg-999-1' \
  -d '{"integration_id":"<id интеграции>","text":"Собери проект"}'
# 201 — задача поставлена; тот же POST /tasks, что и у web (никакой
# отдельной Telegram-логики).
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

## BDD-приёмка Gherkin-сценариев (тикет 11.2)

Приёмочные сценарии `docs/User_stories_Gherkin.md` §1–§10 (ТЗ §127 «Весь
функционал покрыт автоматическими тестами») исполняются как `.feature`-файлы
через [godog](https://github.com/cucumber/godog):

- `.feature`-файлы (Gherkin на русском) — [`orchestrator/features/`](features);
- степы (Go, поверх настоящего `api.NewRouter` + Postgres в testcontainers) —
  [`orchestrator/internal/bddsteps/`](internal/bddsteps) (build-тег `bdd`, как
  `integration` у `*_integration_test.go` — обычный `go build`/`go test` их не
  видит, Docker не требуется);
- запуск: `make bdd` из корня репозитория.

Что покрыто, что сознательно нет (install-скрипт/Redpanda-транспорт/allowlist
агента/ещё не реализованный safe-stop) и как добавлять новые сценарии —
[`orchestrator/features/README.md`](features/README.md).
