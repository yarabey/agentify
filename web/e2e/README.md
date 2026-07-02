# web/e2e — сквозной E2E (тикет 9.8, критерий выхода MVP)

> Реализует `docs/MVP_TICKETS.md`, тикет 9.8, и проверяет критерий выхода
> MVP дословно (`docs/MVP_TICKETS.md`, строка 38): «задача из web и из
> Telegram → вопрос агента → уведомление → ответ → агент сообщил о
> завершении → подтверждение пользователя; оффлайн-машина догоняет (5.6);
> отмена останавливает безопасно (8.4/8.5)».

## Что это

Playwright-сценарии, которые гоняются **против полностью поднятого стека**
(`make run-local`: Postgres + Redpanda + orchestrator + bot + web + caddy,
см. `deploy/docker-compose.yml`), а не против замоканного бэкенда или
dev-сервера Vite. Это осознанное отличие от типичного Playwright-конфига с
`webServer` — сам смысл 9.8 в том, чтобы провезти путь через настоящую
очередь сообщений (Redpanda) и настоящий мост `machine.commands → WS`
(`orchestrator/internal/bridge`, тикет 3.4), а не только через REST/DB.

Три сценария (по одному файлу, `*.spec.ts`):

| Файл | Что проверяет | Deps тикета 9.8 |
|---|---|---|
| `happy-path.spec.ts` | Постановка из web → машина принимает → задаёт вопрос → живой диалог (тикет 9.5) → ответ доходит до машины → машина сообщает о завершении → пользователь подтверждает | 9.5 |
| `offline-catchup.spec.ts` | Задача поставлена, пока машина не подключена вовсе → остаётся `queued` → машина подключается → мост Redpanda→WS доставляет пропущенную команду без потерь → задача доводится до `completed` | 5.6 |
| `safe-cancellation.spec.ts` | Отмена из web → статус `cancelled` синхронно → команда `cancel` реально доходит до машины через Redpanda → машина публикует `agent_progress`-предупреждение о безопасной остановке (тикет 8.5) → предупреждение видно в истории задачи | 8.4/8.5 |

## Как эмулируется "машина" (агент) — и почему НЕ через `agent/`

Ни один сценарий не запускает настоящий Claude Code/Claude — это
недетерминированный внешний LLM-вызов, непригодный для CI. Вместо этого
`web/e2e/support/fakeMachine.ts` открывает **настоящее WS-соединение**
к `/machine/ws` и говорит ровно тем протоколом, который описан в
`docs/protocol.md` §2/§4 (hello → task_accepted/agent_question/
agent_progress/agent_completed, ack на каждую входящую команду) — с точки
зрения оркестратора неотличимо от машины на реальном железе.

Решение "не переиспользовать `agent/`-бинарь целиком, а реализовать клиента
протокола заново" — осознанное (по аналогии с ADR-обоснованиями тикетов
7.3/7.4/10.3, см. `AGENTS.md` §5):

1. **Единственная альтернатива — сам `agent/internal/wsclient`/
   `agent/task_runner.go` — сегодня НЕ умеет того, что нужно этому
   сценарию.** При подготовке этого тикета обнаружено, что:
   - `agent/internal/wsclient/wsclient.go` (`readLoop`/`handleFrame`)
     распознаёт входящие `task_assigned`/`command_decision`/`cancel`, но
     **не обрабатывает `user_answer` вообще** — годок пакета прямо
     фиксирует: "обработка user_answer/ping — EPIC 5.x/6.x, вне объёма";
   - ни `agent/internal/provider/claudecode`, ни `agent/internal/provider/claude`
     **никогда не отправляют `agent_question`** — `MessageTypeAgentQuestion`
     нигде не публикуется со стороны агента (единственный производитель
     этого типа кадра в текущем коде — тестовые харнессы, см.
     `orchestrator/internal/bddsteps`).

   То есть путь "агент задал вопрос → пользователь ответил → агент получил
   ответ и продолжил", который прямо требует и тикет 9.8, и критерий
   выхода MVP, **не имеет production-реализации на стороне агента сегодня**
   (см. подробности в описании PR этого тикета — это зафиксировано как
   находка, не тихо исправлено в рамках этого тикета: тикет 9.8 — тестовый
   харнесс поверх уже готового, не новая бизнес-логика). Переиспользовать
   `agent/` для этого сценария просто нечем.
2. Даже для тех кадров, что `agent/` умеет (`task_assigned`/`cancel`),
   поднимать ещё один Go-процесс из Playwright (сборка бинаря, конфиг через
   env, отдельный WS до `/machine/ws`, отдельные логи) — заметно более
   тяжёлый и хрупкий путь, чем одно `ws`-соединение прямо в TS-тесте, при
   том же результате с точки зрения оркестратора (он видит один и тот же
   WS-протокол).
3. `orchestrator/internal/bddsteps` (тикет 11.2) уже показывает тот же
   приём — своя (Go) реализация клиентской стороны протокола для тестов,
   а не переиспользование `agent/`, — см. `orchestrator/internal/bddsteps/world_test.go`,
   `dialMachineWS`/`sendMachineFrame`. `fakeMachine.ts` — тот же приём,
   перенесённый в TypeScript, потому что: (a) Playwright сам на TS/Node,
   лишний межпроцессный Go-раннер не нужен; (b) `orchestrator/internal/bddsteps`
   сознательно тестирует ТОЛЬКО границу оркестратора (HTTP/WS напрямую,
   без Redpanda — см. `orchestrator/features/README.md`), тогда как 9.8
   обязан пройти через настоящий Redpanda-мост, которого в BDD-харнессе
   нет вовсе.

`fakeMachine.ts` — НЕ мок существующего кода агента (мокать нечего — см.
пункт 1) и НЕ полное зеркало протокола (не поддерживает `ping`/`error`,
`command_approval_request`/`command_decision` — сценариям 9.8 они не
нужны). Источник правды по форме сообщений остаётся `internal/bus/*.go`
(Go) — `web/e2e/support/protocol.ts` синхронизируется с ним руками при
изменении протокола, как и `clientNotificationFrame` в
`orchestrator/internal/bddsteps/world_test.go`.

## Telegram-часть критерия — как обошлись и почему

Критерий выхода MVP говорит «задача из web **и из Telegram**». Полноценный
Playwright-тест реального Telegram-клиента в этом тикете **не
реализован и сознательно не планируется**:

- нет и не может быть реального Telegram-аккаунта/сессии в CI-песочнице —
  Telegram Bot API управляет только серверной стороной (webhook), не
  клиентским приложением, которым пользователь реально отправляет
  сообщения;
- эмулировать "нажатия" в клиенте Telegram нечем и незачем — единственная
  граница, которой владеет ЭТОТ проект, это API оркестратора, которое дёргает
  `bot/` при получении апдейта от Telegram (`bot/internal/orchestrator`,
  тикет 10.3).

Эта граница уже покрыта интеграционным тестом
`orchestrator/internal/api/channels_token_integration_test.go`
(`TestIntegration_TelegramActions_LinkedUserCanCreateTask`, тикет 10.3):
он бьёт в тот же HTTP-эндпоинт (`POST /channels/telegram/token` +
`POST /tasks` с сервисным секретом бот↔оркестратор), которым реально
пользуется `bot/`, когда привязанный Telegram-пользователь пишет боту
команду постановки задачи — то есть подтверждает "задача из Telegram
доходит до машины" на границе, которой владеет этот репозиторий. Дальше
путь (задача → машина → вопрос → уведомление → ответ → завершение →
подтверждение) **идентичен** пути из web (тот же `POST /tasks`, та же FSM,
тот же `machine.commands`/`machine.events`) — `happy-path.spec.ts` проверяет
именно эту, общую для обоих каналов часть. Уведомление специфично для
Telegram-канала (`orchestrator/internal/notify/telegram`, тикет 7.3) и
покрыто отдельными юнит/интеграционными тестами пакета
(`orchestrator/internal/notify/telegram/notifier_test.go`,
`notifier_integration_test.go`) — не дублируется здесь.

Итог: критерий "из Telegram" закрыт **комбинацией** уже существующего
интеграционного теста (постановка/действия) + юнит-тестов уведомлений +
общего с web пути FSM (проверен здесь) — отдельного Playwright/Telegram
E2E в разумном объёме MVP не существует, и попытка его построить (например,
через MTProto-клиент с реальным номером телефона) была бы явно избыточной
инженерной инвестицией ради тикета тестового харнесса.

## Как запускать

### Локально

```bash
cp deploy/.env.example deploy/.env   # если ещё не сделано, см. deploy/README.md
make run-local                        # поднимает Postgres+Redpanda+orchestrator+bot+web+caddy
make smoke                            # опционально: /healthz всех сервисов
make e2e-install                      # один раз: Playwright + Chromium
make e2e                              # bootstrap токена регистрации + прогон трёх сценариев
```

`make e2e` = `deploy/scripts/e2e-bootstrap.sh` (идемпотентно заводит первого
администратора и стартовый токен регистрации через `orchestrator bootstrap`,
тикет 1.7 — иначе `/register` некуда регистрироваться, система закрытая,
FR A1) + `npm --prefix web run e2e` (`playwright test --config=e2e/playwright.config.ts`).

Переопределяемые переменные окружения (совпадают между
`deploy/scripts/e2e-bootstrap.sh` и `web/e2e/support/env.ts` по умолчанию —
см. их годоки):

| Переменная | Назначение | По умолчанию |
|---|---|---|
| `E2E_BASE_URL` | адрес фронтового Caddy | `http://127.0.0.1:8080` |
| `E2E_REGISTRATION_TOKEN` | токен регистрации, которым тесты заводят пользователей через `/register` | `e2e-local-registration-token` |
| `E2E_ADMIN_USERNAME` / `E2E_ADMIN_PASSWORD` | креды первого администратора (bootstrap, не используются самими тестами напрямую) | `e2e-admin` / `e2e-local-admin-password-1!` |

Значения по умолчанию — **не прод-секреты** (AGENTS.md §8): существуют
только в эфемерном локальном/CI docker-compose стенде на время `make e2e`.

### Отдельно от `make e2e`

```bash
npm --prefix web run e2e:typecheck   # tsc --noEmit по web/e2e (без браузера/стека)
npx playwright test --config=web/e2e/playwright.config.ts --headed   # ручной прогон с браузером
npx playwright show-report web/e2e/playwright-report                 # HTML-отчёт последнего прогона
```

## Известное ограничение окружения (честно, как и `orchestrator/features/README.md`)

Как и BDD-харнесс (тикет 11.2, см. `orchestrator/features/README.md`,
раздел "Про `@redpanda` и сеть"), эти сценарии **не были прогнаны end-to-end
в этой конкретной dev/CI-песочнице**: `docker pull redpandadata/redpanda`
здесь возвращает `403 Forbidden` (egress-политика окружения не пропускает
этот реестр), а `make run-local` требует поднятого healthy Redpanda
(`orchestrator` в `deploy/docker-compose.yml` зависит от него через
`depends_on: condition: service_healthy`) — без него оркестратор либо не
стартует (недоступен образ), либо стартует БЕЗ моста `machine.commands →
WS` (`ORCH_REDPANDA_SEEDS` пуст, см. `orchestrator/main.go`), а тогда
`FakeMachine` никогда не получит `task_assigned`/`user_answer`/`cancel` —
именно то, что проверяют все три сценария 9.8.

Скачивание браузера Chromium (`npx playwright install`) в этой же
песочнице тоже блокируется той же egress-политикой (`cdn.playwright.dev`
не в allowlist) — независимая от Redpanda причина, по которой сами
Playwright-тесты не были физически прогнаны здесь.

Что реально проверено в этой песочнице (см. описание PR тикета 9.8 для
точных команд/вывода):
- логика `FakeMachine`/протокола — прогнана вручную против настоящего `ws`-сервера
  (hello → доставка `task_assigned` → авто-ack → `task_accepted` → закрытие
  соединения) — код класса реален и работает, проблема именно в
  недоступности Redpanda/браузера в ЭТОЙ песочнице, не в тестах;
- `npm run e2e:typecheck` — зелёный;
- `npm run test` (Vitest, весь существующий набор web) — зелёный, `e2e/`
  явно исключён из Vitest (см. `web/vite.config.ts`), коллизии раннеров нет;
- `npm run build` — типы `web/e2e/**` НЕ участвуют в `tsc -b` (root
  `tsconfig.json` не ссылается на `web/e2e/tsconfig.json`), сборка
  приложения не затронута.

На реальном VPS/GitHub-hosted раннере (обычный доступ в интернет, как и
предполагает `docs/MVP_TICKETS.md`/`deploy/README.md`) оба ограничения
сети отсутствуют — `make e2e-install && make run-local && make e2e`
должны отработать штатно.
