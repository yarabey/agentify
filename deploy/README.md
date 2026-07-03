# deploy — локальный запуск стека agentify

Этот каталог содержит всё для локального поднятия системы в Docker Compose
(тикет 0.3): инфраструктуру (Postgres, Redpanda) и наши сервисы (orchestrator,
bot, web за фронтовым Caddy). Прод-деплой через GHCR/SSH — тикет 0.5, описан
ниже в разделе [«Прод-релиз»](#прод-релиз-cd-тикет-05).

## Состав

| Файл | Назначение |
|---|---|
| `docker-compose.yml` | описание сервисов, healthcheck'ов, томов, зависимостей (база для локального запуска и для прода) |
| `docker-compose.prod.yml` | прод-оверрай: `build` → `image` из GHCR + домен-роутинг Caddy (тикет 0.5) |
| `Caddyfile` | фронтовый reverse proxy для локального запуска: `/api/*`→orchestrator, `/bot/*`→bot, остальное→web, без TLS |
| `Caddyfile.prod` | прод-вариант: отдельные домены `app./api./bot.<домен>`, автоматический Let's Encrypt TLS (тикет 0.5) |
| `caddy.Dockerfile` | образ фронтового Caddy с запечённым `Caddyfile` (локальный конфиг; на проде подменяется bind-mount'ом `Caddyfile.prod`) |
| `.env.example` | шаблон секретов/настроек (скопировать в `.env`) |
| `scripts/smoke.sh` | smoke-проверка: `/healthz` всех сервисов отвечает 200 |
| `scripts/e2e-bootstrap.sh` | тикет 9.8: идемпотентный `orchestrator bootstrap` (админ + токен регистрации) перед сквозным E2E (`make e2e`, см. `web/e2e/README.md`) |
| `docker-compose.backup.yml` | опциональный оверрай: сервис регулярного бэкапа Postgres по cron (тикет 11.5) |
| `backup/` | образ бэкап-сервиса (`Dockerfile`+`entrypoint.sh`), скрипт `backup.sh` (pg_dump), `restore-check.sh` (проверка восстановления) и Go-тест `pgbackup/` |
| `setup/` | скрипты первичной установки прода: автоматизируют ручные шаги MANUAL_STEPS.md §2–§3 и подготовку VPS до первого деплоя — см. [`setup/README.md`](setup/README.md) |

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
- Кроме `POSTGRES_PASSWORD` compose требует `JWT_SIGNING_KEY` и
  `APP_ENCRYPTION_KEY` (маппятся в `ORCH_JWT_SIGNING_KEY`/
  `ORCH_APP_ENCRYPTION_KEY` — при поднятой БД оркестратор без них фатально
  падает на старте, см. `orchestrator/main.go`). В `.env.example` уже заданы
  локальные не-секретные dev-значения; для прода — сгенерированные
  (MANUAL_STEPS.md §2).

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

## Бэкап и восстановление Postgres (тикет 11.5, FR I2)

**Зачем.** История задач/событий в agentify хранится **бессрочно** (FR I2,
[`01_tech_stack`](../docs/01_tech_stack_and_architecture.md) §"Бэкап БД"):
авто-удаления нет. Регулярный `pg_dump` — страховка от потери этой истории при
гибели диска/инстанса. Бэкап — инфраструктурная забота деплоя, поэтому вынесен
в отдельный лёгкий сервис рядом с Postgres, а не в сервисный Go-код.

**Состав** (каталог `deploy/backup/`):

| Файл | Назначение |
|---|---|
| `Dockerfile` | образ бэкап-сервиса: `postgres:16-alpine` (совместимые `pg_dump`/`pg_restore` мажора 16) + busybox `crond` |
| `entrypoint.sh` | настраивает crontab из `BACKUP_SCHEDULE`, делает стартовый бэкап, держит `crond` в foreground |
| `backup.sh` | один прогон `pg_dump -Fc` → `<db>_<UTC>.dump` в `BACKUP_DIR` + ретенция по возрасту |
| `restore-check.sh` | host-скрипт: поднять **чистый** инстанс и восстановить последний дамп (приёмка FR I2) |
| `pgbackup/` | Go integration-тест того же сценария на testcontainers (CI-джоба `integration`) |

**Как включить бэкап** (опциональный оверрай поверх базового compose):

```bash
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.backup.yml up -d
```

Сервис `backup` по расписанию (`BACKUP_SCHEDULE`, по умолчанию `0 3 * * *` —
ежедневно 03:00 UTC) снимает `pg_dump` в custom-формате (`-Fc`) в именованный
том `pg_backups` (в проекте — `agentify_pg_backups`). Файлы старше
`BACKUP_RETENTION_DAYS` (по умолчанию 7) удаляются. Секретов бэкап **не
добавляет** — использует те же `POSTGRES_*` из `deploy/.env`, что и Postgres.

Разовый бэкап вручную (Postgres должен быть запущен):

```bash
make backup
```

**Проверка восстановления (FR I2).** Бэкап ценен только если из него
восстанавливаются данные. Воспроизводимая проверка:

```bash
make restore-check          # или: deploy/backup/restore-check.sh
```

Скрипт поднимает **отдельный, заведомо чистый** `postgres:16-alpine`, монтирует
том дампов только для чтения, `pg_restore` последнего `*.dump` и проверяет, что
в схеме `public` появились таблицы и из них читаются строки. Throwaway-контейнер
удаляется всегда (`trap`), состояние хоста не меняется. Источник дампов
настраивается: `BACKUP_VOLUME` (docker-том, по умолчанию `agentify_pg_backups`)
или `BACKUP_HOST_DIR` (каталог на хосте); конкретный файл — аргументом или
`DUMP_FILE`.

Автоматический аналог для CI — `deploy/backup/pgbackup` (Go-тест с тегом
`integration`): поднимает Postgres на testcontainers, наполняет данными, снимает
`pg_dump`, поднимает **второй чистый** Postgres, `pg_restore` и сверяет строки.
Гоняется джобой `integration` (`go test -tags=integration ./...`); обычный
`make test` его не запускает (docker не требуется).

## Прод-релиз (CD, тикет 0.5)

Прод-деплой реализован в [`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml):
собрать образы → запушить в GHCR → по SSH на VPS выполнить `docker compose pull
&& up` (решение зафиксировано в
[`01_tech_stack_and_architecture.md`](../docs/01_tech_stack_and_architecture.md)
§2 [РЕШЕНИЕ 5]). В отличие от `ci.yml`, этот workflow не запускается
автоматически — каждый прод-релиз инициирует человек осознанно.

### Что должно быть настроено заранее (один раз, руками)

Это делает человек, не агент — полный чеклист и точные имена секретов в
[`docs/MANUAL_STEPS.md`](../docs/MANUAL_STEPS.md) §1–§3. Всё из этого списка,
кроме покупки домена/VPS/создания бота, автоматизировано скриптами
[`deploy/setup/`](setup/README.md) — четыре команды на macOS выполняют
генерацию секретов, настройку GitHub, подготовку VPS и первый деплой. Здесь не
дублируем значения, только перечисляем, что именно требуется для этого
workflow:

- VPS поднят, на нём Docker + Docker Compose, есть деплой-пользователь по SSH.
- На VPS **один раз вручную склонирован этот репозиторий в `~/agentify`** под
  тем же деплой-пользователем (`git clone git@github.com:yarabey/agentify.git
  ~/agentify`) — это предусловие для шага синхронизации конфигов (см. ниже).
  Если в `docs/MANUAL_STEPS.md` такого пункта ещё нет — стоит добавить его в
  раздел §1/§2 явно (агент не редактирует этот файл, фиксируем как замечание).
- Рядом, в `~/agentify/deploy/.env`, на VPS заданы переменные для
  `docker-compose.prod.yml`: `POSTGRES_PASSWORD` (как и для локального
  запуска) и домены `APP_DOMAIN`/`API_DOMAIN`/`BOT_DOMAIN`, соответствующие
  DNS A-записям `app./api./bot.<домен>` из `MANUAL_STEPS.md` §1. Без них
  `docker compose ... up` на VPS откажется стартовать (явные `${VAR:?...}` в
  `docker-compose.prod.yml`).
- DNS A-записи `app.`, `api.`, `bot.<домен>` указывают на IP VPS (иначе Caddy
  не сможет пройти ACME HTTP-01 challenge и получить TLS-сертификат).
- В GitHub: окружение `prod` с **required reviewer**, секреты
  `SSH_DEPLOY_KEY`/`SSH_HOST`/`SSH_USER` в этом окружении, включён package
  write для Actions (публикация в GHCR) — всё это `docs/MANUAL_STEPS.md` §3.

### Как запустить деплой

1. Убедиться, что `main` в нужном состоянии (зелёный CI).
2. GitHub → вкладка **Actions** → workflow **Deploy** → **Run workflow** →
   выбрать ветку `main` → запустить. Входных параметров нет: деплоится ровно
   тот коммит (`github.sha`), на котором запущен workflow.
3. Джоба `build-and-push` соберёт и запушит в GHCR 4 образа
   (`ghcr.io/yarabey/agentify-{orchestrator,bot,web,caddy}`), тегированных и
   SHA коммита, и `latest`.
4. Джоба `deploy` стартует только после **подтверждения required reviewer-а**
   окружения `prod` (GitHub покажет запрос на approve во вкладке Actions —
   это и есть защита от случайного релиза). После approve workflow по SSH
   обновляет конфиги на VPS и выполняет `docker compose pull && up -d` с
   только что собранными образами.
5. Проверить результат: `https://api.<домен>/healthz`,
   `https://bot.<домен>/healthz`, `https://app.<домен>/healthz` отвечают 200
   (тот же смысл, что у `make smoke` локально, но через реальные домены и TLS).

### Как обновляются конфиги на VPS

`docker-compose.yml`, `docker-compose.prod.yml`, `Caddyfile.prod` **не
копируются отдельным шагом** (не scp) — деплой обновляет уже склонированный на
VPS репозиторий командами `git fetch` + `git checkout <sha>` перед
`pull`/`up`, тем же SSH-шагом, что катит образы. Это решение, а не scp-копия
файлов перед SSH-командой, потому что:

- проще: один SSH-шаг вместо «скопировать файлы» + «выполнить команды»
  отдельными шагами/экшенами;
- надёжнее: на VPS гарантированно оказывается ровно то дерево конфигов,
  которое соответствует только что собранным образам (тот же коммит) — нет
  риска, что scp скопирует не все нужные файлы или собьётся с версией.

Из этого следует одно предусловие, которое настраивается один раз руками (см.
выше): репозиторий должен быть заранее склонирован на VPS в `~/agentify` под
тем же пользователем, что указан в секрете `SSH_USER`.

### Секреты на проде

GitHub Actions-секреты (`SSH_DEPLOY_KEY` и т.д.) живут только в рамках
GitHub — на VPS не персистятся. `GITHUB_TOKEN`, которым деплой логинится в
GHCR на VPS (`docker login ghcr.io`), используется только в рамках SSH-сессии
конкретного запуска и в конце явно разлогинивается (`docker logout`); в логи
workflow токен не печатается. Секреты рантайма самих сервисов
(`JWT_SIGNING_KEY`, `POSTGRES_PASSWORD` и т.п. — полный список в
`docs/MANUAL_STEPS.md` §2–§3) этот workflow не трогает: они живут в
`~/agentify/deploy/.env` на самом VPS, как и при локальном запуске.
