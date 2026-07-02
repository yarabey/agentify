# MANUAL_STEPS — что делаешь руками

> Всё, что нельзя или не нужно отдавать кодовому агенту: деньги, аккаунты, секреты, продуктовые решения. Остальное (репо-структуру, CI/CD, миграции, инфра-конфиги) агент делает сам в тикетах EPIC 0.

## 1. Аккаунты и деньги
- [ ] Создать **приватный GitHub-репозиторий** (можно пустой — структуру создаст агент).
- [ ] Купить **домен** + завести 3 DNS A-записи на IP сервера: `app.`, `api.`, `bot.`
- [ ] Арендовать **VPS Ubuntu 22.04/24.04** (2–4 vCPU, 4–8 ГБ RAM): поставить Docker + docker compose, создать деплой-пользователя, firewall открыть только 22/80/443.
- [ ] **Telegram-бот** через @BotFather → получить **bot token**.
- [ ] (Для отладки провайдера «Claude») аккаунт **Anthropic** → API-ключ. В проде ключ приносит каждый пользователь сам.

## 2. Ключи и SSH (сгенерировать локально)
```bash
ssh-keygen -t ed25519 -f deploy_key      # приватный → в GitHub Secrets, публичный → на VPS
openssl rand -base64 48   # JWT_SIGNING_KEY
openssl rand -base64 32   # APP_ENCRYPTION_KEY
openssl rand -base64 24   # POSTGRES_PASSWORD
openssl rand -hex 16      # INITIAL_REGISTRATION_TOKEN
openssl rand -hex 16      # BOT_WEBHOOK_SECRET (секрет пути webhook бота, тикет 10.1)
openssl rand -base64 18   # BOOTSTRAP_ADMIN_PASSWORD (пароль первого администратора)
```
`BOOTSTRAP_ADMIN_USERNAME` — не секрет, просто логин первого администратора (например, `admin`), придумать самому.

> **`APP_ENCRYPTION_KEY`** (в окружении оркестратора — с префиксом `ORCH_APP_ENCRYPTION_KEY`) — мастер-ключ шифрования at-rest: ровно 32 байта после base64-декодирования (то есть `openssl rand -base64 32`). Из него оркестратор выводит подключи (AES-256-GCM) для шифрования чувствительных колонок БД: `integrations.uuid_enc` (UUID-секрет машины, FR B2), `tasks.text_enc` (текст задачи) и `task_events.payload_enc` (содержимое событий) — последние два закрывает тикет 11.1 (FR I1). Генерируй один раз и **не меняй** после первого запуска с данными: смена ключа сделает уже зашифрованные значения нечитаемыми. Никогда не коммить сам ключ — только имя переменной.

## 3. Положить в GitHub → Settings → Secrets (Actions, окружение `prod`)
`SSH_DEPLOY_KEY`, `SSH_HOST`, `SSH_USER`, `JWT_SIGNING_KEY`, `APP_ENCRYPTION_KEY`, `POSTGRES_PASSWORD`, `TELEGRAM_BOT_TOKEN` (→ `BOT_TOKEN`), `BOT_WEBHOOK_SECRET`, `INITIAL_REGISTRATION_TOKEN`, `BOOTSTRAP_ADMIN_USERNAME`, `BOOTSTRAP_ADMIN_PASSWORD`.
- [ ] Создать окружение **`prod`** с **required reviewer = ты** (защита деплоя).
- [ ] Включить **GHCR** (Actions: `packages: write`) и **Releases** (`contents: write`).
- [ ] Branch protection на `main`: PR + зелёный CI + ≥1 ревью.

> ⚠️ Секреты генерируй сам, никогда не коммить в репозиторий.

## 4. Продуктовые решения (зафиксировать до спринта 3)
Без них упрутся EPIC 6 и 8. Достаточно записать ответы в `docs/`:
- [ ] **Дефолтный allowlist команд** Claude Code (что без согласования, что всегда спрашивает). Можно начать с пустого.
- [ ] **Таймауты:** через сколько машина «зависла»; через сколько без ответа — напоминание; что дальше (ждать / авто-отмена).
- [ ] **«Критическая операция»** при отмене — какие операции агент доводит до конца ради сохранности данных.

## 5. Разово в процессе
- [ ] Webhook бота регистрируется **автоматически** при старте, если задан `BOT_PUBLIC_URL` (тикет 10.1): бот сам вызывает `SetWebhook` на адрес `https://bot.<домен>/webhook/<BOT_WEBHOOK_SECRET>`. Руками ничего выставлять не нужно — достаточно положить `BOT_TOKEN`, `BOT_WEBHOOK_SECRET`, `BOT_PUBLIC_URL` в окружение (см. `bot/README.md`).
- [ ] Иметь **чистую тестовую macOS/Ubuntu** для проверки установки агента «одной командой» (EPIC 4).

Всё. Дальше — запускаешь агента промптом из ответа.
