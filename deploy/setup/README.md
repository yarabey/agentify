# deploy/setup — первый деплой набором скриптов

Автоматизация оставшихся ручных шагов [`docs/MANUAL_STEPS.md`](../../docs/MANUAL_STEPS.md)
(§2, §3, серверная часть §1 и первый прогон §5) после того, как выполнен §1
«Аккаунты и деньги»: куплен домен с A-записями `app./api./bot.<домен>` на IP
VPS, арендован VPS Ubuntu 22.04/24.04, создан Telegram-бот.

## Что и где запускать

Все четыре нумерованных скрипта выполняются **на вашем macOS** по порядку —
на сервер руками заходить не нужно:

| # | Скрипт | Где | Что делает |
|---|---|---|---|
| 1 | `macos-01-secrets.sh` | macOS | Спрашивает вводные (домен, IP VPS, токен бота), генерирует все секреты из MANUAL_STEPS.md §2 и два SSH-ключа. Состояние — в `~/.agentify-deploy` (вне репозитория). |
| 2 | `macos-02-github.sh` | macOS | MANUAL_STEPS.md §3: ставит `gh` (через Homebrew) при отсутствии, создаёт окружение `prod` c required reviewer = вы, кладёт все секреты, регистрирует read-only deploy key репозитория для VPS, включает write-права workflow (GHCR/Releases), настраивает branch protection на `main`. |
| 3 | `macos-03-server.sh` | macOS (работает по SSH) | Прогоняет `server-setup.sh` на VPS (Docker+Compose, ufw 22/80/443, деплой-пользователь), настраивает доступ VPS к приватному репо, клонирует его в `~/agentify` и пишет `~/agentify/deploy/.env` со всеми секретами рантайма — предусловия [`deploy.yml`](../../.github/workflows/deploy.yml). |
| 4 | `macos-04-deploy.sh` | macOS | Запускает workflow **Deploy**, сам подтверждает окружение `prod`, ждёт раскатки, проверяет `https://{api,bot,app}.<домен>/healthz` и идемпотентно выполняет `orchestrator bootstrap` (первый админ + токен регистрации). |
| — | `server-setup.sh` | Ubuntu VPS (root) | Вызывается шагом 3 автоматически; можно запустить и вручную — см. шапку файла. |
| — | `lib.sh` | — | Общие помощники; отдельно не запускается. |

```bash
deploy/setup/macos-01-secrets.sh
deploy/setup/macos-02-github.sh
deploy/setup/macos-03-server.sh
deploy/setup/macos-04-deploy.sh
```

## Свойства

- **Идемпотентность.** Любой скрипт можно запускать повторно: секреты не
  перегенерируются (критично для `APP_ENCRYPTION_KEY`, см. MANUAL_STEPS.md §2),
  установленное не переустанавливается, ключи не задваиваются; существующий
  `deploy/.env` на VPS с другим содержимым бэкапится рядом перед перезаписью.
- **Учёт уже установленного.** Homebrew/`gh` на macOS и Docker/Compose/ufw на
  VPS ставятся только при отсутствии.
- **Секреты вне git.** Всё состояние — в `~/.agentify-deploy` (права 600/700):
  `config.env` (вводные), `secrets.env` (секреты), `ssh_deploy_key` (GitHub
  Actions → VPS), `github_read_key` (VPS → GitHub). Каталог переопределяется
  переменной `AGENTIFY_SETUP_DIR`.
- **Неинтерактивный режим.** Вводные шага 1 можно передать переменными:
  `DOMAIN=example.com SERVER_IP=1.2.3.4 TELEGRAM_BOT_TOKEN=... deploy/setup/macos-01-secrets.sh`.
- **Соло-разработка.** Branch protection по умолчанию требует 1 ревью
  (MANUAL_STEPS.md §3), но одобрить собственный PR GitHub не даёт — для работы
  в одиночку перезапустите шаг 2 с `REQUIRED_REVIEWS=0`.

## Что остаётся руками

Скриптами не покрывается (см. `docs/MANUAL_STEPS.md`):

- §1 «Аккаунты и деньги» — домен, DNS, VPS, бот, аккаунт Anthropic;
- §4 продуктовые решения (allowlist, таймауты и т.п.) — записать в `docs/`;
- §6 внешний uptime-мониторинг (`https://api.<домен>/healthz`, например
  UptimeRobot) — нужен аккаунт во внешнем сервисе;
- §5 «чистая тестовая macOS/Ubuntu» для проверки установки агента (EPIC 4).
