#!/usr/bin/env bash
# deploy/setup/macos-03-server.sh — шаг 3: подготовка VPS по SSH (macOS).
#
# Назначение (бизнес): доводит VPS до предусловий деплой-workflow
# (deploy/README.md §«Прод-релиз»): Docker + Compose + firewall +
# деплой-пользователь (через server-setup.sh), доступ VPS к приватному
# репозиторию (deploy key из шага 02), клон репозитория в ~/agentify и
# заполненный ~/agentify/deploy/.env со всеми секретами рантайма.
#
# Как устроено (тех): всё выполняется С ВАШЕГО macOS по SSH, на сервере руками
# ничего делать не нужно. Два подключения: (1) PROVISION_SSH (root/sudo) —
# только для server-setup.sh; (2) деплой-пользователь по ключу из шага 01 —
# всё остальное (ровно тот же путь доступа, каким потом ходит GitHub Actions).
# Идемпотентен: повторный запуск ничего не ломает; существующий deploy/.env с
# другим содержимым бэкапится рядом перед перезаписью.
#
# Где выполнять: на вашем macOS, после macos-02-github.sh.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/setup/lib.sh
. "$SCRIPT_DIR/lib.sh"

require_state
require_cmd ssh "входит в macOS по умолчанию"

# --- Проверка DNS (MANUAL_STEPS.md §1): без неё Caddy не получит TLS ---------------
log "проверяю DNS A-записи app./api./bot.$DOMAIN → $SERVER_IP"
DNS_OK=1
for sub in app api bot; do
	resolved="$(dig +short A "$sub.$DOMAIN" 2>/dev/null | tail -n1)"
	if [ "$resolved" = "$SERVER_IP" ]; then
		log "  $sub.$DOMAIN → $resolved — ок"
	else
		DNS_OK=0
		warn "  $sub.$DOMAIN → '${resolved:-нет записи}' (ожидался $SERVER_IP)"
	fi
done
if [ "$DNS_OK" -eq 0 ]; then
	warn "DNS ещё не указывает на VPS — деплой пройдёт, но Caddy не сможет получить TLS-сертификаты Let's Encrypt, пока записи не обновятся"
fi

# --- 1) Базовая настройка сервера от root/sudo (server-setup.sh) -------------------
log "выполняю server-setup.sh на VPS через $PROVISION_SSH (может спросить пароль sudo)"
DEPLOY_PUBKEY="$(cat "$DEPLOY_KEY.pub")"
ssh -o StrictHostKeyChecking=accept-new "$PROVISION_SSH" \
	"sudo env DEPLOY_USER='$DEPLOY_USER' DEPLOY_PUBKEY='$DEPLOY_PUBKEY' bash -s" \
	<"$SCRIPT_DIR/server-setup.sh"

log "проверяю вход деплой-пользователем и docker без sudo"
ssh_deploy "docker compose version >/dev/null && echo ok" | grep -qx ok ||
	die "деплой-пользователь $DEPLOY_USER не может выполнять docker compose"

# --- 2) Доступ VPS к приватному репозиторию (deploy key из шага 02) ----------------
log "настраиваю SSH-ключ чтения GitHub на VPS"
ssh_deploy "umask 077; mkdir -p ~/.ssh; cat > ~/.ssh/agentify_github; chmod 600 ~/.ssh/agentify_github" <"$GITHUB_READ_KEY"
# Отдельный Host-блок: ключ используется только для github.com и не мешает
# другим ключам пользователя.
ssh_deploy "grep -q 'IdentityFile ~/.ssh/agentify_github' ~/.ssh/config 2>/dev/null || printf '%s\n' 'Host github.com' '  IdentityFile ~/.ssh/agentify_github' '  IdentitiesOnly yes' >> ~/.ssh/config; chmod 600 ~/.ssh/config"
ssh_deploy "grep -q github.com ~/.ssh/known_hosts 2>/dev/null || ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null"

# --- 3) Клон репозитория в ~/agentify (предусловие deploy-workflow) ----------------
log "клонирую/обновляю репозиторий в ~/agentify на VPS"
ssh_deploy "if [ -d ~/agentify/.git ]; then git -C ~/agentify fetch --quiet origin; else git clone --quiet git@github.com:$GITHUB_REPO.git ~/agentify; fi"

# --- 4) deploy/.env на VPS: секреты рантайма (deploy/README.md §«Секреты на проде») -
# Имена host-переменных = имена GitHub Secrets (MANUAL_STEPS.md §3); маппинг в
# ORCH_*/BOT_* делает deploy/docker-compose.yml. BOT_PUBLIC_URL включает
# авто-регистрацию Telegram-webhook на старте бота (MANUAL_STEPS.md §5).
ENV_CONTENT="# Сгенерировано deploy/setup/macos-03-server.sh — секреты рантайма прода.
# НЕ коммитить. Пересоздаётся повторным запуском шага 03 (старый файл бэкапится).

# --- Postgres ---
POSTGRES_USER=agentify
POSTGRES_PASSWORD=$POSTGRES_PASSWORD
POSTGRES_DB=agentify

# --- Оркестратор ---
ORCH_ENV=prod
ORCH_LOG_LEVEL=info
ORCH_LOG_FORMAT=json
JWT_SIGNING_KEY=$JWT_SIGNING_KEY
APP_ENCRYPTION_KEY=$APP_ENCRYPTION_KEY

# --- Telegram-бот ---
BOT_ENV=prod
BOT_LOG_LEVEL=info
BOT_LOG_FORMAT=json
BOT_TOKEN=$TELEGRAM_BOT_TOKEN
BOT_WEBHOOK_SECRET=$BOT_WEBHOOK_SECRET
BOT_PUBLIC_URL=https://bot.$DOMAIN
BOT_SERVICE_SECRET=$BOT_SERVICE_SECRET

# --- Домены (docker-compose.prod.yml / Caddyfile.prod) ---
APP_DOMAIN=app.$DOMAIN
API_DOMAIN=api.$DOMAIN
BOT_DOMAIN=bot.$DOMAIN

# --- Бэкап Postgres (опциональный оверрай docker-compose.backup.yml) ---
BACKUP_SCHEDULE=0 3 * * *
BACKUP_RETENTION_DAYS=7
"
log "записываю ~/agentify/deploy/.env на VPS"
printf '%s' "$ENV_CONTENT" | ssh_deploy "umask 077; cat > ~/agentify/deploy/.env.new
	if [ -f ~/agentify/deploy/.env ] && ! cmp -s ~/agentify/deploy/.env ~/agentify/deploy/.env.new; then
		cp ~/agentify/deploy/.env ~/agentify/deploy/.env.bak.\$(date +%Y%m%d%H%M%S)
		echo 'прежний deploy/.env отличался — сохранён рядом как .env.bak.<время>'
	fi
	mv ~/agentify/deploy/.env.new ~/agentify/deploy/.env"

log "готово: VPS соответствует предусловиям деплоя. Следующий шаг: deploy/setup/macos-04-deploy.sh"
