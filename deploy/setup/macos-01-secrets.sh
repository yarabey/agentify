#!/usr/bin/env bash
# deploy/setup/macos-01-secrets.sh — шаг 1: конфигурация и секреты (macOS).
#
# Назначение (бизнес): автоматизирует docs/MANUAL_STEPS.md §2 («Ключи и SSH») —
# один раз собирает вводные (домен, IP VPS, токен бота) и генерирует ВСЕ
# секреты первого деплоя. Секреты хранятся вне репозитория (~/.agentify-deploy)
# и никогда не коммитятся (AGENTS.md §8).
#
# Как устроено (тех): идемпотентен — уже заданные значения конфига и уже
# сгенерированные секреты НЕ перезаписываются при повторном запуске (это
# критично для APP_ENCRYPTION_KEY: его смена после первого запуска с данными
# делает зашифрованные колонки нечитаемыми, см. MANUAL_STEPS.md §2).
# Неинтерактивный режим: любую вводную можно передать переменной окружения,
# например `DOMAIN=example.com SERVER_IP=1.2.3.4 ./macos-01-secrets.sh`.
#
# Где выполнять: на вашем macOS, из корня репозитория или из deploy/setup.
# Порядок: 01 → 02 → 03 → 04.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/setup/lib.sh
. "$SCRIPT_DIR/lib.sh"

require_cmd openssl "входит в macOS по умолчанию"
require_cmd ssh-keygen "входит в macOS по умолчанию"

umask 077
mkdir -p "$STATE_DIR"
chmod 700 "$STATE_DIR"
load_state

# --- Вводные (конфиг) ---------------------------------------------------------
# prompt_var VAR "вопрос" [дефолт] — берёт уже известное значение (env/конфиг),
# иначе спрашивает; без терминала и без дефолта — падает с подсказкой.
prompt_var() {
	local var="$1" question="$2" default="${3:-}" current value
	eval "current=\${$var:-}"
	if [ -n "$current" ]; then
		return 0
	fi
	if [ ! -t 0 ] && [ -z "$default" ]; then
		die "значение $var не задано — передайте переменной окружения ($var=... $0) или запустите интерактивно"
	fi
	if [ -t 0 ]; then
		if [ -n "$default" ]; then
			read -r -p "$question [$default]: " value
		else
			read -r -p "$question: " value
		fi
	fi
	value="${value:-$default}"
	[ -n "$value" ] || die "значение $var обязательно"
	eval "$var=\$value"
}

log "собираю вводные (уже заданные значения не переспрашиваю)"
prompt_var GITHUB_REPO "GitHub-репозиторий (owner/repo)" "yarabey/agentify"
prompt_var DOMAIN "домен (без поддоменов; DNS A-записи app./api./bot. уже заведены на IP VPS)"
prompt_var SERVER_IP "IP-адрес VPS (тот, куда смотрят DNS A-записи)"
prompt_var DEPLOY_USER "деплой-пользователь на VPS (SSH_USER)" "deploy"
prompt_var PROVISION_SSH "SSH-адрес для первичной настройки VPS (пользователь с sudo/root)" "root@$SERVER_IP"
prompt_var TELEGRAM_BOT_TOKEN "токен Telegram-бота от @BotFather"
prompt_var BOOTSTRAP_ADMIN_USERNAME "логин первого администратора" "admin"

# Одинарные кавычки: значения могут содержать ':' (токен бота) и т.п.
cat >"$CONFIG_FILE" <<EOF
# Сгенерировано deploy/setup/macos-01-secrets.sh — вводные первого деплоя.
GITHUB_REPO='$GITHUB_REPO'
DOMAIN='$DOMAIN'
SERVER_IP='$SERVER_IP'
DEPLOY_USER='$DEPLOY_USER'
PROVISION_SSH='$PROVISION_SSH'
TELEGRAM_BOT_TOKEN='$TELEGRAM_BOT_TOKEN'
BOOTSTRAP_ADMIN_USERNAME='$BOOTSTRAP_ADMIN_USERNAME'
EOF
chmod 600 "$CONFIG_FILE"
log "конфиг сохранён: $CONFIG_FILE"

# --- Секреты (MANUAL_STEPS.md §2) ----------------------------------------------
# gen_secret VAR "команда" — генерирует только отсутствующие значения:
# повторный запуск ничего не меняет (важно для APP_ENCRYPTION_KEY, см. шапку).
GENERATED=""
gen_secret() {
	local var="$1" cmd="$2" current value
	eval "current=\${$var:-}"
	if [ -n "$current" ]; then
		return 0
	fi
	value="$(eval "$cmd")"
	eval "$var=\$value"
	GENERATED="$GENERATED $var"
}

log "генерирую недостающие секреты (openssl)"
gen_secret JWT_SIGNING_KEY            "openssl rand -base64 48"
gen_secret APP_ENCRYPTION_KEY         "openssl rand -base64 32"
gen_secret POSTGRES_PASSWORD          "openssl rand -base64 24"
gen_secret INITIAL_REGISTRATION_TOKEN "openssl rand -hex 16"
gen_secret BOT_WEBHOOK_SECRET         "openssl rand -hex 16"
gen_secret BOT_SERVICE_SECRET         "openssl rand -base64 32"
gen_secret BOOTSTRAP_ADMIN_PASSWORD   "openssl rand -base64 18"

cat >"$SECRETS_FILE" <<EOF
# Сгенерировано deploy/setup/macos-01-secrets.sh — НЕ коммитить, НЕ пересоздавать
# без необходимости (APP_ENCRYPTION_KEY нельзя менять после первого запуска с
# данными — уже зашифрованные значения станут нечитаемыми, MANUAL_STEPS.md §2).
JWT_SIGNING_KEY='$JWT_SIGNING_KEY'
APP_ENCRYPTION_KEY='$APP_ENCRYPTION_KEY'
POSTGRES_PASSWORD='$POSTGRES_PASSWORD'
INITIAL_REGISTRATION_TOKEN='$INITIAL_REGISTRATION_TOKEN'
BOT_WEBHOOK_SECRET='$BOT_WEBHOOK_SECRET'
BOT_SERVICE_SECRET='$BOT_SERVICE_SECRET'
BOOTSTRAP_ADMIN_PASSWORD='$BOOTSTRAP_ADMIN_PASSWORD'
EOF
chmod 600 "$SECRETS_FILE"
if [ -n "$GENERATED" ]; then
	log "сгенерированы:$GENERATED"
else
	log "все секреты уже были сгенерированы ранее — ничего не изменено"
fi

# --- SSH-ключи ------------------------------------------------------------------
# 1) ssh_deploy_key: приватная часть → GitHub Secret SSH_DEPLOY_KEY (шаг 02),
#    публичная → authorized_keys деплой-пользователя на VPS (шаг 03).
# 2) github_read_key: публичная часть → deploy key репозитория (шаг 02),
#    приватная → на VPS (шаг 03), чтобы VPS мог делать git fetch приватного
#    репозитория (предусловие deploy-workflow, см. deploy/README.md).
if [ ! -f "$DEPLOY_KEY" ]; then
	log "генерирую SSH-ключ деплоя (GitHub Actions → VPS)"
	ssh-keygen -t ed25519 -N "" -f "$DEPLOY_KEY" -C "agentify-deploy" >/dev/null
fi
if [ ! -f "$GITHUB_READ_KEY" ]; then
	log "генерирую SSH-ключ чтения репозитория (VPS → GitHub)"
	ssh-keygen -t ed25519 -N "" -f "$GITHUB_READ_KEY" -C "agentify-vps-github-read" >/dev/null
fi

log "готово. Состояние в $STATE_DIR. Следующий шаг: deploy/setup/macos-02-github.sh"
