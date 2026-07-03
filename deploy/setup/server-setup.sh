#!/usr/bin/env bash
# deploy/setup/server-setup.sh — подготовка Ubuntu-VPS (выполняется НА сервере).
#
# Назначение (бизнес): закрывает серверную часть docs/MANUAL_STEPS.md §1 —
# Docker + Docker Compose, деплой-пользователь, firewall только 22/80/443.
# Обычно запускается автоматически скриптом macos-03-server.sh по SSH; можно
# выполнить и вручную от root на чистом Ubuntu 22.04/24.04.
#
# Как устроено (тех): идемпотентен — уже установленное не переустанавливает,
# существующего пользователя не пересоздаёт, ключ в authorized_keys не
# задваивает. Docker ставится официальным скриптом get.docker.com (поддерживает
# все актуальные версии Ubuntu); firewall — ufw (сначала allow, потом enable,
# чтобы не отрезать текущую SSH-сессию).
#
# Входные переменные окружения:
#   DEPLOY_USER    имя деплой-пользователя (по умолчанию deploy)
#   DEPLOY_PUBKEY  публичный SSH-ключ деплоя (обязателен: строка ssh-ed25519 ...)
#
# Пример ручного запуска (от root):
#   DEPLOY_USER=deploy DEPLOY_PUBKEY='ssh-ed25519 AAAA... agentify-deploy' \
#     bash server-setup.sh

set -euo pipefail

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31mОШИБКА:\033[0m %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "нужны права root (запустите через sudo)"
DEPLOY_USER="${DEPLOY_USER:-deploy}"
[ -n "${DEPLOY_PUBKEY:-}" ] || die "не задан DEPLOY_PUBKEY (публичный SSH-ключ деплой-пользователя)"

export DEBIAN_FRONTEND=noninteractive

log "обновляю индекс пакетов и ставлю базовые утилиты"
apt-get update -qq
apt-get install -y -qq ca-certificates curl git ufw >/dev/null

# --- Docker + Compose v2 ---------------------------------------------------------
if command -v docker >/dev/null 2>&1; then
	log "docker уже установлен: $(docker --version)"
else
	log "устанавливаю Docker (официальный скрипт get.docker.com)"
	curl -fsSL https://get.docker.com | sh
fi
if docker compose version >/dev/null 2>&1; then
	log "docker compose уже установлен: $(docker compose version --short)"
else
	log "устанавливаю docker-compose-plugin"
	apt-get install -y -qq docker-compose-plugin >/dev/null ||
		die "не удалось поставить docker-compose-plugin — Docker установлен не из официального apt-репозитория? Установите Compose v2 вручную"
fi
systemctl enable --now docker >/dev/null 2>&1 || true

# --- Firewall: только 22/80/443 (MANUAL_STEPS.md §1) -------------------------------
# Порядок важен: сначала allow 22, потом enable — иначе ufw может оборвать
# текущую SSH-сессию.
log "настраиваю ufw (22/80/443, остальное закрыто)"
ufw allow 22/tcp >/dev/null
ufw allow 80/tcp >/dev/null
ufw allow 443/tcp >/dev/null
ufw default deny incoming >/dev/null
ufw default allow outgoing >/dev/null
ufw --force enable >/dev/null
log "ufw: $(ufw status | head -1)"

# --- Деплой-пользователь -----------------------------------------------------------
if id -u "$DEPLOY_USER" >/dev/null 2>&1; then
	log "пользователь $DEPLOY_USER уже существует"
else
	log "создаю пользователя $DEPLOY_USER"
	useradd -m -s /bin/bash "$DEPLOY_USER"
fi
# Группа docker: деплой-workflow выполняет docker compose без sudo.
usermod -aG docker "$DEPLOY_USER"

DEPLOY_HOME="$(getent passwd "$DEPLOY_USER" | cut -d: -f6)"
install -d -m 700 -o "$DEPLOY_USER" -g "$DEPLOY_USER" "$DEPLOY_HOME/.ssh"
AUTH_KEYS="$DEPLOY_HOME/.ssh/authorized_keys"
touch "$AUTH_KEYS"
if grep -qxF "$DEPLOY_PUBKEY" "$AUTH_KEYS"; then
	log "SSH-ключ деплоя уже в authorized_keys"
else
	log "добавляю SSH-ключ деплоя в authorized_keys"
	printf '%s\n' "$DEPLOY_PUBKEY" >>"$AUTH_KEYS"
fi
chmod 600 "$AUTH_KEYS"
chown "$DEPLOY_USER:$DEPLOY_USER" "$AUTH_KEYS"

log "сервер готов: docker + compose, ufw 22/80/443, пользователь $DEPLOY_USER"
