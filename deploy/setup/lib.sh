# shellcheck shell=bash
# deploy/setup/lib.sh — общие помощники скриптов первичной установки.
#
# Назначение (бизнес): единое место для состояния установки (конфиг + секреты +
# SSH-ключи), чтобы все четыре macos-скрипта видели одни и те же значения и
# повторный запуск любого из них был идемпотентным (ничего не перегенерируется
# и не задваивается).
#
# Как устроено (тех): подключается через `source` из macos-*.sh, отдельно не
# выполняется. Состояние живёт ВНЕ репозитория (~/.agentify-deploy, права 700):
# секреты никогда не попадают в git (AGENTS.md §8). Переопределить каталог —
# переменной AGENTIFY_SETUP_DIR.

set -euo pipefail

STATE_DIR="${AGENTIFY_SETUP_DIR:-$HOME/.agentify-deploy}"
CONFIG_FILE="$STATE_DIR/config.env"
SECRETS_FILE="$STATE_DIR/secrets.env"
# Ключ, которым GitHub Actions (и эти скрипты) ходят на VPS по SSH.
DEPLOY_KEY="$STATE_DIR/ssh_deploy_key"
# Ключ, которым VPS читает приватный репозиторий с GitHub (deploy key репозитория).
GITHUB_READ_KEY="$STATE_DIR/github_read_key"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33mВНИМАНИЕ:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31mОШИБКА:\033[0m %s\n' "$*" >&2; exit 1; }

# load_state подгружает конфиг и секреты, если они уже созданы (шаг 01).
load_state() {
	if [ -f "$CONFIG_FILE" ]; then
		# shellcheck source=/dev/null
		. "$CONFIG_FILE"
	fi
	if [ -f "$SECRETS_FILE" ]; then
		# shellcheck source=/dev/null
		. "$SECRETS_FILE"
	fi
}

# require_state гарантирует, что шаг 01 (генерация секретов) уже выполнен.
require_state() {
	if [ ! -f "$CONFIG_FILE" ] || [ ! -f "$SECRETS_FILE" ]; then
		die "не найдено состояние установки в $STATE_DIR — сначала выполните deploy/setup/macos-01-secrets.sh"
	fi
	load_state
}

# require_cmd падает с понятной ошибкой, если команды нет в системе.
require_cmd() {
	command -v "$1" >/dev/null 2>&1 || die "не найдена команда '$1' — $2"
}

# ssh_deploy выполняет команду на VPS от имени деплой-пользователя тем же
# ключом, что положен в GitHub Secret SSH_DEPLOY_KEY (единый путь доступа —
# скрипты проверяют ровно то, чем потом будет пользоваться CD).
ssh_deploy() {
	ssh -i "$DEPLOY_KEY" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "$DEPLOY_USER@$SERVER_IP" "$@"
}

# ensure_gh проверяет наличие и авторизацию GitHub CLI; ставит его через
# Homebrew при отсутствии (а сам Homebrew — при его отсутствии).
ensure_gh() {
	if ! command -v gh >/dev/null 2>&1; then
		if ! command -v brew >/dev/null 2>&1; then
			log "Homebrew не найден — устанавливаю (потребуется пароль macOS)"
			NONINTERACTIVE=1 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
			# brew кладётся в разные префиксы на Intel/Apple Silicon — подхватываем оба.
			if [ -x /opt/homebrew/bin/brew ]; then eval "$(/opt/homebrew/bin/brew shellenv)"; fi
			if [ -x /usr/local/bin/brew ]; then eval "$(/usr/local/bin/brew shellenv)"; fi
		fi
		log "устанавливаю GitHub CLI (gh)"
		brew install gh
	fi
	if ! gh auth status >/dev/null 2>&1; then
		log "gh не авторизован — запускаю вход (браузер)"
		gh auth login --hostname github.com --git-protocol https --web
	fi
}
