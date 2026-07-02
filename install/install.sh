#!/usr/bin/env bash
# install/install.sh — тикет 4.2 (deps: 4.1).
#
# Назначение (бизнес): «установка агента одной командой» на чистой
# macOS/Ubuntu (FR C1; docs/User_stories_Gherkin.md «Установка агента одной
# командой», сценарий «Установка на чистой системе»). Этот скрипт закрывает
# РОВНО FR C1 — скачивание бинаря агента (тикет 4.1, GoReleaser →
# GitHub Releases) и установку системной зависимости Node.js runtime, без
# которой не запустится ни один Node-CLI, включая будущий провайдер
# Claude Code (docs/01_tech_stack_and_architecture.md §2 [РЕШЕНИЕ 2]:
# «Node нужен только как зависимость самого Claude Code, и её ставит
# install-скрипт»).
#
# Границы (осознанно НЕ входит в этот тикет, см. docs/MVP_TICKETS.md EPIC 4):
#   - интерактивный опрос (адрес оркестратора / UUID / провайдер) и запись
#     конфига агента — тикет 4.3 (FR C2), deps: 4.2;
#   - демонизация (systemd/launchd, автозапуск/рестарт) — тикет 4.4 (FR C5),
#     deps: 4.3;
#   - установка npm-пакета самого Claude Code CLI (`claude`) — тикет 4.5,
#     deps: 4.4, 6.1 (это интеграция провайдера, а не системная зависимость).
# Этот скрипт НИЧЕГО не спрашивает интерактивно, не пишет конфиг агента, не
# ставит демон-юниты и не устанавливает пакет `claude`.
#
# Как устроено (тех): определяем OS/ARCH через uname, резолвим тег релиза
# (явно заданный либо latest через GitHub API), скачиваем tar.gz + checksums.txt
# ровно с того URL, по которому GoReleaser публикует GitHub Releases
# (см. .goreleaser.yaml, тикет 4.1: `archives` без явного name_template →
# дефолт GoReleaser `<project>_<version-без-v>_<os>_<arch>.tar.gz`, внутри —
# бинарь `agentify-agent` плоско в корне архива), проверяем sha256, ставим
# бинарь в AGENTIFY_INSTALL_DIR и проверяем/ставим Node.js runtime нужной
# major-версии. Финальная самопроверка — `agentify-agent --version`.
#
# Переменные окружения (все опциональны):
#   AGENTIFY_REPO              GitHub owner/repo релиза (default: yarabey/agentify)
#   AGENTIFY_VERSION           тег релиза для установки, СО значением "v"
#                              (например v0.1.0). Если не задана — резолвится
#                              автоматически через GitHub API .../releases/latest.
#   AGENTIFY_RELEASE_BASE_URL  база для скачивания релизов (default:
#                              https://github.com/${AGENTIFY_REPO}/releases/download).
#                              Итоговый URL всегда ${AGENTIFY_RELEASE_BASE_URL}/${tag}/<filename>
#                              — ровно то, как устроены GitHub Releases;
#                              переопределение нужно для прогона против
#                              локального фейкового сервера, см.
#                              install/test-install.sh.
#   AGENTIFY_INSTALL_DIR       куда ставить бинарь (default: /usr/local/bin)
#   AGENTIFY_NODE_MAJOR        целевая major-версия Node.js (default: 22,
#                              совпадает с env.NODE_VERSION в .github/workflows/ci.yml
#                              — единая версия Node по всему репо)

set -euo pipefail

AGENTIFY_REPO="${AGENTIFY_REPO:-yarabey/agentify}"
AGENTIFY_VERSION="${AGENTIFY_VERSION:-}"
AGENTIFY_RELEASE_BASE_URL="${AGENTIFY_RELEASE_BASE_URL:-https://github.com/${AGENTIFY_REPO}/releases/download}"
AGENTIFY_INSTALL_DIR="${AGENTIFY_INSTALL_DIR:-/usr/local/bin}"
AGENTIFY_NODE_MAJOR="${AGENTIFY_NODE_MAJOR:-22}"
# Минимальная major-версия Node, на которой в принципе запускается
# современный Node-CLI (в т.ч. будущий Claude Code, тикет 4.5) — используется
# только как порог «уже достаточно свежий, не трогаем», не как целевая версия
# новой установки (для новой установки всегда ставим AGENTIFY_NODE_MAJOR).
AGENTIFY_NODE_MIN_MAJOR=18

log()  { printf '==> %s\n' "$*"; }
ok()   { printf 'OK:   %s\n' "$*"; }
die()  { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

# --- 1. OS -------------------------------------------------------------------
uname_s="$(uname -s)"
case "${uname_s}" in
	Darwin) OS="darwin" ;;
	Linux)  OS="linux" ;;
	*) die "неподдерживаемая ОС '${uname_s}' — install.sh поддерживает только macOS (Darwin) и Ubuntu/Linux" ;;
esac

# --- 2. ARCH -------------------------------------------------------------------
uname_m="$(uname -m)"
case "${uname_m}" in
	x86_64) ARCH="amd64" ;;
	arm64|aarch64) ARCH="arm64" ;;
	*) die "неподдерживаемая архитектура '${uname_m}' — install.sh поддерживает только amd64/arm64" ;;
esac
ok "система: ${OS}/${ARCH}"

# --- 3. sudo -------------------------------------------------------------------
SUDO=""
if [ "$(id -u)" -ne 0 ]; then
	if command -v sudo >/dev/null 2>&1; then
		SUDO="sudo"
	else
		die "нужны права root или установленный sudo — запустите install.sh от root либо установите sudo"
	fi
fi

# --- 4. Резолв тега версии -----------------------------------------------------
if [ -z "${AGENTIFY_VERSION}" ]; then
	log "AGENTIFY_VERSION не задана — резолвлю последний релиз через GitHub API"
	latest_url="https://api.github.com/repos/${AGENTIFY_REPO}/releases/latest"
	latest_json="$(curl -fsSL "${latest_url}" 2>/dev/null || true)"
	AGENTIFY_VERSION="$(printf '%s' "${latest_json}" | grep -m1 '"tag_name"' | sed -E 's/.*"tag_name"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')"
	if [ -z "${AGENTIFY_VERSION}" ]; then
		die "не удалось определить последнюю версию через ${latest_url} (сеть недоступна, репозиторий без релизов, или ограничен rate-limit GitHub API) — задайте версию явно: AGENTIFY_VERSION=vX.Y.Z ./install.sh"
	fi
fi
ok "версия для установки: ${AGENTIFY_VERSION}"

# Имя архива в GitHub Releases от GoReleaser = тег БЕЗ ведущей 'v' (проверено
# на реальном .goreleaser.yaml этого репо: `goreleaser release --snapshot
# --clean --skip=publish,announce,sign,validate` даёт файлы вида
# agentify-agent_<version-без-v>_<os>_<arch>.tar.gz).
version_no_v="${AGENTIFY_VERSION#v}"
archive="agentify-agent_${version_no_v}_${OS}_${ARCH}.tar.gz"
checksums_file="checksums.txt"

# --- 6. Временная рабочая директория -------------------------------------------
workdir="$(mktemp -d)"
trap 'rm -rf "${workdir}"' EXIT

# --- 7. Скачивание архива + checksums.txt --------------------------------------
release_url="${AGENTIFY_RELEASE_BASE_URL}/${AGENTIFY_VERSION}"
log "скачиваю ${release_url}/${archive}"
curl -fsSL "${release_url}/${archive}" -o "${workdir}/${archive}" \
	|| die "не удалось скачать ${release_url}/${archive} — проверьте AGENTIFY_REPO/AGENTIFY_VERSION/сеть"
log "скачиваю ${release_url}/${checksums_file}"
curl -fsSL "${release_url}/${checksums_file}" -o "${workdir}/${checksums_file}" \
	|| die "не удалось скачать ${release_url}/${checksums_file}"

# --- 8. Проверка контрольной суммы ----------------------------------------------
log "проверяю sha256 контрольную сумму архива"
checksum_line="$(grep " ${archive}\$" "${workdir}/${checksums_file}" || true)"
if [ -z "${checksum_line}" ]; then
	die "в ${checksums_file} не найдена строка для ${archive} — релиз повреждён или несовместим"
fi
printf '%s\n' "${checksum_line}" > "${workdir}/${archive}.sha256"

(
	cd "${workdir}"
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum -c "${archive}.sha256"
	elif command -v shasum >/dev/null 2>&1; then
		shasum -a 256 -c "${archive}.sha256"
	else
		die "не найден ни sha256sum, ни shasum — невозможно проверить контрольную сумму"
	fi
) || die "контрольная сумма ${archive} не совпадает — скачанный файл повреждён либо подменён"
ok "контрольная сумма совпадает"

# --- 9. Распаковка ТОЛЬКО бинаря -------------------------------------------------
log "распаковываю agentify-agent из ${archive}"
tar -xzf "${workdir}/${archive}" -C "${workdir}" agentify-agent \
	|| die "не удалось распаковать agentify-agent из ${archive}"

# --- 10. Установка бинаря --------------------------------------------------------
log "устанавливаю agentify-agent в ${AGENTIFY_INSTALL_DIR}"
${SUDO} mkdir -p "${AGENTIFY_INSTALL_DIR}" \
	|| die "не удалось создать директорию ${AGENTIFY_INSTALL_DIR}"
# Именно "-m 0755 src dst" без флага -D: GNU install поддерживает -D, BSD
# install (macOS) — нет; -D не нужен, т.к. директория уже создана mkdir -p
# выше, и такой вызов работает одинаково на обеих ОС.
${SUDO} install -m 0755 "${workdir}/agentify-agent" "${AGENTIFY_INSTALL_DIR}/agentify-agent" \
	|| die "не удалось установить бинарь в ${AGENTIFY_INSTALL_DIR}"
ok "agentify-agent установлен в ${AGENTIFY_INSTALL_DIR}/agentify-agent"

# --- 11. Node.js runtime ----------------------------------------------------------
node_major=""
if command -v node >/dev/null 2>&1; then
	node_major="$(node --version | sed -E 's/^v([0-9]+).*/\1/')"
fi

if [ -n "${node_major}" ] && [ "${node_major}" -ge "${AGENTIFY_NODE_MIN_MAJOR}" ]; then
	ok "Node.js уже установлен (major=${node_major} >= ${AGENTIFY_NODE_MIN_MAJOR}) — не трогаю"
else
	log "Node.js отсутствует либо устарел (найдено: ${node_major:-нет}) — устанавливаю Node.js ${AGENTIFY_NODE_MAJOR}.x"
	case "${OS}" in
		linux)
			curl -fsSL "https://deb.nodesource.com/setup_${AGENTIFY_NODE_MAJOR}.x" | ${SUDO} bash - \
				|| die "не удалось подключить репозиторий NodeSource для Node.js ${AGENTIFY_NODE_MAJOR}.x"
			${SUDO} apt-get install -y nodejs \
				|| die "не удалось установить пакет nodejs через apt-get"
			;;
		darwin)
			if command -v brew >/dev/null 2>&1; then
				brew install "node@${AGENTIFY_NODE_MAJOR}" || brew install node \
					|| die "не удалось установить Node.js через Homebrew"
			else
				die "Homebrew не найден — установите https://brew.sh и запустите install.sh повторно"
			fi
			;;
	esac
	ok "Node.js ${AGENTIFY_NODE_MAJOR}.x установлен"
fi

# --- 12. Финальная самопроверка ----------------------------------------------------
log "самопроверка: ${AGENTIFY_INSTALL_DIR}/agentify-agent --version"
installed_version="$("${AGENTIFY_INSTALL_DIR}/agentify-agent" --version)" \
	|| die "agentify-agent --version завершился с ошибкой — установка повреждена"
ok "agentify-agent --version -> ${installed_version}"

# --- 13. Финальное сообщение --------------------------------------------------------
echo
echo "agentify-agent ${installed_version} установлен в ${AGENTIFY_INSTALL_DIR}/agentify-agent."
echo "Node.js: $(node --version 2>/dev/null || echo "версия не определена")."
echo
echo "Дальше нужно настроить агента: адрес оркестратора, UUID машины и провайдера ИИ."
