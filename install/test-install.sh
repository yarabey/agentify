#!/usr/bin/env bash
# install/test-install.sh — тестовый харнесс для install.sh (тикет 4.2, deps: 4.1).
#
# Назначение (бизнес): приёмка тикета 4.2 требует «прогон в чистых
# Docker-образах ubuntu» (docs/MVP_TICKETS.md, 4.2 «Тесты»). Реальный
# GitHub Release для прогона в CI недоступен (репозиторий ещё не выпустил
# тег), поэтому этот скрипт поднимает локальный HTTP-сервер с фейковым
# GitHub-Releases-лэйаутом поверх артефактов, которые CI уже собрал через
# GoReleaser (см. .github/workflows/ci.yml, джоба agent-install-test), и
# запускает install.sh целиком (скачивание + sha256 + установка + Node +
# самопроверка --version) через официальную точку переопределения источника
# — AGENTIFY_RELEASE_BASE_URL/AGENTIFY_VERSION (это НЕ хак самого install.sh:
# переопределение источника — штатный сценарий именно для тестирования и
# локальных зеркал, см. комментарий в install.sh).
#
# Как устроено (тех): раскладываем dist/*.tar.gz + dist/checksums.txt под
# ${tag}/ в служебном каталоге, отдаём его `python3 -m http.server`, ждём
# готовности ретраями (тот же паттерн, что check_target в
# deploy/scripts/smoke.sh), затем гоняем install.sh с переопределённым
# AGENTIFY_RELEASE_BASE_URL, указывающим на этот локальный сервер, и
# проверяем результат: бинарь установлен, исполняем, --version печатает
# версию тега без ведущей 'v'.
#
# Использование:
#   install/test-install.sh <tag> <dist-dir>
# Пример:
#   install/test-install.sh v0.0.1-snapshot-abc123 dist
#
# Переменные окружения:
#   AGENTIFY_INSTALL_DIR   куда install.sh поставит бинарь (default: /usr/local/bin);
#                          пробрасывается напрямую в install.sh.
#   TEST_INSTALL_PORT      порт локального HTTP-сервера (default: 8991).

set -euo pipefail

if [ "$#" -ne 2 ]; then
	echo "использование: $0 <tag> <dist-dir>" >&2
	exit 1
fi

TAG="$1"
DIST_DIR="$2"
PORT="${TEST_INSTALL_PORT:-8991}"
AGENTIFY_INSTALL_DIR="${AGENTIFY_INSTALL_DIR:-/usr/local/bin}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

log()  { printf '==> %s\n' "$*"; }
ok()   { printf 'OK:   %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; }

if [ ! -d "${DIST_DIR}" ]; then
	fail "dist-dir '${DIST_DIR}' не найден"
	exit 1
fi

# --- 1. Служебный каталог + фоновый HTTP-сервер ---------------------------------
serve_root="$(mktemp -d)"
server_pid=""

cleanup() {
	if [ -n "${server_pid}" ] && kill -0 "${server_pid}" 2>/dev/null; then
		kill "${server_pid}" 2>/dev/null || true
		wait "${server_pid}" 2>/dev/null || true
	fi
	rm -rf "${serve_root}"
}
trap cleanup EXIT

# --- 2. Разложить артефакты релиза под ${tag}/ ----------------------------------
mkdir -p "${serve_root}/${TAG}"
shopt -s nullglob
archives=("${DIST_DIR}"/*.tar.gz)
shopt -u nullglob
if [ "${#archives[@]}" -eq 0 ]; then
	fail "в ${DIST_DIR} не найдено ни одного *.tar.gz — сначала собери релиз (goreleaser release --snapshot ...)"
	exit 1
fi
cp "${archives[@]}" "${serve_root}/${TAG}/"

if [ ! -f "${DIST_DIR}/checksums.txt" ]; then
	fail "в ${DIST_DIR} не найден checksums.txt"
	exit 1
fi
cp "${DIST_DIR}/checksums.txt" "${serve_root}/${TAG}/checksums.txt"

# --- 3. Поднять локальный HTTP-сервер -------------------------------------------
log "поднимаю локальный HTTP-сервер на 127.0.0.1:${PORT}, отдаю ${serve_root}"
python3 -m http.server "${PORT}" --directory "${serve_root}" >/tmp/test-install-http.log 2>&1 &
server_pid="$!"

# --- 4. Дождаться готовности сервера (ретраи, без бездумного sleep) -------------
base_url="http://127.0.0.1:${PORT}"
ready=0
for ((i = 1; i <= 30; i++)); do
	code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "${base_url}/${TAG}/checksums.txt" || true)"
	if [ "${code}" = "200" ]; then
		ready=1
		break
	fi
	sleep 0.5
done
if [ "${ready}" -ne 1 ]; then
	fail "локальный HTTP-сервер не поднялся за отведённое время"
	exit 1
fi
ok "локальный HTTP-сервер готов (${base_url})"

# --- 5. Запустить install.sh с переопределённым источником ---------------------
log "запускаю install.sh: AGENTIFY_VERSION=${TAG} AGENTIFY_RELEASE_BASE_URL=${base_url} AGENTIFY_INSTALL_DIR=${AGENTIFY_INSTALL_DIR}"
if ! AGENTIFY_VERSION="${TAG}" \
	AGENTIFY_RELEASE_BASE_URL="${base_url}" \
	AGENTIFY_INSTALL_DIR="${AGENTIFY_INSTALL_DIR}" \
	"${REPO_ROOT}/install/install.sh"; then
	fail "install.sh завершился с ненулевым кодом"
	exit 1
fi

# --- 6. Проверить результат -------------------------------------------------------
expected_version="${TAG#v}"
bin_path="${AGENTIFY_INSTALL_DIR}/agentify-agent"

failed=0

if [ -x "${bin_path}" ]; then
	ok "бинарь установлен и исполняем: ${bin_path}"
else
	fail "бинарь не найден либо не исполняем: ${bin_path}"
	failed=1
fi

if [ "${failed}" -eq 0 ]; then
	actual_version="$("${bin_path}" --version)"
	if [ "${actual_version}" = "${expected_version}" ]; then
		ok "agentify-agent --version -> '${actual_version}' (совпадает с ожидаемым)"
	else
		fail "agentify-agent --version вернул '${actual_version}', ожидалось '${expected_version}'"
		failed=1
	fi
fi

if command -v node >/dev/null 2>&1; then
	ok "Node.js доступен: $(node --version)"
else
	fail "Node.js не найден после install.sh"
	failed=1
fi

if [ "${failed}" -ne 0 ]; then
	echo "test-install: ПРОВАЛ" >&2
	exit 1
fi

echo "test-install: УСПЕХ — install.sh корректно поставил agentify-agent ${expected_version} и Node.js"
