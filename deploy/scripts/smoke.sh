#!/usr/bin/env bash
# deploy/scripts/smoke.sh — smoke-проверка локального стека (тикет 0.3).
#
# Назначение (бизнес): подтвердить приёмку 0.3 «/healthz всех сервисов отвечает».
# Скрипт пингует через фронтовый Caddy liveness каждого сервиса и падает с
# ненулевым кодом, если хоть кто-то не ответил 200. Используется вручную после
# `make run-local` и в CI (тикет 0.4).
#
# Как устроено (тех): снаружи опубликован только порт фронтового Caddy, поэтому
# проверяем сервисы через его маршруты:
#   - фронт:        GET /healthz
#   - оркестратор:  GET /api/healthz   (handle_path /api/* → orchestrator:8080)
#   - бот:          GET /bot/healthz   (handle_path /bot/* → bot:8080)
#   - web:          GET /              (статика, ответ 200)
# Каждую цель опрашиваем с ретраями, давая контейнерам подняться.
#
# Переменные окружения:
#   BASE_URL   базовый URL фронта (по умолчанию http://127.0.0.1:${HTTP_PORT:-8080})
#   RETRIES    число попыток на цель (по умолчанию 30)
#   DELAY      пауза между попытками, сек (по умолчанию 2)

set -euo pipefail

HTTP_PORT="${HTTP_PORT:-8080}"
BASE_URL="${BASE_URL:-http://127.0.0.1:${HTTP_PORT}}"
RETRIES="${RETRIES:-30}"
DELAY="${DELAY:-2}"

# Цели проверки: "человекочитаемое-имя путь".
TARGETS=(
	"caddy        /healthz"
	"orchestrator /api/healthz"
	"bot          /bot/healthz"
	"web          /"
)

# check_target опрашивает один путь с ретраями; возвращает 0 при первом 200.
check_target() {
	local name="$1" path="$2" url i code
	url="${BASE_URL}${path}"
	for ((i = 1; i <= RETRIES; i++)); do
		code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$url" || true)"
		if [[ "$code" == "200" ]]; then
			printf 'OK    %-13s %s -> 200\n' "$name" "$url"
			return 0
		fi
		printf '...   %-13s %s -> %s (попытка %d/%d)\n' "$name" "$url" "${code:-нет ответа}" "$i" "$RETRIES"
		sleep "$DELAY"
	done
	printf 'FAIL  %-13s %s — не ответил 200 за %d попыток\n' "$name" "$url" "$RETRIES" >&2
	return 1
}

echo "smoke: проверяю /healthz всех сервисов через ${BASE_URL}"
failed=0
for entry in "${TARGETS[@]}"; do
	# shellcheck disable=SC2086 — намеренно разбиваем "имя путь" на два аргумента.
	set -- $entry
	if ! check_target "$1" "$2"; then
		failed=1
	fi
done

if [[ "$failed" -ne 0 ]]; then
	echo "smoke: ПРОВАЛ — не все сервисы ответили 200" >&2
	exit 1
fi

echo "smoke: УСПЕХ — все сервисы ответили 200"

# CI: no-op строка для прогона всех проверок в throwaway-PR (мёржить не планируется).
