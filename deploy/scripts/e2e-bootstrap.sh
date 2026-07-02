#!/usr/bin/env bash
# deploy/scripts/e2e-bootstrap.sh — подготовка локального/CI-стенда к
# сквозному E2E (тикет 9.8, критерий выхода MVP).
#
# Назначение (бизнес): система закрытая (FR A1) — обычная регистрация
# (POST /auth/register, экран /register) сама требует УЖЕ действующего
# токена регистрации, которому больше неоткуда взяться, кроме как через
# `orchestrator bootstrap` (тикет 1.7, см. orchestrator/cmd_bootstrap.go).
# Playwright-сценарии (web/e2e/*.spec.ts) регистрируют СВОИХ пользователей
# через реальный экран `/register`, поэтому перед их прогоном нужен один
# действующий токен — этот скрипт заводит его (идемпотентно, безопасно
# гонять повторно, см. orchestrator/internal/bootstrap.Run) в уже поднятом
# стеке (`make run-local`).
#
# Как устроено (тех): выполняет `orchestrator bootstrap` ВНУТРИ уже
# запущенного контейнера `orchestrator` через `docker compose exec`
# (контейнер уже видит ORCH_DATABASE_URL/ORCH_APP_ENCRYPTION_KEY и т.п. из
# своего окружения compose, см. deploy/docker-compose.yml — доопределяем
# ТОЛЬКО специфичные для bootstrap переменные через `exec -e`, без изменения
# самого docker-compose.yml, см. orchestrator/cmd_bootstrap.go). Значения по
# умолчанию НЕ являются прод-секретами (AGENTS.md §8) — они существуют
# только в эфемерном локальном/CI docker-compose стенде, поднятом на время
# `make e2e`, и синхронизированы с дефолтами в web/e2e/support/env.ts.
#
# Переменные окружения (все опциональны):
#   E2E_ADMIN_USERNAME       имя первого администратора стенда
#   E2E_ADMIN_PASSWORD       его пароль
#   E2E_REGISTRATION_TOKEN   стартовый токен регистрации (см. web/e2e/support/env.ts)
#   COMPOSE_FILE             см. Makefile (по умолчанию deploy/docker-compose.yml)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

COMPOSE_FILE="${COMPOSE_FILE:-deploy/docker-compose.yml}"
E2E_ADMIN_USERNAME="${E2E_ADMIN_USERNAME:-e2e-admin}"
E2E_ADMIN_PASSWORD="${E2E_ADMIN_PASSWORD:-e2e-local-admin-password-1!}"
E2E_REGISTRATION_TOKEN="${E2E_REGISTRATION_TOKEN:-e2e-local-registration-token}"

echo "==> e2e-bootstrap: жду, пока контейнер orchestrator запустится (${COMPOSE_FILE})"
retries=30
until docker compose -f "${COMPOSE_FILE}" exec -T orchestrator true >/dev/null 2>&1; do
	retries=$((retries - 1))
	if [ "${retries}" -le 0 ]; then
		echo "e2e-bootstrap: ПРОВАЛ — контейнер orchestrator не поднялся вовремя (docker compose up -d/make run-local запущен?)" >&2
		exit 1
	fi
	sleep 2
done

echo "==> e2e-bootstrap: orchestrator bootstrap (идемпотентно) — admin=${E2E_ADMIN_USERNAME}"
docker compose -f "${COMPOSE_FILE}" exec -T \
	-e "ORCH_BOOTSTRAP_ADMIN_USERNAME=${E2E_ADMIN_USERNAME}" \
	-e "ORCH_BOOTSTRAP_ADMIN_PASSWORD=${E2E_ADMIN_PASSWORD}" \
	-e "ORCH_INITIAL_REGISTRATION_TOKEN=${E2E_REGISTRATION_TOKEN}" \
	orchestrator /usr/local/bin/orchestrator bootstrap

echo "==> e2e-bootstrap: готово — активный токен регистрации доступен для web/e2e/support/env.ts (E2E_REGISTRATION_TOKEN)"
