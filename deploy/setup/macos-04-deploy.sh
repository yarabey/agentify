#!/usr/bin/env bash
# deploy/setup/macos-04-deploy.sh — шаг 4: первый деплой (macOS).
#
# Назначение (бизнес): запускает прод-релиз ровно тем путём, которым он будет
# катиться всегда (deploy/README.md §«Прод-релиз»): workflow Deploy → сборка
# образов в GHCR → подтверждение окружения prod → раскатка на VPS. После
# успешной раскатки проверяет /healthz через реальные домены и TLS и
# идемпотентно выполняет `orchestrator bootstrap` (первый администратор +
# стартовый токен регистрации, тикет 1.7 — без него в закрытую систему
# невозможно войти, FR A1/A2).
#
# Как устроено (тех): подтверждение required reviewer-а окружения prod делается
# через API pending_deployments от имени того же пользователя gh, которого шаг
# 02 назначил required reviewer-ом — руками в UI ничего нажимать не нужно.
# Bootstrap выполняется внутри контейнера orchestrator на VPS тем же способом,
# что deploy/scripts/e2e-bootstrap.sh локально (`docker compose exec -e ...`).
#
# Где выполнять: на вашем macOS, после macos-03-server.sh.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/setup/lib.sh
. "$SCRIPT_DIR/lib.sh"

require_state
ensure_gh

# --- Запуск workflow Deploy (workflow_dispatch на main) ---------------------------
log "запускаю workflow Deploy (ветка main)"
gh workflow run deploy.yml --repo "$GITHUB_REPO" --ref main
sleep 5
RUN_ID="$(gh run list --repo "$GITHUB_REPO" --workflow=deploy.yml --limit 1 --json databaseId -q '.[0].databaseId')"
[ -n "$RUN_ID" ] || die "не нашёл запущенный run workflow Deploy"
log "run: https://github.com/$GITHUB_REPO/actions/runs/$RUN_ID"

# --- Ожидание + авто-подтверждение окружения prod ---------------------------------
# Статус run становится waiting, когда сборка готова и джоба deploy ждёт
# required reviewer-а; подтверждаем и ждём завершения.
APPROVED=0
DEADLINE=$(( $(date +%s) + 3600 ))
while :; do
	[ "$(date +%s)" -lt "$DEADLINE" ] || die "деплой не завершился за час — смотрите gh run view $RUN_ID --repo $GITHUB_REPO"
	STATUS="$(gh run view "$RUN_ID" --repo "$GITHUB_REPO" --json status -q .status)"
	case "$STATUS" in
	completed)
		break
		;;
	waiting)
		if [ "$APPROVED" -eq 0 ]; then
			ENV_ID="$(gh api "repos/$GITHUB_REPO/actions/runs/$RUN_ID/pending_deployments" -q '.[0].environment.id' 2>/dev/null || true)"
			if [ -n "$ENV_ID" ]; then
				log "джоба deploy ждёт подтверждения окружения prod — подтверждаю"
				printf '{"environment_ids":[%s],"state":"approved","comment":"первый деплой: авто-подтверждение из deploy/setup/macos-04-deploy.sh"}' "$ENV_ID" |
					gh api -X POST "repos/$GITHUB_REPO/actions/runs/$RUN_ID/pending_deployments" --input - >/dev/null
				APPROVED=1
			fi
		fi
		;;
	esac
	printf '.'
	sleep 15
done
echo
CONCLUSION="$(gh run view "$RUN_ID" --repo "$GITHUB_REPO" --json conclusion -q .conclusion)"
[ "$CONCLUSION" = "success" ] || die "workflow завершился со статусом '$CONCLUSION' — детали: gh run view $RUN_ID --repo $GITHUB_REPO --log-failed"
log "workflow Deploy завершился успешно"

# --- Smoke через реальные домены и TLS (аналог make smoke, deploy/README.md) -------
# Первому запросу может понадобиться время: Caddy получает сертификаты
# Let's Encrypt при первом обращении к домену.
check_url() {
	local name="$1" url="$2" i code
	for i in $(seq 1 40); do
		code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$url" || true)"
		if [ "$code" = "200" ]; then
			log "  OK  $name  $url → 200"
			return 0
		fi
		printf '...   %s → %s (попытка %s/40, жду TLS/старта)\n' "$url" "${code:-нет ответа}" "$i"
		sleep 5
	done
	warn "$name: $url не ответил 200 — проверьте DNS/логи на VPS (docker compose logs caddy)"
	return 1
}

log "проверяю /healthz через прод-домены"
SMOKE_OK=1
check_url orchestrator "https://api.$DOMAIN/healthz" || SMOKE_OK=0
check_url bot "https://bot.$DOMAIN/healthz" || SMOKE_OK=0
check_url web "https://app.$DOMAIN/healthz" || SMOKE_OK=0
[ "$SMOKE_OK" -eq 1 ] || die "smoke не прошёл — деплой раскатан, но сервисы недоступны снаружи"

# --- Bootstrap: первый администратор + стартовый токен регистрации (тикет 1.7) -----
log "выполняю orchestrator bootstrap на VPS (идемпотентно)"
ssh_deploy "cd ~/agentify && docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.prod.yml exec -T \
	-e ORCH_BOOTSTRAP_ADMIN_USERNAME='$BOOTSTRAP_ADMIN_USERNAME' \
	-e ORCH_BOOTSTRAP_ADMIN_PASSWORD='$BOOTSTRAP_ADMIN_PASSWORD' \
	-e ORCH_INITIAL_REGISTRATION_TOKEN='$INITIAL_REGISTRATION_TOKEN' \
	orchestrator /usr/local/bin/orchestrator bootstrap"

# --- Итог --------------------------------------------------------------------------
log "ПЕРВЫЙ ДЕПЛОЙ ЗАВЕРШЁН"
cat <<EOF

  Веб-интерфейс:       https://app.$DOMAIN
  API:                 https://api.$DOMAIN/healthz
  Бот (webhook):       https://bot.$DOMAIN/healthz

  Администратор:       $BOOTSTRAP_ADMIN_USERNAME (пароль — BOOTSTRAP_ADMIN_PASSWORD в $SECRETS_FILE)
  Токен регистрации:   INITIAL_REGISTRATION_TOKEN в $SECRETS_FILE (для приглашения пользователей, FR A2)

  Осталось руками (не автоматизируется скриптами):
   - внешний uptime-мониторинг на https://api.$DOMAIN/healthz (MANUAL_STEPS.md §6, например UptimeRobot);
   - продуктовые решения из MANUAL_STEPS.md §4 (записать в docs/ до спринта 3).

  Следующие релизы: GitHub → Actions → Deploy → Run workflow (или повторный запуск этого скрипта).
EOF
