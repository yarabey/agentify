#!/usr/bin/env bash
# deploy/setup/macos-02-github.sh — шаг 2: настройка GitHub (macOS).
#
# Назначение (бизнес): автоматизирует docs/MANUAL_STEPS.md §3 — кладёт секреты
# в окружение `prod` GitHub Actions, создаёт само окружение с required
# reviewer (защита деплоя), включает write-права workflow (GHCR/Releases),
# настраивает branch protection на `main` и регистрирует deploy key
# репозитория для VPS (git fetch приватного репо — предусловие
# .github/workflows/deploy.yml, см. deploy/README.md).
#
# Как устроено (тех): всё через GitHub CLI (`gh`); при его отсутствии ставит
# через Homebrew (а Homebrew — при отсутствии). Идемпотентен: PUT-вызовы API
# перезаписывают настройки теми же значениями, `gh secret set` обновляет
# секреты, deploy key добавляется только если его ещё нет.
#
# Где выполнять: на вашем macOS, после macos-01-secrets.sh.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/setup/lib.sh
. "$SCRIPT_DIR/lib.sh"

require_state
ensure_gh

log "проверяю доступ к репозиторию $GITHUB_REPO"
gh repo view "$GITHUB_REPO" --json name >/dev/null || die "нет доступа к $GITHUB_REPO под текущей авторизацией gh"

# --- Окружение prod с required reviewer (MANUAL_STEPS.md §3) --------------------
# Required reviewer = текущий пользователь gh: джоба deploy стартует только
# после явного подтверждения человеком (двойная защита от случайного релиза,
# см. .github/workflows/deploy.yml). Шаг 04 подтверждает деплой тем же
# пользователем через API.
log "создаю/обновляю окружение prod с required reviewer = вы"
USER_ID="$(gh api user -q .id)"
printf '{"wait_timer":0,"reviewers":[{"type":"User","id":%s}],"deployment_branch_policy":null}' "$USER_ID" |
	gh api -X PUT "repos/$GITHUB_REPO/environments/prod" --input - >/dev/null

# --- Секреты окружения prod (точные имена — MANUAL_STEPS.md §3) ------------------
set_secret() {
	local name="$1" value="$2"
	[ -n "$value" ] || die "пустое значение для секрета $name — перезапустите macos-01-secrets.sh"
	gh secret set "$name" --env prod --repo "$GITHUB_REPO" --body "$value"
}

log "кладу секреты в окружение prod"
set_secret SSH_DEPLOY_KEY "$(cat "$DEPLOY_KEY")"
set_secret SSH_HOST "$SERVER_IP"
set_secret SSH_USER "$DEPLOY_USER"
set_secret JWT_SIGNING_KEY "$JWT_SIGNING_KEY"
set_secret APP_ENCRYPTION_KEY "$APP_ENCRYPTION_KEY"
set_secret POSTGRES_PASSWORD "$POSTGRES_PASSWORD"
set_secret TELEGRAM_BOT_TOKEN "$TELEGRAM_BOT_TOKEN"
set_secret BOT_WEBHOOK_SECRET "$BOT_WEBHOOK_SECRET"
set_secret BOT_SERVICE_SECRET "$BOT_SERVICE_SECRET"
set_secret INITIAL_REGISTRATION_TOKEN "$INITIAL_REGISTRATION_TOKEN"
set_secret BOOTSTRAP_ADMIN_USERNAME "$BOOTSTRAP_ADMIN_USERNAME"
set_secret BOOTSTRAP_ADMIN_PASSWORD "$BOOTSTRAP_ADMIN_PASSWORD"

# --- Deploy key репозитория для VPS ----------------------------------------------
# Read-only ключ, которым VPS делает git fetch приватного репозитория
# (deploy-workflow обновляет рабочую копию в ~/agentify перед `compose up`).
DEPLOY_KEY_TITLE="agentify-vps-read"
if gh api "repos/$GITHUB_REPO/keys" -q '.[].title' | grep -qx "$DEPLOY_KEY_TITLE"; then
	log "deploy key '$DEPLOY_KEY_TITLE' уже зарегистрирован — пропускаю"
else
	log "регистрирую deploy key репозитория (read-only) для VPS"
	gh repo deploy-key add "$GITHUB_READ_KEY.pub" --repo "$GITHUB_REPO" --title "$DEPLOY_KEY_TITLE"
fi

# --- Права workflow: GHCR (packages: write) + Releases (contents: write) ---------
log "включаю write-права GITHUB_TOKEN для Actions (GHCR + Releases)"
gh api -X PUT "repos/$GITHUB_REPO/actions/permissions/workflow" \
	-f default_workflow_permissions=write \
	-F can_approve_pull_request_reviews=false >/dev/null

# --- Branch protection на main (MANUAL_STEPS.md §3) -------------------------------
# Список required status checks — канонический из README.md §«CI».
# REQUIRED_REVIEWS=0 отключает требование ревью (для соло-разработки: GitHub не
# даёт одобрить СВОЙ PR, т.е. с 1 required review вы не сможете мёржить в
# одиночку). Дефолт — 1, как требует MANUAL_STEPS.md §3.
REQUIRED_REVIEWS="${REQUIRED_REVIEWS:-1}"
if [ "$REQUIRED_REVIEWS" = "0" ]; then
	REVIEWS_JSON="null"
	warn "REQUIRED_REVIEWS=0 — мёрж в main не будет требовать ревью (отклонение от MANUAL_STEPS.md §3)"
else
	REVIEWS_JSON="{\"required_approving_review_count\":$REQUIRED_REVIEWS}"
	warn "с required review=$REQUIRED_REVIEWS вы НЕ сможете одобрять собственные PR — для соло-работы можно перезапустить с REQUIRED_REVIEWS=0"
fi

log "настраиваю branch protection на main (PR + зелёный CI + ревью)"
cat <<EOF | gh api -X PUT "repos/$GITHUB_REPO/branches/main/protection" --input - >/dev/null
{
  "required_status_checks": {
    "strict": false,
    "contexts": ["lint", "build", "test", "generate-check", "docs-check", "bdd", "integration", "agent-release-build", "agent-install-test", "agent-service-test", "web"]
  },
  "enforce_admins": false,
  "required_pull_request_reviews": $REVIEWS_JSON,
  "restrictions": null
}
EOF

log "готово. Следующий шаг: deploy/setup/macos-03-server.sh"
