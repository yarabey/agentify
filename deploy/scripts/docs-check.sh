#!/usr/bin/env bash
#
# docs-check.sh — проверка дисциплины документации (тикет 0.7, AGENTS.md §0/§3/§7).
#
# Назначение (бизнес): «код без актуальной документации считается незавершённым»
# (AGENTS.md). Эта проверка — исполняемая часть Definition of Done: её гоняют
# локально (`make docs-check`) и в CI, она блокирует мёрж недокументированного кода.
#
# Как устроено (тех): три независимые проверки, каждая запускаема на текущем дереве.
#   (a) Нет публичных Go-символов без godoc — golangci-lint только с правилами
#       документации (revive exported/package-comments + godot).
#   (b) Нет TODO без номера задачи — греп по шаблону TODO(#<число>) / TODO(<ID>).
#   (c) OpenAPI согласован — api/openapi.yaml парсится как YAML и содержит
#       обязательные корневые ключи. Полный codegen-diff — заглушка до тикета 0.2.
#
# Скрипт самодостаточен: не зависит от GOPATH/bin, путь к линтеру берётся из
# переменной GOLANGCI_LINT (по умолчанию системный /usr/local/bin/golangci-lint).

set -euo pipefail

# Корень репозитория — каталог на два уровня выше этого скрипта (deploy/scripts/..).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

# Системный линтер фиксированной версии (AGENTS.md: НЕ полагаемся на GOPATH/bin).
GOLANGCI_LINT="${GOLANGCI_LINT:-/usr/local/bin/golangci-lint}"
OPENAPI_FILE="api/openapi.yaml"

# Итоговый код выхода: 0 — всё зелено, 1 — есть нарушения.
rc=0

log()  { printf '==> %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; rc=1; }
ok()   { printf 'OK:   %s\n' "$*"; }

# --- (a) Нет публичных Go-символов без godoc ----------------------------------
# Переиспользуем .golangci.yml (там настроены revive exported/package-comments и
# godot), но ограничиваем набор линтеров строго документационными через
# --enable-only, чтобы docs-check падал ИМЕННО на недокументированном символе,
# а не на постороннем замечании качества кода.
log "(a) godoc на экспортируемых Go-символах (revive exported/package-comments, godot)"
if ! command -v "${GOLANGCI_LINT}" >/dev/null 2>&1 && [ ! -x "${GOLANGCI_LINT}" ]; then
	fail "(a) не найден golangci-lint по пути '${GOLANGCI_LINT}' (переопредели через GOLANGCI_LINT=...)"
else
	if "${GOLANGCI_LINT}" run --enable-only=revive,godot ./... ; then
		ok "(a) все экспортируемые символы и пакеты документированы"
	else
		fail "(a) есть недокументированные экспортируемые символы/пакеты (см. вывод golangci-lint выше)"
	fi
fi

# --- (b) Нет TODO без номера задачи -------------------------------------------
# Разрешённый шаблон (AGENTS.md §8): TODO(#<число>) или TODO(<ID-тикета>, напр. 0.2).
# Маркером считаем код-комментарий вида `TODO(...)`; прозаические упоминания слова
# в markdown-доках (напр. «не оставляем TODO без номера») наказывать не нужно —
# это правило, а не маркер. Поэтому:
#   * в файлах КОДА (Go/shell/SQL/yaml/Makefile) нарушение — и голый TODO, и
#     TODO(...) с невалидным ID;
#   * везде (вкл. markdown) нарушение — только TODO(...) с невалидным ID, т.к.
#     круглая скобка однозначно указывает на маркер задачи, а не на прозу.
# Маркер собираем из частей, чтобы строки-примеры в ЭТОМ скрипте не ловили сами
# себя; сам скрипт исключаем из поиска.
log "(b) каждый TODO имеет номер задачи: TODO(#NNN) или TODO(<ID-тикета>)"
TODO_MARK="TO""DO"
GOOD_RE="${TODO_MARK}\((#[0-9]+|[0-9A-Za-z][0-9A-Za-z._/-]*)\)"

# (b1) TODO(...) с невалидным ID — нарушение в любом трекуемом файле.
bracketed="$(git grep -n -I -E "${TODO_MARK}\(" -- ':!deploy/scripts/docs-check.sh' || true)"
bad_bracketed="$(printf '%s\n' "${bracketed}" | grep -v '^$' | grep -Ev "${GOOD_RE}" || true)"

# (b2) Голый TODO (без скобок) — нарушение только в файлах кода, не в *.md.
naked="$(git grep -n -I -E "${TODO_MARK}" \
	-- ':!deploy/scripts/docs-check.sh' ':!*.md' || true)"
bad_naked="$(printf '%s\n' "${naked}" | grep -v '^$' | grep -Ev "${TODO_MARK}\(" || true)"

combined="$(printf '%s\n%s\n' "${bad_bracketed}" "${bad_naked}" | grep -v '^$' | sort -u || true)"
if [ -z "${combined}" ]; then
	ok "(b) все TODO имеют номер задачи"
else
	fail "(b) найдены TODO без номера задачи:"
	printf '%s\n' "${combined}" >&2
fi

# --- (c) OpenAPI согласован ---------------------------------------------------
# Полный diff сгенерированного кода vs openapi.yaml невозможен до тикета 0.2
# (кодогенерации ещё нет). Сейчас делаем посильную проверку: openapi.yaml парсится
# как корректный YAML и содержит обязательные корневые ключи.
log "(c) ${OPENAPI_FILE}: валидный YAML + обязательные корневые ключи (openapi/info/paths)"
if [ ! -f "${OPENAPI_FILE}" ]; then
	fail "(c) не найден ${OPENAPI_FILE}"
else
	# Парсер выбираем по доступности: python3+pyyaml (точно), иначе go (yaml через go test недоступен) — фолбэк на grep.
	if command -v python3 >/dev/null 2>&1 && python3 -c 'import yaml' >/dev/null 2>&1; then
		if python3 - "${OPENAPI_FILE}" <<-'PY'
			import sys, yaml
			path = sys.argv[1]
			with open(path, encoding="utf-8") as fh:
			    doc = yaml.safe_load(fh)
			if not isinstance(doc, dict):
			    print(f"{path}: корень не является YAML-объектом", file=sys.stderr)
			    sys.exit(1)
			required = ("openapi", "info", "paths")
			missing = [k for k in required if k not in doc]
			if missing:
			    print(f"{path}: отсутствуют обязательные корневые ключи: {', '.join(missing)}", file=sys.stderr)
			    sys.exit(1)
			print(f"{path}: валидный YAML, корневые ключи на месте ({', '.join(required)})")
		PY
		then
			ok "(c) openapi.yaml валиден и содержит обязательные ключи"
		else
			fail "(c) openapi.yaml не прошёл валидацию (см. выше)"
		fi
	else
		# Фолбэк без pyyaml: грубая проверка наличия корневых ключей грепом.
		log "(c) python3+pyyaml недоступны — фолбэк на проверку наличия ключей грепом"
		miss=""
		for key in openapi info paths; do
			grep -Eq "^${key}:" "${OPENAPI_FILE}" || miss="${miss} ${key}"
		done
		if [ -n "${miss}" ]; then
			fail "(c) в ${OPENAPI_FILE} нет корневых ключей:${miss}"
		else
			ok "(c) корневые ключи openapi/info/paths присутствуют (грубая проверка)"
		fi
	fi
fi

# --- (c+) Полный codegen-diff (тикет 0.2) -------------------------------------
# Реализовано в тикете 0.2: `make generate` детерминированно генерит Go-типы и
# chi-сервер (oapi-codegen), TS-схему web (openapi-typescript) и модели БД (sqlc)
# из api/openapi.yaml + миграций; `make generate-check` затем сравнивает результат
# с закоммиченным деревом. Расхождение => генерёнка устарела => exit!=0.
#
# Тулчейн кодогенерации (oapi-codegen, sqlc, node) есть не во всех окружениях, где
# гоняется docs-check (напр. голый pre-commit). Поэтому проверку запускаем, ТОЛЬКО
# если инструменты доступны; иначе осознанно пропускаем — полноценный codegen-diff
# обеспечивает CI (тикет 0.4) через отдельную цель `make generate-check`.
log "(c+) полный codegen-diff: make generate && git diff --exit-code (если тулчейн доступен)"
GOBIN_DIR="$(go env GOPATH 2>/dev/null)/bin"
if [ -x "${GOBIN_DIR}/oapi-codegen" ] && [ -x "${GOBIN_DIR}/sqlc" ] && command -v npm >/dev/null 2>&1; then
	if make -s generate-check OAPI_CODEGEN="${GOBIN_DIR}/oapi-codegen" SQLC="${GOBIN_DIR}/sqlc" >/dev/null 2>&1; then
		ok "(c+) сгенерированный код синхронен с api/openapi.yaml"
	else
		fail "(c+) генерёнка разошлась с контрактом — запусти 'make generate' и закоммить результат"
	fi
else
	log "(c+) тулчейн кодогенерации недоступен (oapi-codegen/sqlc/npm) — пропуск; полный diff обеспечит CI (make generate-check)"
fi

# --- Итог ---------------------------------------------------------------------
if [ "${rc}" -eq 0 ]; then
	log "docs-check: все проверки пройдены"
else
	log "docs-check: есть нарушения документации (см. FAIL выше)"
fi
exit "${rc}"
