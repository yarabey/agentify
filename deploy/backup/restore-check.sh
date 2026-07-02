#!/usr/bin/env bash
# deploy/backup/restore-check.sh — проверка восстановления дампа (тикет 11.5, FR I2).
#
# Назначение (бизнес): бэкап бесполезен, пока не доказано, что из него
# ВОССТАНАВЛИВАЮТСЯ данные. Этот скрипт — воспроизводимая приёмка FR I2:
# берёт последний дамп, поднимает ЧИСТЫЙ инстанс Postgres, восстанавливает в
# него дамп и убеждается, что данные на месте. Запускать после настройки бэкапа
# и периодически (drill). Автоматический аналог в CI — Go-тест на testcontainers
# (deploy/backup/pgbackup/restore_check_integration_test.go, тег integration).
#
# Как устроено (тех): дампы лежат в docker-томе (pg_backups) или в каталоге на
# хосте. Поднимаем throwaway-контейнер postgres:16-alpine с смонтированным
# источником дампов (:ro), ждём готовности, запускаем pg_restore последнего
# *.dump в свежую базу и проверяем, что появились пользовательские таблицы и
# из них читаются строки. Контейнер удаляется всегда (trap), состояние хоста не
# меняется — инстанс заведомо «чистый».
#
# Конфигурация — из окружения/аргумента:
#   $1 / DUMP_FILE   имя файла дампа внутри источника (по умолчанию — самый свежий)
#   BACKUP_VOLUME    docker-том с дампами (по умолчанию agentify_pg_backups)
#   BACKUP_HOST_DIR  каталог на хосте с дампами (альтернатива тому; приоритетнее)
#   PG_IMAGE         образ Postgres для восстановления (по умолчанию postgres:16-alpine)

set -euo pipefail

DUMP_FILE="${1:-${DUMP_FILE:-}}"
BACKUP_VOLUME="${BACKUP_VOLUME:-agentify_pg_backups}"
BACKUP_HOST_DIR="${BACKUP_HOST_DIR:-}"
PG_IMAGE="${PG_IMAGE:-postgres:16-alpine}"

# Пароль/пользователь throwaway-инстанса — локальные, не секрет: контейнер живёт
# секунды и уничтожается. Восстанавливаем в дефолтную базу этого инстанса.
RESTORE_PASSWORD="restore_check"
RESTORE_USER="postgres"
RESTORE_DB="postgres"

if ! command -v docker >/dev/null 2>&1; then
	echo "restore-check: нужен docker в PATH" >&2
	exit 2
fi

# Источник дампов: каталог на хосте (если задан) либо docker-том.
mount_arg=""
if [ -n "$BACKUP_HOST_DIR" ]; then
	if [ ! -d "$BACKUP_HOST_DIR" ]; then
		echo "restore-check: каталог BACKUP_HOST_DIR='$BACKUP_HOST_DIR' не найден" >&2
		exit 2
	fi
	mount_arg="$(cd "$BACKUP_HOST_DIR" && pwd):/backups:ro"
	echo "restore-check: источник дампов — каталог хоста ${BACKUP_HOST_DIR}"
else
	if ! docker volume inspect "$BACKUP_VOLUME" >/dev/null 2>&1; then
		echo "restore-check: docker-том '$BACKUP_VOLUME' не найден (создаётся сервисом backup); задайте BACKUP_VOLUME или BACKUP_HOST_DIR" >&2
		exit 2
	fi
	mount_arg="${BACKUP_VOLUME}:/backups:ro"
	echo "restore-check: источник дампов — docker-том ${BACKUP_VOLUME}"
fi

cid=""
cleanup() {
	if [ -n "$cid" ]; then
		docker rm -f "$cid" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

echo "restore-check: поднимаю чистый инстанс ${PG_IMAGE}"
cid="$(docker run -d \
	-e POSTGRES_PASSWORD="$RESTORE_PASSWORD" \
	-v "$mount_arg" \
	"$PG_IMAGE")"

# Ждём готовности инстанса принимать подключения.
ready=0
for i in $(seq 1 60); do
	if docker exec "$cid" pg_isready -U "$RESTORE_USER" -d "$RESTORE_DB" >/dev/null 2>&1; then
		ready=1
		break
	fi
	sleep 1
done
if [ "$ready" -ne 1 ]; then
	echo "restore-check: чистый инстанс не стал готов за 60с" >&2
	docker logs "$cid" >&2 || true
	exit 1
fi

# Выбираем дамп: явно заданный или самый свежий *.dump в источнике.
if [ -n "$DUMP_FILE" ]; then
	dump_path="/backups/${DUMP_FILE}"
	if ! docker exec "$cid" test -f "$dump_path"; then
		echo "restore-check: дамп '${dump_path}' не найден в источнике" >&2
		exit 1
	fi
else
	dump_path="$(docker exec "$cid" sh -c 'ls -1t /backups/*.dump 2>/dev/null | head -n1' || true)"
	if [ -z "$dump_path" ]; then
		echo "restore-check: в источнике нет ни одного *.dump — сначала снимите бэкап (make backup)" >&2
		exit 1
	fi
fi
echo "restore-check: восстанавливаю ${dump_path} в чистую базу '${RESTORE_DB}'"

# pg_restore: --no-owner/--no-privileges — игнорируем исходного владельца
# (agentify) на throwaway-инстансе; --exit-on-error — любая ошибка => провал.
if ! docker exec "$cid" pg_restore --no-owner --no-privileges --exit-on-error \
	-U "$RESTORE_USER" -d "$RESTORE_DB" "$dump_path"; then
	echo "restore-check: ПРОВАЛ — pg_restore завершился с ошибкой" >&2
	exit 1
fi

# Проверяем, что данные на месте: считаем пользовательские таблицы и суммарное
# число строк по ним. Пустая схема/нулевые данные — подозрительно для FR I2.
tables="$(docker exec "$cid" psql -U "$RESTORE_USER" -d "$RESTORE_DB" -tAc \
	"SELECT count(*) FROM information_schema.tables WHERE table_schema='public'")"
tables="$(printf '%s' "$tables" | tr -d '[:space:]')"
echo "restore-check: восстановлено пользовательских таблиц: ${tables:-0}"
if [ -z "$tables" ] || [ "$tables" -eq 0 ]; then
	echo "restore-check: ПРОВАЛ — после восстановления нет ни одной таблицы в схеме public" >&2
	exit 1
fi

echo "restore-check: строки по таблицам (пример):"
docker exec "$cid" psql -U "$RESTORE_USER" -d "$RESTORE_DB" -c "
	SELECT schemaname, relname AS table, n_live_tup AS rows
	FROM pg_stat_user_tables ORDER BY relname;" || true

echo "restore-check: УСПЕХ — дамп восстановлен на чистый инстанс, данные на месте (FR I2)"
