#!/bin/sh
# deploy/backup/backup.sh — один прогон резервного копирования Postgres (тикет 11.5).
#
# Назначение (бизнес): история задач/событий хранится в БД бессрочно (FR I2,
# docs/01_tech_stack §"Бэкап БД"). Регулярный pg_dump — страховка от потери этой
# истории: если диск/инстанс погибнет, восстанавливаемся из последнего дампа.
# Скрипт вызывается по расписанию (crond, см. entrypoint.sh) и вручную
# (`make backup`).
#
# Как устроено (тех): pg_dump в custom-формате (-Fc, сжатый, восстанавливается
# через pg_restore — см. restore-check.sh), запись в $BACKUP_DIR под именем
# <db>_<UTC-таймстамп>.dump. Запись атомарна: пишем в *.partial и переименовываем
# только после успешного дампа, чтобы прерванный бэкап не сошёл за валидный.
# После успешной записи применяется ретенция по возрасту файла.
#
# Конфигурация — из окружения (секреты не хардкодятся, см. AGENTS.md §8):
#   PGHOST                 хост Postgres (по умолчанию postgres — имя сервиса compose)
#   PGPORT                 порт (по умолчанию 5432)
#   POSTGRES_USER          пользователь БД (по умолчанию agentify)
#   POSTGRES_DB            имя базы (по умолчанию agentify)
#   POSTGRES_PASSWORD      пароль (обязателен; тот же секрет, что у сервиса postgres)
#   BACKUP_DIR             каталог дампов (по умолчанию /backups — том pg_backups)
#   BACKUP_RETENTION_DAYS  сколько дней хранить дампы (по умолчанию 7)

set -eu

PGHOST="${PGHOST:-postgres}"
PGPORT="${PGPORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-agentify}"
POSTGRES_DB="${POSTGRES_DB:-agentify}"
BACKUP_DIR="${BACKUP_DIR:-/backups}"
BACKUP_RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-7}"

# PGPASSWORD читает pg_dump напрямую; отсутствие пароля — фатально (нет смысла
# продолжать, дамп всё равно не снимется).
PGPASSWORD="${POSTGRES_PASSWORD:?POSTGRES_PASSWORD обязателен — задайте в deploy/.env}"
export PGPASSWORD

mkdir -p "$BACKUP_DIR"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
file="${BACKUP_DIR}/${POSTGRES_DB}_${ts}.dump"
tmp="${file}.partial"

echo "[backup] $(date -u '+%Y-%m-%dT%H:%M:%SZ') pg_dump ${POSTGRES_DB}@${PGHOST}:${PGPORT} -> ${file}"
pg_dump -h "$PGHOST" -p "$PGPORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc -f "$tmp"
mv "$tmp" "$file"
echo "[backup] готово, размер $(du -h "$file" | cut -f1)"

# Ретенция: сами данные в БД бессрочны (FR I2), но старые ФАЙЛЫ дампов ротируем,
# чтобы том не рос бесконечно — для восстановимости достаточно свежих копий.
find "$BACKUP_DIR" -maxdepth 1 -type f -name "${POSTGRES_DB}_*.dump" \
	-mtime +"$BACKUP_RETENTION_DAYS" -print -delete
echo "[backup] ретенция применена (удалены дампы старше ${BACKUP_RETENTION_DAYS} дн.)"
