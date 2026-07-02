#!/bin/sh
# deploy/backup/entrypoint.sh — точка входа сервиса backup (тикет 11.5).
#
# Назначение (бизнес): держать демон, который по расписанию снимает pg_dump —
# регулярный бэкап истории (FR I2). Отдельный лёгкий сервис рядом с Postgres,
# а не задача в оркестраторе: бэкап — инфраструктурная забота деплоя, не
# бизнес-логика сервисов (не трогаем сервисный Go-код).
#
# Как устроено (тех): busybox crond из образа postgres:16-alpine. crond
# запускает джобы с МИНИМАЛЬНЫМ окружением (без переменных контейнера), поэтому
# конфигурацию/секреты сохраняем в /etc/backup.env (безопасно экранируя) и
# джоба сначала подхватывает его, затем зовёт backup.sh. Один прогон делаем
# сразу на старте — чтобы у свежего стека сразу была базовая копия, не дожидаясь
# первого срабатывания cron. Далее crond в foreground держит контейнер живым.
#
# Конфигурация — из окружения (см. backup.sh + BACKUP_SCHEDULE ниже):
#   BACKUP_SCHEDULE  crontab-расписание (по умолчанию "0 3 * * *" — ежедневно 03:00 UTC)

set -eu

BACKUP_SCHEDULE="${BACKUP_SCHEDULE:-0 3 * * *}"
BACKUP_DIR="${BACKUP_DIR:-/backups}"

mkdir -p "$BACKUP_DIR"

# Пробрасываем окружение в файл, читаемый cron-джобой. Значения экранируем
# одинарными кавычками (пароль может содержать спецсимволы/пробелы).
esc() { printf "%s" "$1" | sed "s/'/'\\\\''/g"; }
put() { printf "export %s='%s'\n" "$1" "$(esc "$2")"; }
{
	put PGHOST "${PGHOST:-postgres}"
	put PGPORT "${PGPORT:-5432}"
	put POSTGRES_USER "${POSTGRES_USER:-agentify}"
	put POSTGRES_DB "${POSTGRES_DB:-agentify}"
	put POSTGRES_PASSWORD "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD обязателен — задайте в deploy/.env}"
	put BACKUP_DIR "$BACKUP_DIR"
	put BACKUP_RETENTION_DAYS "${BACKUP_RETENTION_DAYS:-7}"
} > /etc/backup.env
chmod 600 /etc/backup.env

# crontab: подхватить окружение и запустить бэкап; вывод — в лог тома и в stdout
# контейнера (/proc/1/fd/1), чтобы `docker logs` показывал ход бэкапа.
mkdir -p /etc/crontabs
printf '%s . /etc/backup.env; /usr/local/bin/backup.sh >> %s/backup.log 2>&1\n' \
	"$BACKUP_SCHEDULE" "$BACKUP_DIR" > /etc/crontabs/root

echo "[backup] расписание: '${BACKUP_SCHEDULE}', каталог: ${BACKUP_DIR}"

# Базовый бэкап на старте (не валит контейнер, если БД ещё не готова — cron
# доснимет по расписанию).
if . /etc/backup.env && /usr/local/bin/backup.sh; then
	echo "[backup] стартовый бэкап снят"
else
	echo "[backup] стартовый бэкап не удался (снимется по расписанию)" >&2
fi

# crond в foreground: -f не демонизировать, -l 8 — уровень логирования.
exec crond -f -l 8
