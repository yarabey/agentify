# deploy/backup/Dockerfile — образ сервиса регулярного бэкапа Postgres (тикет 11.5).
#
# Назначение (бизнес): контейнер, который по cron снимает pg_dump базы agentify
# (страховка бессрочной истории, FR I2).
#
# Как устроено (тех): база — тот же postgres:16-alpine, что и сервис postgres в
# docker-compose.yml. Это важно: версия клиентских утилит (pg_dump/pg_restore)
# обязана совпадать с версией сервера (мажор 16), иначе pg_dump может отказать.
# В образе уже есть pg_dump/pg_restore/psql и busybox crond — ставить ничего не
# нужно. Кладём наши скрипты и запускаем entrypoint (настраивает cron и держит
# crond в foreground).

FROM postgres:16-alpine

# restore-check.sh НЕ кладём в образ: это host-скрипт (нужен docker CLI), он
# поднимает отдельный throwaway-инстанс снаружи (см. deploy/README.md).
COPY backup.sh /usr/local/bin/backup.sh
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/backup.sh /usr/local/bin/entrypoint.sh

# Каталог дампов по умолчанию (монтируется том pg_backups в compose-оверрае).
ENV BACKUP_DIR=/backups
VOLUME ["/backups"]

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
