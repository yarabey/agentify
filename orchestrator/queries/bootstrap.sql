-- bootstrap.sql — единственный «затравочный» запрос для sqlc (тикет 0.2).
--
-- Назначение (тех): sqlc v1.27.0 завершается ошибкой, если каталог queries пуст
-- («no queries contained in paths»). Бизнес-запросы добавит тикет 1.1; до тех пор
-- этот тривиальный health-запрос нужен, чтобы `make generate` (sqlc) отрабатывал
-- и генерил orchestrator/internal/db/models.go из схемы миграций.
--
-- Запрос не обращается к таблицам (SELECT 1) и удалится/заменится в 1.1.

-- name: BootstrapPing :one
SELECT 1 AS ok;
