-- +goose Up
-- ============================================================================
-- 00005 — Изоляция данных между пользователями (Row Level Security)
-- Бизнес: данные одного пользователя никогда не должны быть видны/доступны
-- другому. Основной механизм — фильтр по user_id в каждом запросе на уровне
-- приложения; эта миграция добавляет Postgres RLS как страховку поверх него
-- (см. docs/01_tech_stack_and_architecture.md: «фильтр по user_id + (опц.)
-- Postgres RLS как страховка»).
-- Закрывает: FR A4, I3. Gherkin §1 «Изоляция данных между пользователями».
-- ============================================================================

-- Сессионная переменная app.user_id выставляется приложением в начале каждой
-- транзакции (см. orchestrator/internal/db — SetAppUserID) через
-- set_config('app.user_id', $1, true) — параметризованно, без конкатенации
-- строк. missing_ok=true в current_setting ниже: если переменная не
-- выставлена, current_setting возвращает NULL, сравнение user_id = NULL даёт
-- NULL (не true) — политика по умолчанию запрещает доступ (fail-closed), а
-- не падает с ошибкой.
--
-- ВАЖНО: RLS не действует на владельца таблицы и суперпользователя, даже с
-- FORCE ROW LEVEL SECURITY — это ограничение самого Postgres. Та же роль БД,
-- что выполняет миграции (владелец таблиц), используется сейчас и
-- приложением (см. deploy/docker-compose.yml ORCH_DATABASE_URL), поэтому в
-- текущей топологии этот слой реально как «страховка для будущего» (напр.
-- перевод runtime-соединения на менее привилегированную роль) — основным
-- механизмом изоляции остаётся обязательный фильтр user_id в каждом запросе
-- к tasks/integrations на уровне приложения. FORCE ROW LEVEL SECURITY всё
-- равно включаем — это правильное поведение по умолчанию и не вредит.

ALTER TABLE integrations ENABLE ROW LEVEL SECURITY;
ALTER TABLE integrations FORCE ROW LEVEL SECURITY;
CREATE POLICY integrations_isolation ON integrations
    USING (user_id = current_setting('app.user_id', true)::uuid);
COMMENT ON POLICY integrations_isolation ON integrations IS
    'Страховка поверх обязательного фильтра user_id в запросах приложения (FR A4, I3).';

ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE tasks FORCE ROW LEVEL SECURITY;
CREATE POLICY tasks_isolation ON tasks
    USING (user_id = current_setting('app.user_id', true)::uuid);
COMMENT ON POLICY tasks_isolation ON tasks IS
    'Страховка поверх обязательного фильтра user_id в запросах приложения (FR A4, I3).';

-- +goose Down
DROP POLICY tasks_isolation ON tasks;
ALTER TABLE tasks DISABLE ROW LEVEL SECURITY;

DROP POLICY integrations_isolation ON integrations;
ALTER TABLE integrations DISABLE ROW LEVEL SECURITY;
