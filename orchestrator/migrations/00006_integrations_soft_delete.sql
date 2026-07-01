-- +goose Up
-- ============================================================================
-- 00006 — Мягкое удаление интеграций
-- Бизнес: DELETE /integrations/{id} требует подтверждения при активных задачах
-- и корректно завершает/отменяет их (FR B5, Gherkin §2 «Удаление требует
-- подтверждения»). Удаление — soft-delete (ADR 0004,
-- docs/adr/0004-integration-soft-delete.md): tasks.integration_id объявлен
-- ON DELETE RESTRICT (миграция 00003), а история задач хранится бессрочно
-- (FR I2) — физический DELETE integrations упал бы на FK у любой интеграции
-- с хотя бы одной когда-либо созданной задачей.
-- Закрывает: FR B5, Gherkin §2 «Удаление интеграции требует подтверждения».
-- ============================================================================

ALTER TABLE integrations ADD COLUMN deleted_at TIMESTAMPTZ;
COMMENT ON COLUMN integrations.deleted_at IS 'NULL = активна; иначе — момент мягкого удаления (ADR 0004). Owner-scoped запросы и аутентификация машины (GetIntegrationByUUIDHMAC) фильтруют deleted_at IS NULL — удалённая интеграция считается несуществующей.';

-- +goose Down
ALTER TABLE integrations DROP COLUMN deleted_at;
