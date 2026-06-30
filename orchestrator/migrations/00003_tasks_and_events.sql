-- +goose Up
-- ============================================================================
-- 00003 — Задачи и журнал событий
-- Бизнес: явный жизненный цикл задачи; «завершена» только по явному подтверждению
-- пользователя; полная история с аудитом.
-- Закрывает: FR E1–E7, F1–F4, H1–H3, I2. Gherkin §4–§10. FSM: docs/Жизненный цикл задачи.md.
-- ============================================================================

CREATE TABLE tasks (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    integration_id UUID NOT NULL REFERENCES integrations(id) ON DELETE RESTRICT,
    text_enc       BYTEA NOT NULL,                     -- текст запроса, зашифрован at-rest (FR I1)
    status         TEXT NOT NULL DEFAULT 'created'
        CHECK (status IN ('created','queued','running','waiting_user',
                          'awaiting_confirm','completed','failed','cancelled','stale')),
    -- Дедуп постановки: одинаковый ключ от одного пользователя не создаёт дубль (FR E7).
    idempotency_key TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_tasks_user ON tasks(user_id);
CREATE INDEX idx_tasks_integration ON tasks(integration_id);
CREATE UNIQUE INDEX uq_tasks_idempotency ON tasks(user_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;                 -- защита от двойной отправки (FR E7)
COMMENT ON TABLE tasks IS 'Задачи. Статус — FSM (docs/Жизненный цикл задачи.md). История бессрочна (FR I2).';
COMMENT ON COLUMN tasks.status IS 'completed выставляется ТОЛЬКО явным подтверждением пользователя (FR E2).';

-- Единый упорядоченный журнал событий задачи: вопросы агента, ответы пользователя,
-- запросы/решения по согласованию команд, смены статуса, прогресс, ошибки.
-- Служит и историей (FR H1), и аудитом (FR F4).
CREATE TABLE task_events (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id    UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    seq        BIGINT NOT NULL,                        -- порядок в рамках задачи (FR F2, §126)
    type       TEXT NOT NULL CHECK (type IN (
                   'status_change','agent_question','user_answer',
                   'command_approval_request','user_decision',
                   'agent_progress','agent_completed','error')),
    -- Привязка ответа к конкретному вопросу/запросу (FR F2, Gherkin §5).
    ref_event_id UUID REFERENCES task_events(id),
    payload_enc  BYTEA NOT NULL,                       -- содержимое зашифровано at-rest (FR I1)
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_task_events_seq ON task_events(task_id, seq);
CREATE INDEX idx_task_events_task ON task_events(task_id);
COMMENT ON TABLE task_events IS 'Журнал событий = история (FR H1) + аудит (FR F4). seq хранит порядок (FR F2).';
COMMENT ON COLUMN task_events.ref_event_id IS 'Ответ/решение ссылается на свой вопрос/запрос (FR F2).';

-- +goose Down
DROP TABLE task_events;
DROP TABLE tasks;
