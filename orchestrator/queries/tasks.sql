-- tasks.sql — запросы FSM задач (тикет 5.2, FR E1).
--
-- Назначение (бизнес): единственная точка смены tasks.status — функция
-- Transition (orchestrator/internal/task/transition.go). Эти три запроса
-- составляют её транзакцию: прочитать актуальный статус под блокировкой
-- строки → проверить переход (task.NextStatus, чистая функция) → записать
-- новый статус и добавить task_events(status_change). Схема —
-- orchestrator/migrations/00003_tasks_and_events.sql (не редактируется).

-- name: GetTaskStatusForUpdate :one
-- Читает АКТУАЛЬНЫЙ статус задачи с блокировкой строки (FOR UPDATE) —
-- единственный источник истины для Transition: вызывающая сторона НЕ
-- передаёт текущий статус явно, что исключает TOCTOU-гонки между
-- параллельными Transition для одной задачи. Блокировка также сериализует
-- конкурентные вставки в task_events для этой задачи (см. InsertNextTaskEvent
-- ниже) — без неё гонка за MAX(seq) была бы возможна.
SELECT status FROM tasks WHERE id = $1 FOR UPDATE;

-- name: UpdateTaskStatus :exec
-- Меняет статус задачи; единственное место записи tasks.status. Вызывается
-- только из Transition, ПОСЛЕ проверки допустимости перехода (NextStatus) и
-- ВНУТРИ той же транзакции, что и GetTaskStatusForUpdate/InsertNextTaskEvent.
UPDATE tasks SET status = $2, updated_at = now() WHERE id = $1;

-- name: InsertNextTaskEvent :one
-- Вставляет запись журнала событий с автоматически вычисленным следующим seq
-- в рамках задачи (FR F2) — безопасно под блокировкой строки tasks той же
-- транзакции (см. GetTaskStatusForUpdate): конкурентные Transition для одной
-- задачи сериализованы, поэтому гонка за MAX(seq) исключена. Transition
-- (тикет 5.2) использует это для события type='status_change', ref_event_id
-- всегда NULL (смена статуса не является ответом на вопрос/запрос, FR F2).
INSERT INTO task_events (task_id, seq, type, ref_event_id, payload_enc)
VALUES ($1, (SELECT COALESCE(MAX(seq), 0) + 1 FROM task_events WHERE task_id = $1), $2, $3, $4)
RETURNING *;

-- name: CreateTask :one
-- Вставляет новую задачу СО СТАТУСОМ ПО УМОЛЧАНИЮ 'created' (схема,
-- migrations/00003) — переход в 'queued' и запись task_events(status_change)
-- выполняются ОТДЕЛЬНО, сразу после вставки, через
-- task.Transitioner.Transition(ctx, id, task.TriggerEnqueued): тикет 5.2
-- сделал Transitioner единственной точкой смены tasks.status, поэтому
-- обработчик POST /tasks (тикет 5.3) не пишет 'queued'/task_events напрямую.
-- Коллизия (user_id, idempotency_key) — SQLSTATE 23505 (uq_tasks_idempotency);
-- обработчик отвечает 409 (полноценное «вернуть существующую задачу, 200» —
-- отдельный тикет 5.5).
INSERT INTO tasks (user_id, integration_id, text_enc, idempotency_key)
VALUES ($1, $2, $3, $4)
RETURNING *;
