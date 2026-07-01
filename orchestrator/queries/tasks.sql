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
-- обработчик перехватывает её и обслуживает повтор через
-- GetTaskByUserAndIdempotencyKey ниже (тикет 5.5, FR E7, §4 «Защита от
-- двойной отправки»): существующая задача возвращается с 200, дубль не
-- создаётся.
INSERT INTO tasks (user_id, integration_id, text_enc, idempotency_key)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetTaskByUserAndIdempotencyKey :one
-- Находит уже созданную задачу по (user_id, idempotency_key) после того, как
-- INSERT в CreateTask упал на uq_tasks_idempotency (SQLSTATE 23505) — это и
-- есть дедуп повторной постановки (тикет 5.5, FR E7): вместо создания дубля
-- обработчик POST /tasks перечитывает уже существующую строку и возвращает
-- её с 200, не трогая Transitioner и не публикуя task_assigned повторно (эта
-- задача уже прошла весь путь при первой, не повторной, постановке).
SELECT * FROM tasks WHERE user_id = $1 AND idempotency_key = $2;

-- name: GetTaskByIDAndUser :one
-- Ищет задачу по id, owner-scoped прямо в SQL (FR A4, I3) — чужая/несуществующая
-- задача неотличимы, единый 404 (тот же приём, что GetIntegrationByIDAndUser,
-- тикет 2.2). Используется PostTasksIdAnswer (тикет 6.1) для проверки владения
-- задачей перед применением ответа пользователя.
SELECT * FROM tasks WHERE id = $1 AND user_id = $2;

-- name: GetTaskByIDAndIntegration :one
-- Ищет задачу по id, scoped по integration_id, а не по user_id (FR A4, I3,
-- аналогия с GetTaskByIDAndUser выше) — используется на WS-пути (handleAgentQuestion,
-- machine_ws.go, тикет 6.1), где аутентифицирована МАШИНА (integration_id), а не
-- пользователь: проверяет, что вопрос агента адресован задаче именно ЭТОЙ
-- интеграции, не давая одной машине инжектировать событие в чужую задачу.
SELECT * FROM tasks WHERE id = $1 AND integration_id = $2;

-- name: ListAgentQuestionEventsByTask :many
-- Возвращает все события agent_question задачи, самые новые первыми — источник
-- для сопоставления ответа пользователя (question_id из тела запроса) с
-- конкретной записью task_events (её id становится ref_event_id ответа, тикет
-- 6.1, FR F2). Сопоставление по question_id внутри payload_enc выполняется НА
-- СТОРОНЕ GO (после json.Unmarshal), а не SQL-выражением вроде payload_enc::jsonb —
-- payload_enc помимо джейсона со временем станет зашифрованным (TODO(11.1),
-- как text_enc), и SQL-side JSON-экстракция тогда молча сломается.
SELECT * FROM task_events WHERE task_id = $1 AND type = 'agent_question' ORDER BY seq DESC;

-- name: ListCommandApprovalRequestEventsByTask :many
-- Возвращает все события command_approval_request задачи, самые новые первыми —
-- источник для сопоставления request_id из тела PostTasksIdApprove с конкретной
-- записью task_events (её id становится ref_event_id решения, тикет 6.4, FR F3).
-- Тот же приём, что и у ListAgentQuestionEventsByTask выше (тикет 6.1):
-- сопоставление по request_id внутри payload_enc выполняется НА СТОРОНЕ GO
-- (после json.Unmarshal), а не SQL-выражением вроде payload_enc::jsonb —
-- payload_enc помимо джейсона со временем станет зашифрованным (TODO(11.1),
-- как text_enc), и SQL-side JSON-экстракция тогда молча сломается.
SELECT * FROM task_events WHERE task_id = $1 AND type = 'command_approval_request' ORDER BY seq DESC;

-- name: ListRunningTasksWithStaleMachine :many
-- «Зависание» машины (тикет 5.7, FR E5, protocol.md §6: STALE_THRESHOLD):
-- находит активные (running/waiting_user) задачи, чья интеграция не подавала
-- heartbeat дольше порога — cutoff считает вызывающая сторона
-- (task.StaleWorker), здесь только сравнение с last_seen_at. last_seen_at
-- IS NULL тоже считается «зависла» — защитный случай: MarkIntegrationOnline
-- (queries/integrations.sql) всегда пишет last_seen_at ОДНОВРЕМЕННО с
-- status='online', так что NULL означает «от этой машины вообще никогда не
-- было heartbeat», а активная задача у такой интеграции быть не должна.
-- Результат передаётся task.Transitioner.Transition(..., TriggerTimeout).
SELECT tasks.id FROM tasks
JOIN integrations ON integrations.id = tasks.integration_id
WHERE tasks.status IN ('running', 'waiting_user')
  AND (integrations.last_seen_at IS NULL OR integrations.last_seen_at < $1);

-- name: ListStaleTasksWithRecoveredMachine :many
-- Возврат «зависшей» машины (тикет 5.7, FR E5, protocol.md §6): находит
-- задачи в статусе stale, чья интеграция снова свежо подавала heartbeat
-- (last_seen_at не старше cutoff) — задача должна продолжиться, stale →
-- running. Результат передаётся
-- task.Transitioner.Transition(..., TriggerMachineRecovered).
SELECT tasks.id FROM tasks
JOIN integrations ON integrations.id = tasks.integration_id
WHERE tasks.status = 'stale'
  AND integrations.last_seen_at IS NOT NULL
  AND integrations.last_seen_at >= $1;

-- name: ListWaitingUserTasksWithStaleQuestion :many
-- «Таймаут ответа» (тикет 6.7, FR F5): находит задачи в waiting_user, чей
-- САМЫЙ ПОСЛЕДНИЙ (по seq) agent_question устарел дольше порога — cutoff
-- считает вызывающая сторона (task.AnswerTimeoutWorker), здесь только
-- сравнение с created_at найденной записи. Оконная функция ROW_NUMBER() (а не
-- JOIN LATERAL — sqlc v1.27.0 без live-database анализа не разрешает голый
-- параметр $1 при сравнении с колонкой derived table/LATERAL без явного
-- приведения типа) и не просто MAX(created_at) по task_events, потому что
-- вызывающей стороне нужен именно id КОНКРЕТНОЙ записи task_events
-- (question_event_id) — для дедупликации повторных напоминаний по одному и
-- тому же вопросу между тиками воркера, а не только временная метка
-- последнего вопроса. Результат передаётся в notify.Notification
-- (напоминание пользователю) и, при настроенном поведении auto_cancel, в
-- task.Transitioner.Transition(..., TriggerCancelRequested).
SELECT tasks.id AS task_id, tasks.user_id AS user_id, latest.id AS question_event_id, latest.created_at AS question_created_at
FROM tasks
JOIN (
    SELECT id, task_id, created_at,
           ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY seq DESC) AS rn
    FROM task_events
    WHERE type = 'agent_question'
) latest ON latest.task_id = tasks.id AND latest.rn = 1
WHERE tasks.status = 'waiting_user'
  AND latest.created_at < $1::timestamptz;
