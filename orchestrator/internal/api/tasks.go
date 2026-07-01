package api

// tasks.go — постановка задачи в очередь к машине (тикет 5.3, FR E1, E4, §4
// «Постановка задачи из канала» (web/telegram)).
//
// Назначение (бизнес): владелец интеграции ставит задачу своей машине текстом
// (POST /tasks + заголовок Idempotency-Key, FR E7 — сам дедуп по ключу вне
// скоупа этого тикета, см. ниже). Владение интеграцией проверяется
// owner-scoped запросом (FR A4, I3), как и в integrations.go: чужая или
// несуществующая интеграция неотличимы, единый 404. Задача создаётся со
// статусом 'created', СРАЗУ переводится в 'queued' через
// task.Transitioner (тикет 5.2, единственная точка смены tasks.status и
// записи task_events(status_change) — FR E1), и публикуется машине конвертом
// task_assigned в топик machine.commands (protocol.md §4), партиционированным
// по integration_id (ADR 0001, PartitionKeyIntegrationID — НЕ task_id, вопреки
// неточной формулировке текста тикета: machine.commands ВСЕГДА
// партиционируется по машине, чтобы сохранить порядок команд для неё).
// GET /tasks* (список/история задачи) — отдельный тикет 8.6, здесь не
// реализуется (остаётся 501 через Unimplemented). Дедуп постановки по
// Idempotency-Key (возврат существующей задачи с 200 вместо создания дубля)
// — отдельный тикет 5.5; здесь при коллизии уникального индекса
// uq_tasks_idempotency (SQLSTATE 23505) обработчик временно отвечает 409
// Conflict.
//
// Как устроено (тех): INSERT новой задачи (db.CreateTask,
// orchestrator/queries/tasks.sql) вставляет строку со статусом ПО УМОЛЧАНИЮ
// 'created' (схема, migrations/00003) — обработчик НЕ пишет 'queued' или
// task_events напрямую, это исключительно ответственность
// task.Transitioner.Transition(ctx, taskID, task.TriggerEnqueued),
// зарегистрированного через Server.SetTransitioner (orchestrator/main.go).
// Публикация в Redpanda идёт через узкий интерфейс CommandPublisher
// (см. server.go), зарегистрированный через Server.SetCommandPublisher —
// тот же структурный приём, что у AckSink/EventSink: *bus.Producer
// удовлетворяет CommandPublisher без отдельного адаптера.
import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// PostTasks реализует POST /tasks — постановку задачи в очередь к машине
// (FR E1, E4, §4 «Постановка задачи из канала»).
//
// Алгоритм: провалидировать тело (text обязателен) → проверить владение
// интеграцией (integration_id из тела, owner-scoped, единый 404) → вставить
// задачу (db.CreateTask, статус 'created'; коллизия Idempotency-Key —
// SQLSTATE 23505 — временный 409, полноценный дедуп — тикет 5.5) → перевести
// в 'queued' через task.Transitioner.Transition (единственная точка смены
// статуса, тикет 5.2) → опубликовать конверт task_assigned в
// machine.commands, партиционированный по integration_id (ADR 0001) →
// вернуть 201 с Task.
func (s *Server) PostTasks(w http.ResponseWriter, r *http.Request, params PostTasksParams) {
	ctx := r.Context()

	userID, ok := UserIDFromContext(ctx)
	if !ok {
		// Защищённый маршрут (auth-middleware, тикет 1.4) — отсутствие user_id
		// в контексте означало бы дыру в middleware, не штатный
		// пользовательский случай. Тот же 401, что и у остальных обработчиков.
		writeUnauthorized(w)
		return
	}

	var req TaskCreate
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "тело запроса не является валидным JSON")
		return
	}
	if strings.TrimSpace(req.Text) == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "text обязателен")
		return
	}

	integrationID := uuid.UUID(req.IntegrationId)

	// Владение интеграцией — owner-scoped прямо в SQL, как и в integrations.go:
	// чужая/несуществующая интеграция неотличимы, единый 404.
	if _, err := s.queries.GetIntegrationByIDAndUser(ctx, db.GetIntegrationByIDAndUserParams{
		ID:     pgtype.UUID{Bytes: integrationID, Valid: true},
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeIntegrationNotFound(w)
			return
		}
		s.logError("GetIntegrationByIDAndUser", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	idempotencyKey := params.IdempotencyKey
	row, err := s.queries.CreateTask(ctx, db.CreateTaskParams{
		UserID:         pgtype.UUID{Bytes: userID, Valid: true},
		IntegrationID:  pgtype.UUID{Bytes: integrationID, Valid: true},
		TextEnc:        []byte(req.Text), // TODO(11.1): открытым текстом до единого крипто-модуля at-rest
		IdempotencyKey: &idempotencyKey,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			// uq_tasks_idempotency: повтор с тем же (user_id, idempotency_key).
			// Полноценный «вернуть существующую задачу, 200» — тикет 5.5.
			writeError(w, http.StatusConflict, "idempotency_key_conflict", "задача с этим Idempotency-Key уже существует")
			return
		}
		s.logError("CreateTask", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	transitioner := s.getTransitioner()
	if transitioner == nil {
		s.logError("PostTasks", errors.New("transitioner не настроен"))
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}
	_, to, err := transitioner.Transition(ctx, row.ID, task.TriggerEnqueued)
	if err != nil {
		s.logError("Transition(TriggerEnqueued)", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	publisher := s.getCommandPublisher()
	if publisher == nil {
		s.logError("PostTasks", errors.New("CommandPublisher не настроен"))
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	payload, err := json.Marshal(bus.TaskAssignedPayload{Text: req.Text})
	if err != nil {
		s.logError("json.Marshal(TaskAssignedPayload)", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}
	taskIDStr := uuid.UUID(row.ID.Bytes).String()
	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		TaskID:          &taskIDStr,
		IntegrationID:   integrationID.String(),
		Type:            bus.MessageTypeTaskAssigned,
		Seq:             1, // первая команда в командном потоке этой новой задачи
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}
	if err := publisher.PublishKeyed(ctx, bus.TopicMachineCommands, bus.PartitionKeyIntegrationID, env); err != nil {
		s.logError("PublishKeyed(task_assigned)", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	writeJSON(w, http.StatusCreated, toTask(row, to))
}

// toTask конвертирует строку БД (уже после Transition) в контрактный Task.
// status берём из аргумента to (результат Transition), а не row.Status —
// row получена ДО вызова Transition и содержит ещё 'created'. updated_at
// приближённо берётся как момент ответа (Transition сам обновляет
// updated_at=now() в БД, но не возвращает обновлённую строку — перечитывать
// её отдельным запросом ради одного поля не нужно, точность в пределах
// одного HTTP-запроса не имеет бизнес-значения).
func toTask(row db.Task, status task.Status) Task {
	id := uuid.UUID(row.ID.Bytes)
	integrationID := uuid.UUID(row.IntegrationID.Bytes)
	text := string(row.TextEnc)
	taskStatus := TaskStatus(status)
	createdAt := row.CreatedAt.Time
	updatedAt := time.Now().UTC()

	return Task{
		Id:            &id,
		IntegrationId: &integrationID,
		Text:          &text,
		Status:        &taskStatus,
		CreatedAt:     &createdAt,
		UpdatedAt:     &updatedAt,
	}
}
