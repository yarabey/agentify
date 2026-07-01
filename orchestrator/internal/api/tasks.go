package api

// tasks.go — постановка задачи в очередь к машине (тикет 5.3, FR E1, E4, §4
// «Постановка задачи из канала» (web/telegram)) и ответ пользователя на
// вопрос агента (тикет 6.1, FR F1, F2, PostTasksIdAnswer, Gherkin §5 «Агент
// задаёт вопрос и получает ответ»).
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
// Idempotency-Key (тикет 5.5, FR E7, §4 «Защита от двойной отправки»):
// уникальный индекс uq_tasks_idempotency (user_id, idempotency_key) в БД —
// источник истины, обработчик лишь реагирует на его коллизию (SQLSTATE
// 23505), перечитывает уже существующую задачу и возвращает её с 200, не
// создавая дубль и не повторяя Transition/публикацию task_assigned.
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
// (FR E1, E4, §4 «Постановка задачи из канала») с дедупом повторной
// постановки по заголовку Idempotency-Key (тикет 5.5, FR E7, §4 «Защита от
// двойной отправки»).
//
// Алгоритм: провалидировать тело (text обязателен) → проверить владение
// интеграцией (integration_id из тела, owner-scoped, единый 404) → вставить
// задачу (db.CreateTask, статус 'created'). Если INSERT упал на коллизии
// уникального индекса uq_tasks_idempotency (user_id, idempotency_key,
// SQLSTATE 23505) — это повтор той же постановки: перечитать существующую
// задачу (GetTaskByUserAndIdempotencyKey) и вернуть её с 200, НЕ вызывая
// Transitioner и НЕ публикуя task_assigned повторно (задача уже прошла этот
// путь при первой, не повторной, постановке). Иначе (задача только что
// создана) — перевести в 'queued' через task.Transitioner.Transition
// (единственная точка смены статуса, тикет 5.2) → опубликовать конверт
// task_assigned в machine.commands, партиционированный по integration_id
// (ADR 0001) → вернуть 201 с Task.
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
			// uq_tasks_idempotency: повтор с тем же (user_id, idempotency_key) —
			// дедуп постановки (тикет 5.5, FR E7). Задача уже создана и прошла
			// свой путь (queued + task_assigned) при первой постановке —
			// перечитываем её и возвращаем 200, не дублируя ни строку в tasks,
			// ни Transition, ни публикацию.
			existing, getErr := s.queries.GetTaskByUserAndIdempotencyKey(ctx, db.GetTaskByUserAndIdempotencyKeyParams{
				UserID:         pgtype.UUID{Bytes: userID, Valid: true},
				IdempotencyKey: &idempotencyKey,
			})
			if getErr != nil {
				// Гипотетическая гонка: INSERT сообщил о конфликте, но строка
				// почему-то не находится (например, конкурентная транзакция ещё
				// не закоммитилась). Это не штатный пользовательский случай —
				// внутренняя ошибка, а не 409/404.
				s.logError("GetTaskByUserAndIdempotencyKey", getErr)
				writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
				return
			}
			writeJSON(w, http.StatusOK, toTask(existing, task.Status(existing.Status)))
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

// PostTasksIdAnswer реализует POST /tasks/{id}/answer — ответ пользователя на
// вопрос агента (тикет 6.1, FR F1, F2, Gherkin §5 «Агент задаёт вопрос и
// получает ответ»).
//
// Алгоритм: авторизация (JWT, тот же путь, что и PostTasks) → декодировать и
// провалидировать тело (question_id, text обязательны) → проверить владение
// задачей (id пути, owner-scoped, единый 404 — как и с интеграцией в
// PostTasks) → сопоставить question_id тела запроса с КОНКРЕТНОЙ записью
// task_events(agent_question) этой задачи (ListAgentQuestionEventsByTask +
// сравнение payload.question_id на стороне Go — см. godoc самого запроса в
// queries/tasks.sql про то, почему не SQL-side JSON-экстракция; сопоставление
// именно по question_id, а не «последний вопрос задачи», важно уже для этого
// тикета, а не только для 6.2 «несколько вопросов сопоставляются корректно»)
// → перевести задачу waiting_user→running через
// task.Transitioner.TransitionWithEvent, атомарно записав user_answer с
// ref_event_id найденного вопроса (FR F2) → опубликовать конверт user_answer
// в machine.commands, партиционированный по integration_id (ADR 0001) → 202.
//
// Несопоставленный question_id (валиден как UUID, но не найден среди
// agent_question именно этой задачи) — отдельный 404 not_found: тот же код,
// что и «задача не найдена», но другое сообщение, различать причины 404
// здесь не требуется ни FR, ни Gherkin (в отличие от единого 404 при владении
// задачей/интеграцией, где неразличимость — сознательное требование FR A4/I3
// против утечки существования чужого объекта; вопрос агента не является
// объектом, которым можно чужим владеть).
func (s *Server) PostTasksIdAnswer(w http.ResponseWriter, r *http.Request, id IdPath) {
	ctx := r.Context()

	userID, ok := UserIDFromContext(ctx)
	if !ok {
		writeUnauthorized(w)
		return
	}

	var req PostTasksIdAnswerJSONBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "тело запроса не является валидным JSON")
		return
	}
	if strings.TrimSpace(req.Text) == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "text обязателен")
		return
	}

	taskUUID := uuid.UUID(id)
	taskID := pgtype.UUID{Bytes: taskUUID, Valid: true}

	// Владение задачей — owner-scoped прямо в SQL (FR A4, I3), как и владение
	// интеграцией в PostTasks: чужая/несуществующая задача неотличимы, единый
	// 404.
	row, err := s.queries.GetTaskByIDAndUser(ctx, db.GetTaskByIDAndUserParams{
		ID:     taskID,
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeTaskNotFound(w)
			return
		}
		s.logError("GetTaskByIDAndUser", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	questionEvents, err := s.queries.ListAgentQuestionEventsByTask(ctx, taskID)
	if err != nil {
		s.logError("ListAgentQuestionEventsByTask", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	questionIDStr := uuid.UUID(req.QuestionId).String()
	var matched *db.TaskEvent
	for i := range questionEvents {
		var qp bus.AgentQuestionPayload
		if err := json.Unmarshal(questionEvents[i].PayloadEnc, &qp); err != nil {
			// Битый/несовместимый payload у конкретной записи не должен ронять
			// весь поиск — пропускаем её и продолжаем сопоставление остальных.
			continue
		}
		if qp.QuestionID == questionIDStr {
			matched = &questionEvents[i]
			break
		}
	}
	if matched == nil {
		writeError(w, http.StatusNotFound, "not_found", "вопрос не найден")
		return
	}

	transitioner := s.getTransitioner()
	if transitioner == nil {
		s.logError("PostTasksIdAnswer", errors.New("transitioner не настроен"))
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	answerPayload, err := json.Marshal(bus.UserAnswerPayload{QuestionID: questionIDStr, Text: req.Text})
	if err != nil {
		s.logError("json.Marshal(UserAnswerPayload)", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	if _, _, err := transitioner.TransitionWithEvent(ctx, taskID, task.TriggerUserAnswered, "user_answer", matched.ID, answerPayload); err != nil {
		s.logError("TransitionWithEvent(user_answer)", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	publisher := s.getCommandPublisher()
	if publisher == nil {
		s.logError("PostTasksIdAnswer", errors.New("CommandPublisher не настроен"))
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	taskIDStr := taskUUID.String()
	integrationID := uuid.UUID(row.IntegrationID.Bytes)
	env := bus.Envelope{
		MessageID:     bus.NewMessageID(),
		TaskID:        &taskIDStr,
		IntegrationID: integrationID.String(),
		Type:          bus.MessageTypeUserAnswer,
		// Seq: упрощение по образцу PostTasks (тикет 5.3) — сквозной счётчик
		// seq для machine.commands конкретной задачи здесь не заводится
		// (вне объёма 6.1); порядок и так гарантирован партиционированием по
		// integration_id (ADR 0001) и тем, что до user_answer агент уже
		// получил ack на этот WS-путь синхронно.
		Seq:             1,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         answerPayload,
	}
	if err := publisher.PublishKeyed(ctx, bus.TopicMachineCommands, bus.PartitionKeyIntegrationID, env); err != nil {
		s.logError("PublishKeyed(user_answer)", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	w.WriteHeader(http.StatusAccepted)
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
