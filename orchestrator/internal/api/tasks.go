package api

// tasks.go — постановка задачи в очередь к машине (тикет 5.3, FR E1, E4, §4
// «Постановка задачи из канала» (web/telegram)), ответ пользователя на
// вопрос агента (тикет 6.1, FR F1, F2, PostTasksIdAnswer, Gherkin §5 «Агент
// задаёт вопрос и получает ответ»), согласование команды вне allowlist
// (тикет 6.4, FR F3, PostTasksIdApprove, Gherkin §5 «Команда вне allowlist
// требует согласования»), подтверждение пользователем завершения задачи
// (тикет 8.2, FR E2, PostTasksIdConfirm, Gherkin §7 «Пользователь
// подтверждает завершение») и отклонение результата на доработку (тикет 8.3,
// FR E2, PostTasksIdReject, Gherkin §7 «Пользователь отклоняет результат») и
// отмена задачи, доходящая до машины (тикет 8.4, FR E6, PostTasksIdCancel,
// Gherkin §8 «Отмена доходит до машины»), и состав истории — список/карточка
// задачи и журнал её событий (тикет 8.6, FR H1, GetTasks/GetTasksId/
// GetTasksIdEvents, Gherkin §10 «Состав записи о задаче»).
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
// GET /tasks, GET /tasks/{id}, GET /tasks/{id}/events (список/карточка/
// журнал событий задачи, owner-scoped, тикет 8.6, FR H1) реализованы ниже,
// см. GetTasks/GetTasksId/GetTasksIdEvents. Дедуп постановки по
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
	"io"
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

// PostTasksIdApprove реализует POST /tasks/{id}/approve — согласование
// пользователем команды агента вне allowlist (тикет 6.4, FR F3, Gherkin §5
// «Команда вне allowlist требует согласования»).
//
// Алгоритм: авторизация (JWT, тот же путь, что и PostTasksIdAnswer) →
// декодировать и провалидировать тело (request_id обязателен по схеме типа;
// decision обязан быть ровно "approve" или "reject" — тип
// PostTasksIdApproveJSONBodyDecision в контракте (types.gen.go) это просто
// string, JSON-декодирование само по себе не проверяет словарь значений) →
// проверить владение задачей (id пути, owner-scoped, единый 404 — как и в
// PostTasksIdAnswer) → сопоставить request_id тела запроса с КОНКРЕТНОЙ
// записью task_events(command_approval_request) этой задачи
// (ListCommandApprovalRequestEventsByTask + сравнение payload.request_id на
// стороне Go — тот же приём и то же обоснование, что и у
// ListAgentQuestionEventsByTask/question_id в PostTasksIdAnswer, см. годок
// самого запроса в queries/tasks.sql) → перевести задачу
// waiting_user→running через task.Transitioner.TransitionWithEvent, атомарно
// записав user_decision с ref_event_id найденного запроса (FR F3) →
// опубликовать конверт command_decision в machine.commands,
// партиционированный по integration_id (ADR 0001) → 202.
//
// И "approve", и "reject" — валидные значения decision в рамках ЭТОГО
// тикета: контракт (FR F3, protocol.md §4) их не различает на уровне
// перехода FSM/публикации — обе публикуют command_decision с ref_event_id
// найденного запроса и оба переводят задачу обратно в running, само решение
// прозрачно передаётся агенту (который на стороне Provider.Approve решает,
// продолжать ли выполнение команды CLI или сообщить об отказе, тикет 4.5).
// Поведенческая приёмка именно "агент учёл отказ и не выполнил команду" —
// предмет ОТДЕЛЬНОГО тикета 6.5 (deps: 6.4), не проверяется здесь: здесь
// закрывается контракт (оба значения decision корректно публикуются) и
// приёмка САМОГО 6.4 — «команда не выполняется, пока я её не одобрю» (до
// approve публикации command_decision не происходит вовсе, см.
// TestPostTasksIdApprove_CommandNotPublishedBeforeApprove).
//
// Несопоставленный request_id (валиден как UUID, но не найден среди
// command_approval_request именно этой задачи) — отдельный 404 not_found:
// та же логика, что и с question_id в PostTasksIdAnswer (не является
// объектом, которым можно чужим владеть, различать причины 404 здесь не
// требует ни FR, ни Gherkin).
func (s *Server) PostTasksIdApprove(w http.ResponseWriter, r *http.Request, id IdPath) {
	ctx := r.Context()

	userID, ok := UserIDFromContext(ctx)
	if !ok {
		writeUnauthorized(w)
		return
	}

	var req PostTasksIdApproveJSONBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "тело запроса не является валидным JSON")
		return
	}
	if req.Decision != Approve && req.Decision != Reject {
		writeError(w, http.StatusBadRequest, "validation_error", "decision должен быть approve или reject")
		return
	}

	taskUUID := uuid.UUID(id)
	taskID := pgtype.UUID{Bytes: taskUUID, Valid: true}

	// Владение задачей — owner-scoped прямо в SQL (FR A4, I3), как и владение
	// задачей в PostTasksIdAnswer: чужая/несуществующая задача неотличимы,
	// единый 404.
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

	requestEvents, err := s.queries.ListCommandApprovalRequestEventsByTask(ctx, taskID)
	if err != nil {
		s.logError("ListCommandApprovalRequestEventsByTask", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	requestIDStr := uuid.UUID(req.RequestId).String()
	var matched *db.TaskEvent
	for i := range requestEvents {
		var rp bus.CommandApprovalRequestPayload
		if err := json.Unmarshal(requestEvents[i].PayloadEnc, &rp); err != nil {
			// Битый/несовместимый payload у конкретной записи не должен ронять
			// весь поиск — пропускаем её и продолжаем сопоставление остальных.
			continue
		}
		if rp.RequestID == requestIDStr {
			matched = &requestEvents[i]
			break
		}
	}
	if matched == nil {
		writeError(w, http.StatusNotFound, "not_found", "запрос на согласование не найден")
		return
	}

	transitioner := s.getTransitioner()
	if transitioner == nil {
		s.logError("PostTasksIdApprove", errors.New("transitioner не настроен"))
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	decisionPayload, err := json.Marshal(bus.CommandDecisionPayload{RequestID: requestIDStr, Decision: string(req.Decision)})
	if err != nil {
		s.logError("json.Marshal(CommandDecisionPayload)", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	if _, _, err := transitioner.TransitionWithEvent(ctx, taskID, task.TriggerCommandDecision, "user_decision", matched.ID, decisionPayload); err != nil {
		s.logError("TransitionWithEvent(user_decision)", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	publisher := s.getCommandPublisher()
	if publisher == nil {
		s.logError("PostTasksIdApprove", errors.New("CommandPublisher не настроен"))
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	taskIDStr := taskUUID.String()
	integrationID := uuid.UUID(row.IntegrationID.Bytes)
	env := bus.Envelope{
		MessageID:     bus.NewMessageID(),
		TaskID:        &taskIDStr,
		IntegrationID: integrationID.String(),
		Type:          bus.MessageTypeCommandDecision,
		// Seq: то же упрощение, что и в PostTasksIdAnswer (вне объёма 6.4) —
		// порядок гарантирован партиционированием по integration_id (ADR 0001).
		Seq:             1,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         decisionPayload,
	}
	if err := publisher.PublishKeyed(ctx, bus.TopicMachineCommands, bus.PartitionKeyIntegrationID, env); err != nil {
		s.logError("PublishKeyed(command_decision)", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// PostTasksIdConfirm реализует POST /tasks/{id}/confirm — явное подтверждение
// пользователем завершения задачи (тикет 8.2, FR E2, Gherkin §7 «Пользователь
// подтверждает завершение»: Дано задача в статусе "ожидает подтверждения",
// Когда я явно подтверждаю завершение, Тогда задача переходит в статус
// "завершена"). Это ЕДИНСТВЕННОЕ действие, которое закрывает задачу — сама
// задача уже фактически выполнена агентом и переведена в awaiting_confirm
// (тикет 8.1), но статус completed выставляется только явным подтверждением
// человека, не автоматически.
//
// Алгоритм: авторизация (JWT, тот же путь, что и PostTasksIdApprove/
// PostTasksIdAnswer) → проверить владение задачей (id пути, owner-scoped,
// единый 404 — как и в PostTasksIdApprove/PostTasksIdAnswer) → перевести
// задачу awaiting_confirm→completed через task.Transitioner.Transition с
// триггером task.TriggerUserConfirmed (единственная точка смены tasks.status
// и записи task_events(status_change), тикет 5.2; edge
// {StatusAwaitingConfirm, TriggerUserConfirmed}: StatusCompleted уже заведён в
// fsm.go тикетом 5.2 специально под этот тикет) → 200 с обновлённым Task.
//
// В отличие от PostTasksIdAnswer/PostTasksIdApprove здесь НЕТ requestBody:
// контракт (api/openapi.yaml) не описывает тело для /tasks/{id}/confirm —
// подтверждение не сопоставляется ни с каким конкретным событием задачи
// (нет question_id/request_id, которые нужно было бы найти среди
// task_events), поэтому и не нужен generated JSONBody-тип для декодирования.
//
// В отличие от PostTasksIdAnswer/PostTasksIdApprove здесь НЕТ публикации
// через CommandPublisher/PublishKeyed: подтверждение — чисто orchestrator-side
// переход статуса, агент ни во что не вовлечён и уведомлять его не о чем —
// задача с его точки зрения уже завершена (тикет 8.1, awaiting_confirm
// достигается именно сообщением агента о завершении). В internal/bus/
// messages.go нет и не должно быть MessageType для confirm/reject: этот
// переход не порождает никакой команды machine.commands.
//
// Недопустимый переход (задача не в awaiting_confirm) — ошибка от
// task.NextStatus внутри transitioner.Transition, транслируется в 500 общей
// веткой ниже, тем же паттерном, что и TransitionError в
// PostTasksIdApprove/PostTasksIdAnswer — отдельного статуса/ветки для этого
// случая в проекте не заведено.
func (s *Server) PostTasksIdConfirm(w http.ResponseWriter, r *http.Request, id IdPath) {
	ctx := r.Context()

	userID, ok := UserIDFromContext(ctx)
	if !ok {
		writeUnauthorized(w)
		return
	}

	taskUUID := uuid.UUID(id)
	taskID := pgtype.UUID{Bytes: taskUUID, Valid: true}

	// Владение задачей — owner-scoped прямо в SQL (FR A4, I3), как и владение
	// задачей в PostTasksIdAnswer/PostTasksIdApprove: чужая/несуществующая
	// задача неотличимы, единый 404.
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

	transitioner := s.getTransitioner()
	if transitioner == nil {
		s.logError("PostTasksIdConfirm", errors.New("transitioner не настроен"))
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	_, to, err := transitioner.Transition(ctx, taskID, task.TriggerUserConfirmed)
	if err != nil {
		s.logError("Transition(user_confirmed)", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	writeJSON(w, http.StatusOK, toTask(row, to))
}

// PostTasksIdReject реализует POST /tasks/{id}/reject — пользователь отклоняет
// результат работы агента и просит доработку (тикет 8.3, FR E2, Gherkin §7
// «Пользователь отклоняет результат»): Дано задача в статусе "ожидает
// подтверждения", Когда я отклоняю результат и прошу доработку, Тогда задача
// возвращается в статус "выполняется". Как и PostTasksIdConfirm (тикет 8.2),
// это действие НЕ уведомляет агента (нет CommandPublisher) — awaiting_confirm
// это состояние, в котором агент уже неактивен и ждёт внешнего решения
// пользователя.
//
// Тело запроса необязательно целиком (api/openapi.yaml: requestBody без
// required), опциональное поле comment принимается и валидируется как JSON,
// но не персистится — в текущей схеме task_events (миграция 00003, не
// редактируется) нет подходящего типа события для комментария об отклонении;
// это вне объёма тикета 8.3 (приёмка — только переход reject → running).
func (s *Server) PostTasksIdReject(w http.ResponseWriter, r *http.Request, id IdPath) {
	ctx := r.Context()

	userID, ok := UserIDFromContext(ctx)
	if !ok {
		writeUnauthorized(w)
		return
	}

	var req PostTasksIdRejectJSONBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_body", "тело запроса не является валидным JSON")
		return
	}

	taskUUID := uuid.UUID(id)
	taskID := pgtype.UUID{Bytes: taskUUID, Valid: true}

	// Владение задачей — owner-scoped прямо в SQL (FR A4, I3), как и владение
	// задачей в PostTasksIdConfirm/PostTasksIdApprove/PostTasksIdAnswer: чужая/
	// несуществующая задача неотличимы, единый 404.
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

	transitioner := s.getTransitioner()
	if transitioner == nil {
		s.logError("PostTasksIdReject", errors.New("transitioner не настроен"))
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	_, to, err := transitioner.Transition(ctx, taskID, task.TriggerCompletionRejected)
	if err != nil {
		s.logError("Transition(completion_rejected)", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	writeJSON(w, http.StatusOK, toTask(row, to))
}

// PostTasksIdCancel реализует POST /tasks/{id}/cancel — отмена задачи
// пользователем, доходящая до машины (тикет 8.4, FR E6, Gherkin §8 «Отмена
// доходит до машины»: Дано задача выполняется на машине, Когда я отменяю
// задачу, Тогда команда отмены доходит до машины, И агент останавливается, И
// статус во всех каналах становится "отменена").
//
// Алгоритм: авторизация (JWT, тот же путь, что и остальные обработчики этого
// файла) → проверить владение задачей (id пути, owner-scoped, единый 404 —
// как и в PostTasksIdConfirm/PostTasksIdApprove/PostTasksIdAnswer) →
// перевести задачу в cancelled через task.Transitioner.Transition с
// триггером task.TriggerCancelRequested (единственная точка смены
// tasks.status и записи task_events(status_change), тикет 5.2; рёбра FSM
// Queued/Running/WaitingUser/AwaitingConfirm/Stale → Cancelled уже заведены
// тикетом 5.2 в fsm.go) → опубликовать конверт cancel в machine.commands,
// партиционированный по integration_id (ADR 0001), чтобы агент реально
// остановил активного провайдера задачи (agent/task_runner.go, onCancel
// вызывает runner.Close() → claudecode.Provider.Close, тикет 4.5 — уже
// готовый SIGKILL подпроцессу) → 202 без тела.
//
// Как и PostTasksIdConfirm/PostTasksIdReject (в отличие от
// PostTasksIdAnswer/PostTasksIdApprove), здесь используется именно
// task.Transitioner.Transition, а НЕ TransitionWithEvent: в CHECK-констрейнте
// task_events.type (миграция 00003, не редактируется) нет отдельного типа
// события под отмену, а аудит самого перехода статуса и так фиксируется
// штатной записью status_change ({from, to, trigger}) внутри Transition —
// дополнительно писать нечего.
//
// Порядок Transition→Publish — тот же принятый риск, что и в PostTasks/
// PostTasksIdApprove/PostTasksIdAnswer: если Publish упадёт уже ПОСЛЕ
// успешного Transition, задача в БД уже cancelled, а агент не уведомлён о
// необходимости остановиться — это не новая проблема, существующий паттерн
// во всех обработчиках, публикующих команду машине.
//
// Недопустимый переход (задача уже в терминальном статусе — completed/
// cancelled/failed) — ошибка от task.NextStatus внутри
// transitioner.Transition, транслируется в 500 общей веткой ниже, тем же
// паттерном, что и в остальных обработчиках этого файла — отдельной ветки
// для этого случая не заведено.
//
// НЕ реализует safe-stop/graceful-stop семантику (проверка «критическая
// операция», предупреждение о невозможности мгновенной остановки,
// Gherkin §8 «Приоритет сохранности данных при отмене») — это отдельный
// тикет 8.5 (deps: 8.4, 4.5). Здесь остановка — это просто SIGKILL через уже
// готовый Close() агента, без какой-либо проверки состояния выполняемой
// команды.
func (s *Server) PostTasksIdCancel(w http.ResponseWriter, r *http.Request, id IdPath) {
	ctx := r.Context()

	userID, ok := UserIDFromContext(ctx)
	if !ok {
		writeUnauthorized(w)
		return
	}

	taskUUID := uuid.UUID(id)
	taskID := pgtype.UUID{Bytes: taskUUID, Valid: true}

	// Владение задачей — owner-scoped прямо в SQL (FR A4, I3), как и владение
	// задачей в PostTasksIdConfirm/PostTasksIdReject/PostTasksIdApprove/
	// PostTasksIdAnswer: чужая/несуществующая задача неотличимы, единый 404.
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

	transitioner := s.getTransitioner()
	if transitioner == nil {
		s.logError("PostTasksIdCancel", errors.New("transitioner не настроен"))
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	if _, _, err := transitioner.Transition(ctx, taskID, task.TriggerCancelRequested); err != nil {
		s.logError("Transition(cancel_requested)", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	publisher := s.getCommandPublisher()
	if publisher == nil {
		s.logError("PostTasksIdCancel", errors.New("CommandPublisher не настроен"))
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	taskIDStr := taskUUID.String()
	integrationID := uuid.UUID(row.IntegrationID.Bytes)
	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		TaskID:          &taskIDStr,
		IntegrationID:   integrationID.String(),
		Type:            bus.MessageTypeCancel,
		Seq:             1,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         json.RawMessage("{}"),
	}
	if err := publisher.PublishKeyed(ctx, bus.TopicMachineCommands, bus.PartitionKeyIntegrationID, env); err != nil {
		s.logError("PublishKeyed(cancel)", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// GetTasks реализует GET /tasks — список задач владельца с опциональными
// фильтрами по интеграции и статусу (тикет 8.6, FR H1, Gherkin §10 «Состав
// записи о задаче»).
//
// Бизнес: только свои задачи (owner-scoped, FR A4, I3, тот же принцип, что и
// GetIntegrations в integrations.go) — ListTasksByUser (queries/tasks.sql)
// фильтрует по user_id прямо в SQL. integration_id/status (params) — опциональные
// query-фильтры контракта (GetTasksParams); если оба не заданы — возвращается
// вся история задач владельца, самые новые первыми (ORDER BY created_at DESC,
// см. годок ListTasksByUser). Указание чужой integration_id не даёт увидеть
// чужие задачи и не является ошибкой — просто пустой список (AND user_id = $1
// в запросе уже исключает такие строки).
func (s *Server) GetTasks(w http.ResponseWriter, r *http.Request, params GetTasksParams) {
	ctx := r.Context()

	userID, ok := UserIDFromContext(ctx)
	if !ok {
		writeUnauthorized(w)
		return
	}

	arg := db.ListTasksByUserParams{
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
	}
	if params.IntegrationId != nil {
		arg.IntegrationID = pgtype.UUID{Bytes: *params.IntegrationId, Valid: true}
	}
	if params.Status != nil {
		status := string(*params.Status)
		arg.Status = &status
	}

	rows, err := s.queries.ListTasksByUser(ctx, arg)
	if err != nil {
		s.logError("ListTasksByUser", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	result := make([]Task, 0, len(rows))
	for _, row := range rows {
		result = append(result, toTaskRow(row))
	}
	writeJSON(w, http.StatusOK, result)
}

// GetTasksId реализует GET /tasks/{id} — карточка одной задачи (тикет 8.6,
// FR H1, Gherkin §10 «Состав записи о задаче»): целевая интеграция, текст,
// статус, таймстемпы создания/обновления — все поля, которые Gherkin §10
// требует видеть по конкретной задаче.
//
// Владение задачей — owner-scoped прямо в SQL (GetTaskByIDAndUser, FR A4,
// I3), тот же паттерн 404, что и в PostTasksIdAnswer/PostTasksIdConfirm:
// чужая/несуществующая задача неотличимы, единый 404.
func (s *Server) GetTasksId(w http.ResponseWriter, r *http.Request, id IdPath) {
	ctx := r.Context()

	userID, ok := UserIDFromContext(ctx)
	if !ok {
		writeUnauthorized(w)
		return
	}

	row, err := s.queries.GetTaskByIDAndUser(ctx, db.GetTaskByIDAndUserParams{
		ID:     pgtype.UUID{Bytes: id, Valid: true},
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

	writeJSON(w, http.StatusOK, toTaskRow(row))
}

// GetTasksIdEvents реализует GET /tasks/{id}/events — журнал событий задачи
// (тикет 8.6, FR H1, Gherkin §10 «Состав записи о задаче»): вопросы агента,
// ответы пользователя с привязкой к запросу (payload содержит question_id/
// request_id, на который отвечает событие — см. bus.AgentQuestionPayload,
// bus.UserAnswerPayload, bus.CommandApprovalRequestPayload,
// bus.CommandDecisionPayload в internal/bus/messages.go), согласования,
// смены статуса.
//
// Владение задачей проверяется СНАЧАЛА, тем же owner-scoped запросом
// (GetTaskByIDAndUser), что и в GetTasksId — единый 404 для чужой/
// несуществующей задачи, — и только затем читается журнал
// (ListTaskEventsByTask, полная история по seq по возрастанию, см. годок
// запроса в queries/tasks.sql).
func (s *Server) GetTasksIdEvents(w http.ResponseWriter, r *http.Request, id IdPath) {
	ctx := r.Context()

	userID, ok := UserIDFromContext(ctx)
	if !ok {
		writeUnauthorized(w)
		return
	}

	taskID := pgtype.UUID{Bytes: id, Valid: true}

	if _, err := s.queries.GetTaskByIDAndUser(ctx, db.GetTaskByIDAndUserParams{
		ID:     taskID,
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeTaskNotFound(w)
			return
		}
		s.logError("GetTaskByIDAndUser", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	rows, err := s.queries.ListTaskEventsByTask(ctx, taskID)
	if err != nil {
		s.logError("ListTaskEventsByTask", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	result := make([]TaskEvent, 0, len(rows))
	for _, row := range rows {
		result = append(result, toTaskEvent(row))
	}
	writeJSON(w, http.StatusOK, result)
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

// toTaskRow конвертирует строку БД в контрактный Task для read-only путей
// (GetTasks, GetTasksId, тикет 8.6, FR H1) — в отличие от toTask (используется
// ТОЛЬКО в write-путях сразу после Transition, где status/updated_at ещё не
// отражены в уже прочитанной row), здесь Transition в рамках этого запроса не
// происходил: row уже содержит актуальные status и updated_at, оба берутся
// прямо из неё.
func toTaskRow(row db.Task) Task {
	id := uuid.UUID(row.ID.Bytes)
	integrationID := uuid.UUID(row.IntegrationID.Bytes)
	text := string(row.TextEnc)
	status := TaskStatus(row.Status)
	createdAt := row.CreatedAt.Time
	updatedAt := row.UpdatedAt.Time

	return Task{
		Id:            &id,
		IntegrationId: &integrationID,
		Text:          &text,
		Status:        &status,
		CreatedAt:     &createdAt,
		UpdatedAt:     &updatedAt,
	}
}

// toTaskEvent конвертирует строку журнала событий в контрактный TaskEvent
// (тикет 8.6, FR H1, §10 «Состав записи о задаче»). PayloadEnc сейчас хранит
// открытый JSON (TODO(11.1) — шифрование at-rest, вне объёма); если
// Unmarshal вдруг не удался (данные должны быть валидным JSON, т.к. пишутся
// только через json.Marshal в этом же кодовом пути — падение здесь означало
// бы порчу данных, не штатный случай), Payload остаётся nil, но остальные
// поля события (id, seq, type, created_at) всё равно возвращаются — история
// бессрочна (FR I2) и не должна терять записи целиком из-за одного плохого
// payload.
func toTaskEvent(row db.TaskEvent) TaskEvent {
	id := uuid.UUID(row.ID.Bytes)
	seq := int(row.Seq)
	eventType := TaskEventType(row.Type)
	createdAt := row.CreatedAt.Time

	result := TaskEvent{
		Id:        &id,
		Seq:       &seq,
		Type:      &eventType,
		CreatedAt: &createdAt,
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(row.PayloadEnc, &payload); err == nil {
		result.Payload = &payload
	}
	return result
}
