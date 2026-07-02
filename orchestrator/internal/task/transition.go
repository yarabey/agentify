package task

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yarabey/agentify/internal/crypto"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// eventTypeStatusChange — task_events.type для событий, порождённых
// Transition (значение из CHECK-ограничения migrations/00003).
const eventTypeStatusChange = "status_change"

// EventPayloadKeyPurpose — purpose (см. crypto.DeriveKey) подключа at-rest
// шифрования task_events.payload_enc (FR I1, тикет 11.1). Экспортируется,
// потому что тот же подключ должен вывести и читатель payload_enc на стороне
// API (orchestrator/internal/api.Server): Transitioner ЕДИНСТВЕННЫЙ пишет
// payload_enc (шифрует здесь, на записи), а расшифровывает его уже API при
// показе истории/сопоставлении вопросов (tasks.go) — оба обязаны вывести
// подключ из одного мастер-ключа под ОДНИМ purpose, иначе GCM-тег не сойдётся.
// Значение произвольно, но должно быть СТАБИЛЬНО между рестартами (иначе ранее
// зашифрованные payload_enc перестанут расшифровываться).
const EventPayloadKeyPurpose = "task-event-payload-aead"

// Transitioner — единственная точка смены статуса задачи в БД (FR E1):
// проверяет переход через NextStatus и атомарно (одна транзакция) пишет
// новый tasks.status/updated_at и добавляет task_events(status_change).
//
// Держит *pgxpool.Pool напрямую (а не узкий sqlc-интерфейс, как остальные
// пакеты оркестратора, например presence.onlineMarker) — Transition первым в
// кодовой базе требует многошаговую транзакцию (SELECT ... FOR UPDATE +
// UPDATE + INSERT), которую узкие one-query-интерфейсы не выражают.
type Transitioner struct {
	pool    *pgxpool.Pool
	queries *db.Queries

	// payloadKey — подключ AES-256-GCM для at-rest шифрования
	// task_events.payload_enc (FR I1, тикет 11.1), выведенный из мастер-ключа
	// приложения через crypto.DeriveKey под EventPayloadKeyPurpose. Никогда не
	// nil после NewTransitioner (см. её godoc про фолбэк).
	payloadKey []byte
}

// TransitionerOption — функциональная опция NewTransitioner (тот же приём, что
// у NewStaleWorker/NewAnswerTimeoutWorker в этом пакете).
type TransitionerOption func(*Transitioner)

// WithMasterKey задаёт мастер-ключ шифрования приложения (декодированный
// APP_ENCRYPTION_KEY, см. orchestrator/main.go и docs/MANUAL_STEPS.md), из
// которого Transitioner выводит подключ для at-rest шифрования payload_enc
// (FR I1, тикет 11.1). В проде вызывается ВСЕГДА (main.go) — без неё
// Transitioner использует детерминированный фолбэк-подключ (см. NewTransitioner),
// пригодный только для тестов, не читающих payload_enc через API под реальным
// мастер-ключом.
func WithMasterKey(masterKey []byte) TransitionerOption {
	return func(t *Transitioner) {
		t.payloadKey = crypto.DeriveKey(masterKey, EventPayloadKeyPurpose)
	}
}

// NewTransitioner — конструктор Transitioner. Мастер-ключ шифрования
// payload_enc передаётся через WithMasterKey (FR I1, тикет 11.1); если опция не
// задана, подключ выводится из пустого мастер-ключа — детерминированный, но
// НЕсекретный фолбэк, приемлемый лишь для тестов/каркасных прогонов, где
// payload_enc не читается через API под настоящим ключом (в проде main.go
// всегда передаёт WithMasterKey).
func NewTransitioner(pool *pgxpool.Pool, opts ...TransitionerOption) *Transitioner {
	t := &Transitioner{pool: pool, queries: db.New(pool)}
	for _, opt := range opts {
		opt(t)
	}
	if t.payloadKey == nil {
		t.payloadKey = crypto.DeriveKey(nil, EventPayloadKeyPurpose)
	}
	return t
}

// statusChangePayload — содержимое payload_enc события status_change. Само
// значение шифруется AES-256-GCM at-rest перед записью в БД (FR I1, тикет
// 11.1, см. transition/encryptPayload); тикет 5.2 отвечает только за FR E1
// (сама FSM).
type statusChangePayload struct {
	From    Status  `json:"from"`
	To      Status  `json:"to"`
	Trigger Trigger `json:"trigger"`
}

// Transition — ЕДИНСТВЕННАЯ точка смены статуса задачи (FR E1). Текущий
// статус читается из БД под блокировкой строки (SELECT ... FOR UPDATE), а не
// передаётся вызывающей стороной — это исключает гонки (TOCTOU) между
// конкурентными Transition для одной задачи. Недопустимый переход
// (NextStatus вернул ошибку) откатывает транзакцию и НЕ пишет ничего —
// возвращённый from при этом всё равно заполнен (полезно вызывающей стороне
// для сообщения об ошибке), to — пустая строка.
//
// Тонкая обёртка над transition без дополнительного бизнес-события — см.
// TransitionWithEvent для варианта, атомарно пишущего ещё и
// agent_question/user_answer.
func (t *Transitioner) Transition(ctx context.Context, taskID pgtype.UUID, trigger Trigger) (from, to Status, err error) {
	return t.transition(ctx, taskID, trigger, "", pgtype.UUID{}, nil)
}

// TransitionWithEvent атомарно пишет дополнительное бизнес-событие
// (agent_question/user_answer, тикет 6.1, FR F1/F2) и status_change В ОДНОЙ
// транзакции под той же блокировкой строки tasks (FOR UPDATE), что и сам
// переход статуса — это необходимо, чтобы избежать гонки за seq при
// отдельной от Transition вставке (см. комментарий InsertNextTaskEvent в
// queries/tasks.sql: гонка за MAX(seq) исключена ТОЛЬКО внутри транзакции,
// держащей FOR UPDATE-лок строки tasks).
//
// eventType/refEventID/eventPayload описывают событие, которое должно быть
// записано ДО status_change (например, agent_question при переходе в
// waiting_user, или user_answer с ref_event_id исходного вопроса при
// переходе обратно в running). eventType не должен быть пустым — для
// перехода без дополнительного события используй Transition.
func (t *Transitioner) TransitionWithEvent(ctx context.Context, taskID pgtype.UUID, trigger Trigger, eventType string, refEventID pgtype.UUID, eventPayload []byte) (from, to Status, err error) {
	return t.transition(ctx, taskID, trigger, eventType, refEventID, eventPayload)
}

// RecordEvent атомарно добавляет запись task_events БЕЗ смены статуса задачи
// (в отличие от Transition/TransitionWithEvent) — под тем же FOR UPDATE-локом
// строки tasks (см. GetTaskStatusForUpdate), что исключает гонку за seq с
// конкурентным Transition той же задачи (см. годок InsertNextTaskEvent в
// queries/tasks.sql). Нужен для событий, не являющихся переходом FSM
// (agent_progress, тикет 8.5, FR E6) — статус задачи в момент записи может
// быть любым, в т.ч. терминальным (cancelled/completed/failed): агент может
// сообщать о прогрессе критической операции уже ПОСЛЕ того, как оркестратор
// перевёл задачу в cancelled (тикет 8.4 — переход в cancelled происходит
// немедленно по запросу пользователя, ДО того как критическая операция на
// машине фактически завершится, см. Gherkin §8) — статус НЕ проверяется и не
// ограничивается.
func (t *Transitioner) RecordEvent(ctx context.Context, taskID pgtype.UUID, eventType string, refEventID pgtype.UUID, eventPayload []byte) (seq int64, err error) {
	tx, err := t.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("task: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := t.queries.WithTx(tx)

	if _, err := q.GetTaskStatusForUpdate(ctx, taskID); err != nil {
		return 0, fmt.Errorf("task: получить текущий статус: %w", err)
	}

	encPayload, err := encryptPayload(t.payloadKey, eventPayload)
	if err != nil {
		return 0, fmt.Errorf("task: зашифровать payload события %s: %w", eventType, err)
	}

	row, err := q.InsertNextTaskEvent(ctx, db.InsertNextTaskEventParams{
		TaskID:     taskID,
		Type:       eventType,
		RefEventID: refEventID,
		PayloadEnc: encPayload,
	})
	if err != nil {
		return 0, fmt.Errorf("task: записать task_events(%s): %w", eventType, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("task: commit tx: %w", err)
	}
	return row.Seq, nil
}

// transition — общая реализация Transition/TransitionWithEvent (см. их
// godoc). Если eventType непуст, ПЕРЕД записью status_change (но ПОСЛЕ
// UpdateTaskStatus, внутри той же транзакции) вставляется дополнительная
// запись task_events с переданными eventType/refEventID/eventPayload.
func (t *Transitioner) transition(ctx context.Context, taskID pgtype.UUID, trigger Trigger, eventType string, refEventID pgtype.UUID, eventPayload []byte) (from, to Status, err error) {
	tx, err := t.pool.Begin(ctx)
	if err != nil {
		return "", "", fmt.Errorf("task: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op после успешного Commit

	q := t.queries.WithTx(tx)

	current, err := q.GetTaskStatusForUpdate(ctx, taskID)
	if err != nil {
		return "", "", fmt.Errorf("task: получить текущий статус: %w", err)
	}
	from = Status(current)

	to, err = NextStatus(from, trigger)
	if err != nil {
		return from, "", err
	}

	if uerr := q.UpdateTaskStatus(ctx, db.UpdateTaskStatusParams{
		ID:     taskID,
		Status: string(to),
	}); uerr != nil {
		return from, "", fmt.Errorf("task: обновить статус: %w", uerr)
	}

	if eventType != "" {
		encEventPayload, eerr := encryptPayload(t.payloadKey, eventPayload)
		if eerr != nil {
			return from, "", fmt.Errorf("task: зашифровать payload события %s: %w", eventType, eerr)
		}
		if _, ierr := q.InsertNextTaskEvent(ctx, db.InsertNextTaskEventParams{
			TaskID:     taskID,
			Type:       eventType,
			RefEventID: refEventID,
			PayloadEnc: encEventPayload,
		}); ierr != nil {
			return from, "", fmt.Errorf("task: записать task_events(%s): %w", eventType, ierr)
		}
	}

	payload, merr := json.Marshal(statusChangePayload{From: from, To: to, Trigger: trigger})
	if merr != nil {
		return from, "", fmt.Errorf("task: сериализовать payload события: %w", merr)
	}
	encStatusPayload, eerr := encryptPayload(t.payloadKey, payload)
	if eerr != nil {
		return from, "", fmt.Errorf("task: зашифровать payload события status_change: %w", eerr)
	}

	if _, ierr := q.InsertNextTaskEvent(ctx, db.InsertNextTaskEventParams{
		TaskID:     taskID,
		Type:       eventTypeStatusChange,
		RefEventID: pgtype.UUID{}, // NULL: смена статуса не отвечает на конкретный вопрос/запрос (FR F2)
		PayloadEnc: encStatusPayload,
	}); ierr != nil {
		return from, "", fmt.Errorf("task: записать task_events(status_change): %w", ierr)
	}

	if cerr := tx.Commit(ctx); cerr != nil {
		return from, "", fmt.Errorf("task: commit tx: %w", cerr)
	}

	return from, to, nil
}

// encryptPayload шифрует содержимое task_events.payload_enc at-rest (FR I1,
// тикет 11.1): plaintext-JSON события никогда не ложится в БД в открытом виде,
// хранится AES-256-GCM-шифротекстом (nonce||ciphertext+tag) под payloadKey.
// Пустой/nil payload (например, Transition без бизнес-события не проходит сюда,
// но RecordEvent теоретически может получить пустой) шифруется как есть —
// результат всё равно непустой (nonce+tag) и корректно расшифровывается в
// пустой срез; читателю (api.Server.decryptEventPayload) не нужно различать
// этот случай.
func encryptPayload(key, payload []byte) ([]byte, error) {
	return crypto.Encrypt(key, payload)
}
