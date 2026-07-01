package task

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// eventTypeStatusChange — task_events.type для событий, порождённых
// Transition (значение из CHECK-ограничения migrations/00003).
const eventTypeStatusChange = "status_change"

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
}

// NewTransitioner — конструктор Transitioner.
func NewTransitioner(pool *pgxpool.Pool) *Transitioner {
	return &Transitioner{pool: pool, queries: db.New(pool)}
}

// statusChangePayload — TODO(11.1): содержимое payload_enc хранится ОТКРЫТЫМ
// текстом до тикета 11.1 (единый крипто-модуль AEAD для
// text_enc/payload_enc/uuid_enc, FR I1); тикет 5.2 отвечает только за FR E1
// (сама FSM), не за шифрование at-rest.
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
func (t *Transitioner) Transition(ctx context.Context, taskID pgtype.UUID, trigger Trigger) (from, to Status, err error) {
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

	payload, merr := json.Marshal(statusChangePayload{From: from, To: to, Trigger: trigger})
	if merr != nil {
		return from, "", fmt.Errorf("task: сериализовать payload события: %w", merr)
	}

	if _, ierr := q.InsertNextTaskEvent(ctx, db.InsertNextTaskEventParams{
		TaskID:     taskID,
		Type:       eventTypeStatusChange,
		RefEventID: pgtype.UUID{}, // NULL: смена статуса не отвечает на конкретный вопрос/запрос (FR F2)
		PayloadEnc: payload,
	}); ierr != nil {
		return from, "", fmt.Errorf("task: записать task_events(status_change): %w", ierr)
	}

	if cerr := tx.Commit(ctx); cerr != nil {
		return from, "", fmt.Errorf("task: commit tx: %w", cerr)
	}

	return from, to, nil
}
