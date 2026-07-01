package task

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// defaultStalePollInterval — как часто StaleWorker опрашивает БД в поисках
// зависших/восстановившихся машин (внутренняя деталь реализации, НЕ
// настраивается через env пользователем — в отличие от STALE_THRESHOLD, тот
// же принцип, что и у presence.defaultOfflinePollInterval).
const defaultStalePollInterval = 10 * time.Second

// defaultStaleThreshold — порог устаревания last_seen_at по умолчанию
// (STALE_THRESHOLD, тикет 5.7, FR E5, protocol.md §6): если heartbeat не
// приходил дольше этого времени при активной (running/waiting_user) задаче,
// она помечается зависшей (stale) с уведомлением (task_events(status_change),
// trigger=timeout). Окончательное значение — незакрытое продуктовое решение
// (docs/MANUAL_STEPS.md §4, «Таймауты: через сколько машина «зависла»» — чекбокс
// не отмечен); дефолт 120s выбран ЗАМЕТНО больше OFFLINE_THRESHOLD (45s,
// presence.defaultOfflineThreshold, FR B4), чтобы задача помечалась зависшей
// (тяжёлый, видимый пользователю сигнал) уже ПОСЛЕ того, как сама интеграция
// определённо помечена offline. Настраивается через ORCH_STALE_THRESHOLD в
// orchestrator/main.go (WithStaleThreshold).
const defaultStaleThreshold = 120 * time.Second

// staleLister — узкий интерфейс sqlc-запросов, нужных StaleWorker (тикет 5.7,
// FR E5): найти активные задачи с устаревшим heartbeat интеграции и найти
// зависшие задачи, чья интеграция снова ожила. Реализуется *db.Queries;
// сужение позволяет юнит-тестам подменить запросы фейком без поднятия
// Postgres (тот же приём, что presence.offlineMarker).
type staleLister interface {
	// ListRunningTasksWithStaleMachine возвращает id активных (running/
	// waiting_user) задач, чья интеграция не подавала heartbeat дольше cutoff
	// (или не подавала вообще — last_seen_at IS NULL).
	ListRunningTasksWithStaleMachine(ctx context.Context, cutoff pgtype.Timestamptz) ([]pgtype.UUID, error)
	// ListStaleTasksWithRecoveredMachine возвращает id задач в статусе stale,
	// чья интеграция снова свежо подавала heartbeat (last_seen_at не старше
	// cutoff).
	ListStaleTasksWithRecoveredMachine(ctx context.Context, cutoff pgtype.Timestamptz) ([]pgtype.UUID, error)
}

// transitionFunc — сигнатура перехода FSM задачи, нужная StaleWorker
// (совпадает с (*Transitioner).Transition). Вынесена отдельным типом, а не
// хранением *Transitioner напрямую, потому что Transitioner — конкретный тип
// без интерфейса (см. godoc transition.go: первый пакет с многошаговой
// транзакцией, узкие sqlc-интерфейсы её не выражают) — так юнит-тесты
// StaleWorker подставляют функцию-шпион и проверяют факт вызова с ожидаемым
// Trigger, не поднимая Postgres; в проде NewStaleWorker получает
// transitioner.Transition как значение этого типа.
type transitionFunc func(ctx context.Context, taskID pgtype.UUID, trigger Trigger) (from, to Status, err error)

// StaleWorkerOption — функциональная опция NewStaleWorker (по аналогии с
// presence.WorkerOption): неположительные значения длительности и nil-логгер
// игнорируются — нужно тестам, чтобы не ждать реальные секунды/минуты (см.
// defaultStalePollInterval/defaultStaleThreshold).
type StaleWorkerOption func(*StaleWorker)

// WithStalePollInterval задаёт паузу между опросами БД (см.
// defaultStalePollInterval). Неположительное значение игнорируется.
func WithStalePollInterval(d time.Duration) StaleWorkerOption {
	return func(w *StaleWorker) {
		if d > 0 {
			w.pollInterval = d
		}
	}
}

// WithStaleThreshold задаёт порог устаревания last_seen_at (см.
// defaultStaleThreshold, STALE_THRESHOLD, тикет 5.7, FR E5). Неположительное
// значение игнорируется.
func WithStaleThreshold(d time.Duration) StaleWorkerOption {
	return func(w *StaleWorker) {
		if d > 0 {
			w.staleThreshold = d
		}
	}
}

// WithStaleWorkerLogger задаёт логгер StaleWorker (по умолчанию —
// slog.Default()).
func WithStaleWorkerLogger(logger *slog.Logger) StaleWorkerOption {
	return func(w *StaleWorker) {
		if logger != nil {
			w.logger = logger
		}
	}
}

// StaleWorker — фоновый воркер «зависания» машины (тикет 5.7, FR E5,
// protocol.md §6): за каждый тик выполняет ДВА прохода с ОДНИМ и тем же
// cutoff = now - staleThreshold — переводит в stale активные задачи (running/
// waiting_user) с устаревшим heartbeat интеграции (TriggerTimeout) и
// возвращает в running задачи в stale, чья интеграция снова ожила
// (TriggerMachineRecovered). Работает НАПРЯМУЮ по integrations.last_seen_at —
// той же колонке, что и presence.OfflineWorker, но с ОТДЕЛЬНЫМ порогом
// (STALE_THRESHOLD, ощутимо больше OFFLINE_THRESHOLD) и независимо от того,
// успел ли OfflineWorker выставить integrations.status='offline'. Собирается
// через NewStaleWorker; нулевое значение не готово к использованию (нет
// queries/transition).
type StaleWorker struct {
	queries    staleLister
	transition transitionFunc
	logger     *slog.Logger

	pollInterval   time.Duration
	staleThreshold time.Duration
}

// NewStaleWorker собирает StaleWorker поверх sqlc-запросов и перехода FSM.
// queries и transitioner обязательны (nil — ошибка конструктора, не паника,
// тот же принцип, что и у presence.NewOfflineWorker/bridge.New).
func NewStaleWorker(transitioner *Transitioner, queries staleLister, opts ...StaleWorkerOption) (*StaleWorker, error) {
	if transitioner == nil {
		return nil, fmt.Errorf("task: nil transitioner")
	}
	if queries == nil {
		return nil, fmt.Errorf("task: nil queries")
	}
	w := &StaleWorker{
		queries:        queries,
		transition:     transitioner.Transition,
		logger:         slog.Default(),
		pollInterval:   defaultStalePollInterval,
		staleThreshold: defaultStaleThreshold,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w, nil
}

// Run — основной цикл StaleWorker: каждые pollInterval выполняет один проход
// (см. tick). Возвращает управление только при отмене ctx (nil, штатное
// завершение) — ошибка отдельного запроса/перехода логируется, но не
// останавливает цикл (временный сбой БД или гонка по конкретной задаче не
// должны навсегда остановить воркер, следующий тик попробует снова).
func (w *StaleWorker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.tick(ctx)
		}
	}
}

// tick выполняет один проход: считает ОДИН cutoff = now - staleThreshold и
// применяет его к обоим спискам (зависшие/ожившие), чтобы не рассинхронизировать
// решения о переходах в рамках одного тика.
func (w *StaleWorker) tick(ctx context.Context) {
	cutoff := pgtype.Timestamptz{Time: time.Now().UTC().Add(-w.staleThreshold), Valid: true}

	w.markStale(ctx, cutoff)
	w.recoverStale(ctx, cutoff)
}

// markStale находит активные задачи с устаревшим heartbeat интеграции и
// переводит каждую в stale (TriggerTimeout). Ошибка самого listing-запроса
// логируется и проход пропускается до следующего тика; ошибка перехода для
// отдельного id (например, статус успел смениться конкурентно) логируется и
// НЕ прерывает обработку остальных id.
func (w *StaleWorker) markStale(ctx context.Context, cutoff pgtype.Timestamptz) {
	ids, err := w.queries.ListRunningTasksWithStaleMachine(ctx, cutoff)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.logger.Warn("task: ListRunningTasksWithStaleMachine не удался", slog.String("error", err.Error()))
		return
	}
	for _, id := range ids {
		if _, _, terr := w.transition(ctx, id, TriggerTimeout); terr != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("task: перевод задачи в stale не удался",
				slog.String("task_id", id.String()), slog.String("error", terr.Error()))
		}
	}
}

// recoverStale находит зависшие задачи, чья интеграция снова ожила, и
// возвращает каждую в running (TriggerMachineRecovered). Тот же принцип
// терпимости к частичным ошибкам, что и markStale.
func (w *StaleWorker) recoverStale(ctx context.Context, cutoff pgtype.Timestamptz) {
	ids, err := w.queries.ListStaleTasksWithRecoveredMachine(ctx, cutoff)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.logger.Warn("task: ListStaleTasksWithRecoveredMachine не удался", slog.String("error", err.Error()))
		return
	}
	for _, id := range ids {
		if _, _, terr := w.transition(ctx, id, TriggerMachineRecovered); terr != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("task: возврат задачи из stale в running не удался",
				slog.String("task_id", id.String()), slog.String("error", terr.Error()))
		}
	}
}
