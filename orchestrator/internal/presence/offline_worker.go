package presence

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// defaultOfflinePollInterval — как часто OfflineWorker опрашивает БД в
// поисках устаревших интеграций (внутренняя деталь реализации, НЕ
// настраивается через env пользователем — в отличие от OFFLINE_THRESHOLD).
// Значение с запасом меньше дефолтного OFFLINE_THRESHOLD (45s, protocol.md
// §6), чтобы переход в offline не запаздывал заметно относительно самого
// порога.
const defaultOfflinePollInterval = 5 * time.Second

// defaultOfflineThreshold — порог устаревания last_seen_at по умолчанию
// (OFFLINE_THRESHOLD, protocol.md §6): если heartbeat не приходил дольше
// этого времени, интеграция считается offline. Настраивается через
// ORCH_OFFLINE_THRESHOLD в orchestrator/main.go (WithOfflineThreshold).
const defaultOfflineThreshold = 45 * time.Second

// offlineMarker — узкий интерфейс sqlc-запроса, нужного OfflineWorker:
// перевести в offline все интеграции с устаревшим last_seen_at (тикет 3.6,
// FR B4). Реализуется *db.Queries; сужение позволяет юнит-тестам подменить
// запрос фейком без поднятия Postgres.
type offlineMarker interface {
	MarkStaleIntegrationsOffline(ctx context.Context, cutoff pgtype.Timestamptz) error
}

// WorkerOption — функциональная опция NewOfflineWorker (по аналогии с
// bridge.Option): неположительные значения длительности игнорируются — нужно
// тестам, чтобы не ждать реальные секунды/минуты (см. defaultOfflinePollInterval/
// defaultOfflineThreshold).
type WorkerOption func(*OfflineWorker)

// WithPollInterval задаёт паузу между опросами БД (см.
// defaultOfflinePollInterval). Неположительное значение игнорируется.
func WithPollInterval(d time.Duration) WorkerOption {
	return func(w *OfflineWorker) {
		if d > 0 {
			w.pollInterval = d
		}
	}
}

// WithOfflineThreshold задаёт порог устаревания last_seen_at (см.
// defaultOfflineThreshold, OFFLINE_THRESHOLD protocol.md §6). Неположительное
// значение игнорируется.
func WithOfflineThreshold(d time.Duration) WorkerOption {
	return func(w *OfflineWorker) {
		if d > 0 {
			w.offlineThreshold = d
		}
	}
}

// WithWorkerLogger задаёт логгер OfflineWorker (по умолчанию —
// slog.Default()).
func WithWorkerLogger(logger *slog.Logger) WorkerOption {
	return func(w *OfflineWorker) {
		if logger != nil {
			w.logger = logger
		}
	}
}

// OfflineWorker — фоновый воркер, переводящий в offline интеграции с
// устаревшим last_seen_at (тикет 3.6, FR B4, protocol.md §6, см. godoc
// пакета). Работает НЕЗАВИСИМО от Consumer/Sink — статус не требует живого
// соединения ни машины, ни даже самого факта чтения шины прямо сейчас (FR
// B4: «статус не требует прямого синхронного соединения»). Собирается через
// NewOfflineWorker; нулевое значение не готово к использованию (нет queries).
type OfflineWorker struct {
	queries offlineMarker
	logger  *slog.Logger

	pollInterval     time.Duration
	offlineThreshold time.Duration
}

// NewOfflineWorker собирает OfflineWorker поверх sqlc-запросов. queries
// обязателен (nil — ошибка конструктора, не паника, тот же принцип, что и у
// bridge.New).
func NewOfflineWorker(queries offlineMarker, opts ...WorkerOption) (*OfflineWorker, error) {
	if queries == nil {
		return nil, fmt.Errorf("presence: nil queries")
	}
	w := &OfflineWorker{
		queries:          queries,
		logger:           slog.Default(),
		pollInterval:     defaultOfflinePollInterval,
		offlineThreshold: defaultOfflineThreshold,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w, nil
}

// Run — основной цикл OfflineWorker: каждые pollInterval переводит в offline
// все интеграции, чей last_seen_at старше offlineThreshold относительно
// текущего момента. Возвращает управление только при отмене ctx (nil,
// штатное завершение) — ошибка запроса логируется, но не останавливает
// цикл (временный сбой БД не должен навсегда остановить воркер, следующий
// тик попробует снова).
func (w *OfflineWorker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.markStale(ctx)
		}
	}
}

// markStale выполняет один проход: считает cutoff = now - offlineThreshold и
// переводит в offline все интеграции с last_seen_at раньше cutoff.
func (w *OfflineWorker) markStale(ctx context.Context) {
	cutoff := time.Now().UTC().Add(-w.offlineThreshold)
	if err := w.queries.MarkStaleIntegrationsOffline(ctx, pgtype.Timestamptz{Time: cutoff, Valid: true}); err != nil {
		if ctx.Err() != nil {
			return
		}
		w.logger.Warn("presence: MarkStaleIntegrationsOffline не удался", slog.String("error", err.Error()))
	}
}
