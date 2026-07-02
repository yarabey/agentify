package task

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/notify"
)

// defaultAnswerTimeoutPollInterval — как часто AnswerTimeoutWorker опрашивает
// БД в поисках задач с устаревшим agent_question (внутренняя деталь
// реализации, НЕ настраивается через env пользователем — тот же принцип, что
// и у defaultStalePollInterval).
const defaultAnswerTimeoutPollInterval = 10 * time.Second

// defaultAnswerTimeoutThreshold — порог устаревания последнего agent_question
// активной (waiting_user) задачи по умолчанию (FR F5, тикет 6.7). Продуктовое
// решение об окончательном значении не зафиксировано (docs/MANUAL_STEPS.md:
// «через сколько без ответа — напоминание» — чекбокс не отмечен); дефолт 5m
// выбран заметно больше времени, обычно нужного человеку на прочтение и
// написание ответа, чтобы напоминание не приходило слишком рано и не
// раздражало пользователя, но и не было бесполезно поздним. Настраивается
// через ORCH_ANSWER_TIMEOUT_THRESHOLD в orchestrator/main.go
// (WithAnswerTimeoutThreshold).
const defaultAnswerTimeoutThreshold = 5 * time.Minute

// AnswerTimeoutBehavior — настраиваемое поведение после напоминания (FR F5,
// решение F2, docs/MANUAL_STEPS.md — окончательное значение не зафиксировано
// продуктом, дефолт AnswerTimeoutBehaviorWait как менее разрушительный).
type AnswerTimeoutBehavior string

const (
	// AnswerTimeoutBehaviorWait — после напоминания просто продолжать ждать
	// ответа пользователя бессрочно (дефолт).
	AnswerTimeoutBehaviorWait AnswerTimeoutBehavior = "wait"
	// AnswerTimeoutBehaviorAutoCancel — после напоминания автоматически
	// отменить задачу (переиспользует УЖЕ существующий переход FSM
	// TriggerCancelRequested, построенный в тикете 5.2 для тикета 8.4).
	// ВАЖНО: это ТОЛЬКО перевод статуса задачи в cancelled в БД — гарантированная
	// доставка команды отмены на машину агента (чтобы сам процесс агента
	// реально остановился) — отдельный, ещё не реализованный функционал тикета
	// 8.4 («Отмена доходит до машины»). Эта авто-отмена НЕ публикует ничего в
	// machine.commands.
	AnswerTimeoutBehaviorAutoCancel AnswerTimeoutBehavior = "auto_cancel"
)

// answerTimeoutLister — узкий интерфейс sqlc-запроса, нужного
// AnswerTimeoutWorker (тикет 6.7, FR F5): найти задачи в waiting_user, чей
// самый свежий agent_question устарел дольше порога. Реализуется
// *db.Queries; сужение позволяет юнит-тестам подменить запрос фейком без
// поднятия Postgres (тот же приём, что staleLister).
type answerTimeoutLister interface {
	// ListWaitingUserTasksWithStaleQuestion возвращает задачи в waiting_user,
	// чей самый свежий (по seq) agent_question создан раньше cutoff — вместе с
	// id этой конкретной записи task_events (QuestionEventID), нужным для
	// дедупликации повторных напоминаний по одному и тому же вопросу.
	ListWaitingUserTasksWithStaleQuestion(ctx context.Context, cutoff pgtype.Timestamptz) ([]db.ListWaitingUserTasksWithStaleQuestionRow, error)
}

// answerNotifier — узкий интерфейс доставки доменного уведомления
// (см. notify.Notification), нужный AnswerTimeoutWorker. Объявлен ЛОКАЛЬНО в
// пакете task (а не через orchestrator/internal/api.Notifier), чтобы не
// создавать циклический импорт task↔api; пакет task при этом свободно
// импортирует orchestrator/internal/notify — там нет цикла, это чистый
// доменный тип без зависимостей на task/api.
type answerNotifier interface {
	Notify(ctx context.Context, n notify.Notification) error
}

// AnswerTimeoutWorkerOption — функциональная опция NewAnswerTimeoutWorker (по
// аналогии со StaleWorkerOption): неположительные значения длительности,
// нераспознанное поведение и nil-логгер игнорируются — нужно тестам, чтобы не
// ждать реальные секунды/минуты и не подставлять невалидные значения.
type AnswerTimeoutWorkerOption func(*AnswerTimeoutWorker)

// WithAnswerTimeoutPollInterval задаёт паузу между опросами БД (см.
// defaultAnswerTimeoutPollInterval). Неположительное значение игнорируется.
func WithAnswerTimeoutPollInterval(d time.Duration) AnswerTimeoutWorkerOption {
	return func(w *AnswerTimeoutWorker) {
		if d > 0 {
			w.pollInterval = d
		}
	}
}

// WithAnswerTimeoutThreshold задаёт порог устаревания последнего
// agent_question (см. defaultAnswerTimeoutThreshold, ORCH_ANSWER_TIMEOUT_THRESHOLD,
// тикет 6.7, FR F5). Неположительное значение игнорируется.
func WithAnswerTimeoutThreshold(d time.Duration) AnswerTimeoutWorkerOption {
	return func(w *AnswerTimeoutWorker) {
		if d > 0 {
			w.threshold = d
		}
	}
}

// WithAnswerTimeoutBehavior задаёт поведение после напоминания (см.
// AnswerTimeoutBehavior, ORCH_ANSWER_TIMEOUT_BEHAVIOR, FR F5, решение F2).
// Принимает ТОЛЬКО AnswerTimeoutBehaviorWait/AnswerTimeoutBehaviorAutoCancel —
// любое другое значение (в т.ч. пустая строка или опечатка в env) игнорируется,
// дефолт (AnswerTimeoutBehaviorWait) сохраняется, чтобы некорректная
// конфигурация не привела к неожиданной авто-отмене задач.
func WithAnswerTimeoutBehavior(b AnswerTimeoutBehavior) AnswerTimeoutWorkerOption {
	return func(w *AnswerTimeoutWorker) {
		switch b {
		case AnswerTimeoutBehaviorWait, AnswerTimeoutBehaviorAutoCancel:
			w.behavior = b
		}
	}
}

// WithAnswerTimeoutLogger задаёт логгер AnswerTimeoutWorker (по умолчанию —
// slog.Default()).
func WithAnswerTimeoutLogger(logger *slog.Logger) AnswerTimeoutWorkerOption {
	return func(w *AnswerTimeoutWorker) {
		if logger != nil {
			w.logger = logger
		}
	}
}

// AnswerTimeoutWorker — фоновый воркер «таймаута ответа» (тикет 6.7, FR F5,
// docs/MANUAL_STEPS.md): за каждый тик находит задачи в waiting_user, чей
// самый свежий agent_question устарел дольше threshold, и для каждой (не
// более одного раза на конкретный вопрос) отправляет напоминание
// (notify.KindAnswerReminder) через notifier, а при настроенном поведении
// AnswerTimeoutBehaviorAutoCancel — дополнительно переводит задачу в cancelled
// через УЖЕ существующий переход FSM TriggerCancelRequested (построен в
// тикете 5.2 для тикета 8.4). В отличие от StaleWorker (который следит за
// heartbeat интеграции — техническим разрывом соединения машины),
// AnswerTimeoutWorker следит за ЧЕЛОВЕКОМ — тем, что пользователь долго не
// отвечает на уже заданный вопрос; источник данных — task_events, а не
// integrations.last_seen_at.
//
// Граница с тикетом 8.4 («Отмена доходит до машины»): AnswerTimeoutBehaviorAutoCancel
// здесь — ТОЛЬКО перевод tasks.status в cancelled в БД. Гарантированная доставка
// команды отмены на машину агента (чтобы сам процесс агента реально
// остановился) — отдельный, ещё не реализованный функционал тикета 8.4; этот
// воркер НИЧЕГО не публикует в machine.commands.
//
// Граница с тикетами 7.2/7.3/7.4 («Уведомления»): доставка самого
// напоминания пользователю (web WS / Telegram) реализуется notifier —
// конкретная реализация подключается ОТДЕЛЬНО (см. main.go, notifier=nil до
// тех пор); notifier == nil для AnswerTimeoutWorker штатен — напоминание
// тогда просто не доставляется, дедупликация (handled) при этом всё равно
// работает, чтобы при появлении реального notifier не случилось лавины
// напоминаний по старым вопросам.
//
// Собирается через NewAnswerTimeoutWorker; нулевое значение не готово к
// использованию (нет queries/transition).
type AnswerTimeoutWorker struct {
	queries    answerTimeoutLister
	transition transitionFunc
	notifier   answerNotifier
	logger     *slog.Logger

	pollInterval time.Duration
	threshold    time.Duration
	behavior     AnswerTimeoutBehavior

	// handled — taskID → id последнего обработанного agent_question
	// (question_event_id): дедупликация повторных напоминаний между тиками по
	// одному и тому же вопросу. Доступ ТОЛЬКО из одной горутины внутри
	// Run/tick — как и остальные поля AnswerTimeoutWorker (нет мьютекса, тот
	// же принцип, что у StaleWorker).
	handled map[pgtype.UUID]pgtype.UUID
}

// NewAnswerTimeoutWorker собирает AnswerTimeoutWorker поверх sqlc-запроса,
// перехода FSM и (опционального) получателя уведомлений. transitioner и
// queries обязательны (nil — ошибка конструктора, не паника, тот же принцип,
// что и у NewStaleWorker). notifier МОЖЕТ быть nil — это штатно: напоминания
// тогда просто не доставляются, пока каналы доставки (тикеты 7.2/7.3) не
// подключат реальный экземпляр (тот же nil-safe принцип, что у
// AckSink/EventSink/Notifier в api.Server).
func NewAnswerTimeoutWorker(transitioner *Transitioner, queries answerTimeoutLister, notifier answerNotifier, opts ...AnswerTimeoutWorkerOption) (*AnswerTimeoutWorker, error) {
	if transitioner == nil {
		return nil, fmt.Errorf("task: nil transitioner")
	}
	if queries == nil {
		return nil, fmt.Errorf("task: nil queries")
	}
	w := &AnswerTimeoutWorker{
		queries:      queries,
		transition:   transitioner.Transition,
		notifier:     notifier,
		logger:       slog.Default(),
		pollInterval: defaultAnswerTimeoutPollInterval,
		threshold:    defaultAnswerTimeoutThreshold,
		behavior:     AnswerTimeoutBehaviorWait,
		handled:      make(map[pgtype.UUID]pgtype.UUID),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w, nil
}

// Run — основной цикл AnswerTimeoutWorker: каждые pollInterval выполняет один
// проход (см. tick). Возвращает управление только при отмене ctx (nil,
// штатное завершение) — ошибка отдельного запроса/перехода/уведомления
// логируется, но не останавливает цикл (тот же принцип, что у StaleWorker.Run).
func (w *AnswerTimeoutWorker) Run(ctx context.Context) error {
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

// tick выполняет один проход: считает cutoff = now - threshold, находит
// задачи с устаревшим вопросом и обрабатывает каждую (см.
// handleStaleQuestion), затем прунит handled от id задач, отсутствующих в
// текущем результате (задача перестала ждать ответ — ответили, отменили,
// зависла и т.п. — следующее появление ТОЙ ЖЕ задачи должно снова считаться
// новым напоминанием, а не подавляться устаревшей записью дедупа).
func (w *AnswerTimeoutWorker) tick(ctx context.Context) {
	cutoff := pgtype.Timestamptz{Time: time.Now().UTC().Add(-w.threshold), Valid: true}

	rows, err := w.queries.ListWaitingUserTasksWithStaleQuestion(ctx, cutoff)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.logger.Warn("task: ListWaitingUserTasksWithStaleQuestion не удался", slog.String("error", err.Error()))
		return
	}

	current := make(map[pgtype.UUID]struct{}, len(rows))
	for _, row := range rows {
		current[row.TaskID] = struct{}{}
		w.handleStaleQuestion(ctx, row)
	}

	for taskID := range w.handled {
		if _, ok := current[taskID]; !ok {
			delete(w.handled, taskID)
		}
	}
}

// handleStaleQuestion обрабатывает одну задачу с устаревшим agent_question:
// если этот КОНКРЕТНЫЙ вопрос (по question_event_id) уже был обработан на
// предыдущем тике — ничего не делает (дедуп); иначе запоминает вопрос ПЕРЕД
// попытками уведомления/отмены (чтобы временная ошибка не вызывала лавину
// повторов на следующем тике), опционально отправляет напоминание через
// notifier и, при поведении AnswerTimeoutBehaviorAutoCancel, переводит задачу
// в cancelled (TriggerCancelRequested). Ошибки уведомления и перехода
// логируются, но не прерывают обработку остальных задач тика.
func (w *AnswerTimeoutWorker) handleStaleQuestion(ctx context.Context, row db.ListWaitingUserTasksWithStaleQuestionRow) {
	if last, ok := w.handled[row.TaskID]; ok && last == row.QuestionEventID {
		return
	}
	w.handled[row.TaskID] = row.QuestionEventID

	if w.notifier != nil {
		if err := w.notifier.Notify(ctx, notify.Notification{
			TaskID:    row.TaskID,
			UserID:    row.UserID,
			Kind:      notify.KindAnswerReminder,
			Payload:   nil,
			CreatedAt: time.Now().UTC(),
		}); err != nil {
			w.logger.Warn("task: отправка напоминания о таймауте ответа не удалась",
				slog.String("task_id", row.TaskID.String()), slog.String("error", err.Error()))
		}
	}

	if w.behavior == AnswerTimeoutBehaviorAutoCancel {
		if _, _, err := w.transition(ctx, row.TaskID, TriggerCancelRequested); err != nil {
			w.logger.Warn("task: авто-отмена задачи по таймауту ответа не удалась",
				slog.String("task_id", row.TaskID.String()), slog.String("error", err.Error()))
		}
	}
}
