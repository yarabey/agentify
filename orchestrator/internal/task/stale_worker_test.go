// Юнит-тесты StaleWorker (тикет 5.7, FR E5, protocol.md §6) — без Postgres:
// оба sqlc-запроса подменяются фейком (staleLister), а сам переход FSM —
// функцией-шпионом (transitionFunc), поскольку Transitioner — конкретный тип
// без интерфейса (см. godoc stale_worker.go/transition.go). Опции
// WithStalePollInterval/WithStaleThreshold позволяют прогонять цикл на
// миллисекундах, не дожидаясь реальных секунд/минут.
package task

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// fakeStaleLister — фейковая реализация staleLister: потокобезопасно
// возвращает заранее заданные списки id и запоминает все cutoff, с которыми
// был вызван каждый из двух методов.
type fakeStaleLister struct {
	mu sync.Mutex

	staleIDs     []pgtype.UUID
	recoveredIDs []pgtype.UUID

	staleCutoffs     []time.Time
	recoveredCutoffs []time.Time
}

func (f *fakeStaleLister) ListRunningTasksWithStaleMachine(_ context.Context, cutoff pgtype.Timestamptz) ([]pgtype.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.staleCutoffs = append(f.staleCutoffs, cutoff.Time)
	return f.staleIDs, nil
}

func (f *fakeStaleLister) ListStaleTasksWithRecoveredMachine(_ context.Context, cutoff pgtype.Timestamptz) ([]pgtype.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recoveredCutoffs = append(f.recoveredCutoffs, cutoff.Time)
	return f.recoveredIDs, nil
}

func (f *fakeStaleLister) staleCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.staleCutoffs)
}

func (f *fakeStaleLister) recoveredCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.recoveredCutoffs)
}

func (f *fakeStaleLister) lastStaleCutoff() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.staleCutoffs[len(f.staleCutoffs)-1]
}

func (f *fakeStaleLister) lastRecoveredCutoff() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.recoveredCutoffs[len(f.recoveredCutoffs)-1]
}

// transitionCall — один вызов transitionFunc-шпиона: какая задача и с каким
// триггером.
type transitionCall struct {
	taskID  pgtype.UUID
	trigger Trigger
}

// spyTransitioner — функция-шпион вместо *Transitioner: потокобезопасно
// запоминает все вызовы и опционально возвращает ошибку для конкретного id
// (нужно проверить, что ошибка одного перехода не прерывает обработку
// остальных).
type spyTransitioner struct {
	mu      sync.Mutex
	calls   []transitionCall
	failIDs map[pgtype.UUID]bool
}

func (s *spyTransitioner) transition(_ context.Context, taskID pgtype.UUID, trigger Trigger) (Status, Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, transitionCall{taskID: taskID, trigger: trigger})
	if s.failIDs[taskID] {
		return "", "", fmt.Errorf("spy: искусственная ошибка перехода для %s", taskID.String())
	}
	return StatusRunning, StatusStale, nil
}

func (s *spyTransitioner) callsSnapshot() []transitionCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]transitionCall, len(s.calls))
	copy(out, s.calls)
	return out
}

func uuidFromByte(b byte) pgtype.UUID {
	var u pgtype.UUID
	u.Bytes[15] = b
	u.Valid = true
	return u
}

// newTestStaleWorker собирает StaleWorker в обход NewStaleWorker (который
// требует настоящий *Transitioner), напрямую подставляя лишь необходимые для
// теста поля — тот же приём, что применяют другие пакеты этого репозитория
// для юнит-тестов, минующих конструктор, требующий реальных зависимостей.
func newTestStaleWorker(t *testing.T, lister staleLister, spy *spyTransitioner, opts ...StaleWorkerOption) *StaleWorker {
	t.Helper()
	w := &StaleWorker{
		queries:        lister,
		transition:     spy.transition,
		logger:         slog.Default(),
		pollInterval:   defaultStalePollInterval,
		staleThreshold: defaultStaleThreshold,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// TestStaleWorker_Run_CallsBothListersWithExpectedCutoff — с быстрыми
// WithStalePollInterval/WithStaleThreshold воркер за разумное время делает
// хотя бы один вызов ОБОИХ listing-запросов с одним и тем же cutoff ~
// now-threshold (аналог presence.TestOfflineWorker_Run_CallsMarkStaleWithExpectedCutoff).
func TestStaleWorker_Run_CallsBothListersWithExpectedCutoff(t *testing.T) {
	lister := &fakeStaleLister{}
	spy := &spyTransitioner{}
	threshold := 50 * time.Millisecond
	w := newTestStaleWorker(t, lister, spy,
		WithStalePollInterval(10*time.Millisecond), WithStaleThreshold(threshold))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for lister.staleCallCount() == 0 || lister.recoveredCallCount() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("оба listing-запроса ни разу не были вызваны за отведённое время")
		case <-time.After(5 * time.Millisecond):
		}
	}

	before := time.Now().UTC().Add(-threshold)
	for _, got := range []time.Time{lister.lastStaleCutoff(), lister.lastRecoveredCutoff()} {
		if diff := before.Sub(got); diff < -time.Second || diff > time.Second {
			t.Fatalf("cutoff=%v далеко от ожидаемого ~%v (diff=%v)", got, before, diff)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run вернул ошибку после отмены ctx: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run не завершился после отмены ctx за отведённое время")
	}
}

// TestStaleWorker_MarkStale_UsesTriggerTimeout — id, найденные
// ListRunningTasksWithStaleMachine, передаются в переход FSM РОВНО с
// TriggerTimeout — без реального Postgres.
func TestStaleWorker_MarkStale_UsesTriggerTimeout(t *testing.T) {
	id1, id2 := uuidFromByte(1), uuidFromByte(2)
	lister := &fakeStaleLister{staleIDs: []pgtype.UUID{id1, id2}}
	spy := &spyTransitioner{}
	w := newTestStaleWorker(t, lister, spy)

	w.markStale(context.Background(), pgtype.Timestamptz{Time: time.Now(), Valid: true})

	calls := spy.callsSnapshot()
	if len(calls) != 2 {
		t.Fatalf("ожидалось 2 вызова перехода, получено %d", len(calls))
	}
	for i, want := range []pgtype.UUID{id1, id2} {
		if calls[i].taskID != want {
			t.Fatalf("вызов %d: taskID=%v, хотим %v", i, calls[i].taskID, want)
		}
		if calls[i].trigger != TriggerTimeout {
			t.Fatalf("вызов %d: trigger=%s, хотим %s", i, calls[i].trigger, TriggerTimeout)
		}
	}
}

// TestStaleWorker_RecoverStale_UsesTriggerMachineRecovered — id, найденные
// ListStaleTasksWithRecoveredMachine, передаются в переход FSM РОВНО с
// TriggerMachineRecovered.
func TestStaleWorker_RecoverStale_UsesTriggerMachineRecovered(t *testing.T) {
	id1 := uuidFromByte(3)
	lister := &fakeStaleLister{recoveredIDs: []pgtype.UUID{id1}}
	spy := &spyTransitioner{}
	w := newTestStaleWorker(t, lister, spy)

	w.recoverStale(context.Background(), pgtype.Timestamptz{Time: time.Now(), Valid: true})

	calls := spy.callsSnapshot()
	if len(calls) != 1 {
		t.Fatalf("ожидался 1 вызов перехода, получено %d", len(calls))
	}
	if calls[0].taskID != id1 {
		t.Fatalf("taskID=%v, хотим %v", calls[0].taskID, id1)
	}
	if calls[0].trigger != TriggerMachineRecovered {
		t.Fatalf("trigger=%s, хотим %s", calls[0].trigger, TriggerMachineRecovered)
	}
}

// TestStaleWorker_MarkStale_OneFailureDoesNotStopOthers — ошибка перехода для
// одного id (например, гонка — статус успел смениться конкурентно) не должна
// прерывать обработку остальных id из того же среза.
func TestStaleWorker_MarkStale_OneFailureDoesNotStopOthers(t *testing.T) {
	id1, id2, id3 := uuidFromByte(1), uuidFromByte(2), uuidFromByte(3)
	lister := &fakeStaleLister{staleIDs: []pgtype.UUID{id1, id2, id3}}
	spy := &spyTransitioner{failIDs: map[pgtype.UUID]bool{id2: true}}
	w := newTestStaleWorker(t, lister, spy)

	w.markStale(context.Background(), pgtype.Timestamptz{Time: time.Now(), Valid: true})

	calls := spy.callsSnapshot()
	if len(calls) != 3 {
		t.Fatalf("ожидалось 3 вызова перехода (несмотря на ошибку одного из них), получено %d", len(calls))
	}
	for i, want := range []pgtype.UUID{id1, id2, id3} {
		if calls[i].taskID != want {
			t.Fatalf("вызов %d: taskID=%v, хотим %v", i, calls[i].taskID, want)
		}
	}
}

// TestNewStaleWorker_NilArgs — nil transitioner или nil queries — ошибка
// конструктора, не паника.
func TestNewStaleWorker_NilArgs(t *testing.T) {
	if _, err := NewStaleWorker(nil, &fakeStaleLister{}); err == nil {
		t.Fatal("NewStaleWorker(nil transitioner, ...): ожидалась ошибка")
	}
	if _, err := NewStaleWorker(NewTransitioner(nil), nil); err == nil {
		t.Fatal("NewStaleWorker(..., nil queries): ожидалась ошибка")
	}
}

// TestStaleWorker_Run_StopsOnContextCancel — Run завершается (nil) сразу
// после отмены ctx, даже если ни одного тика ещё не было.
func TestStaleWorker_Run_StopsOnContextCancel(t *testing.T) {
	w := newTestStaleWorker(t, &fakeStaleLister{}, &spyTransitioner{}, WithStalePollInterval(time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run вернул ошибку после отмены ctx: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run не завершился после отмены ctx за отведённое время")
	}
}

// TestWithStalePollInterval_IgnoresNonPositive — неположительное значение
// WithStalePollInterval игнорируется (дефолт сохраняется).
func TestWithStalePollInterval_IgnoresNonPositive(t *testing.T) {
	w, err := NewStaleWorker(NewTransitioner(nil), &fakeStaleLister{}, WithStalePollInterval(0), WithStalePollInterval(-time.Second))
	if err != nil {
		t.Fatalf("NewStaleWorker: %v", err)
	}
	if w.pollInterval != defaultStalePollInterval {
		t.Fatalf("pollInterval=%v, ожидался дефолт %v", w.pollInterval, defaultStalePollInterval)
	}
}

// TestWithStaleThreshold_IgnoresNonPositive — неположительное значение
// WithStaleThreshold игнорируется (дефолт сохраняется).
func TestWithStaleThreshold_IgnoresNonPositive(t *testing.T) {
	w, err := NewStaleWorker(NewTransitioner(nil), &fakeStaleLister{}, WithStaleThreshold(0), WithStaleThreshold(-time.Second))
	if err != nil {
		t.Fatalf("NewStaleWorker: %v", err)
	}
	if w.staleThreshold != defaultStaleThreshold {
		t.Fatalf("staleThreshold=%v, ожидался дефолт %v", w.staleThreshold, defaultStaleThreshold)
	}
}
