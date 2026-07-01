// Юнит-тесты OfflineWorker (тикет 3.6, FR B4, protocol.md §6) — без
// Postgres: запрос подменяется фейком (offlineMarker). Опции
// WithPollInterval/WithOfflineThreshold позволяют прогонять цикл на
// миллисекундах, не дожидаясь реальных 45s.
package presence

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// fakeOfflineMarker — фейковая реализация offlineMarker: потокобезопасно
// запоминает все cutoff, с которыми был вызван MarkStaleIntegrationsOffline.
type fakeOfflineMarker struct {
	mu      sync.Mutex
	cutoffs []time.Time
}

func (f *fakeOfflineMarker) MarkStaleIntegrationsOffline(_ context.Context, cutoff pgtype.Timestamptz) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cutoffs = append(f.cutoffs, cutoff.Time)
	return nil
}

func (f *fakeOfflineMarker) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.cutoffs)
}

func (f *fakeOfflineMarker) lastCutoff() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cutoffs[len(f.cutoffs)-1]
}

// TestOfflineWorker_Run_CallsMarkStaleWithExpectedCutoff — с быстрыми
// WithPollInterval/WithOfflineThreshold воркер за разумное время делает хотя
// бы один вызов MarkStaleIntegrationsOffline с cutoff ~ now-threshold.
func TestOfflineWorker_Run_CallsMarkStaleWithExpectedCutoff(t *testing.T) {
	marker := &fakeOfflineMarker{}
	threshold := 50 * time.Millisecond
	w, err := NewOfflineWorker(marker, WithPollInterval(10*time.Millisecond), WithOfflineThreshold(threshold))
	if err != nil {
		t.Fatalf("NewOfflineWorker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for marker.callCount() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("MarkStaleIntegrationsOffline ни разу не вызван за отведённое время")
		case <-time.After(5 * time.Millisecond):
		}
	}

	before := time.Now().UTC().Add(-threshold)
	gotCutoff := marker.lastCutoff()
	// Допуск: cutoff должен быть близко к now-threshold на момент вызова, с
	// запасом на планировщик/тики (существенно меньше самого threshold).
	if diff := before.Sub(gotCutoff); diff < -time.Second || diff > time.Second {
		t.Fatalf("cutoff=%v далеко от ожидаемого ~%v (diff=%v)", gotCutoff, before, diff)
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

// TestOfflineWorker_Run_StopsOnContextCancel — Run завершается (nil) сразу
// после отмены ctx, даже если ни одного тика ещё не было.
func TestOfflineWorker_Run_StopsOnContextCancel(t *testing.T) {
	marker := &fakeOfflineMarker{}
	w, err := NewOfflineWorker(marker, WithPollInterval(time.Hour))
	if err != nil {
		t.Fatalf("NewOfflineWorker: %v", err)
	}

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

// TestNewOfflineWorker_NilQueries — nil queries — ошибка конструктора, не
// паника.
func TestNewOfflineWorker_NilQueries(t *testing.T) {
	if _, err := NewOfflineWorker(nil); err == nil {
		t.Fatal("NewOfflineWorker(nil): ожидалась ошибка")
	}
}

// TestWithPollInterval_IgnoresNonPositive — неположительное значение
// WithPollInterval игнорируется (дефолт сохраняется).
func TestWithPollInterval_IgnoresNonPositive(t *testing.T) {
	w, err := NewOfflineWorker(&fakeOfflineMarker{}, WithPollInterval(0), WithPollInterval(-time.Second))
	if err != nil {
		t.Fatalf("NewOfflineWorker: %v", err)
	}
	if w.pollInterval != defaultOfflinePollInterval {
		t.Fatalf("pollInterval=%v, ожидался дефолт %v", w.pollInterval, defaultOfflinePollInterval)
	}
}

// TestWithOfflineThreshold_IgnoresNonPositive — неположительное значение
// WithOfflineThreshold игнорируется (дефолт сохраняется).
func TestWithOfflineThreshold_IgnoresNonPositive(t *testing.T) {
	w, err := NewOfflineWorker(&fakeOfflineMarker{}, WithOfflineThreshold(0), WithOfflineThreshold(-time.Second))
	if err != nil {
		t.Fatalf("NewOfflineWorker: %v", err)
	}
	if w.offlineThreshold != defaultOfflineThreshold {
		t.Fatalf("offlineThreshold=%v, ожидался дефолт %v", w.offlineThreshold, defaultOfflineThreshold)
	}
}
