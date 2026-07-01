package outbox

import (
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/yarabey/agentify/internal/bus"
)

// testEnvelope собирает минимальный валидный конверт для тестов outbox —
// поля, требуемые Envelope.Validate (вызывается Enqueue через env.Marshal).
func testEnvelope(t *testing.T, messageID string) bus.Envelope {
	t.Helper()
	if messageID == "" {
		messageID = bus.NewMessageID()
	}
	return bus.Envelope{
		MessageID:       messageID,
		IntegrationID:   uuid.NewString(),
		Type:            bus.MessageTypeAgentProgress,
		Ts:              "2026-06-30T12:00:00Z",
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         []byte(`{}`),
	}
}

// TestEnqueuePendingOrder проверяет, что Pending возвращает события строго в
// порядке Enqueue (FIFO по monotonic seq, а не по message_id — см. godoc
// пакета про то, почему UUID не годится как ключ сортировки).
func TestEnqueuePendingOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	ids := []string{bus.NewMessageID(), bus.NewMessageID(), bus.NewMessageID()}
	for _, id := range ids {
		if err := s.Enqueue(testEnvelope(t, id)); err != nil {
			t.Fatalf("Enqueue(%s): %v", id, err)
		}
	}

	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != len(ids) {
		t.Fatalf("len(pending) = %d, want %d", len(pending), len(ids))
	}
	for i, id := range ids {
		if pending[i].MessageID != id {
			t.Fatalf("pending[%d].MessageID = %q, want %q (порядок вставки нарушен)", i, pending[i].MessageID, id)
		}
	}
}

// TestDeleteRemovesOnlyTargetEvent проверяет, что Delete убирает ровно ту
// запись, чей message_id передан, не трогая соседние.
func TestDeleteRemovesOnlyTargetEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	first := bus.NewMessageID()
	second := bus.NewMessageID()
	third := bus.NewMessageID()
	for _, id := range []string{first, second, third} {
		if err := s.Enqueue(testEnvelope(t, id)); err != nil {
			t.Fatalf("Enqueue(%s): %v", id, err)
		}
	}

	if err := s.Delete(second); err != nil {
		t.Fatalf("Delete(%s): %v", second, err)
	}

	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("len(pending) = %d, want 2", len(pending))
	}
	if pending[0].MessageID != first || pending[1].MessageID != third {
		t.Fatalf("pending = [%s %s], want [%s %s]", pending[0].MessageID, pending[1].MessageID, first, third)
	}
}

// TestDeleteUnknownMessageIDIsNoop проверяет, что Delete неизвестного (или
// уже удалённого) message_id не возвращает ошибку и не задевает остальные
// записи — at-least-once допускает повторные/поздние ack (см. godoc Delete).
func TestDeleteUnknownMessageIDIsNoop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	known := bus.NewMessageID()
	if err := s.Enqueue(testEnvelope(t, known)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := s.Delete(uuid.NewString()); err != nil {
		t.Fatalf("Delete(неизвестный id): %v", err)
	}
	// Повторный Delete уже удалённого id — тоже no-op.
	if err := s.Delete(known); err != nil {
		t.Fatalf("Delete(known): %v", err)
	}
	if err := s.Delete(known); err != nil {
		t.Fatalf("повторный Delete(known): %v", err)
	}

	pending, err := s.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("len(pending) = %d, want 0", len(pending))
	}
}

// TestDurabilityAcrossRestart — ключевой тест приёмки outbox: Enqueue →
// Close (имитация штатной остановки процесса агента) → повторный Open по
// ТОМУ ЖЕ пути (имитация рестарта процесса) → Pending должен вернуть то же
// самое, что было до Close. Это и есть durability, ради которой outbox
// вообще существует (docs/protocol.md §1 «локальный durable outbox на
// агенте», приёмка тикета 3.5 «переживает … рестарт агента»).
func TestDurabilityAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")

	s1, err := Open(path)
	if err != nil {
		t.Fatalf("Open (1): %v", err)
	}

	first := bus.NewMessageID()
	second := bus.NewMessageID()
	if err := s1.Enqueue(testEnvelope(t, first)); err != nil {
		t.Fatalf("Enqueue(1): %v", err)
	}
	if err := s1.Enqueue(testEnvelope(t, second)); err != nil {
		t.Fatalf("Enqueue(2): %v", err)
	}

	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("Open (2, после рестарта): %v", err)
	}
	defer func() { _ = s2.Close() }()

	pending, err := s2.Pending()
	if err != nil {
		t.Fatalf("Pending после рестарта: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("len(pending) после рестарта = %d, want 2", len(pending))
	}
	if pending[0].MessageID != first || pending[1].MessageID != second {
		t.Fatalf("pending после рестарта = [%s %s], want [%s %s]",
			pending[0].MessageID, pending[1].MessageID, first, second)
	}
}

// TestEnqueueEmptyMessageID проверяет, что Enqueue отклоняет конверт без
// message_id — Delete по нему впоследствии был бы невозможен (индекс строится
// по message_id, см. godoc пакета).
func TestEnqueueEmptyMessageID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	env := testEnvelope(t, "irrelevant")
	env.MessageID = ""
	if err := s.Enqueue(env); err == nil {
		t.Fatal("ожидалась ошибка на пустом message_id")
	}
}
