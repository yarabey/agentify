//go:build integration

// Integration-тесты bus-слоя на РЕАЛЬНОЙ Redpanda через testcontainers-go
// (тикет 3.2; стек — docs/01_tech_stack_and_architecture.md §3). Помечены тегом
// integration, чтобы обычный `make test` (unit) не требовал docker и был быстрым;
// CI-джоба `integration` гоняет `go test -tags=integration ./...`.
//
// Проверяемые сценарии (приёмка 3.2, ADR 0001):
//   (a) дубль сообщения (один message_id опубликован дважды) применяется Handler
//       РОВНО ОДИН РАЗ (дедуп по message_id; FR E7);
//   (b) серия сообщений с одним ключом партиции (task_id) видна Handler в порядке
//       seq (порядок в рамках задачи; §126).
//
// Контейнер чистится через testcontainers terminate (defer) + Ryuk reaper.

package bus

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"
)

// startRedpanda поднимает одиночный брокер Redpanda в контейнере и возвращает
// seed-адрес (host:port) и функцию очистки (terminate). Образ тянется через
// настроенный daemon registry-mirror (тикет 0.3, mirror.gcr.io) — прокси не обходим.
func startRedpanda(ctx context.Context, t *testing.T) (string, func()) {
	t.Helper()
	container, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.2.7")
	if err != nil {
		t.Fatalf("поднять Redpanda-контейнер: %v", err)
	}
	cleanup := func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate Redpanda: %v", err)
		}
	}
	seed, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		cleanup()
		t.Fatalf("получить seed-брокер: %v", err)
	}
	return seed, cleanup
}

// waitTopicReady ждёт, пока продьюсер сможет писать в топик (брокер готов).
func waitTopicReady(ctx context.Context, t *testing.T, seeds []string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
		if err == nil {
			if pingErr := cl.Ping(ctx); pingErr == nil {
				cl.Close()
				return
			}
			cl.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("брокер Redpanda не стал готов за отведённое время")
}

// newEnvelope собирает валидный конверт уровня задачи для тестов.
func newEnvelope(messageID, taskID, integrationID, typ string, seq int64) Envelope {
	tid := taskID
	return Envelope{
		MessageID:       messageID,
		TaskID:          &tid,
		IntegrationID:   integrationID,
		Type:            typ,
		Seq:             seq,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: ProtocolVersion,
		Payload:         json.RawMessage(`{}`),
	}
}

// TestIntegration_DuplicateAppliedOnce: публикуем ОДИН message_id дважды в
// machine.events и убеждаемся, что Handler сработал ровно один раз (дедуп по
// message_id поверх at-least-once; FR E7, приёмка 3.2).
func TestIntegration_DuplicateAppliedOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	seed, cleanup := startRedpanda(ctx, t)
	defer cleanup()
	seeds := []string{seed}
	waitTopicReady(ctx, t, seeds)

	if err := EnsureMVPTopics(ctx, seeds); err != nil {
		t.Fatalf("провижининг топиков: %v", err)
	}

	producer, err := NewProducer(seeds)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	dupID := NewMessageID()
	env := newEnvelope(dupID, "task-dup", "int-dup", "agent_progress", 1)

	// Публикуем один и тот же message_id дважды (имитация переотправки at-least-once).
	for i := 0; i < 2; i++ {
		if err := producer.PublishKeyed(ctx, TopicMachineEvents, PartitionKeyTaskID, env); err != nil {
			t.Fatalf("publish #%d: %v", i, err)
		}
	}

	var mu sync.Mutex
	applied := map[string]int{}
	handler := func(_ context.Context, e Envelope) error {
		mu.Lock()
		applied[e.MessageID]++
		mu.Unlock()
		return nil
	}

	consumer, err := NewConsumer(ConsumerConfig{
		Seeds:  seeds,
		Group:  "test-dedup-group",
		Topics: []string{TopicMachineEvents},
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	go func() { _ = consumer.Run(runCtx, handler) }()

	// Дать консьюмеру обработать обе записи.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := applied[dupID]
		mu.Unlock()
		if n >= 1 {
			// Подождём ещё немного, чтобы ВТОРАЯ копия точно дошла и была отвергнута.
			time.Sleep(2 * time.Second)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	mu.Lock()
	got := applied[dupID]
	mu.Unlock()
	if got != 1 {
		t.Fatalf("дубль должен примениться ровно один раз: Handler вызван %d раз для message_id=%s", got, dupID)
	}
	t.Logf("OK: дубль message_id=%s применён Handler ровно 1 раз", dupID)
}

// TestIntegration_OrderPreservedWithinTaskID: публикуем серию событий с одним
// task_id (один ключ партиции) и проверяем, что Handler видит их строго в порядке
// seq (порядок в рамках задачи; §126, приёмка 3.2).
func TestIntegration_OrderPreservedWithinTaskID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	seed, cleanup := startRedpanda(ctx, t)
	defer cleanup()
	seeds := []string{seed}
	waitTopicReady(ctx, t, seeds)

	if err := EnsureMVPTopics(ctx, seeds); err != nil {
		t.Fatalf("провижининг топиков: %v", err)
	}

	producer, err := NewProducer(seeds)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	const n = 50
	const taskID = "task-order"
	for i := 1; i <= n; i++ {
		env := newEnvelope(NewMessageID(), taskID, "int-order", "agent_progress", int64(i))
		if err := producer.PublishKeyed(ctx, TopicMachineEvents, PartitionKeyTaskID, env); err != nil {
			t.Fatalf("publish seq=%d: %v", i, err)
		}
	}

	var mu sync.Mutex
	var seen []int64
	handler := func(_ context.Context, e Envelope) error {
		mu.Lock()
		seen = append(seen, e.Seq)
		mu.Unlock()
		return nil
	}

	consumer, err := NewConsumer(ConsumerConfig{
		Seeds:  seeds,
		Group:  "test-order-group",
		Topics: []string{TopicMachineEvents},
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	go func() { _ = consumer.Run(runCtx, handler) }()

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := len(seen)
		mu.Unlock()
		if got >= n {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Fatalf("ожидалось %d сообщений, получено %d: %v", n, len(seen), seen)
	}
	for i, s := range seen {
		if s != int64(i+1) {
			t.Fatalf("порядок нарушен на позиции %d: seq=%d (ожидался %d); вся серия=%v", i, s, i+1, seen)
		}
	}
	t.Logf("OK: %d событий task_id=%s обработаны в порядке seq 1..%d", n, taskID, n)
}
