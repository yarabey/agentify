// Юнит-тесты моста (тикет 3.4, protocol.md §5) — без Redpanda/Docker (тег
// integration не нужен): консьюмер подменяется фейком (busConsumer), а
// WS-соединение — настоящее (httptest.NewServer + coder/websocket), как и в
// orchestrator/internal/api/machine_ws_integration_test.go, только без БД.
//
// Покрывает приёмку тикета 3.4 на уровне одной записи (deliver):
//   - офлайн-машина (нет активного соединения) → offset НЕ коммитится;
//   - нет ack за ackTimeout → запись доставляется повторно, offset НЕ коммитится;
//   - ack получен → offset коммитится РОВНО один раз, без лишних повторов;
//   - битый конверт записи → offset коммитится (пропуск, как у Consumer.applyRecord);
//   - HandleAck безопасен к неизвестному/повторному message_id (не паникует).
//
// Полный сквозной сценарий (реальная Redpanda + commit/no-commit на УРОВНЕ
// топика) — в bridge_integration_test.go (тег integration).
package bridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yarabey/agentify/internal/bus"
)

// fakeConsumer — фейковая реализация busConsumer для юнит-тестов: PollFetches
// не используется тестами deliver() напрямую (блокируется до отмены ctx,
// чтобы не паниковать, если случайно вызван), CommitRecords копит
// закоммиченные записи под мьютексом.
type fakeConsumer struct {
	mu        sync.Mutex
	committed []*kgo.Record
}

func (f *fakeConsumer) PollFetches(ctx context.Context) kgo.Fetches {
	<-ctx.Done()
	return nil
}

func (f *fakeConsumer) CommitRecords(_ context.Context, recs ...*kgo.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed = append(f.committed, recs...)
	return nil
}

func (f *fakeConsumer) committedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.committed)
}

// fakeConnRegistry — фейковая реализация ConnRegistry: статическая карта
// integration_id → *websocket.Conn, без реального реестра Server.machineConns.
type fakeConnRegistry struct {
	mu    sync.Mutex
	conns map[uuid.UUID]*websocket.Conn
}

func (f *fakeConnRegistry) MachineConn(id uuid.UUID) (*websocket.Conn, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.conns[id]
	return c, ok
}

// newWSPair поднимает настоящую пару WS-соединений (сервер/клиент) через
// httptest.NewServer — без Redpanda/Docker. serverConn — то, что регистрируется
// в fakeConnRegistry (deliver пишет именно в него, как в проде пишет в
// соединение из Server.machineConns); clientConn — сторона теста, играющая
// роль агента (читает команды, по необходимости шлёт ack через b.HandleAck
// напрямую — см. godoc файла, почему ack не идёт реальным WS-кадром в этих
// тестах).
func newWSPair(t *testing.T) (serverConn, clientConn *websocket.Conn, cleanup func()) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		accepted <- c
	}))

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1)
	clientConn, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		ts.Close()
		t.Fatalf("websocket.Dial: %v", err)
	}

	select {
	case serverConn = <-accepted:
	case <-time.After(5 * time.Second):
		ts.Close()
		t.Fatal("сервер не принял WS-соединение за отведённое время")
	}

	cleanup = func() {
		// CloseNow (а не Close) — без закрывающего рукопожатия: в тестах нет
		// смысла ждать ответный close-кадр от другой стороны (по умолчанию
		// в coder/websocket это до 5s на сторону и заметно замедляет сьют).
		_ = clientConn.CloseNow()
		_ = serverConn.CloseNow()
		ts.Close()
	}
	return serverConn, clientConn, cleanup
}

// testCommandEnvelope собирает валидный конверт команды (machine.commands,
// protocol.md §4) для тестов.
func testCommandEnvelope(messageID, integrationID string) bus.Envelope {
	return bus.Envelope{
		MessageID:       messageID,
		IntegrationID:   integrationID,
		Type:            bus.MessageTypeTaskAssigned,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         json.RawMessage(`{"text":"do something"}`),
	}
}

// testRecord маршалит конверт в запись Redpanda с ключом партиции
// integration_id (ADR 0001) — то, что Run передал бы воркеру машины.
func testRecord(t *testing.T, key string, env bus.Envelope) *kgo.Record {
	t.Helper()
	data, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return &kgo.Record{Key: []byte(key), Value: data}
}

// TestNew_RequiresConsumerAndConns — nil consumer/ConnRegistry — ошибка
// конструктора, не паника.
func TestNew_RequiresConsumerAndConns(t *testing.T) {
	if _, err := New(nil, &fakeConnRegistry{conns: map[uuid.UUID]*websocket.Conn{}}); err == nil {
		t.Fatal("ожидалась ошибка New при nil consumer")
	}

	consumer, err := bus.NewConsumer(bus.ConsumerConfig{
		Seeds:  []string{"127.0.0.1:1"},
		Group:  "test-group",
		Topics: []string{bus.TopicMachineCommands},
	})
	if err != nil {
		t.Fatalf("bus.NewConsumer (без реального брокера, лениво): %v", err)
	}
	defer consumer.Close()

	if _, err := New(consumer, nil); err == nil {
		t.Fatal("ожидалась ошибка New при nil ConnRegistry")
	}
}

// TestDeliver_OfflineMachine_NoCommit — машина офлайн (нет записи в
// ConnRegistry) → deliver не коммитит offset, пока не истечёт ctx (мост
// должен ждать и повторять поиск соединения, не коммитя запись, см. godoc
// пакета и protocol.md §5).
func TestDeliver_OfflineMachine_NoCommit(t *testing.T) {
	integrationID := uuid.New()
	conns := &fakeConnRegistry{conns: map[uuid.UUID]*websocket.Conn{}}
	consumer := &fakeConsumer{}
	b, err := New(consumer, conns, WithOfflineRetryInterval(10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	messageID := bus.NewMessageID()
	rec := testRecord(t, integrationID.String(), testCommandEnvelope(messageID, integrationID.String()))

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	b.deliver(ctx, rec)

	if got := consumer.committedCount(); got != 0 {
		t.Fatalf("офлайн-машина: offset не должен коммититься, committed=%d", got)
	}
}

// TestDeliver_AckReceived_CommitsOnce — машина онлайн, отвечает ack
// (имитируется прямым вызовом b.HandleAck, см. godoc newWSPair) → ровно один
// commit, без лишних переотправок («после ACK — нет» переотправки, приёмка
// тикета 3.4).
func TestDeliver_AckReceived_CommitsOnce(t *testing.T) {
	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	integrationID := uuid.New()
	conns := &fakeConnRegistry{conns: map[uuid.UUID]*websocket.Conn{integrationID: serverConn}}
	consumer := &fakeConsumer{}
	b, err := New(consumer, conns,
		WithAckTimeout(2*time.Second),
		WithOfflineRetryInterval(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	messageID := bus.NewMessageID()
	rec := testRecord(t, integrationID.String(), testCommandEnvelope(messageID, integrationID.String()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		b.deliver(ctx, rec)
		close(done)
	}()

	// Сторона "агента" читает доставленную команду и сверяет message_id.
	_, data, err := clientConn.Read(ctx)
	if err != nil {
		t.Fatalf("чтение команды агентом: %v", err)
	}
	var gotEnv bus.Envelope
	if err := json.Unmarshal(data, &gotEnv); err != nil {
		t.Fatalf("unmarshal доставленного конверта: %v", err)
	}
	if gotEnv.MessageID != messageID {
		t.Fatalf("доставлен не тот конверт: message_id=%s, ожидался %s", gotEnv.MessageID, messageID)
	}

	b.HandleAck(messageID)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("deliver не завершился после ack")
	}

	if got := consumer.committedCount(); got != 1 {
		t.Fatalf("после ack ожидался ровно 1 commit, получено %d", got)
	}
}

// TestDeliver_NoAck_RedeliversAndDoesNotCommit — машина онлайн, но НИКОГДА
// не отвечает ack → deliver переотправляет ту же команду (минимум дважды за
// отведённое время) и НЕ коммитит offset, пока не отменён ctx («без ACK
// сообщение переотправляется», приёмка тикета 3.4).
func TestDeliver_NoAck_RedeliversAndDoesNotCommit(t *testing.T) {
	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	integrationID := uuid.New()
	conns := &fakeConnRegistry{conns: map[uuid.UUID]*websocket.Conn{integrationID: serverConn}}
	consumer := &fakeConsumer{}
	b, err := New(consumer, conns,
		WithAckTimeout(50*time.Millisecond),
		WithOfflineRetryInterval(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	messageID := bus.NewMessageID()
	rec := testRecord(t, integrationID.String(), testCommandEnvelope(messageID, integrationID.String()))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		b.deliver(ctx, rec)
		close(done)
	}()

	readCtx, readCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer readCancel()
	deliveries := 0
	for deliveries < 2 {
		if _, _, err := clientConn.Read(readCtx); err != nil {
			t.Fatalf("чтение попытки доставки #%d: %v", deliveries+1, err)
		}
		deliveries++
	}
	cancel() // ack так и не пришёл — отменяем ctx, чтобы deliver завершился.

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("deliver не завершился после отмены ctx")
	}

	if got := consumer.committedCount(); got != 0 {
		t.Fatalf("без ack offset не должен коммититься, committed=%d", got)
	}
	if deliveries < 2 {
		t.Fatalf("ожидалась переотправка (>=2 попыток доставки), получено %d", deliveries)
	}
}

// TestDeliver_MalformedEnvelope_CommittedAndSkipped — битый конверт записи
// (невалидный JSON/обязательные поля) доставлять некому — пропускается и
// коммитится сразу, не блокируя машину навсегда (тот же принцип, что
// Consumer.applyRecord для machine.events).
func TestDeliver_MalformedEnvelope_CommittedAndSkipped(t *testing.T) {
	integrationID := uuid.New()
	conns := &fakeConnRegistry{conns: map[uuid.UUID]*websocket.Conn{}}
	consumer := &fakeConsumer{}
	b, err := New(consumer, conns)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	rec := &kgo.Record{Key: []byte(integrationID.String()), Value: []byte("это не json конверт")}
	b.deliver(context.Background(), rec)

	if got := consumer.committedCount(); got != 1 {
		t.Fatalf("битый конверт должен быть закоммичен (пропущен), committed=%d", got)
	}
}

// TestHandleAck_UnknownAndDuplicate_Safe — ack с неизвестным message_id и
// повторный ack для уже разрешённого message_id безопасно игнорируются:
// HandleAck не должен паниковать (в т.ч. на close уже закрытого канала).
func TestHandleAck_UnknownAndDuplicate_Safe(t *testing.T) {
	b, err := New(&fakeConsumer{}, &fakeConnRegistry{conns: map[uuid.UUID]*websocket.Conn{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Неизвестный message_id — нет ожидающей записи вовсе.
	b.HandleAck("unknown-message-id")

	ch := b.registerPending("mid-1")
	b.HandleAck("mid-1")
	select {
	case <-ch:
	default:
		t.Fatal("канал ожидания должен быть закрыт после HandleAck")
	}

	// Повторный ack для уже разрешённого message_id — не паникует.
	b.HandleAck("mid-1")
}
