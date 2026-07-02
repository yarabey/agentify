//go:build integration

// Integration-тесты моста на РЕАЛЬНОЙ Redpanda через testcontainers-go (тикет
// 3.4, protocol.md §5, ADR 0001), в дополнение к юнит-тестам bridge_test.go
// (та же логика deliver/HandleAck, но с фейковым busConsumer — без брокера).
// Юнит-тесты проверяют ВНУТРЕННЕЕ поведение (вызван ли CommitRecords фейка);
// здесь проверяется то, что юнит-тесты принципиально не могут — РЕАЛЬНОЕ
// состояние commit offset НА БРОКЕРЕ: после остановки моста поднимается
// свежий *bus.Consumer той же consumer group и смотрит, с какого offset он
// продолжит чтение topic machine.commands (та же техника, что и в
// internal/bus/integration_test.go).
//
// Покрывает обязательные по тикету 3.4 сценарии ("Тесты: без ACK сообщение
// переотправляется; после ACK — нет"):
//   - TestIntegration_Bridge_NoAck_RedeliversAndDoesNotCommit: мост доставляет
//     команду машине минимум дважды (внутренний ретрай деливери моста) и НЕ
//     коммитит offset — свежий консьюмер ТОЙ ЖЕ группы после остановки моста
//     вычитывает эту же запись заново;
//   - TestIntegration_Bridge_Ack_CommitsOnce_NoRedelivery: после ack от
//     "агента" (вызов b.HandleAck — связка реального ack-кадра WS с этим
//     методом отдельно проверена в orchestrator/internal/api/machine_ws_test.go)
//     мост коммитит offset РОВНО за эту запись — свежий консьюмер той же
//     группы не видит её повторно.
//
// Также покрывает приёмку тикета 5.6 «Оффлайн-постановка» (FR E4, §9 «Задача
// поставлена при оффлайн-машине»):
//   - TestIntegration_Bridge_OfflineThenOnline_DeliversWithoutLoss: команда
//     публикуется в machine.commands, когда машины НЕТ в ConnRegistry вообще
//     (полностью офлайн, а не просто "не отвечает ack") — мост крутится в
//     offline-ретрае (deliver, bridge.go) и не коммитит offset; когда машина
//     "выходит онлайн" (в реестр добавляется WS-соединение), мост её
//     подхватывает и доставляет ровно ту же команду (совпадающий message_id);
//     после ack — offset коммитится один раз, повторной доставки нет. Это тот
//     же offline-путь deliver, что и NoAck_RedeliversAndDoesNotCommit выше, но
//     там машина ONLINE с самого начала (просто не шлёт ack) — здесь же её нет
//     в реестре вообще до момента "подключения", что и есть сценарий 5.6.
//
// Использует общие тестовые помощники из bridge_test.go (тот файл БЕЗ тега
// integration, поэтому компилируется и здесь): newWSPair, fakeConnRegistry,
// testCommandEnvelope.
package bridge

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yarabey/agentify/internal/bus"
)

// startRedpanda поднимает одиночный брокер Redpanda в контейнере (см. тот же
// помощник в internal/bus/integration_test.go — здесь отдельная копия:
// неэкспортированный помощник другого пакета напрямую не переиспользовать).
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

// waitTopicReady ждёт, пока брокер начнёт отвечать на Ping.
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

// fetchWithFreshGroup поднимает СВОЕГО нового *bus.Consumer группы group и
// делает одну попытку PollFetches с таймаутом — нужен, чтобы проверить
// РЕАЛЬНОЕ состояние commit offset на брокере: то, с какого offset группа
// продолжит чтение, прямо отражает, был ли коммит ИМЕННО этой записи.
func fetchWithFreshGroup(ctx context.Context, t *testing.T, seeds []string, group string, timeout time.Duration) []*kgo.Record {
	t.Helper()
	consumer, err := bus.NewConsumer(bus.ConsumerConfig{
		Seeds:  seeds,
		Group:  group,
		Topics: []string{bus.TopicMachineCommands},
	})
	if err != nil {
		t.Fatalf("NewConsumer (проверочный, группа %s): %v", group, err)
	}
	defer consumer.Close()

	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	fetches := consumer.PollFetches(pollCtx)
	var recs []*kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		recs = append(recs, r)
	})
	return recs
}

// recordMessageID возвращает message_id конверта, упакованного в запись —
// помощник проверок в тестах ниже.
func recordMessageID(t *testing.T, rec *kgo.Record) string {
	t.Helper()
	var env bus.Envelope
	if err := json.Unmarshal(rec.Value, &env); err != nil {
		t.Fatalf("unmarshal конверта из записи: %v", err)
	}
	return env.MessageID
}

// TestIntegration_Bridge_NoAck_RedeliversAndDoesNotCommit — приёмочный сценарий
// тикета 3.4 «без ACK сообщение переотправляется»: реальная Redpanda,
// "агент" (тестовый WS-клиент) никогда не отвечает ack → мост доставляет
// команду минимум дважды и НЕ коммитит offset — после остановки моста свежий
// консьюмер ТОЙ ЖЕ consumer group вычитывает ту же запись заново.
func TestIntegration_Bridge_NoAck_RedeliversAndDoesNotCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	seed, cleanupRedpanda := startRedpanda(ctx, t)
	defer cleanupRedpanda()
	seeds := []string{seed}
	waitTopicReady(ctx, t, seeds)

	if err := bus.EnsureMVPTopics(ctx, seeds); err != nil {
		t.Fatalf("провижининг топиков: %v", err)
	}

	const group = "bridge-it-no-ack"
	integrationID := uuid.New()
	messageID := bus.NewMessageID()

	producer, err := bus.NewProducer(seeds)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	env := testCommandEnvelope(messageID, integrationID.String())
	if err := producer.PublishKeyed(ctx, bus.TopicMachineCommands, bus.PartitionKeyIntegrationID, env); err != nil {
		t.Fatalf("publish команды: %v", err)
	}

	bridgeConsumer, err := bus.NewConsumer(bus.ConsumerConfig{
		Seeds:  seeds,
		Group:  group,
		Topics: []string{bus.TopicMachineCommands},
	})
	if err != nil {
		t.Fatalf("NewConsumer (мост): %v", err)
	}

	serverConn, clientConn, cleanupWS := newWSPair(t)
	defer cleanupWS()
	conns := &fakeConnRegistry{conns: map[uuid.UUID]*websocket.Conn{integrationID: serverConn}}

	b, err := New(bridgeConsumer, conns,
		WithAckTimeout(500*time.Millisecond),
		WithOfflineRetryInterval(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		_ = b.Run(runCtx)
		close(runDone)
	}()

	// "Агент" читает попытки доставки и НИКОГДА не отвечает ack — ждём минимум
	// две попытки доставки ОДНОЙ и той же команды (переотправка, §5).
	readCtx, readCancel := context.WithTimeout(ctx, 60*time.Second)
	defer readCancel()
	deliveries := 0
	for deliveries < 2 {
		_, data, err := clientConn.Read(readCtx)
		if err != nil {
			t.Fatalf("чтение попытки доставки #%d: %v", deliveries+1, err)
		}
		var gotEnv bus.Envelope
		if err := json.Unmarshal(data, &gotEnv); err != nil {
			t.Fatalf("unmarshal доставленного конверта: %v", err)
		}
		if gotEnv.MessageID != messageID {
			t.Fatalf("доставлен не тот конверт: message_id=%s, ожидался %s", gotEnv.MessageID, messageID)
		}
		deliveries++
	}
	t.Logf("OK: команда message_id=%s доставлена %d раз(а) без ack", messageID, deliveries)

	// Останавливаем мост — НЕ коммитя offset (ack так и не пришёл) — и закрываем
	// его консьюмера, чтобы он покинул consumer group перед проверкой.
	runCancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Bridge.Run не завершился после отмены ctx")
	}
	bridgeConsumer.Close()

	// Свежий консьюмер ТОЙ ЖЕ группы должен вычитать ту же запись заново —
	// прямое доказательство, что offset НЕ был закоммичен на брокере.
	recs := fetchWithFreshGroup(ctx, t, seeds, group, 20*time.Second)
	if len(recs) == 0 {
		t.Fatal("свежий консьюмер той же группы не получил ни одной записи — offset был ошибочно закоммичен без ack")
	}
	if got := recordMessageID(t, recs[0]); got != messageID {
		t.Fatalf("свежий консьюмер получил message_id=%s, ожидался %s", got, messageID)
	}
	t.Logf("OK: offset НЕ закоммичен — свежий консьюмер группы %s заново получил message_id=%s", group, messageID)
}

// TestIntegration_Bridge_Ack_CommitsOnce_NoRedelivery — приёмочный сценарий
// тикета 3.4 «после ACK — нет» (переотправки): реальная Redpanda, "агент"
// подтверждает доставленную команду через b.HandleAck (связка с реальным
// ack-кадром WS отдельно покрыта unit-тестами api-пакета, см. godoc файла) →
// мост коммитит offset РОВНО один раз — после остановки моста свежий
// консьюмер ТОЙ ЖЕ consumer group НЕ видит эту запись повторно.
func TestIntegration_Bridge_Ack_CommitsOnce_NoRedelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	seed, cleanupRedpanda := startRedpanda(ctx, t)
	defer cleanupRedpanda()
	seeds := []string{seed}
	waitTopicReady(ctx, t, seeds)

	if err := bus.EnsureMVPTopics(ctx, seeds); err != nil {
		t.Fatalf("провижининг топиков: %v", err)
	}

	const group = "bridge-it-ack"
	integrationID := uuid.New()
	messageID := bus.NewMessageID()

	producer, err := bus.NewProducer(seeds)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	env := testCommandEnvelope(messageID, integrationID.String())
	if err := producer.PublishKeyed(ctx, bus.TopicMachineCommands, bus.PartitionKeyIntegrationID, env); err != nil {
		t.Fatalf("publish команды: %v", err)
	}

	bridgeConsumer, err := bus.NewConsumer(bus.ConsumerConfig{
		Seeds:  seeds,
		Group:  group,
		Topics: []string{bus.TopicMachineCommands},
	})
	if err != nil {
		t.Fatalf("NewConsumer (мост): %v", err)
	}

	serverConn, clientConn, cleanupWS := newWSPair(t)
	defer cleanupWS()
	conns := &fakeConnRegistry{conns: map[uuid.UUID]*websocket.Conn{integrationID: serverConn}}

	b, err := New(bridgeConsumer, conns,
		WithAckTimeout(10*time.Second),
		WithOfflineRetryInterval(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		_ = b.Run(runCtx)
		close(runDone)
	}()

	readCtx, readCancel := context.WithTimeout(ctx, 60*time.Second)
	defer readCancel()
	_, data, err := clientConn.Read(readCtx)
	if err != nil {
		t.Fatalf("чтение доставленной команды: %v", err)
	}
	var gotEnv bus.Envelope
	if err := json.Unmarshal(data, &gotEnv); err != nil {
		t.Fatalf("unmarshal доставленного конверта: %v", err)
	}
	if gotEnv.MessageID != messageID {
		t.Fatalf("доставлен не тот конверт: message_id=%s, ожидался %s", gotEnv.MessageID, messageID)
	}

	// "Агент" подтверждает доставку.
	b.HandleAck(messageID)

	// Убеждаемся, что ПОВТОРНОЙ доставки не происходит (ackTimeout — 10s,
	// ждём заметно меньше, чтобы не раздувать тест, но достаточно, чтобы
	// поймать ошибочный ретрай, если бы он случился сразу).
	noRedeliveryCtx, noRedeliveryCancel := context.WithTimeout(ctx, 2*time.Second)
	defer noRedeliveryCancel()
	if _, _, err := clientConn.Read(noRedeliveryCtx); err == nil {
		t.Fatal("получена повторная доставка после ack — не ожидалась")
	}

	runCancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Bridge.Run не завершился после отмены ctx")
	}
	bridgeConsumer.Close()

	// Свежий консьюмер ТОЙ ЖЕ группы НЕ должен получить эту запись повторно —
	// прямое доказательство, что offset был закоммичен на брокере после ack.
	recs := fetchWithFreshGroup(ctx, t, seeds, group, 10*time.Second)
	for _, rec := range recs {
		if got := recordMessageID(t, rec); got == messageID {
			t.Fatalf("свежий консьюмер группы %s заново получил message_id=%s — offset не был закоммичен после ack", group, messageID)
		}
	}
	t.Logf("OK: offset закоммичен после ack — свежий консьюмер группы %s не видит message_id=%s повторно", group, messageID)
}

// TestIntegration_Bridge_OfflineThenOnline_DeliversWithoutLoss — приёмочный
// сценарий тикета 5.6 «Оффлайн-постановка» (FR E4, §9 «Задача поставлена при
// оффлайн-машине»): команда публикуется в machine.commands, когда машины НЕТ
// в ConnRegistry вообще (fakeConnRegistry с пустой картой conns — машина
// офлайн с самого начала, а не просто "онлайн, но без ack", как в
// TestIntegration_Bridge_NoAck_RedeliversAndDoesNotCommit выше). Мост крутится
// в offline-ретрае (deliver, bridge.go) без коммита офсета; когда машина
// "выходит онлайн" (в реестр напрямую добавляется WS-соединение — эмуляция
// момента переподключения агента), мост подхватывает её на следующей
// итерации offline-ретрая и доставляет ИМЕННО ту же команду (совпадающий
// message_id) — без потери сообщения. После ack — offset коммитится ровно
// один раз: свежий консьюмер ТОЙ ЖЕ consumer group не видит запись повторно.
func TestIntegration_Bridge_OfflineThenOnline_DeliversWithoutLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	seed, cleanupRedpanda := startRedpanda(ctx, t)
	defer cleanupRedpanda()
	seeds := []string{seed}
	waitTopicReady(ctx, t, seeds)

	if err := bus.EnsureMVPTopics(ctx, seeds); err != nil {
		t.Fatalf("провижининг топиков: %v", err)
	}

	const group = "bridge-it-offline-then-online"
	integrationID := uuid.New()
	messageID := bus.NewMessageID()

	producer, err := bus.NewProducer(seeds)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	// Публикуем команду ДО того, как машина хоть раз появится в реестре
	// соединений — ровно ситуация тикета 5.6: "машина сейчас оффлайн" в
	// момент постановки задачи.
	env := testCommandEnvelope(messageID, integrationID.String())
	if err := producer.PublishKeyed(ctx, bus.TopicMachineCommands, bus.PartitionKeyIntegrationID, env); err != nil {
		t.Fatalf("publish команды: %v", err)
	}

	bridgeConsumer, err := bus.NewConsumer(bus.ConsumerConfig{
		Seeds:  seeds,
		Group:  group,
		Topics: []string{bus.TopicMachineCommands},
	})
	if err != nil {
		t.Fatalf("NewConsumer (мост): %v", err)
	}

	// Машина офлайн с самого начала: пустая карта соединений (её нет в
	// реестре вообще, в отличие от "онлайн, но без ack" в других тестах).
	conns := &fakeConnRegistry{conns: map[uuid.UUID]*websocket.Conn{}}

	const offlineRetryInterval = 200 * time.Millisecond
	b, err := New(bridgeConsumer, conns,
		WithAckTimeout(10*time.Second),
		WithOfflineRetryInterval(offlineRetryInterval),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		_ = b.Run(runCtx)
		close(runDone)
	}()

	// Выдерживаем несколько интервалов offline-ретрая — доставлять некому
	// (соединения нет вообще ни у кого), мост должен просто крутиться в
	// ретрае, не коммитя offset. Явная пауза показывает, что тест доказывает
	// устойчивость к продолжительному оффлайну, а не полагается на гонку
	// между публикацией и "подключением" машины.
	time.Sleep(5 * offlineRetryInterval)

	// "Машина выходит онлайн": заводим настоящую пару WS-соединений и
	// напрямую (тот же пакет bridge) добавляем serverConn в реестр — это и
	// есть момент переподключения агента с точки зрения моста.
	serverConn, clientConn, cleanupWS := newWSPair(t)
	defer cleanupWS()
	conns.mu.Lock()
	conns.conns[integrationID] = serverConn
	conns.mu.Unlock()

	// Мост должен заметить появившееся соединение на следующей итерации
	// offline-ретрая и доставить ИМЕННО ту команду, что была опубликована в
	// офлайн-период — без потери сообщения.
	readCtx, readCancel := context.WithTimeout(ctx, 15*time.Second)
	defer readCancel()
	_, data, err := clientConn.Read(readCtx)
	if err != nil {
		t.Fatalf("чтение доставленной команды после выхода машины в онлайн: %v", err)
	}
	var gotEnv bus.Envelope
	if err := json.Unmarshal(data, &gotEnv); err != nil {
		t.Fatalf("unmarshal доставленного конверта: %v", err)
	}
	if gotEnv.MessageID != messageID {
		t.Fatalf("доставлен не тот конверт: message_id=%s, ожидался %s", gotEnv.MessageID, messageID)
	}
	t.Logf("OK: команда message_id=%s, опубликованная при полностью оффлайн-машине, доставлена после выхода в онлайн", messageID)

	// "Агент" подтверждает доставку.
	b.HandleAck(messageID)

	// Убеждаемся, что повторной доставки не происходит (ackTimeout — 10s,
	// ждём заметно меньше, чтобы поймать ошибочный ретрай, если бы он
	// случился сразу же после ack).
	noRedeliveryCtx, noRedeliveryCancel := context.WithTimeout(ctx, 2*time.Second)
	defer noRedeliveryCancel()
	if _, _, err := clientConn.Read(noRedeliveryCtx); err == nil {
		t.Fatal("получена повторная доставка после ack — не ожидалась")
	}

	runCancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Bridge.Run не завершился после отмены ctx")
	}
	bridgeConsumer.Close()

	// Свежий консьюмер ТОЙ ЖЕ группы НЕ должен получить эту запись повторно —
	// прямое доказательство, что offset был закоммичен на брокере после ack
	// РОВНО один раз (без лишних передоставок/повторных коммитов).
	recs := fetchWithFreshGroup(ctx, t, seeds, group, 10*time.Second)
	for _, rec := range recs {
		if got := recordMessageID(t, rec); got == messageID {
			t.Fatalf("свежий консьюмер группы %s заново получил message_id=%s — offset не был закоммичен после ack", group, messageID)
		}
	}
	t.Logf("OK: offset закоммичен ровно один раз — свежий консьюмер группы %s не видит message_id=%s повторно", group, messageID)
}
