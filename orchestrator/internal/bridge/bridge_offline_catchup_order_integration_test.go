//go:build integration

// Integration-тест «оффлайн-догона» с НЕСКОЛЬКИМИ накопленными командами на
// РЕАЛЬНОЙ Redpanda через testcontainers-go (тикет 5.6, FR E4, protocol.md
// §5-6, бизнес-ТЗ §149-151 «Асинхронность»: «Задача, поставленная при
// оффлайн-машине, доставляется и выполняется после её подключения — без
// потери»; §126 «порядок команд машине»). Тот же пакет bridge, тот же тег
// integration, что и bridge_integration_test.go — переиспользует его
// неэкспортированные помощники (startRedpanda/waitTopicReady/
// fetchWithFreshGroup/recordMessageID) напрямую, без копирования (тот же
// тестовый бинарь пакета).
//
// Пробел, который закрывает этот файл (тикет 11.3): существующий
// TestIntegration_Bridge_OfflineThenOnline_DeliversWithoutLoss (см. godoc
// bridge_integration_test.go) доказывает, что ОДНА команда, опубликованная
// при полностью оффлайн-машине, не теряется и доставляется после реконнекта.
// Но приёмка тикета 5.6 и текст брифа тикета 11.3 буквально требуют
// «оффлайн-догон... накопленные команды из machine.commands доставляются по
// порядку» — то есть НЕСКОЛЬКО команд, накопившихся за время оффлайна, а не
// одна. Порядок доставки нескольких команд одной машине ни разу не
// проверялся на реальном брокере (internal/bus/integration_test.go проверяет
// порядок только для machine.events/task_id, а не для machine.commands,
// доставляемых через мост конкретной машине — это разные компоненты и разные
// ключи партиции, ADR 0001). TestIntegration_Bridge_OfflineThenOnline_MultipleCommandsDeliveredInOrder
// ниже публикует N>1 команд для ОДНОГО integration_id, пока машины вообще нет
// в ConnRegistry (тот же офлайн-сценарий, что и в
// TestIntegration_Bridge_OfflineThenOnline_DeliversWithoutLoss), затем
// эмулирует реконнект машины и проверяет, что мост доставляет ВСЕ N команд
// СТРОГО В ПОРЯДКЕ ИХ ПУБЛИКАЦИИ (machineWorker, bridge.go, обрабатывает
// записи одной машины последовательно из одного канала — здесь это
// проверяется НАБЛЮДАЕМЫМ порядком доставки по WS на реальном брокере, а не
// только чтением кода), и что после подтверждения (ack) каждой команды
// offset коммитится соответствующим образом — свежий консьюмер той же
// consumer group не видит уже доставленные и подтверждённые записи повторно.
package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/yarabey/agentify/internal/bus"
)

// testCommandEnvelopeSeq — как testCommandEnvelope (bridge_test.go), но с
// РАЗЛИЧИМЫМ payload.text (индекс seq) — нужен, чтобы доказать не только
// совпадение message_id, но и что порядок ПУБЛИКАЦИИ (seq 1..N) совпадает с
// порядком ДОСТАВКИ по WS.
func testCommandEnvelopeSeq(messageID, integrationID string, seq int) bus.Envelope {
	payload, _ := json.Marshal(map[string]string{"text": fmt.Sprintf("cmd-%d", seq)})
	return bus.Envelope{
		MessageID:       messageID,
		IntegrationID:   integrationID,
		Type:            bus.MessageTypeTaskAssigned,
		Seq:             int64(seq),
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}
}

// TestIntegration_Bridge_OfflineThenOnline_MultipleCommandsDeliveredInOrder —
// см. godoc файла: N команд публикуются для ОДНОГО integration_id, пока
// машины НЕТ в ConnRegistry вообще (полностью офлайн — тот же сценарий, что и
// TestIntegration_Bridge_OfflineThenOnline_DeliversWithoutLoss, но с
// несколькими накопленными командами вместо одной). После выхода машины в
// онлайн мост должен доставить ВСЕ N команд СТРОГО в порядке публикации;
// каждая подтверждается ack по мере получения (иначе deliver ждал бы
// ackTimeout перед следующей попыткой ТОЙ ЖЕ записи — таков контракт
// machineWorker, см. bridge.go), и после подтверждения последней команды
// свежий консьюмер той же группы не видит ни одной из них повторно (offset
// закоммичен по каждой).
func TestIntegration_Bridge_OfflineThenOnline_MultipleCommandsDeliveredInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	seed, cleanupRedpanda := startRedpanda(ctx, t)
	defer cleanupRedpanda()
	seeds := []string{seed}
	waitTopicReady(ctx, t, seeds)

	if err := bus.EnsureMVPTopics(ctx, seeds); err != nil {
		t.Fatalf("провижининг топиков: %v", err)
	}

	const group = "bridge-it-offline-order"
	const n = 5
	integrationID := uuid.New()

	producer, err := bus.NewProducer(seeds)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	// Публикуем N команд ПОДРЯД, в известном порядке, ПОКА машина не
	// появилась в реестре соединений ни разу — ровно ситуация тикета 5.6:
	// несколько команд накапливаются, пока машина офлайн.
	messageIDs := make([]string, n)
	for i := 0; i < n; i++ {
		messageIDs[i] = bus.NewMessageID()
		env := testCommandEnvelopeSeq(messageIDs[i], integrationID.String(), i+1)
		if err := producer.PublishKeyed(ctx, bus.TopicMachineCommands, bus.PartitionKeyIntegrationID, env); err != nil {
			t.Fatalf("publish команды #%d: %v", i+1, err)
		}
	}
	t.Logf("опубликовано %d команд для integration_id=%s, пока машина офлайн: %v", n, integrationID, messageIDs)

	bridgeConsumer, err := bus.NewConsumer(bus.ConsumerConfig{
		Seeds:  seeds,
		Group:  group,
		Topics: []string{bus.TopicMachineCommands},
	})
	if err != nil {
		t.Fatalf("NewConsumer (мост): %v", err)
	}

	// Машина офлайн с самого начала: пустая карта соединений (тот же приём,
	// что и TestIntegration_Bridge_OfflineThenOnline_DeliversWithoutLoss).
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

	// Выдерживаем несколько интервалов offline-ретрая — доказываем, что мост
	// действительно ждёт (не коммитит, не доставляет), а не просто выигрывает
	// гонку с публикацией.
	time.Sleep(5 * offlineRetryInterval)

	// "Машина выходит онлайн": заводим пару WS-соединений и напрямую
	// добавляем serverConn в реестр — момент реконнекта агента с точки
	// зрения моста.
	serverConn, clientConn, cleanupWS := newWSPair(t)
	defer cleanupWS()
	conns.mu.Lock()
	conns.conns[integrationID] = serverConn
	conns.mu.Unlock()

	// Читаем и ПОДТВЕРЖДАЕМ N команд одну за другой: machineWorker
	// обрабатывает канал строго последовательно (bridge.go) — deliver для
	// записи #(i+1) не начнётся, пока не подтверждена (либо не истёк
	// ackTimeout) запись #i. Немедленный ack после каждого чтения — то, что
	// заставляет мост перейти к СЛЕДУЮЩЕЙ записи без ожидания ackTimeout, и
	// то, что наблюдаемый здесь порядок доставки прямо доказывает порядок
	// publish (а не переставлен побочным ретраем).
	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readCancel()
	for i := 0; i < n; i++ {
		_, data, err := clientConn.Read(readCtx)
		if err != nil {
			t.Fatalf("чтение команды #%d из %d: %v", i+1, n, err)
		}
		var gotEnv bus.Envelope
		if err := json.Unmarshal(data, &gotEnv); err != nil {
			t.Fatalf("unmarshal доставленного конверта #%d: %v", i+1, err)
		}
		if gotEnv.MessageID != messageIDs[i] {
			t.Fatalf("команда доставлена НЕ по порядку: позиция %d получила message_id=%s, ожидался message_id=%s (порядок публикации %v)",
				i+1, gotEnv.MessageID, messageIDs[i], messageIDs)
		}
		var payload struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(gotEnv.Payload, &payload); err != nil {
			t.Fatalf("unmarshal payload команды #%d: %v", i+1, err)
		}
		wantText := fmt.Sprintf("cmd-%d", i+1)
		if payload.Text != wantText {
			t.Fatalf("payload.text команды #%d = %q, ожидался %q — порядок доставки не совпадает с порядком публикации", i+1, payload.Text, wantText)
		}
		b.HandleAck(gotEnv.MessageID)
	}
	t.Logf("OK: все %d команд, накопленных при оффлайн-машине, доставлены СТРОГО в порядке публикации: %v", n, messageIDs)

	runCancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Bridge.Run не завершился после отмены ctx")
	}
	bridgeConsumer.Close()

	// Свежий консьюмер ТОЙ ЖЕ группы не должен увидеть НИ ОДНУ из N
	// доставленных и подтверждённых записей повторно — offset закоммичен по
	// каждой (не только по последней).
	recs := fetchWithFreshGroup(ctx, t, seeds, group, 10*time.Second)
	seenAgain := map[string]bool{}
	for _, rec := range recs {
		seenAgain[recordMessageID(t, rec)] = true
	}
	for i, id := range messageIDs {
		if seenAgain[id] {
			t.Fatalf("команда #%d (message_id=%s) вычитана свежим консьюмером повторно — offset не был закоммичен после ack", i+1, id)
		}
	}
	t.Logf("OK: offset закоммичен по каждой из %d команд — свежий консьюмер группы %s не видит ни одной повторно", n, group)
}
