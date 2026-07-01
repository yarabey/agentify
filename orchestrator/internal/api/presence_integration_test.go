//go:build integration

// Integration-тест приёмки тикета 3.6 «Heartbeat и статус машины» (FR B4,
// docs/protocol.md §6, docs/User_stories_Gherkin.md «Машина онлайн»/«Машина
// оффлайн») на РЕАЛЬНЫХ Postgres + Redpanda через testcontainers-go — сквозной
// путь через ВСЮ цепочку компонентов тикета, не только их изолированные части
// (в отличие от orchestrator/internal/presence/*_test.go, которые проверяют
// каждый компонент по отдельности фейками, и
// orchestrator/internal/api/machine_ws_test.go, который проверяет только
// перезапись IntegrationID фейковым EventSink):
//
//	агент (тестовый WS-клиент) → hello → GetMachineWs (тикет 2.3) →
//	heartbeat-кадр → handleMachineFrame → handleMachineEvent (переписывает
//	IntegrationID на DB id) → presence.Sink.HandleEvent → Redpanda
//	machine.events → presence.Consumer → MarkIntegrationOnline (БД) →
//	ack-кадр обратно агенту;
//	независимо — presence.OfflineWorker → MarkStaleIntegrationsOffline (БД),
//	если heartbeat давно не приходил.
//
// Покрывает оба обязательных по тикету 3.6 сценария ("Тесты: online on fresh
// heartbeat; offline by timeout (asynchronous)"):
//   - TestIntegration_Presence_HeartbeatMarksOnline: одиночный heartbeat по
//     живому WS-соединению → интеграция становится online в БД (и сервер
//     присылает ack-кадр в ответ — сквозная проверка, что EventSink.HandleEvent
//     не вернул ошибку);
//   - TestIntegration_Presence_OfflineByTimeoutAsync: интеграция становится
//     online от heartbeat, WS-соединение закрывается (агент "ушёл"), НИКАКОГО
//     нового heartbeat/WS-соединения не создаётся — OfflineWorker переводит
//     интеграцию в offline АСИНХРОННО, чисто по истечении OfflineThreshold, без
//     какого-либо синхронного соединения на момент перехода (тот самый смысл
//     FR B4 — "статус не требует прямого синхронного соединения").
//
// startRedpanda/waitTopicReady — локальная копия помощников
// orchestrator/internal/bridge/bridge_integration_test.go (неэкспортированный
// помощник другого пакета напрямую не переиспользовать, тот же принцип, что и
// комментарий в оригинале). setupDB/testJWTSigningKey/testEncryptionKey32/
// createTestUserWithToken/doIntegrationsRequest — из
// register_integration_test.go/integrations_integration_test.go (тот же пакет
// api_test). dialMachineWS/sendHello/wsAccepted — из
// machine_ws_integration_test.go.
package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/presence"
)

// startPresenceRedpanda поднимает одиночный брокер Redpanda в контейнере (та
// же копия помощника, что и bridge_integration_test.go — своё имя, чтобы не
// конфликтовать с другими файлами того же пакета api_test, если появятся).
func startPresenceRedpanda(ctx context.Context, t *testing.T) (string, func()) {
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

// waitPresenceTopicReady ждёт, пока брокер начнёт отвечать на Ping (копия
// bridge_integration_test.go).
func waitPresenceTopicReady(ctx context.Context, t *testing.T, seeds []string) {
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

// heartbeatFrame собирает JSON-конверт heartbeat (protocol.md §2, §6) со
// стороны "агента": integrationID здесь — заведомо ПРОИЗВОЛЬНОЕ значение (в
// т.ч. секрет-UUID, как реально шлёт agent/internal/wsclient) — сервер обязан
// переписать его на аутентифицированный DB id ПЕРЕД публикацией (см. годoc
// orchestrator/internal/api.handleMachineEvent), поэтому для теста
// принципиально неважно, какое именно значение здесь стоит.
func heartbeatFrame(t *testing.T, integrationID string) []byte {
	t.Helper()
	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		TaskID:          nil,
		IntegrationID:   integrationID,
		Type:            bus.MessageTypeHeartbeat,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         json.RawMessage("{}"),
	}
	data, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal heartbeat: %v", err)
	}
	return data
}

// readAckFrame читает следующий кадр с соединения и требует, что это ack на
// ожидаемый ackMessageID (сквозная проверка, что EventSink.HandleEvent не
// вернул ошибку и handleMachineEvent реально отправил ack, см.
// orchestrator/internal/api/machine_ws.go).
func readAckFrame(ctx context.Context, t *testing.T, conn *websocket.Conn, ackMessageID string) {
	t.Helper()
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, data, err := conn.Read(readCtx)
	if err != nil {
		t.Fatalf("чтение ack-кадра: %v", err)
	}
	var env bus.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal ack-кадра: %v", err)
	}
	if env.Type != bus.MessageTypeAck {
		t.Fatalf("type=%q, ожидался %q", env.Type, bus.MessageTypeAck)
	}
	var payload bus.AckPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		t.Fatalf("unmarshal ack payload: %v", err)
	}
	if payload.AckMessageID != ackMessageID {
		t.Fatalf("ack_message_id=%q, ожидался %q", payload.AckMessageID, ackMessageID)
	}
}

// TestIntegration_Presence_HeartbeatMarksOnline — приёмочный сценарий
// «Машина онлайн» (Gherkin §2, FR B4): реальные Postgres + Redpanda, реальное
// WS-соединение шлёт ОДИН heartbeat-кадр → интеграция становится online в БД
// (сквозь Sink → Redpanda → presence.Consumer → MarkIntegrationOnline), а
// сервер отвечает агенту ack-кадром на этот же heartbeat.
func TestIntegration_Presence_HeartbeatMarksOnline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, doneDB := setupDB(ctx, t)
	defer doneDB()
	q := db.New(pool)

	seed, cleanupRedpanda := startPresenceRedpanda(ctx, t)
	defer cleanupRedpanda()
	seeds := []string{seed}
	waitPresenceTopicReady(ctx, t, seeds)
	if err := bus.EnsureMVPTopics(ctx, seeds); err != nil {
		t.Fatalf("провижининг топиков: %v", err)
	}

	user, token := createTestUserWithToken(ctx, t, q, "presence-online")
	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	router := api.NewRouter(server)

	createRec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", token, api.IntegrationCreate{Name: "presence-online-machine"})
	if createRec.Code != http.StatusCreated {
		t.Fatalf("создание интеграции: статус = %d (%s)", createRec.Code, createRec.Body.String())
	}
	var created api.IntegrationWithSecret
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}

	producer, err := bus.NewProducer(seeds)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	sink, err := presence.NewSink(producer)
	if err != nil {
		t.Fatalf("presence.NewSink: %v", err)
	}
	server.SetEventSink(sink)

	heartbeatConsumer, err := bus.NewConsumer(bus.ConsumerConfig{
		Seeds:  seeds,
		Group:  "presence-it-online",
		Topics: []string{bus.TopicMachineEvents},
	})
	if err != nil {
		t.Fatalf("NewConsumer (presence): %v", err)
	}
	defer heartbeatConsumer.Close()

	presenceConsumer, err := presence.NewConsumer(heartbeatConsumer, q)
	if err != nil {
		t.Fatalf("presence.NewConsumer: %v", err)
	}
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()
	go func() { _ = presenceConsumer.Run(consumerCtx) }()

	ts := httptest.NewServer(router)
	defer ts.Close()

	conn := dialMachineWS(ctx, t, ts)
	defer conn.CloseNow()

	// ПРИМЕЧАНИЕ: здесь намеренно НЕ используется wsAccepted (в отличие от
	// machine_ws_integration_test.go) — coder/websocket закрывает соединение,
	// если Read был отменён по истечении ctx-таймаута (ошибочное состояние
	// чтения), а этому тесту соединение нужно ЖИВЫМ для последующей записи
	// heartbeat-кадра. Вместо отдельной проверки acceptance сразу пишем
	// heartbeat и ждём ack — успешный ack сам по себе доказывает, что hello
	// был принят (иначе сервер бы уже закрыл соединение 4401, и Write упал бы).
	sendHello(ctx, t, conn, created.Uuid.String())

	frame := heartbeatFrame(t, created.Uuid.String())
	var sentEnv bus.Envelope
	if err := json.Unmarshal(frame, &sentEnv); err != nil {
		t.Fatalf("unmarshal отправляемого конверта (для проверки ack): %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
		t.Fatalf("write heartbeat: %v", err)
	}

	readAckFrame(ctx, t, conn, sentEnv.MessageID)
	t.Logf("OK: ack-кадр на heartbeat message_id=%s получен", sentEnv.MessageID)

	deadline := time.Now().Add(30 * time.Second)
	var last db.Integration
	for time.Now().Before(deadline) {
		got, err := q.GetIntegrationByIDAndUser(ctx, db.GetIntegrationByIDAndUserParams{
			ID:     pgtype.UUID{Bytes: *created.Id, Valid: true},
			UserID: user.ID,
		})
		if err != nil {
			t.Fatalf("GetIntegrationByIDAndUser: %v", err)
		}
		last = got
		if got.Status == "online" {
			t.Logf("OK: интеграция %s стала online (last_seen_at=%v)", created.Id, got.LastSeenAt.Time)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("интеграция не стала online за отведённое время: status=%q", last.Status)
}

// TestIntegration_Presence_OfflineByTimeoutAsync — приёмочный сценарий
// «Машина оффлайн» (Gherkin §2, FR B4, protocol.md §6): реальные Postgres +
// Redpanda, интеграция становится online от одного heartbeat, WS-соединение
// затем ЗАКРЫВАЕТСЯ и никакого нового heartbeat/соединения не создаётся —
// OfflineWorker переводит интеграцию в offline АСИНХРОННО по истечении
// короткого тестового OfflineThreshold — БЕЗ какого-либо синхронного
// соединения на момент перехода (сам смысл FR B4).
func TestIntegration_Presence_OfflineByTimeoutAsync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, doneDB := setupDB(ctx, t)
	defer doneDB()
	q := db.New(pool)

	seed, cleanupRedpanda := startPresenceRedpanda(ctx, t)
	defer cleanupRedpanda()
	seeds := []string{seed}
	waitPresenceTopicReady(ctx, t, seeds)
	if err := bus.EnsureMVPTopics(ctx, seeds); err != nil {
		t.Fatalf("провижининг топиков: %v", err)
	}

	user, token := createTestUserWithToken(ctx, t, q, "presence-offline")
	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	router := api.NewRouter(server)

	createRec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", token, api.IntegrationCreate{Name: "presence-offline-machine"})
	if createRec.Code != http.StatusCreated {
		t.Fatalf("создание интеграции: статус = %d (%s)", createRec.Code, createRec.Body.String())
	}
	var created api.IntegrationWithSecret
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}

	producer, err := bus.NewProducer(seeds)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	sink, err := presence.NewSink(producer)
	if err != nil {
		t.Fatalf("presence.NewSink: %v", err)
	}
	server.SetEventSink(sink)

	heartbeatConsumer, err := bus.NewConsumer(bus.ConsumerConfig{
		Seeds:  seeds,
		Group:  "presence-it-offline",
		Topics: []string{bus.TopicMachineEvents},
	})
	if err != nil {
		t.Fatalf("NewConsumer (presence): %v", err)
	}
	defer heartbeatConsumer.Close()

	presenceConsumer, err := presence.NewConsumer(heartbeatConsumer, q)
	if err != nil {
		t.Fatalf("presence.NewConsumer: %v", err)
	}
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()
	go func() { _ = presenceConsumer.Run(consumerCtx) }()

	// Короткие тестовые пороги — иначе тест ждал бы реальные 45s+ (protocol.md
	// §6 дефолт). Смысл сценария не в конкретных цифрах, а в самом факте
	// АСИНХРОННОГО перехода без живого соединения.
	const (
		testOfflineThreshold = 2 * time.Second
		testPollInterval     = 200 * time.Millisecond
	)
	offlineWorker, err := presence.NewOfflineWorker(q,
		presence.WithPollInterval(testPollInterval),
		presence.WithOfflineThreshold(testOfflineThreshold),
	)
	if err != nil {
		t.Fatalf("presence.NewOfflineWorker: %v", err)
	}
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()
	go func() { _ = offlineWorker.Run(workerCtx) }()

	ts := httptest.NewServer(router)
	defer ts.Close()

	conn := dialMachineWS(ctx, t, ts)
	// См. комментарий в TestIntegration_Presence_HeartbeatMarksOnline — без
	// wsAccepted, acceptance доказывается успешным ack ниже.
	sendHello(ctx, t, conn, created.Uuid.String())

	frame := heartbeatFrame(t, created.Uuid.String())
	var sentEnv bus.Envelope
	if err := json.Unmarshal(frame, &sentEnv); err != nil {
		t.Fatalf("unmarshal отправляемого конверта: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
		t.Fatalf("write heartbeat: %v", err)
	}
	readAckFrame(ctx, t, conn, sentEnv.MessageID)

	onlineDeadline := time.Now().Add(30 * time.Second)
	becameOnline := false
	for time.Now().Before(onlineDeadline) {
		got, err := q.GetIntegrationByIDAndUser(ctx, db.GetIntegrationByIDAndUserParams{ID: pgtype.UUID{Bytes: *created.Id, Valid: true}, UserID: user.ID})
		if err != nil {
			t.Fatalf("GetIntegrationByIDAndUser: %v", err)
		}
		if got.Status == "online" {
			becameOnline = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !becameOnline {
		t.Fatal("интеграция не стала online от heartbeat — offline-сценарий не может быть проверен")
	}
	t.Logf("OK: интеграция %s online, закрываю соединение и жду асинхронный переход в offline", created.Id)

	// Агент "ушёл": закрываем WS-соединение и НЕ шлём больше ни одного
	// heartbeat/hello. Единственный путь в offline с этого момента —
	// OfflineWorker, полностью независимый от WS.
	conn.CloseNow()

	offlineDeadline := time.Now().Add(30 * time.Second)
	var last db.Integration
	for time.Now().Before(offlineDeadline) {
		got, err := q.GetIntegrationByIDAndUser(ctx, db.GetIntegrationByIDAndUserParams{ID: pgtype.UUID{Bytes: *created.Id, Valid: true}, UserID: user.ID})
		if err != nil {
			t.Fatalf("GetIntegrationByIDAndUser: %v", err)
		}
		last = got
		if got.Status == "offline" {
			t.Logf("OK: интеграция %s асинхронно переведена в offline (last_seen_at=%v)", created.Id, got.LastSeenAt.Time)
			return
		}
		time.Sleep(testPollInterval)
	}
	t.Fatalf("интеграция не стала offline за отведённое время (по истечении OfflineThreshold=%v): status=%q", testOfflineThreshold, last.Status)
}
