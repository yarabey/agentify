// Юнит-тесты Notifier (тикет 7.3, FR G1) — без Redpanda/Docker: продьюсер и
// sqlc-запросы подменяются фейками (тот же приём, что и у
// orchestrator/internal/presence.Sink, см. presence/sink_test.go), Notifier
// собирается прямым структурным литералом (в отличие от NewNotifier, который
// принимает конкретные *bus.Producer/*db.Queries).
package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/notify"
)

// fakeProducer — фейковая busProducer: запоминает (topic, key, env)
// последнего вызова Publish; err (если задан) возвращается вызывающему.
type fakeProducer struct {
	calls []publishCall
	err   error
}

type publishCall struct {
	topic string
	key   string
	env   bus.Envelope
}

func (f *fakeProducer) Publish(_ context.Context, topic, key string, env bus.Envelope) error {
	f.calls = append(f.calls, publishCall{topic: topic, key: key, env: env})
	return f.err
}

// fakeChannels — фейковая channelLinkFinder: возвращает link/err, заданные в
// тесте, независимо от аргументов (единственный сценарий на тест).
type fakeChannels struct {
	link db.ChannelLink
	err  error
}

func (f *fakeChannels) GetChannelLinkByUserAndChannel(context.Context, db.GetChannelLinkByUserAndChannelParams) (db.ChannelLink, error) {
	return f.link, f.err
}

// fakeTasks — фейковая taskIntegrationFinder.
type fakeTasks struct {
	task db.Task
	err  error
}

func (f *fakeTasks) GetTaskByIDAndUser(context.Context, db.GetTaskByIDAndUserParams) (db.Task, error) {
	return f.task, f.err
}

func testNotification(taskID, userID uuid.UUID, kind string, payload []byte) notify.Notification {
	return notify.Notification{
		TaskID:    pgtype.UUID{Bytes: taskID, Valid: true},
		UserID:    pgtype.UUID{Bytes: userID, Valid: true},
		Kind:      kind,
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
	}
}

// TestNotifier_Notify_NoLink_ReturnsNilWithoutPublish — пользователь без
// активной привязки Telegram (pgx.ErrNoRows) — best-effort канал, НЕ ошибка,
// и публикации в Redpanda не происходит (см. годок Notifier.Notify).
func TestNotifier_Notify_NoLink_ReturnsNilWithoutPublish(t *testing.T) {
	fp := &fakeProducer{}
	n := &Notifier{
		producer: fp,
		channels: &fakeChannels{err: pgx.ErrNoRows},
		tasks:    &fakeTasks{},
	}

	err := n.Notify(context.Background(), testNotification(uuid.New(), uuid.New(), notify.KindAgentQuestion, []byte(`{}`)))
	if err != nil {
		t.Fatalf("Notify без привязки Telegram вернул ошибку: %v", err)
	}
	if len(fp.calls) != 0 {
		t.Fatalf("Publish вызван %d раз(а) без привязки Telegram, ожидалось 0", len(fp.calls))
	}
}

// TestNotifier_Notify_ChannelLookupError_Propagated — реальный сбой
// инфраструктуры (не pgx.ErrNoRows) при поиске привязки пробрасывается
// вызывающему.
func TestNotifier_Notify_ChannelLookupError_Propagated(t *testing.T) {
	n := &Notifier{
		producer: &fakeProducer{},
		channels: &fakeChannels{err: errors.New("boom")},
		tasks:    &fakeTasks{},
	}

	err := n.Notify(context.Background(), testNotification(uuid.New(), uuid.New(), notify.KindAgentQuestion, []byte(`{}`)))
	if err == nil {
		t.Fatal("Notify: ожидалась ошибка при сбое поиска привязки")
	}
}

// TestNotifier_Notify_PublishesWithUserIDPartitionKey — при найденной
// привязке публикуется РОВНО одно сообщение в TopicNotificationsTelegram с
// ключом партиции user_id (ADR 0001) и type ==
// MessageTypeTelegramNotification; payload несёт telegram_chat_id, взятый из
// channel_links.external_id.
func TestNotifier_Notify_PublishesWithUserIDPartitionKey(t *testing.T) {
	taskID := uuid.New()
	userID := uuid.New()
	integrationID := uuid.New()

	fp := &fakeProducer{}
	n := &Notifier{
		producer: fp,
		channels: &fakeChannels{link: db.ChannelLink{
			UserID:     pgtype.UUID{Bytes: userID, Valid: true},
			Channel:    telegramChannel,
			ExternalID: "555666777",
		}},
		tasks: &fakeTasks{task: db.Task{
			ID:            pgtype.UUID{Bytes: taskID, Valid: true},
			UserID:        pgtype.UUID{Bytes: userID, Valid: true},
			IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
		}},
	}

	questionPayload, _ := json.Marshal(bus.AgentQuestionPayload{QuestionID: "q-1", Text: "Продолжить установку?"})
	err := n.Notify(context.Background(), testNotification(taskID, userID, notify.KindAgentQuestion, questionPayload))
	if err != nil {
		t.Fatalf("Notify: неожиданная ошибка: %v", err)
	}

	if len(fp.calls) != 1 {
		t.Fatalf("Publish вызван %d раз(а), ожидался 1", len(fp.calls))
	}
	call := fp.calls[0]
	if call.topic != bus.TopicNotificationsTelegram {
		t.Fatalf("topic = %q, ожидался %q", call.topic, bus.TopicNotificationsTelegram)
	}
	if call.key != userID.String() {
		t.Fatalf("key партиции = %q, ожидался user_id %q (ADR 0001)", call.key, userID.String())
	}
	if call.env.Type != bus.MessageTypeTelegramNotification {
		t.Fatalf("env.Type = %q, ожидался %q", call.env.Type, bus.MessageTypeTelegramNotification)
	}
	if call.env.IntegrationID != integrationID.String() {
		t.Fatalf("env.IntegrationID = %q, ожидался %q", call.env.IntegrationID, integrationID.String())
	}
	if call.env.TaskID == nil || *call.env.TaskID != taskID.String() {
		t.Fatalf("env.TaskID = %v, ожидался %q", call.env.TaskID, taskID.String())
	}

	var payload bus.TelegramNotificationPayload
	if err := json.Unmarshal(call.env.Payload, &payload); err != nil {
		t.Fatalf("демаршалинг payload: %v", err)
	}
	if payload.TelegramChatID != 555666777 {
		t.Fatalf("TelegramChatID = %d, ожидался 555666777", payload.TelegramChatID)
	}
	if payload.Kind != notify.KindAgentQuestion {
		t.Fatalf("Kind = %q, ожидался %q", payload.Kind, notify.KindAgentQuestion)
	}
	if payload.TaskID != taskID.String() {
		t.Fatalf("TaskID = %q, ожидался %q", payload.TaskID, taskID.String())
	}
	if payload.Text == "" {
		t.Fatal("Text пуст, ожидался отформатированный текст вопроса")
	}
}

// TestNotifier_Notify_InvalidExternalID_ReturnsError — external_id привязки,
// не являющийся числом (повреждённые данные), — ошибка, публикации не
// происходит.
func TestNotifier_Notify_InvalidExternalID_ReturnsError(t *testing.T) {
	fp := &fakeProducer{}
	n := &Notifier{
		producer: fp,
		channels: &fakeChannels{link: db.ChannelLink{ExternalID: "not-a-number"}},
		tasks:    &fakeTasks{},
	}

	err := n.Notify(context.Background(), testNotification(uuid.New(), uuid.New(), notify.KindAgentQuestion, []byte(`{}`)))
	if err == nil {
		t.Fatal("Notify: ожидалась ошибка при нечисловом external_id")
	}
	if len(fp.calls) != 0 {
		t.Fatalf("Publish вызван %d раз(а) при невалидном external_id, ожидалось 0", len(fp.calls))
	}
}

// TestNotifier_Notify_TaskLookupError_Propagated — сбой резолва задачи
// (нужен для env.IntegrationID, см. годок пакета) пробрасывается вызывающему,
// публикации не происходит.
func TestNotifier_Notify_TaskLookupError_Propagated(t *testing.T) {
	fp := &fakeProducer{}
	n := &Notifier{
		producer: fp,
		channels: &fakeChannels{link: db.ChannelLink{ExternalID: "123"}},
		tasks:    &fakeTasks{err: pgx.ErrNoRows},
	}

	err := n.Notify(context.Background(), testNotification(uuid.New(), uuid.New(), notify.KindAgentQuestion, []byte(`{}`)))
	if err == nil {
		t.Fatal("Notify: ожидалась ошибка при сбое поиска задачи")
	}
	if len(fp.calls) != 0 {
		t.Fatalf("Publish вызван %d раз(а) при сбое поиска задачи, ожидалось 0", len(fp.calls))
	}
}

// TestNotifier_Notify_PublishError_Propagated — ошибка публикации
// пробрасывается вызывающему (тот же принцип, что и у presence.Sink).
func TestNotifier_Notify_PublishError_Propagated(t *testing.T) {
	n := &Notifier{
		producer: &fakeProducer{err: errors.New("kafka недоступна")},
		channels: &fakeChannels{link: db.ChannelLink{ExternalID: "123"}},
		tasks:    &fakeTasks{},
	}

	err := n.Notify(context.Background(), testNotification(uuid.New(), uuid.New(), notify.KindAgentQuestion, []byte(`{}`)))
	if err == nil {
		t.Fatal("Notify: ожидалась ошибка публикации")
	}
}

// TestNewNotifier_NilArgs — nil producer/queries — ошибка конструктора, не
// паника (тот же принцип, что и у presence.NewSink/bridge.New).
func TestNewNotifier_NilArgs(t *testing.T) {
	if _, err := NewNotifier(nil, &db.Queries{}); err == nil {
		t.Fatal("NewNotifier(nil producer, ...): ожидалась ошибка")
	}
	if _, err := NewNotifier(&bus.Producer{}, nil); err == nil {
		t.Fatal("NewNotifier(..., nil queries): ожидалась ошибка")
	}
}

// TestFormatText_KnownKinds — formatText формирует непустой текст для каждого
// известного Kind, с payload'ом соответствующей формы (protocol.md §4).
func TestFormatText_KnownKinds(t *testing.T) {
	taskID := uuid.New()

	cases := []struct {
		name    string
		kind    string
		payload []byte
	}{
		{"agent_question", notify.KindAgentQuestion, mustMarshal(t, bus.AgentQuestionPayload{QuestionID: "q1", Text: "Продолжить?"})},
		{"command_approval_request", notify.KindCommandApprovalRequest, mustMarshal(t, bus.CommandApprovalRequestPayload{RequestID: "r1", Command: "rm -rf /tmp/x", Reason: "вне allowlist"})},
		{"agent_completed", notify.KindAgentCompleted, mustMarshal(t, bus.AgentCompletedPayload{Summary: "Готово"})},
		{"agent_completed_empty_summary", notify.KindAgentCompleted, mustMarshal(t, bus.AgentCompletedPayload{})},
		{"answer_reminder", notify.KindAnswerReminder, nil},
		{"unknown_kind", "some_future_kind", []byte(`{}`)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := notify.Notification{
				TaskID:  pgtype.UUID{Bytes: taskID, Valid: true},
				Kind:    tc.kind,
				Payload: tc.payload,
			}
			text := formatText(n)
			if text == "" {
				t.Fatalf("formatText(%s): пустой текст", tc.name)
			}
		})
	}
}

func mustMarshal(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return b
}
