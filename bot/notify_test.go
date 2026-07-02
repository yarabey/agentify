package main

// Юнит-тесты notifyConsumer (тикет 10.4, FR G1) — без Redpanda/сети: consumer
// подменяется фейком (busConsumer), отправка — фейком (telegramSender), тот
// же приём, что и у остальных unit-тестов пакета main (fakeActor/fakeLinker в
// bot/task_test.go / bot/start_test.go) и у presence.busProducer в
// orchestrator/internal/presence/sink_test.go.
import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	tele "gopkg.in/telebot.v3"

	"github.com/yarabey/agentify/internal/bus"
)

// fakeSender — фейковая telegramSender: запоминает (to, what) последнего
// вызова Send; err (если задан) возвращается вызывающему.
type fakeSender struct {
	calls []sendCall
	err   error
}

type sendCall struct {
	to   tele.Recipient
	what interface{}
}

func (f *fakeSender) Send(to tele.Recipient, what interface{}, _ ...interface{}) (*tele.Message, error) {
	f.calls = append(f.calls, sendCall{to: to, what: what})
	if f.err != nil {
		return nil, f.err
	}
	return &tele.Message{}, nil
}

func envelopeWithPayload(t *testing.T, typ string, payload interface{}) bus.Envelope {
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   "11111111-1111-1111-1111-111111111111",
		Type:            typ,
		Ts:              "2026-07-02T00:00:00Z",
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         b,
	}
}

// TestNotifyConsumer_Handle_SendsTextToChatID — штатный путь: конверт
// telegram_notification → отправка Text в ChatID(telegram_chat_id) (приёмка
// тикета 10.4: "событие → сообщение в чат").
func TestNotifyConsumer_Handle_SendsTextToChatID(t *testing.T) {
	fs := &fakeSender{}
	c := newNotifyConsumer(nil, fs, nil)

	env := envelopeWithPayload(t, bus.MessageTypeTelegramNotification, bus.TelegramNotificationPayload{
		TelegramChatID: 42,
		Kind:           "agent_question",
		TaskID:         "task-1",
		Text:           "Агент задал вопрос",
	})

	if err := c.handle(context.Background(), env); err != nil {
		t.Fatalf("handle: неожиданная ошибка: %v", err)
	}
	if len(fs.calls) != 1 {
		t.Fatalf("Send вызван %d раз(а), ожидался 1", len(fs.calls))
	}
	if fs.calls[0].to.Recipient() != tele.ChatID(42).Recipient() {
		t.Fatalf("to = %v, ожидался ChatID(42)", fs.calls[0].to)
	}
	if fs.calls[0].what != "Агент задал вопрос" {
		t.Fatalf("what = %v, ожидался текст уведомления", fs.calls[0].what)
	}
}

// TestNotifyConsumer_Handle_IgnoresOtherMessageTypes — конверт НЕ типа
// telegram_notification молча пропускается (nil, offset коммитится).
func TestNotifyConsumer_Handle_IgnoresOtherMessageTypes(t *testing.T) {
	fs := &fakeSender{}
	c := newNotifyConsumer(nil, fs, nil)

	env := envelopeWithPayload(t, "some_other_type", map[string]string{})
	if err := c.handle(context.Background(), env); err != nil {
		t.Fatalf("handle: неожиданная ошибка: %v", err)
	}
	if len(fs.calls) != 0 {
		t.Fatalf("Send вызван %d раз(а) для чужого типа, ожидалось 0", len(fs.calls))
	}
}

// TestNotifyConsumer_Handle_BadPayload_SkipsWithoutError — невалидный JSON в
// Payload — повреждённые данные, пропуск без ошибки (см. годок notify.go про
// различие "повреждённые данные" vs "транзиентный сбой").
func TestNotifyConsumer_Handle_BadPayload_SkipsWithoutError(t *testing.T) {
	fs := &fakeSender{}
	c := newNotifyConsumer(nil, fs, nil)

	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   "11111111-1111-1111-1111-111111111111",
		Type:            bus.MessageTypeTelegramNotification,
		Ts:              "2026-07-02T00:00:00Z",
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         []byte(`not json`),
	}
	if err := c.handle(context.Background(), env); err != nil {
		t.Fatalf("handle: ожидался nil для невалидного JSON, получено: %v", err)
	}
	if len(fs.calls) != 0 {
		t.Fatalf("Send вызван %d раз(а) для невалидного payload, ожидалось 0", len(fs.calls))
	}
}

// TestNotifyConsumer_Handle_ZeroChatID_SkipsWithoutError — payload без
// telegram_chat_id (0) — повреждённые данные, пропуск без ошибки.
func TestNotifyConsumer_Handle_ZeroChatID_SkipsWithoutError(t *testing.T) {
	fs := &fakeSender{}
	c := newNotifyConsumer(nil, fs, nil)

	env := envelopeWithPayload(t, bus.MessageTypeTelegramNotification, bus.TelegramNotificationPayload{
		Text: "текст без chat_id",
	})
	if err := c.handle(context.Background(), env); err != nil {
		t.Fatalf("handle: ожидался nil при нулевом chat_id, получено: %v", err)
	}
	if len(fs.calls) != 0 {
		t.Fatalf("Send вызван %d раз(а) при нулевом chat_id, ожидалось 0", len(fs.calls))
	}
}

// TestNotifyConsumer_Handle_EmptyText_SkipsWithoutError — payload с пустым
// Text — повреждённые данные, пропуск без ошибки.
func TestNotifyConsumer_Handle_EmptyText_SkipsWithoutError(t *testing.T) {
	fs := &fakeSender{}
	c := newNotifyConsumer(nil, fs, nil)

	env := envelopeWithPayload(t, bus.MessageTypeTelegramNotification, bus.TelegramNotificationPayload{
		TelegramChatID: 42,
	})
	if err := c.handle(context.Background(), env); err != nil {
		t.Fatalf("handle: ожидался nil при пустом Text, получено: %v", err)
	}
	if len(fs.calls) != 0 {
		t.Fatalf("Send вызван %d раз(а) при пустом Text, ожидалось 0", len(fs.calls))
	}
}

// TestNotifyConsumer_Handle_SendError_Propagated — сбой Send (транзиентный —
// сеть/Bot API) пробрасывается вызывающему (bus.Consumer.Run НЕ закоммитит
// offset, at-least-once, см. годок файла).
func TestNotifyConsumer_Handle_SendError_Propagated(t *testing.T) {
	fs := &fakeSender{err: errors.New("telegram api недоступен")}
	c := newNotifyConsumer(nil, fs, nil)

	env := envelopeWithPayload(t, bus.MessageTypeTelegramNotification, bus.TelegramNotificationPayload{
		TelegramChatID: 42,
		Text:           "текст",
	})
	if err := c.handle(context.Background(), env); err == nil {
		t.Fatal("handle: ожидалась ошибка при сбое Send")
	}
}

// fakeBusConsumer — фейковая busConsumer: запоминает переданный handler и
// вызывает его РОВНО с теми конвертами, что переданы в envs, при вызове Run.
type fakeBusConsumer struct {
	envs []bus.Envelope
	err  error
}

func (f *fakeBusConsumer) Run(ctx context.Context, handler bus.Handler) error {
	for _, env := range f.envs {
		if err := handler(ctx, env); err != nil {
			return err
		}
	}
	return f.err
}

// TestNotifyConsumer_Run_DelegatesToBusConsumer — Run — тонкая обёртка над
// busConsumer.Run(ctx, c.handle): конверты из consumer доходят до sender.
func TestNotifyConsumer_Run_DelegatesToBusConsumer(t *testing.T) {
	fs := &fakeSender{}
	env := envelopeWithPayload(t, bus.MessageTypeTelegramNotification, bus.TelegramNotificationPayload{
		TelegramChatID: 7,
		Text:           "привет",
	})
	fc := &fakeBusConsumer{envs: []bus.Envelope{env}}
	c := newNotifyConsumer(fc, fs, nil)

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: неожиданная ошибка: %v", err)
	}
	if len(fs.calls) != 1 {
		t.Fatalf("Send вызван %d раз(а) через Run, ожидался 1", len(fs.calls))
	}
}
