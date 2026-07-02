package main

// Unit-тесты startReplyText — бизнес-логики /start (тикет 10.2, FR D3).
// Проверяют все ветки без реального telebot.Context/сети через фейковый
// telegramLinker (см. годок bot/start.go про разделение чистой логики и
// telebot-обвязки).
import (
	"context"
	"errors"
	"testing"

	"github.com/yarabey/agentify/bot/internal/orchestrator"
)

// fakeLinker — подменный telegramLinker для unit-тестов startReplyText.
type fakeLinker struct {
	err error

	gotCode           string
	gotTelegramUserID string
	called            bool
}

func (f *fakeLinker) LinkTelegram(_ context.Context, code, telegramUserID string) error {
	f.called = true
	f.gotCode = code
	f.gotTelegramUserID = telegramUserID
	return f.err
}

// TestStartReplyText_NilClient — BOT_ORCHESTRATOR_URL не задан (client —
// nil-интерфейс) → "недоступно", без паники.
func TestStartReplyText_NilClient(t *testing.T) {
	got := startReplyText(context.Background(), nil, []string{"code"}, 42, true, nil)
	if got == "" {
		t.Fatal("пустой ответ при nil client")
	}
}

// TestStartReplyText_NoPayload — `/start` без кода → приветствие, Exchange не
// вызывается.
func TestStartReplyText_NoPayload(t *testing.T) {
	linker := &fakeLinker{}
	got := startReplyText(context.Background(), linker, nil, 42, true, nil)
	if linker.called {
		t.Fatal("LinkTelegram не должен вызываться без payload")
	}
	if got == "" {
		t.Fatal("пустой ответ на /start без payload")
	}
}

// TestStartReplyText_NoSender — апдейт без Sender() → отказ, LinkTelegram не
// вызывается.
func TestStartReplyText_NoSender(t *testing.T) {
	linker := &fakeLinker{}
	got := startReplyText(context.Background(), linker, []string{"code"}, 0, false, nil)
	if linker.called {
		t.Fatal("LinkTelegram не должен вызываться без Sender()")
	}
	if got == "" {
		t.Fatal("пустой ответ без Sender()")
	}
}

// TestStartReplyText_Success — валидный код → LinkTelegram вызван с
// корректными code/telegram_user_id, ответ не пуст (FR D3).
func TestStartReplyText_Success(t *testing.T) {
	linker := &fakeLinker{}
	got := startReplyText(context.Background(), linker, []string{"the-code"}, 12345, true, nil)
	if !linker.called {
		t.Fatal("LinkTelegram не вызван")
	}
	if linker.gotCode != "the-code" {
		t.Fatalf("code = %q, ожидался the-code", linker.gotCode)
	}
	if linker.gotTelegramUserID != "12345" {
		t.Fatalf("telegram_user_id = %q, ожидался 12345", linker.gotTelegramUserID)
	}
	if got == "" {
		t.Fatal("пустой ответ на успешную привязку")
	}
}

// TestStartReplyText_ErrorMapping — каждый сентинел orchestrator.Client
// приводит к непустому, различающемуся ответу (пользователь получает
// понятную причину отказа, приёмка тикета 10.2: истёкший код и повторное
// использование отклоняются).
func TestStartReplyText_ErrorMapping(t *testing.T) {
	cases := []error{
		orchestrator.ErrLinkCodeNotFound,
		orchestrator.ErrLinkCodeExpired,
		orchestrator.ErrLinkCodeUsed,
		orchestrator.ErrAlreadyLinked,
		errors.New("сеть недоступна"), // неизвестная ошибка — общий текст, не паника
	}
	seen := map[string]bool{}
	for _, err := range cases {
		linker := &fakeLinker{err: err}
		got := startReplyText(context.Background(), linker, []string{"code"}, 1, true, nil)
		if got == "" {
			t.Fatalf("пустой ответ для ошибки %v", err)
		}
		seen[got] = true
	}
	if len(seen) != len(cases) {
		t.Fatalf("ожидались %d различных текстов ответа (по одному на причину отказа), получено %d: %v", len(cases), len(seen), seen)
	}
}
