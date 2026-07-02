package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/orchestrator/internal/notify"
)

// fakeNotifier — фейковая реализация api.Notifier: считает вызовы Notify,
// возвращает заранее заданную ошибку.
type fakeNotifier struct {
	calls int
	err   error
}

func (f *fakeNotifier) Notify(context.Context, notify.Notification) error {
	f.calls++
	return f.err
}

func testNotification() notify.Notification {
	return notify.Notification{
		TaskID: pgtype.UUID{Bytes: uuid.New(), Valid: true},
		UserID: pgtype.UUID{Bytes: uuid.New(), Valid: true},
		Kind:   notify.KindAgentQuestion,
	}
}

// TestMultiNotifier_Notify_CallsAllChannels — multiNotifier вызывает Notify
// КАЖДОГО зарегистрированного канала (наивный fan-out до тикета 7.4, см.
// notify_fanout.go).
func TestMultiNotifier_Notify_CallsAllChannels(t *testing.T) {
	web := &fakeNotifier{}
	telegram := &fakeNotifier{}
	m := newMultiNotifier(nil, web, telegram)

	if err := m.Notify(context.Background(), testNotification()); err != nil {
		t.Fatalf("Notify: неожиданная ошибка: %v", err)
	}
	if web.calls != 1 {
		t.Errorf("web.calls = %d, ожидался 1", web.calls)
	}
	if telegram.calls != 1 {
		t.Errorf("telegram.calls = %d, ожидался 1", telegram.calls)
	}
}

// TestMultiNotifier_Notify_OneChannelFails_OthersStillCalled — сбой ОДНОГО
// канала не блокирует доставку по остальным, и multiNotifier.Notify в любом
// случае возвращает nil (см. годок Notify — вызывающий всё равно только
// логирует ошибку).
func TestMultiNotifier_Notify_OneChannelFails_OthersStillCalled(t *testing.T) {
	failing := &fakeNotifier{err: errors.New("boom")}
	ok := &fakeNotifier{}
	m := newMultiNotifier(slog.Default(), failing, ok)

	if err := m.Notify(context.Background(), testNotification()); err != nil {
		t.Fatalf("Notify: ожидался nil даже при сбое одного канала, получено: %v", err)
	}
	if failing.calls != 1 {
		t.Errorf("failing.calls = %d, ожидался 1", failing.calls)
	}
	if ok.calls != 1 {
		t.Errorf("ok.calls = %d, ожидался 1 (сбой первого канала не должен блокировать второй)", ok.calls)
	}
}

// TestNewMultiNotifier_SkipsNilNotifiers — nil-элементы (например, канал не
// настроен) не паникуют при Notify, просто пропускаются.
func TestNewMultiNotifier_SkipsNilNotifiers(t *testing.T) {
	ok := &fakeNotifier{}
	m := newMultiNotifier(nil, nil, ok, nil)

	if err := m.Notify(context.Background(), testNotification()); err != nil {
		t.Fatalf("Notify: неожиданная ошибка: %v", err)
	}
	if ok.calls != 1 {
		t.Errorf("ok.calls = %d, ожидался 1", ok.calls)
	}
	if len(m.notifiers) != 1 {
		t.Errorf("len(m.notifiers) = %d, ожидался 1 (nil-элементы отфильтрованы)", len(m.notifiers))
	}
}
