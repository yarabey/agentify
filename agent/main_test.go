// Юнит-тесты heartbeat-цикла агента (тикет 3.6, FR B4, docs/protocol.md §6)
// — без реального WS/outbox: SendEvent подменяется фейком (eventSender), тот
// же приём, что и в orchestrator/internal/presence для busProducer/
// busConsumer.
package main

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/yarabey/agentify/internal/bus"
)

// fakeEventSender — фейковая реализация eventSender: потокобезопасно
// запоминает все конверты, переданные в SendEvent; err (если задан)
// возвращается вызывающему.
type fakeEventSender struct {
	mu   sync.Mutex
	envs []bus.Envelope
	err  error
}

func (f *fakeEventSender) SendEvent(_ context.Context, env bus.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.envs = append(f.envs, env)
	return f.err
}

func (f *fakeEventSender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.envs)
}

func (f *fakeEventSender) last() bus.Envelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.envs[len(f.envs)-1]
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, nil))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestSendHeartbeat_SendsMachineLevelHeartbeatEnvelope — один вызов
// sendHeartbeat отправляет ровно один конверт type=="heartbeat",
// task_id == nil, с заданным integration_id (protocol.md §2, §6).
func TestSendHeartbeat_SendsMachineLevelHeartbeatEnvelope(t *testing.T) {
	sender := &fakeEventSender{}
	sendHeartbeat(context.Background(), sender, "11111111-1111-1111-1111-111111111111", discardLogger())

	if sender.count() != 1 {
		t.Fatalf("SendEvent вызван %d раз(а), ожидался 1", sender.count())
	}
	env := sender.last()
	if env.Type != bus.MessageTypeHeartbeat {
		t.Fatalf("type=%q, ожидался %q", env.Type, bus.MessageTypeHeartbeat)
	}
	if env.TaskID != nil {
		t.Fatalf("task_id=%v, ожидался nil (machine-level сообщение)", *env.TaskID)
	}
	if env.IntegrationID != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("integration_id=%q, ожидался %q", env.IntegrationID, "11111111-1111-1111-1111-111111111111")
	}
	if env.MessageID == "" {
		t.Fatal("message_id пуст")
	}
	if env.ProtocolVersion != bus.ProtocolVersion {
		t.Fatalf("protocol_version=%q, ожидался %q", env.ProtocolVersion, bus.ProtocolVersion)
	}
}

// TestSendHeartbeat_SendEventError_DoesNotPanic — ошибка SendEvent (сбой
// durable-записи в outbox) логируется, но не паникует и не блокирует
// вызывающего.
func TestSendHeartbeat_SendEventError_DoesNotPanic(t *testing.T) {
	sender := &fakeEventSender{err: context.DeadlineExceeded}
	sendHeartbeat(context.Background(), sender, "11111111-1111-1111-1111-111111111111", discardLogger())
	if sender.count() != 1 {
		t.Fatalf("SendEvent вызван %d раз(а), ожидался 1", sender.count())
	}
}

// TestRunHeartbeatLoop_SendsImmediatelyThenOnEachTick — первый heartbeat
// уходит сразу при старте (не дожидаясь interval), последующие — по каждому
// тику, пока не отменён ctx.
func TestRunHeartbeatLoop_SendsImmediatelyThenOnEachTick(t *testing.T) {
	sender := &fakeEventSender{}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- runHeartbeatLoop(ctx, sender, "22222222-2222-2222-2222-222222222222", 20*time.Millisecond, discardLogger())
	}()

	deadline := time.After(2 * time.Second)
	for sender.count() < 3 {
		select {
		case <-deadline:
			cancel()
			t.Fatalf("получено %d heartbeat-ов за отведённое время, ожидалось хотя бы 3", sender.count())
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runHeartbeatLoop вернул ошибку после отмены ctx: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runHeartbeatLoop не завершился после отмены ctx за отведённое время")
	}
}

// TestRunHeartbeatLoop_StopsOnContextCancel — при уже отменённом ctx Run
// успевает отправить не более одного (первого) heartbeat и завершается
// (nil), не зависая на тикере.
func TestRunHeartbeatLoop_StopsOnContextCancel(t *testing.T) {
	sender := &fakeEventSender{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		done <- runHeartbeatLoop(ctx, sender, "33333333-3333-3333-3333-333333333333", time.Hour, discardLogger())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runHeartbeatLoop вернул ошибку при уже отменённом ctx: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runHeartbeatLoop не завершился при уже отменённом ctx за отведённое время")
	}
}
