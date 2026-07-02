package main

// notify_fanout.go — наивный fan-out Notifier по всем активным каналам
// доставки уведомлений (тикеты 7.2/7.3, FR G1).
//
// Назначение (бизнес): когда у пользователя одновременно активны и web
// (тикет 7.2, всегда доступен — ClientConnHub заведён безусловно), и Telegram
// (тикет 7.3, доступен только при настроенном ORCH_REDPANDA_SEEDS), доменное
// событие уведомления (orchestrator/internal/notify.Notification) должно
// дойти по ОБОИМ каналам — иначе прогонка тикета 7.3 (Gherkin §6 «Уведомление
// в Telegram») не может состояться одновременно с уже работающим 7.2. Тикет
// 7.4 («Маршрутизация каналов», deps: 7.2, 7.3 — вне объёма ЭТОГО PR) добавит
// осмысленную политику (куда слать/дублировать, приоритеты и т.п.); до него
// поведение по умолчанию — простейшее и самое консервативное: слать
// БЕЗУСЛОВНО во ВСЕ настроенные каналы, ничего не выбирая и не дублируя
// специально (естественная отправная точка для будущей политики 7.4, не
// предвосхищающая её решения).
//
// Как устроено (тех): multiNotifier — срез api.Notifier (структурно
// удовлетворяет тому же интерфейсу сам, см. Notify ниже — тот же приём, что и
// у ClientConnHub/telegram.Notifier, реализующих api.Notifier без
// специального объявления). Notify вызывает Notify каждого непустого
// элемента; ошибка ОДНОГО канала только логируется (если задан logger) и НЕ
// прерывает попытку доставки по остальным каналам — тот же принцип
// "недоставка по одному каналу не должна ломать остальные", что и у
// ClientConnHub.Notify при рассылке нескольким открытым вкладкам одного
// пользователя (см. orchestrator/internal/api/client_ws.go). Notify самого
// multiNotifier всегда возвращает nil — вызывающий (handleAgentQuestion и
// т.п.) в любом случае лишь логирует ошибку Notify и не меняет свой поток
// (см. годок api.Notifier), поэтому агрегировать ошибки нескольких каналов в
// одну, теряя, какой конкретно канал отказал, не имеет смысла — каждая уже
// залогирована здесь со своим контекстом.
import (
	"context"
	"log/slog"

	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/notify"
)

// multiNotifier — см. годок файла.
type multiNotifier struct {
	notifiers []api.Notifier
	logger    *slog.Logger
}

// newMultiNotifier собирает multiNotifier поверх переданных каналов. nil-элементы
// notifiers пропускаются молча (удобно вызывающему — не нужно фильтровать
// заранее); logger может быть nil (тогда ошибки отдельных каналов просто не
// логируются, тот же принцип, что и у ClientConnHub).
func newMultiNotifier(logger *slog.Logger, notifiers ...api.Notifier) *multiNotifier {
	filtered := make([]api.Notifier, 0, len(notifiers))
	for _, n := range notifiers {
		if n != nil {
			filtered = append(filtered, n)
		}
	}
	return &multiNotifier{notifiers: filtered, logger: logger}
}

// Notify реализует api.Notifier — см. годок файла.
func (m *multiNotifier) Notify(ctx context.Context, n notify.Notification) error {
	for _, notifier := range m.notifiers {
		if err := notifier.Notify(ctx, n); err != nil && m.logger != nil {
			m.logger.Warn("orchestrator: доставка уведомления по одному из каналов не удалась",
				slog.String("kind", n.Kind), slog.String("error", err.Error()))
		}
	}
	return nil
}
