package main

// notify_fanout.go — маршрутизация уведомлений между активными каналами
// доставки (web — тикет 7.2, Telegram — тикет 7.3), тикет 7.4 «Маршрутизация
// каналов», FR G2.
//
// Назначение (бизнес): FR G2 требует, чтобы при одновременно активных
// нескольких каналах была определена политика — "куда доставлять, нужно ли
// дублировать" (docs/ТЗ_Оркестратор_бизнес-версия.md §G2; docs/MVP_TICKETS.md
// 7.4). ПРИНЯТОЕ РЕШЕНИЕ (см. duplicateToAllChannels ниже): ДУБЛИРОВАТЬ —
// отправлять безусловно во ВСЕ настроенные каналы, ничего не выбирая между
// ними. Это не временная заглушка "до тикета 7.4" (как было сформулировано в
// 7.3/10.4) — это ИТОГОВОЕ, осознанно принятое решение самого тикета 7.4.
// Обоснование:
//
//  1. FR G1 прямо описывает MVP-поведение как одновременную доставку "в web
//     ... и в Telegram" (докс/ТЗ §G1) — союз "и", не "либо" — то есть базовый
//     сценарий продукта уже подразумевает дублирование, а не эксклюзивный
//     выбор канала.
//  2. Оба существующих канала УЖЕ реализуют собственный best-effort критерий
//     "активности" и молча не доставляют, если он не выполнен:
//     ClientConnHub.Notify ничего не делает, если у пользователя нет ни
//     одного открытого WS-соединения (internal/api/client_ws.go), а
//     telegram.Notifier ничего не публикует, если Telegram не привязан
//     (internal/notify/telegram/notifier.go, GetChannelLinkByUserAndChannel
//     → pgx.ErrNoRows → nil). Значит, безусловная рассылка обоим notifier'ам
//     на уровне multiNotifier АВТОМАТИЧЕСКИ сводится ровно к требованию
//     G2 "если активны оба канала — доставить в оба, если активен только
//     один — доставить только в него, если ни одного — не доставлять никуда"
//     — без необходимости дублировать в multiNotifier знание о том, что
//     значит "канал активен" для каждого конкретного канала (это знание уже
//     инкапсулировано внутри самих ClientConnHub/telegram.Notifier).
//  3. Дублирование — самая БЕЗОПАСНАЯ политика с точки зрения продукта:
//     пользователь может не заметить web-уведомление (вкладка в фоне) или
//     пропустить Telegram (уведомления Telegram выключены на телефоне) — в
//     обоих случаях цель G1 "не пропустить и не потерять задачу" (Gherkin §6)
//     достигается только избыточностью, а не выбором "более приоритетного"
//     канала. Эксклюзивный выбор (например, "предпочесть web, если сессия
//     живая") добавил бы риск молчаливой потери уведомления без какой-либо
//     компенсирующей пользы для MVP.
//
// Как устроено (тех): multiNotifier — срез api.Notifier (структурно
// удовлетворяет тому же интерфейсу сам, см. Notify ниже — тот же приём, что и
// у ClientConnHub/telegram.Notifier, реализующих api.Notifier без
// специального объявления) плюс явно поименованная политика выбора
// notifier'ов routingPolicy. Notify применяет политику к списку
// зарегистрированных каналов и вызывает Notify каждого результата; ошибка
// ОДНОГО канала только логируется (если задан logger) и НЕ прерывает попытку
// доставки по остальным каналам — тот же принцип "недоставка по одному
// каналу не должна ломать остальные", что и у ClientConnHub.Notify при
// рассылке нескольким открытым вкладкам одного пользователя (см.
// orchestrator/internal/api/client_ws.go). Notify самого multiNotifier всегда
// возвращает nil — вызывающий (handleAgentQuestion и т.п.) в любом случае
// лишь логирует ошибку Notify и не меняет свой поток (см. годок
// api.Notifier), поэтому агрегировать ошибки нескольких каналов в одну,
// теряя, какой конкретно канал отказал, не имеет смысла — каждая уже
// залогирована здесь со своим контекстом.
//
// routingPolicy — явная точка расширения для будущих тикетов: если продукту
// понадобится более сложная маршрутизация (например, приоритет канала,
// пользовательские настройки "куда слать" — FR G3, вне MVP), её реализуют
// новой функцией того же типа routingPolicy и передают в newMultiNotifier
// вместо duplicateToAllChannels, не трогая остальной multiNotifier.
import (
	"context"
	"log/slog"

	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/notify"
)

// routingPolicy выбирает, каким из зарегистрированных каналов передать
// конкретное уведомление (тикет 7.4, FR G2). См. годок файла про принятое
// решение duplicateToAllChannels и про то, зачем политика вынесена отдельным
// именованным типом, а не зашита в Notify напрямую.
type routingPolicy func(notifiers []api.Notifier, n notify.Notification) []api.Notifier

// duplicateToAllChannels — routingPolicy, принятая тикетом 7.4 в качестве
// итоговой политики маршрутизации MVP: возвращает ВСЕ зарегистрированные
// каналы без исключения (полное обоснование — см. годок файла, пункты 1–3).
// Параметр n не используется — решение не зависит от вида уведомления, оно
// одинаково для agent_question/command_approval_request/agent_completed и
// т.д.
func duplicateToAllChannels(notifiers []api.Notifier, _ notify.Notification) []api.Notifier {
	return notifiers
}

// multiNotifier — см. годок файла.
type multiNotifier struct {
	notifiers []api.Notifier
	policy    routingPolicy
	logger    *slog.Logger
}

// newMultiNotifier собирает multiNotifier поверх переданных каналов с
// политикой маршрутизации duplicateToAllChannels (см. годок файла). nil-
// элементы notifiers пропускаются молча (удобно вызывающему — не нужно
// фильтровать заранее); logger может быть nil (тогда ошибки отдельных
// каналов просто не логируются, тот же принцип, что и у ClientConnHub).
func newMultiNotifier(logger *slog.Logger, notifiers ...api.Notifier) *multiNotifier {
	filtered := make([]api.Notifier, 0, len(notifiers))
	for _, n := range notifiers {
		if n != nil {
			filtered = append(filtered, n)
		}
	}
	return &multiNotifier{notifiers: filtered, policy: duplicateToAllChannels, logger: logger}
}

// Notify реализует api.Notifier — см. годок файла.
func (m *multiNotifier) Notify(ctx context.Context, n notify.Notification) error {
	for _, notifier := range m.policy(m.notifiers, n) {
		if err := notifier.Notify(ctx, n); err != nil && m.logger != nil {
			m.logger.Warn("orchestrator: доставка уведомления по одному из каналов не удалась",
				slog.String("kind", n.Kind), slog.String("error", err.Error()))
		}
	}
	return nil
}
