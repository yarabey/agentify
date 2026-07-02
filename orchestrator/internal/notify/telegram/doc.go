// Package telegram — Telegram-канал доставки доменных уведомлений (тикет 7.3,
// EPIC 7 «Уведомления», deps: 7.1 done, 10.4; FR G1, Gherkin §6 «Уведомление в
// Telegram»).
//
// Назначение (бизнес): пользователь, привязавший Telegram-аккаунт (тикет 10.2,
// channel_links), должен получать в Telegram те же доменные уведомления
// (вопрос агента, запрос согласования команды, отчёт о завершении — см.
// orchestrator/internal/notify), что и в web по WebSocket (тикет 7.2). Ровно
// как ClientConnHub (orchestrator/internal/api/client_ws.go) — второй, уже
// существующий, канал доставки того же доменного события notify.Notification
// — этот пакет РЕАЛИЗУЕТ api.Notifier structurally (без импорта пакета api,
// тот же приём, что у presence.Sink/bridge.Bridge), но публикует НЕ напрямую
// пользователю, а в топик Redpanda notifications.telegram (ADR 0001,
// docs/protocol.md §3), откуда его читает бот (тикет 10.4,
// bot/notify.go) и отправляет через Bot API.
//
// Ключевое архитектурное решение (простой путь, обоснованный в тикете, ADR не
// заводился — раскладка топика уже зафиксирована ADR 0001 тикета 3.1):
// РЕЗОЛВ привязки канала (есть ли у user_id активная запись channel_links с
// channel='telegram', и какой у неё telegram_user_id/chat_id) происходит
// ЗДЕСЬ, на стороне оркестратора, ДО публикации в Redpanda — а не на стороне
// бота при чтении из топика. Причины:
//  1. «Единый API»: оркестратор — единственный владелец связи user_id ↔
//     внешний канал (channel_links, тикет 10.2); бот не имеет и не должен
//     иметь прямого доступа к БД оркестратора.
//  2. Нет привязки Telegram у пользователя — нет смысла публиковать в топик
//     вообще: Notify в этом случае возвращает nil (тот же принцип «best-effort
//     канал, отсутствие получателя — не ошибка», что и у ClientConnHub.Notify,
//     когда нет открытых WS-соединений).
//  3. Бот остаётся МАКСИМАЛЬНО тонким адаптером (см. bot/main.go,
//     bot/notify.go): получает готовый chat_id и готовый текст
//     (bus.TelegramNotificationPayload), просто пересылает в Bot API — никакой
//     бизнес-логики форматирования/резолва на стороне бота.
//
// Как устроено (тех): Notifier.Notify выполняет три шага (см. годок Notify) —
// резолв channel_links (queries.GetChannelLinkByUserAndChannel), резолв
// integration_id задачи (queries.GetTaskByIDAndUser — нужен ТОЛЬКО чтобы
// удовлетворить обязательное поле Envelope.IntegrationID, см. protocol.md §2 —
// сам bus.Envelope переиспользуется как есть, без изменения контракта
// машинного протокола) и публикацию через *bus.Producer.Publish с ключом
// партиции user_id (ADR 0001 — Envelope.PartitionKey не расширяется отдельным
// полем UserID ради этого единственного нового использования, ключ вычисляется
// напрямую вызывающим, см. годок busProducer).
package telegram
