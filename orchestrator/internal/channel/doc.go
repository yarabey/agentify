// Package channel — привязка внешних каналов (Telegram) к аккаунту (FR D3,
// тикет 10.2, Gherkin §6).
//
// Назначение (бизнес): пользователь ставит задачи и получает уведомления не
// только через web, но и через Telegram (§4/§6 бизнес-ТЗ). Прежде чем бот
// сможет действовать от имени конкретного пользователя (тикет 10.3) или
// оркестратор — слать ему уведомления в Telegram (тикет 7.3/10.4), нужно
// один раз связать telegram_user_id с user_id. Связка проходит через
// одноразовый deep-link код: web генерирует код на свой аккаунт (тикет 9.6,
// POST /channels/telegram/link-code), пользователь пересылает его боту
// командой `/start <code>` — бот вызывает Exchange (через HTTP-обёртку
// PostChannelsTelegramLink, orchestrator/internal/api), который проверяет
// код (существует, не использован, не истёк) и атомарно создаёт запись
// channel_links, помечая код использованным — код одноразовый (Gherkin §6).
//
// Как устроено (тех): Linker держит *pgxpool.Pool напрямую (как
// task.Transitioner, а не узкий sqlc-интерфейс, как auth/integrations) —
// Exchange открывает многошаговую транзакцию (SELECT кода + условный UPDATE
// used_at + INSERT channel_links), которую узкие one-query-интерфейсы не
// выражают. Схема — orchestrator/migrations/00004_channels.sql, SQL-запросы
// — orchestrator/queries/channels.sql (sqlc).
package channel
