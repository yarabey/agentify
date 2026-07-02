// Package channel — привязка внешних каналов (Telegram) к аккаунту (FR A2,
// D3, тикеты 9.6/10.2, Gherkin §6).
//
// Назначение (бизнес): пользователь ставит задачи и получает уведомления не
// только через web, но и через Telegram (§4/§6 бизнес-ТЗ). Прежде чем бот
// сможет действовать от имени конкретного пользователя (тикет 10.3) или
// оркестратор — слать ему уведомления в Telegram (тикет 7.3/10.4), нужно
// один раз связать telegram_user_id с user_id. Связка проходит через
// одноразовый deep-link код: аутентифицированный пользователь на экране
// «Настройки» генерирует код на свой аккаунт (тикет 9.6, CodeIssuer,
// POST /channels/telegram/link-code), пересылает его боту командой
// `/start <code>` — бот вызывает Exchange (через HTTP-обёртку
// PostChannelsTelegramLink, orchestrator/internal/api), который проверяет
// код (существует, не использован, не истёк) и атомарно создаёт запись
// channel_links, помечая код использованным — код одноразовый (Gherkin §6).
//
// Как устроено (тех): CodeIssuer (codegen.go, тикет 9.6) и Linker (link.go,
// тикет 10.2) — независимые половины флоу, не знающие друг о друге: первый
// только вставляет код (одна операция, *db.Queries), второй читает/тратит его
// в многошаговой транзакции (SELECT кода + условный UPDATE used_at + INSERT
// channel_links, поэтому держит *pgxpool.Pool напрямую, как task.Transitioner,
// а не узкий sqlc-интерфейс, как auth/integrations) — многошаговость Exchange
// узкие one-query-интерфейсы не выражают. Схема —
// orchestrator/migrations/00004_channels.sql, SQL-запросы —
// orchestrator/queries/channels.sql (sqlc).
package channel
