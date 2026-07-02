-- channels.sql — запросы привязки каналов (FR D3, тикеты 9.6/10.2).
--
-- Назначение (бизнес): пользователь привязывает Telegram-аккаунт к своему
-- аккаунту через одноразовый deep-link код (Gherkin §6). Код генерирует web
-- (тикет 9.6, POST /channels/telegram/link-code), обменивает бот при `/start
-- <code>` (тикет 10.2, POST /channels/telegram/link, см.
-- orchestrator/internal/channel). Здесь — только SQL; бизнес-логика обмена
-- (проверка срока/одноразовости, атомарная транзакция) — в internal/channel.
-- Схема — orchestrator/migrations/00004_channels.sql (не редактируется).

-- name: CreateChannelLinkCode :one
-- Вставляет новый одноразовый код привязки для user_id (тикет 9.6, FR A2, D3).
-- Основной вызывающий в проде — orchestrator/internal/channel.CodeIssuer.IssueLinkCode
-- (HTTP-эндпоинт POST /channels/telegram/link-code, экран «Настройки»); тесты
-- тикета 10.2 (orchestrator/internal/channel/link_integration_test.go)
-- используют запрос напрямую как предусловие для проверки Linker.Exchange.
INSERT INTO channel_link_codes (code, user_id, channel, expires_at)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetChannelLinkCode :one
-- Ищет код привязки по значению кода (сам код — единственный ключ поиска, он
-- же единственное доказательство права на привязку, см. godoc
-- internal/channel.Linker.Exchange). Не найден → pgx.ErrNoRows.
SELECT * FROM channel_link_codes WHERE code = $1;

-- name: MarkChannelLinkCodeUsed :execrows
-- Атомарно помечает код использованным, ТОЛЬКО если он ещё не был использован
-- (WHERE used_at IS NULL) — так конкурентный повторный обмен ТОГО ЖЕ кода
-- (двойной /start <code> одновременно) гарантированно помечает used_at ровно
-- один раз: вызывающая сторона (internal/channel.Linker.Exchange) трактует
-- 0 задетых строк как «код уже использован кем-то ещё» (гонка), 1 — как
-- «этот вызов законно занял код». Выполняется в той же транзакции, что и
-- последующая вставка channel_links (CreateChannelLink) — при её ошибке
-- (например, already-linked конфликт) транзакция откатывается ЦЕЛИКОМ, и
-- used_at тоже откатывается: неудачная попытка не сжигает код впустую.
UPDATE channel_link_codes SET used_at = now() WHERE code = $1 AND used_at IS NULL;

-- name: CreateChannelLink :one
-- Вставляет привязку внешнего канала к аккаунту (тикет 10.2, FR D3). Уникальный
-- индекс (channel, external_id) — один и тот же внешний аккаунт (напр.
-- telegram_user_id) не может быть привязан к двум разным пользователям
-- одновременно; конфликт ловится вызывающей стороной по коду ошибки
-- unique_violation (23505), см. internal/channel.Linker.Exchange.
INSERT INTO channel_links (user_id, channel, external_id)
VALUES ($1, $2, $3)
RETURNING *;
