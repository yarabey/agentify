package channel

// link.go — обмен одноразового кода привязки на запись channel_links
// (тикет 10.2, FR D3 «привязка канала к аккаунту описана явно», Gherkin §6
// «Уведомления» — привязка Telegram-аккаунта — предусловие сценария
// «Уведомление в Telegram», docs/User_stories_Gherkin.md).
//
// Бизнес: код привязки — единственное доказательство права привязать
// telegram_user_id к user_id в этой точке (пользователь ещё не
// аутентифицирован в системе как web-клиент, см. описание
// POST /channels/telegram/link в api/openapi.yaml). Ровно как
// registration_token у POST /auth/register (тикет 1.2), сам факт
// предъявления действительного кода — достаточное основание для действия;
// код одноразовый (used_at) и с ограниченным сроком жизни (expires_at),
// поэтому кража/утечка кода имеет краткое окно применимости.
//
// Как устроено (тех): вся проверка + запись — ОДНА транзакция (Begin/Commit,
// как у task.Transitioner), поэтому неудачная попытка (код не найден/истёк/
// уже использован/конфликт already-linked) не оставляет частичных следов в
// БД: код помечается used_at ТОЛЬКО если INSERT channel_links тоже прошёл
// успешно, иначе весь transaction откатывается.
import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// pgUniqueViolation — код ошибки PostgreSQL «нарушение уникального
// ограничения» (SQLSTATE 23505, см. тот же локальный const в
// orchestrator/internal/api). Используется, чтобы отличить конфликт
// «telegram_user_id уже привязан к другому аккаунту» (UNIQUE
// channel_links(channel, external_id)) от прочих ошибок INSERT.
const pgUniqueViolation = "23505"

// Ошибки Exchange (FR D3) — все сентинелы, вызывающая сторона (HTTP-хендлер,
// orchestrator/internal/api) мапит их на конкретные коды ответа через
// errors.Is.
var (
	// ErrLinkCodeNotFound — код с таким значением не существует (опечатка,
	// код для другого канала, либо никогда не выпускался).
	ErrLinkCodeNotFound = errors.New("channel: код привязки не найден")
	// ErrLinkCodeUsed — код уже был обменян на привязку ранее (одноразовость,
	// Gherkin §6). Возвращается и при гонке двух одновременных обменов ТОГО
	// ЖЕ кода — ровно один из них выигрывает MarkChannelLinkCodeUsed.
	ErrLinkCodeUsed = errors.New("channel: код привязки уже использован")
	// ErrLinkCodeExpired — код найден и ещё не использован, но истёк
	// (expires_at в прошлом). Приёмка тикета 10.2: «истёкший код → ошибка,
	// без привязки».
	ErrLinkCodeExpired = errors.New("channel: код привязки истёк")
	// ErrAlreadyLinked — external_id (напр. telegram_user_id) уже привязан к
	// ДРУГОМУ аккаунту (UNIQUE channel_links(channel, external_id)). Код при
	// этом остаётся неиспользованным (см. godoc пакета) — можно повторить
	// попытку с корректным кодом на свой аккаунт.
	ErrAlreadyLinked = errors.New("channel: внешний аккаунт уже привязан к другому пользователю")
)

// Linker — сервис обмена одноразового кода привязки на запись channel_links
// (FR D3). Держит *pgxpool.Pool напрямую — Exchange открывает многошаговую
// транзакцию (см. godoc пакета).
type Linker struct {
	pool    *pgxpool.Pool
	queries *db.Queries
}

// NewLinker — конструктор Linker.
func NewLinker(pool *pgxpool.Pool) *Linker {
	return &Linker{pool: pool, queries: db.New(pool)}
}

// Exchange обменивает одноразовый код code (выпущенный для канала channel,
// напр. "telegram") на привязку внешнего аккаунта externalID (напр.
// telegram_user_id) к user_id, на который был выпущен код.
//
// Возвращает созданную запись channel_links при успехе (её user_id — владелец
// аккаунта, на который был выпущен код; created_at — момент привязки, нужен
// вызывающей стороне — PostChannelsTelegramLink, orchestrator/internal/api —
// для тела ответа ChannelLink). Ошибки — сентинелы этого файла
// (ErrLinkCodeNotFound/ErrLinkCodeUsed/ErrLinkCodeExpired/ErrAlreadyLinked),
// проверяются вызывающей стороной через errors.Is; любая другая ошибка —
// сбой инфраструктуры (БД недоступна и т.п.), не бизнес-исход.
//
// Шаги (одна транзакция, см. godoc пакета):
//  1. GetChannelLinkCode по значению кода — не найден ИЛИ выпущен для
//     другого канала → ErrLinkCodeNotFound (канал не раскрываем отдельным
//     кодом ошибки: неверный канал для этого кода неотличим от
//     несуществующего кода, ровно как чужая интеграция неотличима от
//     несуществующей, тикет 2.2/1.5).
//  2. used_at уже проставлен → ErrLinkCodeUsed.
//  3. expires_at в прошлом → ErrLinkCodeExpired.
//  4. MarkChannelLinkCodeUsed (условный UPDATE ... WHERE used_at IS NULL) —
//     0 задетых строк означает гонку (кто-то использовал код между шагом 1 и
//     этим шагом) → ErrLinkCodeUsed.
//  5. CreateChannelLink — конфликт unique_violation (channel, external_id)
//     → ErrAlreadyLinked; транзакция откатывается (defer tx.Rollback), т.е.
//     шаг 4 тоже откатывается — код остаётся неиспользованным.
//  6. Commit — только тут код считается фактически потраченным.
func (l *Linker) Exchange(ctx context.Context, channelName, code, externalID string) (link db.ChannelLink, err error) {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return db.ChannelLink{}, fmt.Errorf("channel: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op после успешного Commit

	q := l.queries.WithTx(tx)

	row, err := q.GetChannelLinkCode(ctx, code)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.ChannelLink{}, ErrLinkCodeNotFound
		}
		return db.ChannelLink{}, fmt.Errorf("channel: получить код привязки: %w", err)
	}
	if row.Channel != channelName {
		// Код существует, но выпущен для другого канала — трактуем как
		// «не найден» (см. godoc метода, п.1): не даём сигнала о
		// существовании кода под чужим каналом.
		return db.ChannelLink{}, ErrLinkCodeNotFound
	}
	if row.UsedAt.Valid {
		return db.ChannelLink{}, ErrLinkCodeUsed
	}
	if !row.ExpiresAt.Valid || !time.Now().Before(row.ExpiresAt.Time) {
		return db.ChannelLink{}, ErrLinkCodeExpired
	}

	affected, err := q.MarkChannelLinkCodeUsed(ctx, code)
	if err != nil {
		return db.ChannelLink{}, fmt.Errorf("channel: пометить код использованным: %w", err)
	}
	if affected == 0 {
		// Гонка: код использован конкурентным вызовом между шагами 1 и 4
		// (см. godoc MarkChannelLinkCodeUsed в queries/channels.sql).
		return db.ChannelLink{}, ErrLinkCodeUsed
	}

	created, err := q.CreateChannelLink(ctx, db.CreateChannelLinkParams{
		UserID:     row.UserID,
		Channel:    channelName,
		ExternalID: externalID,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return db.ChannelLink{}, ErrAlreadyLinked
		}
		return db.ChannelLink{}, fmt.Errorf("channel: создать channel_links: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return db.ChannelLink{}, fmt.Errorf("channel: commit tx: %w", err)
	}
	return created, nil
}
