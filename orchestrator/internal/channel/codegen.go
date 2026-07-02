package channel

// codegen.go — генерация одноразового кода привязки Telegram-аккаунта (тикет
// 9.6, FR A2 «доступ по аутентификации», FR D3 «привязка канала к аккаунту
// описана явно», экран «Настройки», Gherkin §6 «Уведомления» — привязка
// Telegram-аккаунта как предусловие сценария «Уведомление в Telegram»).
//
// Назначение (бизнес): прежде чем Linker (link.go, тикет 10.2) сможет что-то
// обменять, кто-то должен код выпустить. Это делает web: аутентифицированный
// пользователь на экране «Настройки» нажимает «Привязать Telegram» —
// CodeIssuer.IssueLinkCode генерирует одноразовый код СТРОГО на его
// собственный user_id (взятый из access-токена, а не из тела запроса —
// подделать код для чужого аккаунта через этот путь невозможно) и сохраняет
// его в channel_link_codes с ограниченным сроком жизни; пользователь
// показывает код боту командой `/start <code>` — дальше в дело вступает
// Linker.Exchange. CodeIssuer и Linker сознательно НЕ знают друг о друге:
// первый только создаёт код (владелец — доказательство личности через
// Bearer), второй только тратит его (владелец — сам факт предъявления кода) —
// симметричные, но независимые половины одного флоу привязки.
//
// Как устроено (тех): в отличие от Linker.Exchange (link.go), которому нужна
// многошаговая транзакция (SELECT + условный UPDATE + INSERT), здесь
// единственная операция — INSERT, поэтому CodeIssuer держит просто
// *db.Queries (без прямого доступа к *pgxpool.Pool и транзакциям). Сам код —
// случайная строка из компактного алфавита без визуально спутываемых
// символов (см. linkCodeAlphabet), пригодная для ручного набора в Telegram;
// коллизия PRIMARY KEY (channel_link_codes.code) — не инфраструктурная
// ошибка, а повод сгенерировать код заново (см. maxIssueLinkCodeAttempts).
import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// linkCodeAlphabet — алфавит символов одноразового кода привязки: заглавные
// буквы и цифры без визуально спутываемых символов (0/O, 1/I/L исключены) —
// тот же довод, что у Crockford base32: код набирается человеком руками в
// Telegram (`/start <code>`), опечатка из-за похожих символов не должна
// стоить пользователю повторного похода в web за новым кодом. Длина алфавита
// (32 = 2^5) намеренно степень двойки — см. generateLinkCode про равномерность
// распределения при переводе случайных байт в символы.
const linkCodeAlphabet = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"

// linkCodeLength — длина случайной части кода в символах linkCodeAlphabet. 8
// символов из 32-символьного алфавита — 40 бит энтропии (2^40 вариантов):
// для короткоживущего одноразового кода (TTL по умолчанию
// DefaultLinkCodeTTL) более чем достаточно против перебора, при этом код
// остаётся удобным для ручного ввода одной строкой (сравним по длине с
// `/start ABCD2345`).
const linkCodeLength = 8

// DefaultLinkCodeTTL — время жизни кода привязки по умолчанию, если
// вызывающая сторона передала неположительное значение (см. NewCodeIssuer).
// Продуктовое решение об окончательном значении НЕ зафиксировано
// (docs/MANUAL_STEPS.md §4 — тот же принцип "дефолт + открытый вопрос", что у
// task.defaultAnswerTimeoutThreshold, orchestrator/internal/task): 15 минут
// выбраны как разумный компромисс — пользователю хватает времени переключиться
// в Telegram-клиент и набрать команду, при этом код не остаётся действительным
// неоправданно долго после того, как реально понадобился. Переопределяется
// через ORCH_TELEGRAM_LINK_CODE_TTL в orchestrator/main.go.
const DefaultLinkCodeTTL = 15 * time.Minute

// maxIssueLinkCodeAttempts — сколько раз IssueLinkCode повторяет генерацию
// кода при коллизии PRIMARY KEY (channel_link_codes.code). При 40 битах
// энтропии (см. linkCodeAlphabet/linkCodeLength) коллизия практически
// недостижима — это предохранитель на маловероятный случай, а не ожидаемый
// путь выполнения.
const maxIssueLinkCodeAttempts = 5

// CodeIssuer — сервис генерации одноразового кода привязки канала (тикет 9.6,
// FR A2, D3). Дополняет Linker (link.go): CodeIssuer выпускает код для СЕБЯ
// (userID вызывающего), Linker обменивает уже выпущенный код на привязку —
// см. godoc файла про разделение ролей.
type CodeIssuer struct {
	queries *db.Queries
	ttl     time.Duration
}

// NewCodeIssuer — конструктор CodeIssuer поверх пула соединений pool.
//
// ttl — время жизни выпускаемых кодов; неположительное значение (включая
// нулевое time.Duration по умолчанию для непереданного конфига) заменяется на
// DefaultLinkCodeTTL — тот же принцип "явный дефолт при некорректном
// значении", что у task.WithAnswerTimeoutThreshold, чтобы по ошибке не
// выпустить код с уже истёкшим или бессрочным сроком действия.
func NewCodeIssuer(pool *pgxpool.Pool, ttl time.Duration) *CodeIssuer {
	if ttl <= 0 {
		ttl = DefaultLinkCodeTTL
	}
	return &CodeIssuer{queries: db.New(pool), ttl: ttl}
}

// IssueLinkCode генерирует новый одноразовый код привязки канала channelName
// для userID и сохраняет его в channel_link_codes со сроком действия
// time.Now().Add(c.ttl) (FR D3, тикет 9.6). Возвращает вставленную запись
// (в частности Code/ExpiresAt) — вызывающая сторона
// (PostChannelsTelegramLinkCode, orchestrator/internal/api) отдаёт их
// пользователю как есть.
//
// Бизнес: userID приходит от вызывающей стороны уже проверенным
// (authMiddleware разобрал access-токен, тикет 1.4) — здесь нет параметра
// "для кого выпустить код", поэтому подделать код на чужой аккаунт через этот
// путь невозможно (FR A2).
//
// Как устроено (тех): в отличие от Linker.Exchange транзакция не нужна —
// единственная операция это INSERT; коллизия PRIMARY KEY (crypto/rand выдал
// уже занятое значение code, см. godoc maxIssueLinkCodeAttempts) не
// инфраструктурная ошибка — генерируем новый код и повторяем, не откатывая
// ничего постороннего.
func (c *CodeIssuer) IssueLinkCode(ctx context.Context, userID pgtype.UUID, channelName string) (db.ChannelLinkCode, error) {
	var lastErr error
	for attempt := 0; attempt < maxIssueLinkCodeAttempts; attempt++ {
		code, err := generateLinkCode()
		if err != nil {
			return db.ChannelLinkCode{}, fmt.Errorf("channel: генерация кода привязки: %w", err)
		}

		created, err := c.queries.CreateChannelLinkCode(ctx, db.CreateChannelLinkCodeParams{
			Code:      code,
			UserID:    userID,
			Channel:   channelName,
			ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(c.ttl), Valid: true},
		})
		if err == nil {
			return created, nil
		}

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			// Коллизия PRIMARY KEY на code (см. godoc maxIssueLinkCodeAttempts) —
			// пробуем новый случайный код, не сдаёмся сразу.
			lastErr = err
			continue
		}
		return db.ChannelLinkCode{}, fmt.Errorf("channel: вставить код привязки: %w", err)
	}
	return db.ChannelLinkCode{}, fmt.Errorf("channel: не удалось подобрать уникальный код привязки за %d попыток: %w", maxIssueLinkCodeAttempts, lastErr)
}

// generateLinkCode генерирует случайный код из linkCodeAlphabet длиной
// linkCodeLength символов через crypto/rand (криптографически стойкий
// источник случайности — тот же принцип, что у auth.GenerateRefreshToken,
// internal/auth/refresh_token.go). Перевод случайного байта в индекс алфавита
// через остаток от деления (%32) равномерен без модульного смещения: длина
// алфавита (32) — делитель 256 (диапазона байта) без остатка.
func generateLinkCode() (string, error) {
	b := make([]byte, linkCodeLength)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	out := make([]byte, linkCodeLength)
	for i, v := range b {
		out[i] = linkCodeAlphabet[int(v)%len(linkCodeAlphabet)]
	}
	return string(out), nil
}
