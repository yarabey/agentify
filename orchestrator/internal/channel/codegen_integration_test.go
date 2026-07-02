//go:build integration

// Integration-тесты channel.CodeIssuer.IssueLinkCode на РЕАЛЬНОМ Postgres
// через testcontainers-go (тикет 9.6, FR A2, D3, экран «Настройки»,
// Gherkin §6) — тот же паттерн и переиспользованные хелперы
// (setupPool/seedUser), что и link_integration_test.go (тикет 10.2).
//
// Приёмка тикета 9.6: «код генерится и отображается» — здесь проверяется
// backend-часть: валидный вызов создаёт РОВНО одну строку
// channel_link_codes с корректным TTL, привязанную к вызвавшему userID
// (FR A2 — код выпускается только для себя); дополнительно — что выпущенный
// код действительно принимается Linker.Exchange (сквозная проверка контракта
// между 9.6 и 10.2).
package channel_test

import (
	"context"
	"testing"
	"time"

	"github.com/yarabey/agentify/orchestrator/internal/channel"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// ttlTolerance — допустимое отклонение при сравнении фактического expires_at
// с ожидаемым (time.Now() вызывается дважды — до и после запроса к БД,
// разница между ними и есть источник допустимого дребезга).
const ttlTolerance = 5 * time.Second

// assertWithinTTL проверяет, что got лежит в окне
// [wantAfter-tolerance, wantAfter+tolerance], где wantAfter — time.Now()+ttl,
// посчитанный ДО вызова IssueLinkCode.
func assertWithinTTL(t *testing.T, got time.Time, baseline time.Time, ttl time.Duration) {
	t.Helper()
	want := baseline.Add(ttl)
	diff := got.Sub(want)
	if diff < 0 {
		diff = -diff
	}
	if diff > ttlTolerance {
		t.Fatalf("expires_at = %v, ожидалось ~%v (baseline+ttl, допуск %v), разница %v", got, want, ttlTolerance, diff)
	}
}

// TestIntegration_IssueLinkCode_CreatesRowWithExpectedTTL — валидный вызов
// создаёт РОВНО одну строку channel_link_codes с ожидаемыми user_id/channel,
// ещё неиспользованную (used_at NULL) и с expires_at ~ now()+ttl (тикет 9.6,
// FR D3).
func TestIntegration_IssueLinkCode_CreatesRowWithExpectedTTL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupPool(ctx, t)
	defer done()
	q := db.New(pool)

	user := seedUser(ctx, t, q, "dave")

	const ttl = 20 * time.Minute
	issuer := channel.NewCodeIssuer(pool, ttl)

	baseline := time.Now()
	created, err := issuer.IssueLinkCode(ctx, user.ID, "telegram")
	if err != nil {
		t.Fatalf("IssueLinkCode: %v, ожидался успех", err)
	}

	if created.Code == "" {
		t.Fatal("created.Code пуст")
	}
	if created.UserID != user.ID {
		t.Fatalf("created.UserID = %v, ожидался %v", created.UserID, user.ID)
	}
	if created.Channel != "telegram" {
		t.Fatalf("created.Channel = %q, ожидался %q", created.Channel, "telegram")
	}
	if created.UsedAt.Valid {
		t.Fatal("свежевыпущенный код уже помечен использованным (used_at)")
	}
	if !created.ExpiresAt.Valid {
		t.Fatal("expires_at не проставлен")
	}
	assertWithinTTL(t, created.ExpiresAt.Time, baseline, ttl)

	// Ровно одна строка в БД на этот код — не дубли.
	stored, err := q.GetChannelLinkCode(ctx, created.Code)
	if err != nil {
		t.Fatalf("GetChannelLinkCode: %v", err)
	}
	if stored.UserID != user.ID || stored.Channel != "telegram" {
		t.Fatalf("stored = %+v, ожидались user_id=%v channel=telegram", stored, user.ID)
	}
	t.Logf("OK: код %s выпущен для %s, TTL ~%v", created.Code, user.ID, ttl)
}

// TestIntegration_IssueLinkCode_DefaultTTLWhenNonPositive — при
// неположительном ttl (в частности 0 — конфиг не задан) IssueLinkCode
// использует channel.DefaultLinkCodeTTL, а не бессрочный/уже истёкший срок
// (см. godoc channel.NewCodeIssuer).
func TestIntegration_IssueLinkCode_DefaultTTLWhenNonPositive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupPool(ctx, t)
	defer done()
	q := db.New(pool)

	user := seedUser(ctx, t, q, "erin")

	issuer := channel.NewCodeIssuer(pool, 0)

	baseline := time.Now()
	created, err := issuer.IssueLinkCode(ctx, user.ID, "telegram")
	if err != nil {
		t.Fatalf("IssueLinkCode: %v, ожидался успех", err)
	}
	assertWithinTTL(t, created.ExpiresAt.Time, baseline, channel.DefaultLinkCodeTTL)
	t.Logf("OK: ttl=0 заменён на DefaultLinkCodeTTL (%v)", channel.DefaultLinkCodeTTL)
}

// TestIntegration_IssueLinkCode_TwoCallsProduceDifferentCodes — два вызова
// подряд для одного и того же пользователя выпускают РАЗНЫЕ коды (иначе
// второй вызов столкнулся бы с PRIMARY KEY на code, а не просто оказался бы
// небезопасным для повторной генерации при потере первого кода).
func TestIntegration_IssueLinkCode_TwoCallsProduceDifferentCodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupPool(ctx, t)
	defer done()
	q := db.New(pool)

	user := seedUser(ctx, t, q, "frank")
	issuer := channel.NewCodeIssuer(pool, time.Hour)

	first, err := issuer.IssueLinkCode(ctx, user.ID, "telegram")
	if err != nil {
		t.Fatalf("первый IssueLinkCode: %v", err)
	}
	second, err := issuer.IssueLinkCode(ctx, user.ID, "telegram")
	if err != nil {
		t.Fatalf("второй IssueLinkCode: %v", err)
	}
	if first.Code == second.Code {
		t.Fatalf("оба вызова выпустили одинаковый код %q", first.Code)
	}
}

// TestIntegration_IssueLinkCode_IssuedCodeIsAcceptedByLinker — сквозная
// проверка контракта между тикетами 9.6 и 10.2: код, выпущенный
// CodeIssuer.IssueLinkCode, успешно обменивается Linker.Exchange на привязку
// (тот же формат/семантика записи channel_link_codes с обеих сторон).
func TestIntegration_IssueLinkCode_IssuedCodeIsAcceptedByLinker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupPool(ctx, t)
	defer done()
	q := db.New(pool)

	user := seedUser(ctx, t, q, "grace")
	issuer := channel.NewCodeIssuer(pool, time.Hour)

	created, err := issuer.IssueLinkCode(ctx, user.ID, "telegram")
	if err != nil {
		t.Fatalf("IssueLinkCode: %v", err)
	}

	linker := channel.NewLinker(pool)
	link, err := linker.Exchange(ctx, "telegram", created.Code, "tg-issued-code")
	if err != nil {
		t.Fatalf("Exchange только что выпущенным кодом: %v, ожидался успех", err)
	}
	if link.UserID != user.ID {
		t.Fatalf("link.UserID = %v, ожидался %v", link.UserID, user.ID)
	}
}
