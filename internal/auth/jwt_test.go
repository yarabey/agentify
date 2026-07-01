// Unit-тесты выпуска/проверки access-JWT (тикет 1.3, FR A3).
//
// Покрывают приёмочный сценарий «истёкший access-токен → 401» на уровне
// пакета auth (полноценный HTTP-middleware — тикет 1.4): валидный токен
// проходит проверку и отдаёт исходный userID, истёкший и токен с неверной
// подписью/ключом — отвергаются единой ErrInvalidAccessToken.
package auth

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestIssueAndParseAccessTokenRoundtrip(t *testing.T) {
	key := []byte("test-signing-key")
	userID := uuid.New()
	now := time.Now()

	token, err := IssueAccessToken(userID.String(), key, now)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	got, err := ParseAccessToken(token, key)
	if err != nil {
		t.Fatalf("ParseAccessToken(валидный токен): %v", err)
	}
	if got != userID {
		t.Fatalf("ParseAccessToken вернул %s, ожидался %s", got, userID)
	}
}

// TestParseAccessTokenRejectsExpired — приёмочный сценарий «истёкший
// access-токен → 401»: токен, выпущенный с now в прошлом (за пределами
// AccessTokenTTL), отвергается.
func TestParseAccessTokenRejectsExpired(t *testing.T) {
	key := []byte("test-signing-key")
	userID := uuid.New()
	issuedAt := time.Now().Add(-2 * AccessTokenTTL) // далеко в прошлом — точно истёк

	token, err := IssueAccessToken(userID.String(), key, issuedAt)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	_, err = ParseAccessToken(token, key)
	if err == nil {
		t.Fatal("ParseAccessToken принял истёкший токен")
	}
	if err != ErrInvalidAccessToken {
		t.Fatalf("ParseAccessToken вернул %v, ожидался ErrInvalidAccessToken", err)
	}
}

// TestParseAccessTokenRejectsWrongKey — токен, подписанный другим ключом
// (например, ключ сменился), не проходит проверку.
func TestParseAccessTokenRejectsWrongKey(t *testing.T) {
	userID := uuid.New()
	token, err := IssueAccessToken(userID.String(), []byte("key-one"), time.Now())
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	if _, err := ParseAccessToken(token, []byte("key-two")); err != ErrInvalidAccessToken {
		t.Fatalf("ParseAccessToken с неверным ключом вернул %v, ожидался ErrInvalidAccessToken", err)
	}
}

// TestParseAccessTokenRejectsGarbage — произвольная не-JWT строка отвергается,
// без паники.
func TestParseAccessTokenRejectsGarbage(t *testing.T) {
	if _, err := ParseAccessToken("not-a-jwt-at-all", []byte("key")); err != ErrInvalidAccessToken {
		t.Fatalf("ParseAccessToken на мусоре вернул %v, ожидался ErrInvalidAccessToken", err)
	}
}

// TestParseAccessTokenRejectsUnexpectedAlgorithm — токен с алгоритмом "none"
// (классическая атака на JWT-библиотеки без проверки alg) отвергается.
func TestParseAccessTokenRejectsUnexpectedAlgorithm(t *testing.T) {
	// Собираем JWT с alg=none вручную: header.payload. без подписи.
	const noneAlgToken = "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0." +
		"eyJzdWIiOiJhYWFhYWFhYS1hYWFhLWFhYWEtYWFhYS1hYWFhYWFhYWFhYWEifQ."

	if _, err := ParseAccessToken(noneAlgToken, []byte("key")); err != ErrInvalidAccessToken {
		t.Fatalf("ParseAccessToken на alg=none вернул %v, ожидался ErrInvalidAccessToken", err)
	}
}
