// Unit-тесты схемы паролей argon2id (тикет 1.2, приёмка FR A1/I1).
//
// Проверяют ключевой инвариант «пароль не хранится в plaintext и проверяем его
// корректно»: верный пароль подтверждается, неверный — отвергается, хэш не равен
// исходному паролю и при повторном хэшировании отличается (случайная соль).
package auth

import (
	"strings"
	"testing"
)

// TestHashAndVerifyRoundtrip — верный пароль проходит проверку, неверный нет.
func TestHashAndVerifyRoundtrip(t *testing.T) {
	const password = "correct horse battery staple"

	hash, err := HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	// FR I1: хэш не должен быть равен исходному паролю (не plaintext).
	if strings.Contains(hash, password) {
		t.Fatal("хэш содержит исходный пароль в открытом виде (нарушение FR I1)")
	}
	if !strings.HasPrefix(hash, "$argon2id$") {
		t.Fatalf("хэш не в argon2id PHC-формате: %q", hash)
	}

	ok, err := VerifyPassword(password, hash)
	if err != nil {
		t.Fatalf("VerifyPassword(верный): %v", err)
	}
	if !ok {
		t.Fatal("VerifyPassword отверг верный пароль")
	}

	ok, err = VerifyPassword("wrong password", hash)
	if err != nil {
		t.Fatalf("VerifyPassword(неверный): %v", err)
	}
	if ok {
		t.Fatal("VerifyPassword принял неверный пароль")
	}
}

// TestHashIsSaltedAndUnique — два хэша одного пароля различаются (случайная соль).
func TestHashIsSaltedAndUnique(t *testing.T) {
	const password = "same-password"

	h1, err := HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword #1: %v", err)
	}
	h2, err := HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword #2: %v", err)
	}
	if h1 == h2 {
		t.Fatal("два хэша одного пароля совпали — соль не случайна")
	}

	// Оба должны проверяться против одного пароля.
	for i, h := range []string{h1, h2} {
		ok, verr := VerifyPassword(password, h)
		if verr != nil || !ok {
			t.Fatalf("VerifyPassword для хэша #%d: ok=%v err=%v", i+1, ok, verr)
		}
	}
}

// TestVerifyRejectsCorruptHash — повреждённый хэш даёт ErrInvalidHash, а не панику
// и не «пароль верен».
func TestVerifyRejectsCorruptHash(t *testing.T) {
	cases := map[string]string{
		"пусто":            "",
		"не-argon":         "$bcrypt$v=19$m=1,t=1,p=1$AAAA$BBBB",
		"мало полей":       "$argon2id$v=19$m=65536,t=3,p=2$AAAA",
		"битый base64":     "$argon2id$v=19$m=65536,t=3,p=2$!!!!$BBBB",
		"неверная версия":  "$argon2id$v=99$m=65536,t=3,p=2$AAAA$BBBB",
	}
	for name, h := range cases {
		ok, err := VerifyPassword("whatever", h)
		if ok {
			t.Errorf("%s: VerifyPassword вернул ok=true на повреждённом хэше", name)
		}
		if err == nil {
			t.Errorf("%s: ожидалась ошибка ErrInvalidHash, получено nil", name)
		}
	}
}
