package crypto_test

import (
	"bytes"
	"testing"

	"github.com/yarabey/agentify/internal/crypto"
)

// testKey32 — детерминированный тестовый ключ ровно 32 байта. Не секрет —
// используется только в тестовом процессе (как testJWTSigningKey в api-тестах).
func testKey32() []byte {
	return bytes.Repeat([]byte{0x42}, 32)
}

func TestEncryptDecrypt_RoundTrip(t *testing.T) {
	key := testKey32()
	plaintext := []byte("super secret uuid value")

	ciphertext, err := crypto.Encrypt(key, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if bytes.Equal(ciphertext, plaintext) {
		t.Fatal("ciphertext совпал с plaintext — шифрования не произошло")
	}

	got, err := crypto.Decrypt(key, ciphertext)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("round-trip не совпал: got %q, want %q", got, plaintext)
	}
}

func TestEncrypt_DifferentNoncePerCall(t *testing.T) {
	key := testKey32()
	plaintext := []byte("same plaintext")

	c1, err := crypto.Encrypt(key, plaintext)
	if err != nil {
		t.Fatalf("Encrypt #1: %v", err)
	}
	c2, err := crypto.Encrypt(key, plaintext)
	if err != nil {
		t.Fatalf("Encrypt #2: %v", err)
	}
	if bytes.Equal(c1, c2) {
		t.Fatal("два вызова Encrypt с одним plaintext дали одинаковый ciphertext — nonce не случаен")
	}
}

func TestDecrypt_WrongKeyFails(t *testing.T) {
	key := testKey32()
	wrongKey := bytes.Repeat([]byte{0x24}, 32)
	plaintext := []byte("secret")

	ciphertext, err := crypto.Encrypt(key, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	if _, err := crypto.Decrypt(wrongKey, ciphertext); err == nil {
		t.Fatal("Decrypt с неверным ключом не вернул ошибку")
	}
}

func TestDecrypt_TamperedCiphertextFails(t *testing.T) {
	key := testKey32()
	plaintext := []byte("secret payload")

	ciphertext, err := crypto.Encrypt(key, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Портим последний байт (часть тега аутентификации GCM) — должно сломать
	// проверку аутентичности при Open.
	tampered := append([]byte(nil), ciphertext...)
	tampered[len(tampered)-1] ^= 0xFF

	if _, err := crypto.Decrypt(key, tampered); err == nil {
		t.Fatal("Decrypt повреждённого ciphertext не вернул ошибку (тег аутентификации не проверен)")
	}
}

func TestDecrypt_TooShortCiphertextFails(t *testing.T) {
	key := testKey32()

	if _, err := crypto.Decrypt(key, []byte("short")); err == nil {
		t.Fatal("Decrypt слишком короткого ciphertext не вернул ошибку")
	}
}

func TestEncrypt_InvalidKeyLength(t *testing.T) {
	plaintext := []byte("secret")

	cases := map[string][]byte{
		"пустой ключ":       {},
		"16 байт (AES-128)": bytes.Repeat([]byte{0x01}, 16),
		"31 байт":           bytes.Repeat([]byte{0x01}, 31),
		"33 байта":          bytes.Repeat([]byte{0x01}, 33),
	}
	for name, key := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := crypto.Encrypt(key, plaintext); err == nil {
				t.Fatalf("Encrypt с ключом длины %d не вернул ошибку", len(key))
			}
		})
	}
}

func TestHMACSHA256_Deterministic(t *testing.T) {
	key := []byte("hmac-key")
	data := []byte("some data to authenticate")

	h1 := crypto.HMACSHA256(key, data)
	h2 := crypto.HMACSHA256(key, data)
	if !bytes.Equal(h1, h2) {
		t.Fatal("HMACSHA256 не детерминирован для одинаковых key+data")
	}
	if len(h1) != 32 {
		t.Fatalf("длина HMACSHA256 = %d, ожидалось 32", len(h1))
	}
}

func TestHMACSHA256_SensitiveToData(t *testing.T) {
	key := []byte("hmac-key")

	h1 := crypto.HMACSHA256(key, []byte("data one"))
	h2 := crypto.HMACSHA256(key, []byte("data two"))
	if bytes.Equal(h1, h2) {
		t.Fatal("HMACSHA256 дал одинаковый результат для разных данных")
	}
}

func TestHMACSHA256_SensitiveToKey(t *testing.T) {
	data := []byte("same data")

	h1 := crypto.HMACSHA256([]byte("key-one"), data)
	h2 := crypto.HMACSHA256([]byte("key-two"), data)
	if bytes.Equal(h1, h2) {
		t.Fatal("HMACSHA256 дал одинаковый результат для разных ключей")
	}
}

func TestDeriveKey_DifferentPurposesGiveDifferentKeys(t *testing.T) {
	master := testKey32()

	k1 := crypto.DeriveKey(master, "purpose-one")
	k2 := crypto.DeriveKey(master, "purpose-two")
	if bytes.Equal(k1, k2) {
		t.Fatal("DeriveKey дал одинаковый подключ для разных purpose")
	}
	if len(k1) != 32 || len(k2) != 32 {
		t.Fatalf("длина подключей = %d/%d, ожидалось 32/32", len(k1), len(k2))
	}
}

func TestDeriveKey_Deterministic(t *testing.T) {
	master := testKey32()

	k1 := crypto.DeriveKey(master, "same-purpose")
	k2 := crypto.DeriveKey(master, "same-purpose")
	if !bytes.Equal(k1, k2) {
		t.Fatal("DeriveKey не детерминирован для одного masterKey+purpose")
	}
}

func TestDeriveKey_UsableForAEAD(t *testing.T) {
	master := testKey32()
	subkey := crypto.DeriveKey(master, "aead-purpose")

	ciphertext, err := crypto.Encrypt(subkey, []byte("payload"))
	if err != nil {
		t.Fatalf("Encrypt с производным ключом: %v", err)
	}
	if _, err := crypto.Decrypt(subkey, ciphertext); err != nil {
		t.Fatalf("Decrypt с производным ключом: %v", err)
	}
}
