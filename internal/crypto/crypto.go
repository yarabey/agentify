// Package crypto — общий крипто-модуль приложения: симметричное шифрование
// и HMAC поверх одного мастер-ключа.
//
// Назначение (бизнес): несколько таблиц БД хранят данные, которые не должны
// лежать в открытом виде, но должны быть либо предъявлены владельцу обратно
// (UUID-секрет интеграции, FR B2, тикет 2.2), либо найдены по детерминированному
// отпечатку без расшифровки (поиск интеграции по UUID при аутентификации
// машины, FR B6, тикет 2.3), либо позже — зашифрованы at-rest без необходимости
// показа (текст задачи/payload событий, FR I1, тикет 11.1). Этот пакет —
// единственное место в монорепо, где это делается, чтобы все таблицы
// использовали одну проверенную реализацию, а не россыпь самопальных
// шифрований по сервисам.
//
// Как устроено (тех): AEAD-шифрование — AES-256-GCM (crypto/aes +
// crypto/cipher), отпечаток — HMAC-SHA256 (crypto/hmac + crypto/sha256).
// Ключи НИКОГДА не генерируются и не дефолтятся внутри пакета — вызывающая
// сторона передаёт их как []byte (в проде это APP_ENCRYPTION_KEY из
// окружения, см. docs/MANUAL_STEPS.md, и его производные — см. DeriveKey).
// API намеренно не завязан на конкретное применение (не «integration»/«uuid» в
// сигнатурах) — это общий примитив, переиспользуемый разными таблицами.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
)

// aeadKeyLen — требуемая длина ключа для Encrypt/Decrypt: AES-256 (32 байта).
const aeadKeyLen = 32

// ErrInvalidKeyLength возвращается Encrypt, когда переданный ключ не равен
// ровно 32 байтам (AES-256). Понятная ошибка вместо паники внутри
// crypto/aes.NewCipher.
var ErrInvalidKeyLength = errors.New("crypto: ключ должен быть длиной ровно 32 байта (AES-256)")

// ErrCiphertextTooShort возвращается Decrypt, когда переданный ciphertext
// короче nonce и не может быть результатом Encrypt этого пакета.
var ErrCiphertextTooShort = errors.New("crypto: ciphertext короче nonce, повреждён или не из этого пакета")

// Encrypt шифрует plaintext алгоритмом AES-256-GCM под ключом key (ровно 32
// байта) и возвращает самодостаточный результат вида nonce || ciphertext+tag:
// для расшифровки достаточно того же ключа и этого среза целиком (см. Decrypt).
//
// Случайный nonce (crypto/rand, длина gcm.NonceSize()) генерируется заново на
// каждый вызов — переиспользование nonce с тем же ключом недопустимо для GCM
// (теряется аутентичность), поэтому Encrypt никогда не принимает nonce
// снаружи.
func Encrypt(key, plaintext []byte) ([]byte, error) {
	if len(key) != aeadKeyLen {
		return nil, ErrInvalidKeyLength
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: создание AES-блока: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: создание GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("crypto: генерация nonce: %w", err)
	}

	// Seal дописывает ciphertext+tag к переданному dst (тут — сам nonce), так
	// результат уже в формате nonce || ciphertext+tag.
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt — обратная операция к Encrypt: разбирает nonce || ciphertext+tag,
// проверяет ключом key (ровно 32 байта) аутентичность и возвращает исходный
// plaintext.
//
// Возвращает ErrCiphertextTooShort, если ciphertext короче nonce (заведомо не
// результат Encrypt). Неверный ключ или повреждённый ciphertext/tag дают
// ошибку самого gcm.Open — отдельно не различаются (тот же принцип единой
// ошибки, что и auth.ParseAccessToken: вызывающей стороне не нужно знать
// причину сбоя аутентификации).
func Decrypt(key, ciphertext []byte) ([]byte, error) {
	if len(key) != aeadKeyLen {
		return nil, ErrInvalidKeyLength
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: создание AES-блока: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: создание GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, ErrCiphertextTooShort
	}
	nonce, sealed := ciphertext[:nonceSize], ciphertext[nonceSize:]

	plaintext, err := gcm.Open(nil, nonce, sealed, nil)
	if err != nil {
		return nil, fmt.Errorf("crypto: расшифровка: %w", err)
	}
	return plaintext, nil
}

// HMACSHA256 считает HMAC-SHA256 от data под ключом key и возвращает сырые 32
// байта отпечатка.
//
// В отличие от Encrypt/Decrypt, длина key не ограничена — HMAC по конструкции
// допускает ключ произвольной длины. Используется там, где нужен
// детерминированный, но не обратимый отпечаток секрета для поиска в БД без
// хранения/расшифровки самого секрета (например — поиск интеграции по UUID
// машины при аутентификации, тикет 2.3, без SELECT по расшифрованным
// значениям).
func HMACSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

// DeriveKey детерминированно выводит независимый 32-байтный подключ из
// masterKey под конкретное назначение purpose (HKDF-Expand в одну итерацию
// через HMAC: DeriveKey(masterKey, purpose) = HMACSHA256(masterKey, purpose)).
//
// masterKey может быть произвольной длины (в проде — 32-байтный декодированный
// APP_ENCRYPTION_KEY, но функция этого не предполагает и не проверяет).
// Назначение: один и тот же мастер-ключ не должен использоваться напрямую
// сразу в Encrypt и в HMACSHA256 — это reuse одного ключа в двух разных
// крипто-примитивах, плохая крипто-гигиена. Вызывающая сторона выводит под
// каждое применение свой подключ через разные строки purpose (например,
// "integration-uuid-aead" и "integration-uuid-hmac") — разные purpose всегда
// дают разные, статистически независимые подключи из одного masterKey.
func DeriveKey(masterKey []byte, purpose string) []byte {
	return HMACSHA256(masterKey, []byte(purpose))
}
