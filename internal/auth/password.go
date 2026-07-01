// Package auth — хэширование и проверка паролей пользователей agentify.
//
// Назначение (бизнес): доступ в систему закрытый (FR A1), а пароли учётных
// записей НИКОГДА не хранятся в открытом виде (FR I1 «не plaintext»). Этот пакет
// инкапсулирует выбранную схему хэширования паролей, чтобы и регистрация
// (тикет 1.2), и логин (тикет 1.3) пользовались одной и той же реализацией и
// одним и тем же кодированным форматом хэша в БД.
//
// Как устроено (тех): используется argon2id (memory-hard KDF, рекомендованная
// схема для паролей; стек — docs/01_tech_stack_and_architecture.md §3) из
// golang.org/x/crypto/argon2. Для каждого пароля генерируется случайная соль;
// результат кодируется в самоописывающую строку PHC-формата
// "$argon2id$v=19$m=<KiB>,t=<iter>,p=<par>$<b64-salt>$<b64-hash>", чтобы при
// проверке параметры брались из самого хэша (можно менять стоимость без миграции
// старых записей). Проверка сравнивает хэши в постоянном времени.
package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// argon2idParams — параметры стоимости argon2id, применяемые при хэшировании.
//
// Значения подобраны как разумный дефолт для интерактивного логина на сервере
// (примерно 64 MiB памяти, 3 прохода, параллелизм 2). Они сериализуются в строку
// хэша, поэтому проверка старых хэшей не зависит от текущих значений — менять
// стоимость можно без миграции БД.
type argon2idParams struct {
	memoryKiB   uint32 // объём памяти в KiB (memory-hardness)
	iterations  uint32 // число проходов (time cost)
	parallelism uint8  // степень параллелизма (lanes)
	saltLen     uint32 // длина соли в байтах
	keyLen      uint32 // длина выходного ключа (хэша) в байтах
}

// defaultParams — текущие параметры хэширования новых паролей.
var defaultParams = argon2idParams{
	memoryKiB:   64 * 1024,
	iterations:  3,
	parallelism: 2,
	saltLen:     16,
	keyLen:      32,
}

// ErrInvalidHash возвращается VerifyPassword, когда сохранённый хэш не разбирается
// (повреждён или сгенерирован несовместимой схемой) — это ошибка данных, а не
// просто несовпадение пароля, поэтому она отделена от обычного «пароль неверен».
var ErrInvalidHash = errors.New("auth: некорректный формат хэша пароля")

// HashPassword хэширует пароль argon2id со случайной солью и возвращает
// самоописывающую PHC-строку, пригодную для хранения в users.password_hash.
//
// Бизнес: пароль не должен попадать в БД в открытом виде (FR I1); вызывается при
// регистрации (тикет 1.2) и при будущей смене пароля. Каждый вызов даёт разный
// результат для одного пароля за счёт случайной соли. Возвращает ошибку только
// при сбое источника случайности.
func HashPassword(password string) (string, error) {
	p := defaultParams
	salt := make([]byte, p.saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("auth: генерация соли: %w", err)
	}

	hash := argon2.IDKey([]byte(password), salt, p.iterations, p.memoryKiB, p.parallelism, p.keyLen)

	b64 := base64.RawStdEncoding
	encoded := fmt.Sprintf(
		"$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, p.memoryKiB, p.iterations, p.parallelism,
		b64.EncodeToString(salt), b64.EncodeToString(hash),
	)
	return encoded, nil
}

// VerifyPassword проверяет, что password соответствует ранее сохранённому
// encodedHash, разбирая параметры argon2id прямо из строки хэша.
//
// Бизнес: путь логина (тикет 1.3) — подтверждение учётных данных без хранения
// plaintext (FR A3, I1). Возвращает (true, nil) при совпадении, (false, nil) при
// корректном, но несовпадающем пароле, и (false, ErrInvalidHash) если строка
// хэша повреждена/несовместима. Сравнение выполняется в постоянном времени, чтобы
// не давать тайминг-сигнала об угадывании.
func VerifyPassword(password, encodedHash string) (bool, error) {
	params, salt, hash, err := decodeHash(encodedHash)
	if err != nil {
		return false, err
	}

	computed := argon2.IDKey([]byte(password), salt, params.iterations, params.memoryKiB, params.parallelism, params.keyLen)
	if subtle.ConstantTimeCompare(hash, computed) == 1 {
		return true, nil
	}
	return false, nil
}

// decodeHash разбирает PHC-строку argon2id обратно в параметры, соль и хэш.
func decodeHash(encodedHash string) (argon2idParams, []byte, []byte, error) {
	parts := strings.Split(encodedHash, "$")
	// Ожидаемый вид: ["", "argon2id", "v=19", "m=..,t=..,p=..", "<salt>", "<hash>"].
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return argon2idParams{}, nil, nil, ErrInvalidHash
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return argon2idParams{}, nil, nil, ErrInvalidHash
	}
	if version != argon2.Version {
		return argon2idParams{}, nil, nil, ErrInvalidHash
	}

	var p argon2idParams
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &p.memoryKiB, &p.iterations, &p.parallelism); err != nil {
		return argon2idParams{}, nil, nil, ErrInvalidHash
	}

	b64 := base64.RawStdEncoding
	salt, err := b64.DecodeString(parts[4])
	if err != nil {
		return argon2idParams{}, nil, nil, ErrInvalidHash
	}
	hash, err := b64.DecodeString(parts[5])
	if err != nil {
		return argon2idParams{}, nil, nil, ErrInvalidHash
	}

	p.saltLen = uint32(len(salt))
	p.keyLen = uint32(len(hash))
	return p, salt, hash, nil
}
