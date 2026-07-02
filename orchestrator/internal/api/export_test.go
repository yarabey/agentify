package api

// export_test.go — тест-онли мост к неэкспортируемым крипто-деталям пакета для
// внешних интеграционных тестов (package api_test, тег integration): они
// сидят/проверяют tasks.text_enc и task_events.payload_enc в реальном Postgres
// и обязаны выводить те же подключи at-rest шифрования, что и рабочий Server
// (FR I1, тикет 11.1) — purpose-строки подключей неэкспортируемы, поэтому
// пробрасываются сюда, а не дублируются в тестах.

import (
	"github.com/yarabey/agentify/internal/crypto"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// EncryptTaskTextForTest шифрует text подключом tasks.text_enc, выведенным из
// masterKey ровно так же, как это делает NewServer (taskTextAEADKeyPurpose).
// Только для интеграционных тестов: позволяет засеять text_enc шифротекстом,
// который рабочий обработчик расшифрует обратно (FR I1, тикет 11.1).
func EncryptTaskTextForTest(masterKey []byte, text string) ([]byte, error) {
	return crypto.Encrypt(crypto.DeriveKey(masterKey, taskTextAEADKeyPurpose), []byte(text))
}

// DecryptTaskTextForTest расшифровывает enc подключом tasks.text_enc
// (taskTextAEADKeyPurpose), выведенным из masterKey так же, как NewServer.
// Только для интеграционных тестов, проверяющих at-rest шифрование text_enc
// (FR I1, тикет 11.1): raw-байты в БД должны отличаться от plaintext, но
// расшифровываться обратно.
func DecryptTaskTextForTest(masterKey, enc []byte) ([]byte, error) {
	return crypto.Decrypt(crypto.DeriveKey(masterKey, taskTextAEADKeyPurpose), enc)
}

// DecryptEventPayloadForTest расшифровывает enc подключом
// task_events.payload_enc (task.EventPayloadKeyPurpose), выведенным из
// masterKey так же, как это делают NewServer и task.Transitioner. Только для
// интеграционных тестов: позволяет прочитать payload_enc, записанный реальным
// Transitioner, минуя HTTP (FR I1, тикет 11.1).
func DecryptEventPayloadForTest(masterKey, enc []byte) ([]byte, error) {
	return crypto.Decrypt(crypto.DeriveKey(masterKey, task.EventPayloadKeyPurpose), enc)
}
