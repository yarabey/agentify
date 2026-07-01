package migrate

import (
	"fmt"
	"log/slog"
)

// gooseLogger адаптирует slog к интерфейсу goose.Logger, чтобы прогресс
// миграций (какая версия применена) попадал в общий структурный лог сервиса, а
// не в отдельный stdout. goose дёргает Printf на каждый шаг и Fatalf на
// фатальной ошибке прогона.
type gooseLogger struct {
	logger *slog.Logger
}

// newGooseLogger оборачивает slog-логгер в адаптер goose.Logger.
func newGooseLogger(logger *slog.Logger) *gooseLogger {
	return &gooseLogger{logger: logger}
}

// Printf пишет информационное сообщение goose (прогресс миграций) на уровне info.
func (l *gooseLogger) Printf(format string, v ...interface{}) {
	l.logger.Info("goose: " + trimNewline(fmt.Sprintf(format, v...)))
}

// Fatalf пишет фатальное сообщение goose на уровне error. Намеренно НЕ вызывает
// os.Exit (в отличие от дефолтного goose-логгера): остановкой процесса
// управляет вызывающий код через возвращённую из Apply ошибку.
func (l *gooseLogger) Fatalf(format string, v ...interface{}) {
	l.logger.Error("goose: " + trimNewline(fmt.Sprintf(format, v...)))
}

// trimNewline убирает один завершающий перевод строки, который goose добавляет к
// своим сообщениям, чтобы slog-строки не содержали висящего \n.
func trimNewline(s string) string {
	if n := len(s); n > 0 && s[n-1] == '\n' {
		return s[:n-1]
	}
	return s
}
