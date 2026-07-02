// Package platform — общий каркас Go-сервисов agentify (оркестратор, бот, агент).
//
// Назначение (бизнес): три наших Go-сервиса (FR D1, E*, C*) должны единообразно
// стартовать и эксплуатироваться: читать конфиг из окружения (12-factor),
// писать структурные логи, отдавать /healthz для проверок живости (нужно
// docker compose / Caddy / CI, тикеты 0.3/0.4) и корректно гаснуть по SIGTERM,
// не теряя данные. Чтобы не дублировать этот код в трёх местах, он живёт здесь
// (тикет 0.6 «Базис трёх Go-сервисов»). Бизнес-логика (auth, WS, очереди,
// Telegram) сюда НЕ входит — это только общий операционный каркас. Тикет 11.4
// (ТЗ «эксплуатация») добавляет сюда же наблюдаемость: GET /metrics в
// Prometheus-формате (см. metrics.go) смонтирован безусловно на каждом из
// трёх сервисов — тем же приёмом, что и /healthz, дублировать по сервисам не
// нужно.
//
// Как устроено (тех): пакет даёт кирпичи и связку из них (Service):
//   - Config — базовый конфиг из env через caarlos0/env (LogLevel, LogFormat,
//     HealthAddr, Env) с разумными дефолтами.
//   - NewLogger — slog-логгер (JSON в prod, text в dev), уровень из конфига.
//   - Metrics — Prometheus-реестр сервиса с базовыми HTTP-метриками; Registry()
//     позволяет сервису добавить свои метрики (см. годок Metrics, metrics.go).
//   - Service.Run — поднимает chi-роутер с GET /healthz и GET /metrics и
//     блокируется до SIGTERM/SIGINT, после чего гасит HTTP-сервер с таймаутом.
//
// Каждый сервис встраивает Config в свою структуру конфига и добавляет
// специфичные поля под собственным env-префиксом.
package platform

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Environment — режим выполнения сервиса. Определяет формат логов по умолчанию
// (text в dev для читаемости, JSON в prod для машинного сбора) и может в
// будущем влиять на прочие операционные настройки.
type Environment string

const (
	// EnvDev — режим разработки: человекочитаемый text-лог по умолчанию.
	EnvDev Environment = "dev"
	// EnvProd — продакшен: структурный JSON-лог по умолчанию (наблюдаемость, FR-эксплуатация).
	EnvProd Environment = "prod"
)

// LogFormat — формат вывода slog: json (машинный сбор в prod) или text (dev).
type LogFormat string

const (
	// LogFormatJSON — структурный JSON-лог (slog.JSONHandler), формат по умолчанию для prod.
	LogFormatJSON LogFormat = "json"
	// LogFormatText — человекочитаемый text-лог (slog.TextHandler), удобен в dev.
	LogFormatText LogFormat = "text"
)

// Config — базовый операционный конфиг, общий для всех Go-сервисов agentify.
//
// Поля заполняются из переменных окружения (12-factor, FR-эксплуатация) через
// caarlos0/env. Каждый сервис встраивает этот тип в свой конфиг и добавляет
// специфичные поля под собственным env-префиксом; см. LoadConfig.
//
// Дефолты подобраны так, чтобы сервис стартовал «из коробки» в dev без единой
// переменной окружения: text-лог уровня info, /healthz на :8080, режим dev.
type Config struct {
	// Env — режим выполнения (dev|prod). Влияет на формат логов по умолчанию.
	// Переменная: <PREFIX>ENV. По умолчанию dev.
	Env Environment `env:"ENV" envDefault:"dev"`

	// LogLevel — минимальный уровень логирования (debug|info|warn|error).
	// Переменная: <PREFIX>LOG_LEVEL. По умолчанию info.
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`

	// LogFormat — формат лога (json|text). Пустое значение означает «по режиму
	// Env»: json в prod, text в dev. Переменная: <PREFIX>LOG_FORMAT.
	LogFormat LogFormat `env:"LOG_FORMAT"`

	// HealthAddr — адрес, на котором поднимается HTTP-сервер с GET /healthz
	// (нужен docker compose / Caddy / CI для проверки живости, тикеты 0.3/0.4).
	// Переменная: <PREFIX>HEALTH_ADDR. По умолчанию :8080.
	HealthAddr string `env:"HEALTH_ADDR" envDefault:":8080"`

	// ShutdownTimeout — крайний срок graceful-остановки HTTP-сервера после
	// получения SIGTERM/SIGINT. По истечении сервер гасится принудительно,
	// чтобы процесс не висел вечно. Переменная: <PREFIX>SHUTDOWN_TIMEOUT.
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"10s"`
}

// LoadConfig загружает конфиг сервиса из переменных окружения в переданную
// структуру cfg (обычно встраивающую platform.Config) и валидирует базовые поля.
//
// Параметр prefix — общий префикс env-переменных сервиса (например "ORCH_",
// "BOT_", "AGENT_"); пустая строка означает «без префикса». Префикс позволяет
// трём сервисам сосуществовать в одном окружении (docker compose) без коллизий
// имён переменных. cfg должен быть указателем на структуру.
//
// Возвращает ошибку, если переменные не парсятся (например, неверный формат
// длительности) или значения базовых полей недопустимы.
func LoadConfig(cfg any, prefix string) error {
	if err := env.ParseWithOptions(cfg, env.Options{Prefix: prefix}); err != nil {
		return fmt.Errorf("разбор env-конфига (префикс %q): %w", prefix, err)
	}
	return nil
}

// Validate проверяет, что базовые поля конфига заполнены допустимыми значениями,
// и нормализует формат лога по режиму Env, если он не задан явно.
//
// Метод изменяет приёмник: при пустом LogFormat подставляется формат по
// умолчанию для текущего Env (json для prod, иначе text). Вызывается сервисами
// после LoadConfig; на нарушении возвращает ошибку с указанием поля.
func (c *Config) Validate() error {
	switch c.Env {
	case EnvDev, EnvProd:
	default:
		return fmt.Errorf("недопустимый ENV %q: ожидается dev|prod", c.Env)
	}

	if _, err := parseLogLevel(c.LogLevel); err != nil {
		return err
	}

	if c.LogFormat == "" {
		c.LogFormat = defaultLogFormat(c.Env)
	}
	switch c.LogFormat {
	case LogFormatJSON, LogFormatText:
	default:
		return fmt.Errorf("недопустимый LOG_FORMAT %q: ожидается json|text", c.LogFormat)
	}

	if c.HealthAddr == "" {
		return fmt.Errorf("пустой HEALTH_ADDR: укажите адрес для /healthz, например \":8080\"")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("недопустимый SHUTDOWN_TIMEOUT %s: ожидается положительная длительность", c.ShutdownTimeout)
	}
	return nil
}

// defaultLogFormat возвращает формат лога по режиму: JSON для prod, иначе text.
func defaultLogFormat(e Environment) LogFormat {
	if e == EnvProd {
		return LogFormatJSON
	}
	return LogFormatText
}

// parseLogLevel переводит строковый уровень (debug|info|warn|error) в slog.Level.
func parseLogLevel(level string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("недопустимый LOG_LEVEL %q: ожидается debug|info|warn|error", level)
	}
}
