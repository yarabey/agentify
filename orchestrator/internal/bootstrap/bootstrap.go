// Package bootstrap — идемпотентное создание первого администратора и
// стартового токена регистрации (тикет 1.7).
//
// Назначение (бизнес): доступ в систему закрытый (FR A1) — нужен способ завести
// самый первый аккаунт (которому больше неоткуда взяться: обычная регистрация
// сама требует активного токена, см. orchestrator/internal/api PostAuthRegister)
// и сам этот токен, который затем виден администратору в веб-интерфейсе
// (FR A2, тикет 1.6). Этим занимается подкоманда `orchestrator bootstrap`
// (orchestrator/cmd_bootstrap.go); здесь — сама бизнес-логика, оторванная от
// CLI/конфига, чтобы её можно было гонять и в integration-тестах напрямую
// поверх реальной БД.
//
// Как устроено (тех): два независимых шага — администратор и токен, каждый
// идемпотентен сам по себе:
//   - администратор: ищем пользователя с username == cfg.AdminUsername; нет —
//     создаём с argon2id-хэшем пароля и is_admin=true; есть, но не admin —
//     доводим до admin (SetUserAdmin); есть и уже admin — no-op;
//   - токен: если уже есть активный токен регистрации (неважно, с каким
//     значением — ротация вне MVP, FR A2) — no-op; иначе создаём с
//     cfg.InitialRegistrationToken.
//
// Гонка с другим конкурентным bootstrap (или ручной вставкой) ловится по
// unique violation (SQLSTATE 23505) на INSERT и трактуется как «уже
// забутстрапено», а не как ошибка — Run в этом случае не падает.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/yarabey/agentify/internal/auth"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// pgUniqueViolation — код ошибки PostgreSQL «нарушение уникального
// ограничения» (SQLSTATE 23505).
//
// Дублирует одноимённую константу orchestrator/internal/api/server.go:
// bootstrap сознательно не зависит от пакета api (его CLI-обвязка живёт в
// orchestrator/main, а не в api), а сама константа — деталь pgx в одну
// строку, не стоящая отдельного общего пакета ради двух потребителей.
const pgUniqueViolation = "23505"

// Querier — часть db.Queries, нужная bootstrap'у: поиск/создание
// администратора и поиск/создание активного токена регистрации.
//
// Реализуется *db.Queries (sqlc) поверх pgxpool; сужение интерфейса позволяет
// integration-тестам пакета подставлять реальный *db.Queries без зависимости
// от прочих sqlc-методов.
type Querier interface {
	// GetUserByUsername ищет аккаунт по username — проверка идемпотентности
	// «администратор с этим username уже есть».
	GetUserByUsername(ctx context.Context, username string) (db.User, error)
	// CreateUser создаёт аккаунт с argon2id-хэшем пароля (FR A1, I1).
	CreateUser(ctx context.Context, arg db.CreateUserParams) (db.User, error)
	// SetUserAdmin помечает существующий аккаунт администратором (FR A2).
	SetUserAdmin(ctx context.Context, arg db.SetUserAdminParams) (db.User, error)
	// GetActiveRegistrationToken возвращает действующий токен регистрации,
	// если он есть — проверка идемпотентности «токен уже есть».
	GetActiveRegistrationToken(ctx context.Context) (db.RegistrationToken, error)
	// CreateRegistrationToken создаёт активный токен регистрации (FR A2).
	CreateRegistrationToken(ctx context.Context, token string) (db.RegistrationToken, error)
}

// Config — параметры bootstrap'а: креды первого администратора и значение
// стартового токена регистрации.
//
// Все три поля обязательны — пустые недопустимы (это секреты из окружения,
// см. orchestrator/cmd_bootstrap.go и docs/MANUAL_STEPS.md); Run возвращает
// ошибку, если что-то пусто, не пытаясь молча подставить дефолт.
type Config struct {
	// AdminUsername — username первого администратора.
	AdminUsername string
	// AdminPassword — пароль первого администратора в открытом виде; Run
	// хэширует его argon2id (internal/auth) перед сохранением (FR I1) — в БД
	// plaintext не попадает.
	AdminPassword string
	// InitialRegistrationToken — значение стартового токена регистрации
	// (FR A2). Сохраняется как есть (секрет в таблице registration_tokens
	// и так хранится в plaintext по дизайну — утечка некритична, см.
	// orchestrator/migrations/00001_users_and_auth.sql).
	InitialRegistrationToken string
}

// Run идемпотентно создаёт первого администратора и активный токен
// регистрации (тикет 1.7, FR A2).
//
// Бизнес: после первого успешного запуска повторные вызовы Run с теми же
// cfg — no-op (с информационным логом «уже забутстрапено»), а не ошибка: ни
// второго администратора, ни нарушения уникальности повторный
// деплой/перезапуск bootstrap-команды вызвать не должен. Запуск с ДРУГИМ
// AdminUsername не считается «повторным bootstrap» в смысле этого тикета —
// это завело бы отдельного второго пользователя; такой сценарий вне границ
// задачи (ротация/смена администратора — post-MVP, см.
// docs/ТЗ_Оркестратор_бизнес-версия.md, FR A2).
func Run(ctx context.Context, q Querier, logger *slog.Logger, cfg Config) error {
	if cfg.AdminUsername == "" || cfg.AdminPassword == "" {
		return errors.New("bootstrap: AdminUsername/AdminPassword обязательны")
	}
	if cfg.InitialRegistrationToken == "" {
		return errors.New("bootstrap: InitialRegistrationToken обязателен")
	}

	if err := ensureAdmin(ctx, q, logger, cfg); err != nil {
		return err
	}
	return ensureRegistrationToken(ctx, q, logger, cfg)
}

// ensureAdmin создаёт первого администратора либо доводит существующего
// пользователя с тем же username до администратора; уже-администратора не
// трогает (идемпотентность, см. godoc Run).
func ensureAdmin(ctx context.Context, q Querier, logger *slog.Logger, cfg Config) error {
	existing, err := q.GetUserByUsername(ctx, cfg.AdminUsername)
	switch {
	case err == nil:
		if existing.IsAdmin {
			logInfo(logger, "администратор уже существует — bootstrap идемпотентен, пропускаю создание",
				slog.String("username", cfg.AdminUsername))
			return nil
		}
		if _, serr := q.SetUserAdmin(ctx, db.SetUserAdminParams{ID: existing.ID, IsAdmin: true}); serr != nil {
			return fmt.Errorf("bootstrap: SetUserAdmin: %w", serr)
		}
		logInfo(logger, "существующий пользователь повышен до администратора",
			slog.String("username", cfg.AdminUsername))
		return nil
	case errors.Is(err, pgx.ErrNoRows):
		// Пользователя ещё нет — создаём ниже.
	default:
		return fmt.Errorf("bootstrap: GetUserByUsername: %w", err)
	}

	hash, err := auth.HashPassword(cfg.AdminPassword)
	if err != nil {
		return fmt.Errorf("bootstrap: HashPassword: %w", err)
	}
	if _, err := q.CreateUser(ctx, db.CreateUserParams{
		Username:     cfg.AdminUsername,
		PasswordHash: hash,
		IsAdmin:      true,
	}); err != nil {
		if isUniqueViolation(err) {
			// Гонка: кто-то создал пользователя между GetUserByUsername и
			// CreateUser (параллельный bootstrap или обычная регистрация под
			// тем же username) — это не ошибка идемпотентности, нас просто
			// опередили; повторный запуск Run доведёт аккаунт до администратора.
			logInfo(logger, "пользователь с этим username уже создан параллельно — bootstrap идемпотентен, пропускаю",
				slog.String("username", cfg.AdminUsername))
			return nil
		}
		return fmt.Errorf("bootstrap: CreateUser: %w", err)
	}
	logInfo(logger, "первый администратор создан", slog.String("username", cfg.AdminUsername))
	return nil
}

// ensureRegistrationToken создаёт стартовый токен регистрации, если активного
// токена ещё нет; если есть — no-op (ротация вне MVP, FR A2; см. godoc Run).
func ensureRegistrationToken(ctx context.Context, q Querier, logger *slog.Logger, cfg Config) error {
	_, err := q.GetActiveRegistrationToken(ctx)
	switch {
	case err == nil:
		logInfo(logger, "активный токен регистрации уже существует — bootstrap идемпотентен, пропускаю создание")
		return nil
	case errors.Is(err, pgx.ErrNoRows):
		// Активного токена ещё нет — создаём ниже.
	default:
		return fmt.Errorf("bootstrap: GetActiveRegistrationToken: %w", err)
	}

	if _, err := q.CreateRegistrationToken(ctx, cfg.InitialRegistrationToken); err != nil {
		if isUniqueViolation(err) {
			// Гонка с параллельным bootstrap — аналогично ensureAdmin.
			logInfo(logger, "токен регистрации уже создан параллельно — bootstrap идемпотентен, пропускаю")
			return nil
		}
		return fmt.Errorf("bootstrap: CreateRegistrationToken: %w", err)
	}
	logInfo(logger, "стартовый токен регистрации создан")
	return nil
}

// isUniqueViolation сообщает, является ли err нарушением уникального
// ограничения PostgreSQL (SQLSTATE 23505, см. pgUniqueViolation).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}

// logInfo пишет info-сообщение через logger, если он задан (nil допустим —
// например, в юнит-тестах без логирования, по аналогии с api.Server.logError).
func logInfo(logger *slog.Logger, msg string, args ...any) {
	if logger != nil {
		logger.Info(msg, args...)
	}
}
