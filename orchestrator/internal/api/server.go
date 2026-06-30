// Package api — каркас HTTP-API оркестратора и реальные обработчики тикета 1.2.
//
// Назначение (бизнес): это первый эндпоинт-тикет, поэтому здесь закладывается
// каркас единого REST-API оркестратора (через который ходят web PWA и
// Telegram-бот, см. orchestrator/README.md), а реально реализуется ТОЛЬКО
// регистрация по токену — POST /auth/register (FR A1, Gherkin §1). Доступ в
// систему закрытый: аккаунт создаётся лишь при предъявлении активного секретного
// токена регистрации; без него — отказ (FR A1). Остальные операции контракта
// (логин/refresh/logout — 1.3, middleware — 1.4, интеграции/задачи — позже)
// пока отвечают 501 Not Implemented и будут реализованы в своих тикетах.
//
// Как устроено (тех): Server реализует сгенерированный из openapi.yaml
// api.ServerInterface. Чтобы не писать все операции сразу, Server встраивает
// сгенерированный api.Unimplemented (каждый его метод отдаёт 501) и переопределяет
// только готовые операции — GetHealthz и PostAuthRegister. NewRouter монтирует
// chi-роутер из сгенерированного HandlerFromMux (он же поднимает GET /healthz и
// все маршруты API от корня — Caddy роутит /api/* со стрипом префикса, поэтому
// пути монтируются от корня: /auth/register, /healthz). Слой данных — sqlc
// *db.Queries поверх pgxpool; бизнес-логика проверки токена и хэширования пароля
// живёт в обработчике, SQL — в db.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/yarabey/agentify/internal/auth"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// pgUniqueViolation — код ошибки PostgreSQL «нарушение уникального ограничения»
// (SQLSTATE 23505). Используется, чтобы отличить занятый username при вставке и
// вернуть 409, не делая лишнего предварительного SELECT.
const pgUniqueViolation = "23505"

// Querier — узкий интерфейс слоя данных, нужный обработчикам тикета 1.2.
//
// Сужает *db.Queries до фактически используемых регистрацией методов: так Server
// не зависит от всего сгенерированного API БД, а тесты могут при необходимости
// подменить слой данных. Реализуется *db.Queries (sqlc) поверх pgxpool.
type Querier interface {
	// GetActiveRegistrationToken возвращает действующий токен регистрации (FR A1).
	GetActiveRegistrationToken(ctx context.Context) (db.RegistrationToken, error)
	// CreateUser создаёт аккаунт с argon2id-хэшем пароля (FR A1, I1).
	CreateUser(ctx context.Context, arg db.CreateUserParams) (db.User, error)
}

// Server — реализация сгенерированного api.ServerInterface для оркестратора.
//
// Встраивает api.Unimplemented (501 для ещё не реализованных операций) и
// переопределяет готовые. Хранит слой данных (queries) и логгер. Создаётся через
// NewServer; HTTP-роутер собирается через NewRouter.
type Server struct {
	Unimplemented

	queries Querier
	logger  *slog.Logger
}

// NewServer собирает обработчик API оркестратора поверх слоя данных и логгера.
//
// queries — sqlc-запросы (обычно db.New(pool)); logger — логгер сервиса (nil
// допустим, тогда серверные ошибки не логируются). Возвращает *Server, готовый к
// монтированию через NewRouter.
func NewServer(queries Querier, logger *slog.Logger) *Server {
	return &Server{queries: queries, logger: logger}
}

// NewRouter монтирует chi-роутер оркестратора: общие middleware (recover,
// request-id) и все маршруты из сгенерированного контракта поверх переданного
// Server.
//
// Маршруты монтируются от корня (Caddy стрипает префикс /api/*), включая
// GET /healthz и POST /auth/register. Возвращаемый chi.Router передаётся в
// platform.Service.SetHandler, сохраняя единый graceful shutdown.
func NewRouter(s *Server) chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	return HandlerFromMux(s, r).(chi.Router)
}

// GetHealthz отвечает 200 на liveness-проверку.
//
// Переопределяет 501-заглушку Unimplemented: /healthz сгенерирован контрактом и
// нужен docker compose / Caddy / smoke-тесту (тикет 0.3). Тело совпадает по духу
// с health-роутером platform: {"status":"ok","service":"orchestrator"}.
func (s *Server) GetHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "orchestrator"})
}

// PostAuthRegister реализует POST /auth/register — регистрацию по токену (FR A1).
//
// Бизнес: доступ в систему закрытый — аккаунт создаётся ТОЛЬКО при предъявлении
// действующего секретного токена регистрации, чтобы система оставалась приватной
// (FR A1, Gherkin §1 «Регистрация без токена запрещена»). Логика:
//   - валидируем тело (username/password/registration_token непусты);
//   - берём активный токен; если его нет ИЛИ предъявленный не совпал — 403;
//   - argon2id-хэшируем пароль (не plaintext, FR I1);
//   - вставляем пользователя; занятый username (unique violation) — 409;
//   - успех — 201 (Gherkin §1 «Успешная регистрация по валидному токену»).
func (s *Server) PostAuthRegister(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "тело запроса не является валидным JSON")
		return
	}
	if msg, ok := validateRegister(req); !ok {
		writeError(w, http.StatusBadRequest, "validation_error", msg)
		return
	}

	// Доступ закрытый: сверяем предъявленный токен с действующим активным (FR A1).
	// Любая причина «нет совпадения» (нет активного токена / не совпал) — 403, без
	// раскрытия, какая именно: это секрет, а не учётные данные.
	active, err := s.queries.GetActiveRegistrationToken(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusForbidden, "registration_forbidden", "регистрация по токену недоступна")
			return
		}
		s.logError("GetActiveRegistrationToken", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}
	// Сравнение в постоянном времени не требуется: токен не выводится наружу и
	// проверяется на точное равенство; тайминг-канал тут не даёт полезного сигнала.
	if req.RegistrationToken != active.Token {
		writeError(w, http.StatusForbidden, "registration_forbidden", "неверный токен регистрации")
		return
	}

	hash, err := auth.HashPassword(req.Password)
	if err != nil {
		s.logError("HashPassword", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	user, err := s.queries.CreateUser(ctx, db.CreateUserParams{
		Username:     req.Username,
		PasswordHash: hash,
		IsAdmin:      false, // обычный пользователь; админ назначается отдельно (FR A2, тикеты 1.6/1.7).
	})
	if err != nil {
		// Занятый username ловим по коду unique violation (23505) — не делаем
		// предварительный SELECT (TOCTOU + лишний запрос).
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			writeError(w, http.StatusConflict, "username_taken", "имя пользователя занято")
			return
		}
		s.logError("CreateUser", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	// 201 без тела: контракт описывает только «Аккаунт создан». Логин — отдельным
	// вызовом POST /auth/login (тикет 1.3).
	w.Header().Set("X-User-Id", user.ID.String())
	w.WriteHeader(http.StatusCreated)
}

// validateRegister проверяет обязательные непустые поля тела регистрации.
//
// Возвращает (сообщение, false) при первом нарушении, иначе ("", true). Контракт
// делает все три поля обязательными; пустые/пробельные значения недопустимы.
func validateRegister(req RegisterRequest) (string, bool) {
	switch {
	case strings.TrimSpace(req.Username) == "":
		return "username обязателен", false
	case strings.TrimSpace(req.Password) == "":
		return "password обязателен", false
	case strings.TrimSpace(req.RegistrationToken) == "":
		// Пустой токен — это «регистрация без токена»: тоже отказ, но на уровне
		// валидации тела (400). Семантический отказ по неверному токену — 403 ниже.
		return "registration_token обязателен", false
	default:
		return "", true
	}
}

// logError пишет ошибку обработчика в лог сервиса (если логгер задан).
func (s *Server) logError(op string, err error) {
	if s.logger != nil {
		s.logger.Error("ошибка обработчика API", slog.String("op", op), slog.Any("error", err))
	}
}

// writeJSON сериализует v в тело ответа с указанным статусом и заголовком JSON.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError отправляет ошибку в формате схемы Error контракта (code+message).
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, Error{Code: code, Message: message})
}
