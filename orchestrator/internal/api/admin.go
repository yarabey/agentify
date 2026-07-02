package api

// admin.go — обработчик показа токена регистрации (тикет 1.6, FR A2).
//
// Назначение (бизнес): доступ в систему закрытый — новый аккаунт можно
// завести только по активному секретному токену регистрации (FR A1,
// PostAuthRegister, тикет 1.2). Кто-то должен видеть этот токен, чтобы
// поделиться им с новым пользователем — это администратор: GET
// /admin/registration-token отдаёт действующий токен в plaintext, но только
// предъявителю access-токена, чей аккаунт помечен is_admin (FR A2, Gherkin §1
// «Администратор видит токен регистрации»). Любому другому аутентифицированному
// пользователю — 403, без утечки самого токена.
//
// Как устроено (тех): маршрут защищён auth-middleware (тикет 1.4, нет
// `security: []` в openapi.yaml для этой операции) — к моменту вызова этого
// обработчика user_id уже проверен и лежит в контексте запроса
// (UserIDFromContext). Обработчик дополнительно сам проверяет наличие
// user_id (как и положено защищённому хендлеру — не полагаться молча на
// инвариант чужого пакета) и отвечает 401, если его почему-то нет, либо
// аккаунт из токена больше не существует в БД. Дальше — GetUserByID для
// проверки is_admin и, если он истинен, GetActiveRegistrationToken (тот же
// запрос, что использует PostAuthRegister, тикет 1.2, и bootstrap, тикет 1.7)
// за текущим значением токена.
import (
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// GetAdminRegistrationToken реализует GET /admin/registration-token — показ
// действующего токена регистрации администратору (FR A2).
//
// Бизнес: видеть токен может только пользователь с is_admin=true (Gherkin §1
// «Администратор видит токен регистрации»); остальным аутентифицированным
// пользователям — 403 (FR A2). Отсутствие активного токена регистрации в БД —
// инвариант-сбой (bootstrap, тикет 1.7, обязан был его создать), не штатный
// сценарий пользователя — отвечаем 500.
func (s *Server) GetAdminRegistrationToken(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Маршрут защищён auth-middleware (тикет 1.4) — user_id уже должен быть в
	// контексте. Перепроверяем сами на случай вызова обработчика в обход
	// штатной цепочки (напр. напрямую из теста) — отказываем тем же 401, что
	// и middleware при отсутствии/невалидности токена.
	userID, ok := UserIDFromContext(ctx)
	if !ok {
		writeUnauthorized(w)
		return
	}

	user, err := s.queries.GetUserByID(ctx, pgtype.UUID{Bytes: userID, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Токен валиден, но аккаунт, на который он выдан, больше не
			// существует (удалён) — трактуем как «не аутентифицирован», тем
			// же 401, что и остальные ошибки проверки токена доступа.
			writeUnauthorized(w)
			return
		}
		s.logError("GetUserByID", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	if !user.IsAdmin {
		writeError(w, http.StatusForbidden, "admin_required", "доступно только администратору")
		return
	}

	active, err := s.queries.GetActiveRegistrationToken(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Инвариант-сбой: активный токен регистрации обязан существовать
			// (bootstrap, тикет 1.7) — это не штатный пользовательский
			// сценарий, поэтому 500, а не 403/404.
			s.logError("GetActiveRegistrationToken", err)
			writeError(w, http.StatusInternalServerError, "internal", "активный токен регистрации не найден")
			return
		}
		s.logError("GetActiveRegistrationToken", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"token": active.Token})
}
