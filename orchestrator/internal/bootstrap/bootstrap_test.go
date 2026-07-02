// Unit-тесты бизнес-логики bootstrap'а (тикет 1.7, FR A2) на фейковом
// Querier — без реальной БД (integration-покрытие с настоящим Postgres и
// проверкой идемпотентности по факту — bootstrap_integration_test.go).
//
// Эти тесты фиксируют контракт каждой ветки Run по отдельности (создание с
// нуля, промоушен существующего пользователя, no-op при уже-администраторе,
// no-op при уже-активном токене, гонка на unique violation, валидация
// обязательных полей конфига) — быстрее и не требуют Docker.
package bootstrap_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/orchestrator/internal/bootstrap"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// fakeQuerier — учебная in-memory реализация bootstrap.Querier для unit-тестов.
type fakeQuerier struct {
	users  map[string]db.User
	nextID int

	activeToken    *db.RegistrationToken
	tokenIDCounter int

	// createUserErr/createTokenErr — если заданы, возвращаются вместо обычной
	// вставки (имитация гонки/сбоя БД).
	createUserErr  error
	createTokenErr error
}

func newFakeQuerier() *fakeQuerier {
	return &fakeQuerier{users: map[string]db.User{}}
}

func (f *fakeQuerier) GetUserByUsername(_ context.Context, username string) (db.User, error) {
	u, ok := f.users[username]
	if !ok {
		return db.User{}, pgx.ErrNoRows
	}
	return u, nil
}

func (f *fakeQuerier) CreateUser(_ context.Context, arg db.CreateUserParams) (db.User, error) {
	if f.createUserErr != nil {
		return db.User{}, f.createUserErr
	}
	if _, exists := f.users[arg.Username]; exists {
		return db.User{}, uniqueViolationErr()
	}
	f.nextID++
	u := db.User{
		ID:           testUUID(f.nextID),
		Username:     arg.Username,
		PasswordHash: arg.PasswordHash,
		IsAdmin:      arg.IsAdmin,
	}
	f.users[arg.Username] = u
	return u, nil
}

func (f *fakeQuerier) SetUserAdmin(_ context.Context, arg db.SetUserAdminParams) (db.User, error) {
	for username, u := range f.users {
		if u.ID == arg.ID {
			u.IsAdmin = arg.IsAdmin
			f.users[username] = u
			return u, nil
		}
	}
	return db.User{}, pgx.ErrNoRows
}

func (f *fakeQuerier) GetActiveRegistrationToken(_ context.Context) (db.RegistrationToken, error) {
	if f.activeToken == nil {
		return db.RegistrationToken{}, pgx.ErrNoRows
	}
	return *f.activeToken, nil
}

func (f *fakeQuerier) CreateRegistrationToken(_ context.Context, token string) (db.RegistrationToken, error) {
	if f.createTokenErr != nil {
		return db.RegistrationToken{}, f.createTokenErr
	}
	if f.activeToken != nil {
		return db.RegistrationToken{}, uniqueViolationErr()
	}
	f.tokenIDCounter++
	t := db.RegistrationToken{ID: testUUID(f.tokenIDCounter), Token: token, IsActive: true}
	f.activeToken = &t
	return t, nil
}

// testUUID строит детерминированный pgtype.UUID из небольшого счётчика —
// тестам важна только различимость id, не их формат.
func testUUID(n int) pgtype.UUID {
	var b [16]byte
	b[15] = byte(n)
	return pgtype.UUID{Bytes: b, Valid: true}
}

// uniqueViolationErr строит ошибку, неотличимую для isUniqueViolation от
// настоящего нарушения unique-ограничения Postgres (SQLSTATE 23505).
func uniqueViolationErr() error {
	return &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"}
}

var validConfig = bootstrap.Config{
	AdminUsername:            "admin",
	AdminPassword:            "s3cr3t-pass",
	InitialRegistrationToken: "initial-token",
}

// TestRun_CreatesAdminAndToken — с нуля: первый Run создаёт и администратора,
// и активный токен регистрации.
func TestRun_CreatesAdminAndToken(t *testing.T) {
	q := newFakeQuerier()
	if err := bootstrap.Run(context.Background(), q, nil, validConfig); err != nil {
		t.Fatalf("Run: %v", err)
	}

	u, ok := q.users[validConfig.AdminUsername]
	if !ok {
		t.Fatal("администратор не создан")
	}
	if !u.IsAdmin {
		t.Fatal("созданный пользователь не администратор")
	}
	if u.PasswordHash == validConfig.AdminPassword {
		t.Fatal("пароль сохранён в plaintext (нарушение FR I1)")
	}

	if q.activeToken == nil {
		t.Fatal("активный токен регистрации не создан")
	}
	if q.activeToken.Token != validConfig.InitialRegistrationToken {
		t.Fatalf("токен = %q, ожидался %q", q.activeToken.Token, validConfig.InitialRegistrationToken)
	}
}

// TestRun_IdempotentWhenAlreadyBootstrapped — повторный Run при уже
// существующем администраторе и активном токене — no-op, без ошибки и без
// повторного создания (приёмка тикета 1.7).
func TestRun_IdempotentWhenAlreadyBootstrapped(t *testing.T) {
	q := newFakeQuerier()
	if err := bootstrap.Run(context.Background(), q, nil, validConfig); err != nil {
		t.Fatalf("первый Run: %v", err)
	}
	firstUser := q.users[validConfig.AdminUsername]
	firstToken := *q.activeToken

	if err := bootstrap.Run(context.Background(), q, nil, validConfig); err != nil {
		t.Fatalf("повторный Run вернул ошибку (идемпотентность нарушена): %v", err)
	}

	if len(q.users) != 1 {
		t.Fatalf("users count = %d, ожидалось 1 (второй администратор не должен создаваться)", len(q.users))
	}
	if q.users[validConfig.AdminUsername].ID != firstUser.ID {
		t.Fatal("повторный Run пересоздал администратора вместо no-op")
	}
	if q.activeToken.ID != firstToken.ID {
		t.Fatal("повторный Run пересоздал токен регистрации вместо no-op")
	}
}

// TestRun_PromotesExistingNonAdmin — пользователь с целевым username уже
// есть, но не администратор (например, заведён обычной регистрацией): Run
// должен довести его до администратора, не создавая второго пользователя.
func TestRun_PromotesExistingNonAdmin(t *testing.T) {
	q := newFakeQuerier()
	q.nextID++
	preexisting := db.User{ID: testUUID(q.nextID), Username: validConfig.AdminUsername, PasswordHash: "already-hashed", IsAdmin: false}
	q.users[preexisting.Username] = preexisting

	if err := bootstrap.Run(context.Background(), q, nil, validConfig); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := q.users[validConfig.AdminUsername]
	if got.ID != preexisting.ID {
		t.Fatal("Run создал нового пользователя вместо промоушена существующего")
	}
	if !got.IsAdmin {
		t.Fatal("существующий пользователь не промоутирован до администратора")
	}
	if got.PasswordHash != preexisting.PasswordHash {
		t.Fatal("Run не должен перезаписывать password_hash при промоушене")
	}
}

// TestRun_ConcurrentCreateUserRace — CreateUser конкурентно ловит unique
// violation (кто-то опередил между GetUserByUsername и CreateUser): Run не
// должен возвращать ошибку — это тоже идемпотентность, а не сбой.
func TestRun_ConcurrentCreateUserRace(t *testing.T) {
	q := newFakeQuerier()
	q.createUserErr = uniqueViolationErr()

	if err := bootstrap.Run(context.Background(), q, nil, validConfig); err != nil {
		t.Fatalf("Run вернул ошибку на гонке unique violation: %v", err)
	}
}

// TestRun_ConcurrentCreateTokenRace — аналогично TestRun_ConcurrentCreateUserRace,
// но для CreateRegistrationToken.
func TestRun_ConcurrentCreateTokenRace(t *testing.T) {
	q := newFakeQuerier()
	q.createTokenErr = uniqueViolationErr()

	if err := bootstrap.Run(context.Background(), q, nil, validConfig); err != nil {
		t.Fatalf("Run вернул ошибку на гонке unique violation: %v", err)
	}
}

// TestRun_RequiresNonEmptyConfig — пустые обязательные поля конфига отклоняются
// до похода в БД (защита от случайно незаданных секретов окружения).
func TestRun_RequiresNonEmptyConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  bootstrap.Config
	}{
		{"empty username", bootstrap.Config{AdminPassword: "p", InitialRegistrationToken: "t"}},
		{"empty password", bootstrap.Config{AdminUsername: "a", InitialRegistrationToken: "t"}},
		{"empty token", bootstrap.Config{AdminUsername: "a", AdminPassword: "p"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := newFakeQuerier()
			err := bootstrap.Run(context.Background(), q, nil, tc.cfg)
			if err == nil {
				t.Fatal("ожидалась ошибка валидации, получен nil")
			}
			if len(q.users) != 0 || q.activeToken != nil {
				t.Fatal("при невалидном конфиге Run не должен ничего создавать")
			}
		})
	}
}

// TestRun_PropagatesUnexpectedQuerierError — неожиданная (не unique violation,
// не ErrNoRows) ошибка слоя данных пробрасывается наверх как есть.
func TestRun_PropagatesUnexpectedQuerierError(t *testing.T) {
	q := newFakeQuerier()
	want := errors.New("боюсь, тут авария на стороне БД")
	q.createUserErr = want

	err := bootstrap.Run(context.Background(), q, nil, validConfig)
	if err == nil {
		t.Fatal("ожидалась ошибка, получен nil")
	}
	if !errors.Is(err, want) {
		t.Fatalf("ошибка = %v, ожидалось оборачивание %v", err, want)
	}
}
