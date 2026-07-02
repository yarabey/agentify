//go:build bdd

// suite_test.go — точка входа BDD-харнесса (тикет 11.2): `go test -tags=bdd
// ./orchestrator/internal/bddsteps/... -run TestFeatures` (обёрнуто
// `make bdd`, см. Makefile).
//
// Назначение (бизнес): TestFeatures — единственный запускаемый тест этого
// пакета; он прогоняет ВСЕ .feature-файлы из orchestrator/features (Gherkin
// на русском, источник — docs/User_stories_Gherkin.md) через godog.
// Ненулевой код возврата godog (есть провалившийся/неопределённый шаг под
// активным фильтром тегов) проваливает TestFeatures — `make bdd` в CI
// становится реальным гейтом, а не заглушкой.
//
// Как устроено (тех): один Postgres testcontainer поднимается ОДИН РАЗ на
// весь прогон (TestSuiteInitializer: BeforeSuite/AfterSuite) — тот же
// образ/wait-стратегия, что и в orchestrator/internal/api/
// register_integration_test.go (см. комментарий там про двойное вхождение
// "database system is ready..."). Изоляция между отдельными сценариями —
// TRUNCATE, не пересоздание контейнера (см. (*World).reset, world_test.go).
// Фильтр тегов по умолчанию исключает @manual/@redpanda/@wip
// (orchestrator/features/README.md объясняет, почему) — переопределяется
// переменной окружения GODOG_TAGS.
package bddsteps

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yarabey/agentify/orchestrator/migrations"
)

const (
	// bddPgImage — тот же образ и та же причина (registry-mirror, тикет
	// 0.3), что и в остальных *_integration_test.go пакета api.
	bddPgImage = "postgres:16-alpine"
	bddPgUser  = "bdd_test"
	bddPgPass  = "bdd_test"
	bddPgDB    = "bdd_test"

	// bddDefaultTagFilter — набор тегов, исключаемых по умолчанию (см.
	// orchestrator/features/README.md «Что НЕ покрыто и почему»).
	bddDefaultTagFilter = "~@manual && ~@redpanda && ~@wip"

	// featuresDir — путь к .feature-файлам относительно этого пакета
	// (orchestrator/internal/bddsteps/../../features).
	featuresDir = "../../features"
)

// pgPool — общий пул подключений к testcontainer-Postgres на весь прогон
// (см. godoc пакета); заполняется InitializeTestSuite (BeforeSuite),
// закрывается AfterSuite. Не экспортирован — деталь этого тестового
// бинаря, используется только (*World).reset.
var pgPool *pgxpool.Pool

// pgContainer — контейнер Postgres, поднятый на весь прогон (terminate в
// AfterSuite).
var pgContainer testcontainers.Container

// TestFeatures — единственный тест пакета: собирает и прогоняет
// godog.TestSuite по всем .feature-файлам orchestrator/features.
func TestFeatures(t *testing.T) {
	tags := os.Getenv("GODOG_TAGS")
	if tags == "" {
		tags = bddDefaultTagFilter
	}

	suite := godog.TestSuite{
		Name:                 "agentify-orchestrator",
		TestSuiteInitializer: InitializeTestSuite,
		ScenarioInitializer:  InitializeScenario,
		Options: &godog.Options{
			Format:    "pretty",
			Paths:     []string{featuresDir},
			Tags:      tags,
			TestingT:  t,
			Strict:    true,
			Randomize: 0,
		},
	}

	if status := suite.Run(); status != 0 {
		t.Fatalf("godog вернул ненулевой статус (%d) — есть провалившиеся/неопределённые шаги, см. вывод выше", status)
	}
}

// InitializeTestSuite поднимает общий Postgres-контейнер и прогоняет
// миграции ОДИН раз на весь прогон (BeforeSuite), гасит его после
// (AfterSuite) — тот же паттерн, что setupDB в orchestrator/internal/api/
// register_integration_test.go, только на весь прогон, а не на тест.
func InitializeTestSuite(ctx *godog.TestSuiteContext) {
	ctx.BeforeSuite(func() {
		bg := context.Background()

		req := testcontainers.ContainerRequest{
			Image:        bddPgImage,
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     bddPgUser,
				"POSTGRES_PASSWORD": bddPgPass,
				"POSTGRES_DB":       bddPgDB,
			},
			// См. подробное обоснование WithOccurrence(2) в
			// orchestrator/internal/api/register_integration_test.go —
			// официальный образ postgres кратко стартует дважды (initdb,
			// затем финальный старт для внешних соединений).
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(2 * time.Minute),
		}
		container, err := testcontainers.GenericContainer(bg, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			panic(fmt.Errorf("bdd: поднять Postgres-контейнер: %w", err))
		}
		pgContainer = container

		host, err := container.Host(bg)
		if err != nil {
			panic(fmt.Errorf("bdd: host контейнера: %w", err))
		}
		port, err := container.MappedPort(bg, "5432/tcp")
		if err != nil {
			panic(fmt.Errorf("bdd: порт контейнера: %w", err))
		}
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			bddPgUser, bddPgPass, host, port.Port(), bddPgDB)

		sqlDB, err := sql.Open("pgx", dsn)
		if err != nil {
			panic(fmt.Errorf("bdd: sql.Open: %w", err))
		}
		goose.SetBaseFS(migrations.FS)
		if err := goose.SetDialect("postgres"); err != nil {
			panic(fmt.Errorf("bdd: goose SetDialect: %w", err))
		}
		if err := goose.UpContext(bg, sqlDB, "."); err != nil {
			panic(fmt.Errorf("bdd: goose Up: %w", err))
		}
		_ = sqlDB.Close()

		pool, err := pgxpool.New(bg, dsn)
		if err != nil {
			panic(fmt.Errorf("bdd: pgxpool.New: %w", err))
		}
		pgPool = pool
	})

	ctx.AfterSuite(func() {
		if pgPool != nil {
			pgPool.Close()
		}
		if pgContainer != nil {
			if err := testcontainers.TerminateContainer(pgContainer); err != nil {
				fmt.Fprintf(os.Stderr, "bdd: terminate Postgres: %v\n", err)
			}
		}
	})
}

// InitializeScenario регистрирует Before/After-хуки сценария и все степы
// (см. register*Steps в steps_*_test.go этого пакета). godog вызывает эту
// функцию ОДИН РАЗ НА КАЖДЫЙ сценарий (см. godoc пакета/World) — поэтому
// `w := &World{}` ниже даёт каждому сценарию собственное, независимое
// состояние.
func InitializeScenario(sc *godog.ScenarioContext) {
	w := &World{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		if err := w.reset(ctx); err != nil {
			return ctx, err
		}
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
		w.teardown()
		return ctx, nil
	})

	registerAuthSteps(sc, w)
	registerIntegrationSteps(sc, w)
	registerTaskSteps(sc, w)
	registerQASteps(sc, w)
	registerNotificationSteps(sc, w)
	registerCompletionSteps(sc, w)
	registerCancellationSteps(sc, w)
	registerOfflineSteps(sc, w)
	registerHistorySteps(sc, w)
}
