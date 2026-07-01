package platform

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
)

// Service — общий операционный каркас одного Go-сервиса agentify.
//
// Назначение (бизнес): даёт оркестратору/боту/агенту единообразный жизненный
// цикл — поднять HTTP-сервер с /healthz и работать, пока не придёт SIGTERM/
// SIGINT, после чего корректно (graceful) остановиться в пределах таймаута, не
// бросая клиентов. Это закрывает приёмку тикета 0.6 «сервис стартует и гасится
// по SIGTERM».
//
// Как устроено (тех): Service хранит имя сервиса, логгер, конфиг и chi-роутер
// (как минимум с /healthz). Метод Run блокируется, слушая сигналы ОС и отмену
// переданного контекста; на любом из событий гасит http.Server через Shutdown
// с дедлайном Config.ShutdownTimeout.
type Service struct {
	name   string
	logger *slog.Logger
	cfg    Config
	router chi.Router
}

// NewService собирает каркас сервиса: валидирует базовый конфиг, строит логгер
// и chi-роутер с GET /healthz.
//
// Параметр name — каноническое имя сервиса (orchestrator|bot|agent): попадает в
// логи и в тело /healthz. cfg — базовый операционный конфиг (обычно извлечённый
// из встраивающей структуры сервиса). Возвращает ошибку, если конфиг невалиден.
func NewService(name string, cfg Config) (*Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("конфиг сервиса %q невалиден: %w", name, err)
	}
	logger := NewLogger(cfg, name)
	return &Service{
		name:   name,
		logger: logger,
		cfg:    cfg,
		router: NewHealthRouter(name, logger),
	}, nil
}

// Logger возвращает настроенный slog-логгер сервиса для использования в main и
// будущих компонентах (auth, WS, очереди появятся в следующих тикетах).
func (s *Service) Logger() *slog.Logger {
	return s.logger
}

// Router возвращает chi-роутер сервиса (как минимум с /healthz), чтобы
// последующие тикеты могли навешивать на него свои маршруты до вызова Run.
func (s *Service) Router() chi.Router {
	return s.router
}

// SetHandler заменяет HTTP-роутер сервиса на переданный handler перед вызовом
// Run, сохраняя весь жизненный цикл (тот же адрес, таймауты и graceful
// shutdown).
//
// Назначение: тикет 1.2 поднимает в оркестраторе сгенерированный из openapi
// chi-роутер (он уже включает собственный GET /healthz и реальные эндпоинты
// API). Чтобы не дублировать /healthz из двух источников и не навешивать
// маршруты на встроенный health-роутер, сервис принимает готовый handler
// целиком. Передавать handler нужно ДО Run; nil игнорируется (остаётся
// health-роутер по умолчанию). handler — любой chi.Router (или иной
// http.Handler, обёрнутый в chi); тип сужен до chi.Router, т.к. весь стек
// сервиса работает на chi.
func (s *Service) SetHandler(handler chi.Router) {
	if handler == nil {
		return
	}
	s.router = handler
}

// Run запускает HTTP-сервер сервиса и блокируется до остановки.
//
// Сервер слушает Config.HealthAddr и обслуживает /healthz. Run возвращает
// управление, когда происходит любое из событий:
//   - получен SIGTERM или SIGINT (штатная остановка контейнера/процесса);
//   - отменён переданный ctx (используется в тестах и для встраивания);
//   - сам HTTP-сервер упал с ошибкой (тогда она и возвращается).
//
// При остановке сервер гасится через http.Server.Shutdown с дедлайном
// Config.ShutdownTimeout: активные запросы доигрываются, новые не принимаются.
// Возвращает nil при корректной остановке по сигналу/контексту и ошибку только
// при сбое запуска или превышении таймаута graceful-остановки.
func (s *Service) Run(ctx context.Context) error {
	// Контекст, который отменяется по SIGTERM/SIGINT ИЛИ по отмене родительского
	// ctx. Так один и тот же путь shutdown покрывает и реальный сигнал, и
	// программную отмену из теста (приёмка graceful shutdown тикета 0.6).
	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	srv := &http.Server{
		Addr:              s.cfg.HealthAddr,
		Handler:           s.router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Ошибка ListenAndServe доставляется в основной select через канал.
	serveErr := make(chan error, 1)
	go func() {
		s.logger.Info("сервис стартует",
			slog.String("addr", s.cfg.HealthAddr),
			slog.String("env", string(s.cfg.Env)),
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		// Сервер упал ещё до сигнала остановки — отдаём причину наверх.
		if err != nil {
			return fmt.Errorf("сервис %q: HTTP-сервер упал: %w", s.name, err)
		}
		return nil
	case <-signalCtx.Done():
		s.logger.Info("получен сигнал остановки, начинаю graceful shutdown",
			slog.Duration("timeout", s.cfg.ShutdownTimeout),
		)
	}

	// Гасим сервер с собственным дедлайном, не наследуя уже отменённый
	// signalCtx, иначе Shutdown завершился бы мгновенно без доигрывания запросов.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("сервис %q: graceful shutdown не уложился в таймаут: %w", s.name, err)
	}

	// Дожидаемся завершения горутины ListenAndServe (вернёт ErrServerClosed → nil).
	if err := <-serveErr; err != nil {
		return fmt.Errorf("сервис %q: HTTP-сервер вернул ошибку при остановке: %w", s.name, err)
	}

	s.logger.Info("сервис остановлен")
	return nil
}
