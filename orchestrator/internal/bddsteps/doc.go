// Package bddsteps — BDD-харнесс оркестратора на godog (тикет 11.2, ТЗ §127
// «Весь функционал покрыт автоматическими тестами»,
// docs/ТЗ_Оркестратор_бизнес-версия.md §127, docs/MVP_TICKETS.md 11.2).
//
// Назначение (бизнес): .feature-файлы в orchestrator/features (на русском,
// см. docs/User_stories_Gherkin.md — единственный источник сценариев; этот
// пакет ничего не придумывает заново, только подключает степы к уже
// написанным сценариям) — исполняемая, читаемая бизнесом приёмка ключевых
// пользовательских историй §1–§10. Степы вызывают РЕАЛЬНЫЙ HTTP/WS API
// оркестратора (тот же api.NewRouter, что и в проде) поверх настоящего
// Postgres в testcontainers — тот же уровень достоверности, что и
// *_integration_test.go пакетов api/task/bootstrap, только описанный
// Gherkin-сценарием, а не Go-assertions напрямую.
//
// Что НЕ входит (полный список и обоснование — orchestrator/features/README.md
// «Что НЕ покрыто и почему»): сценарии, требующие настоящей Redpanda (мост
// оркестратор↔агент, presence по heartbeat, оффлайн-догон) помечены тегом
// @redpanda и по умолчанию пропускаются — они уже покрыты
// `go test -tags=integration` (bridge/presence/tasks integration-тесты) и
// будут дополнительно углублены тикетом 11.3. Сценарии, требующие реального
// provisioning ОС (install-скрипт, §3), — тегом @manual, они вне
// HTTP/WS-поверхности оркестратора. Allowlist-решение агента (§5
// «выполняется без вопроса») и safe-stop критической операции при отмене
// (§8 «Приоритет сохранности данных», тикет 8.5, ещё не реализован) —
// тегом @wip.
//
// Как устроено (тех): package bddsteps сам по себе (этот файл) собирается
// всегда; весь харнесс (степы, godog.TestSuite, testcontainers) — в файлах
// с build-тегом `bdd` (см. suite_test.go), по тому же принципу, что и
// `//go:build integration` у *_integration_test.go в orchestrator/internal/
// api — `go build`/`go test`/`go vet`/golangci-lint без явного тега их не
// видят, поэтому обычный `make test`/`make lint` не требует Docker.
// `make bdd` собирает и гоняет именно с этим тегом (см. Makefile).
package bddsteps
