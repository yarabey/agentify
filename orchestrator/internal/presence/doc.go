// Package presence — статус машины «онлайн/оффлайн» по heartbeat (тикет 3.6,
// FR B4, protocol.md §6, Gherkin §2 «Машина онлайн»/«Машина оффлайн»).
//
// Назначение (бизнес): владелец видит в UI, работает ли сейчас его машина,
// БЕЗ прямого синхронного соединения (FR B4: «статус не требует прямого
// синхронного соединения»). Агент периодически (HEARTBEAT_INTERVAL, по
// умолчанию 15s, protocol.md §6) шлёт через ту же шину, что и остальные
// события (machine.events, ADR 0001), пустой конверт type==heartbeat;
// оркестратор по каждому такому конверту обновляет last_seen_at интеграции и
// помечает её online. Если heartbeat перестал приходить дольше
// OFFLINE_THRESHOLD (по умолчанию 45s, protocol.md §6) — фоновый воркер (а не
// сам факт разрыва TCP-соединения) переводит интеграцию в offline: статус
// асинхронный и не завязан на то, жив ли прямо сейчас конкретный WS-процесс
// оркестратора (Gherkin §2 «статус обновился асинхронно, без живого
// соединения»).
//
// Явно ВНЕ объёма этого тикета (см. docs/MVP_TICKETS.md 3.6,
// docs/MANUAL_STEPS.md §4): STALE_THRESHOLD/«зависание» (FR E5, тикет 5.7) —
// отдельное продуктовое решение о том, что machine формально online (свежий
// heartbeat), но не отвечает на команды.
//
// Как устроено (тех): три независимых компонента, связываемых только через
// orchestrator/main.go (по тому же принципу, что и orchestrator/internal/bridge
// — минимум связей между пакетами):
//   - Sink реализует api.EventSink СТРУКТУРНО (без импорта пакета api) —
//     публикует полученный от GetMachineWs конверт события в топик
//     machine.events (bus.Producer.PublishKeyed), выбирая ключ партиции по
//     общему правилу ADR 0001 (task_id, если он есть в конверте, иначе
//     integration_id), не только для heartbeat;
//   - Consumer читает machine.events отдельной consumer group
//     ("orchestrator-heartbeat", ОБЯЗАНА отличаться от
//     bridgeConsumerGroup="orchestrator-bridge" в orchestrator/main.go — иначе
//     Redpanda ребалансировала бы одни и те же партиции между двумя разными
//     ролями потребления) поверх обычного batch-цикла bus.Consumer.Run
//     (commit-after-success — в отличие от bridge, здесь не нужно ждать
//     дальнейшего ack: durable-персистентность Redpanda + запись в БД
//     достаточны) и по каждому конверту type==heartbeat помечает
//     соответствующую интеграцию online со свежим last_seen_at
//     (MarkIntegrationOnline);
//   - OfflineWorker — тикер, независимо от того, читается ли сейчас шина,
//     периодически (WithPollInterval, внутренняя деталь, не настраивается
//     через env) переводит в offline все интеграции, чей last_seen_at старше
//     OFFLINE_THRESHOLD (WithOfflineThreshold, настраивается через
//     ORCH_OFFLINE_THRESHOLD в orchestrator/main.go).
package presence
