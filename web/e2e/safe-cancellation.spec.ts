// web/e2e/safe-cancellation.spec.ts — сценарий "безопасная отмена" тикета
// 9.8 (критерий выхода MVP, deps 8.4/8.5). Trace: docs/MVP_TICKETS.md строка
// 38 «отмена останавливает безопасно (8.4/8.5)»; docs/User_stories_Gherkin.md
// §8 «Отмена доходит до машины» / «Приоритет сохранности данных при
// отмене»; orchestrator/features/08_task_cancellation.feature.
//
// В отличие от orchestrator/internal/bddsteps (тикет 11.2, степы
// steps_cancellation_test.go), который явным образом проверяет ТОЛЬКО
// границу оркестратора — что команда `cancel` ОПУБЛИКОВАНА в
// CommandPublisher (см. комментарий в начале
// orchestrator/features/08_task_cancellation.feature: "мост... at-least-once,
// доставка оффлайн-машине... отдельно покрыта
// orchestrator/internal/bridge/bridge_integration_test.go... тегом
// integration, РЕАЛЬНАЯ Redpanda"), — этот сценарий идёт дальше и проверяет
// РЕАЛЬНУЮ доставку команды `cancel` физическому (пусть и тестовому)
// процессу машины через настоящий Redpanda-мост, плюс что задача переходит
// в "отменена" в web без перезагрузки.
//
// Транспортный уровень «сохранности данных» (тикет 8.5: агент доводит
// критическую операцию до безопасного состояния перед остановкой,
// warnCancelDeferred → agent_progress) уже полностью реализован и
// юнит-протестирован на стороне провайдера
// (agent/internal/provider/claudecode/provider_test.go) — здесь FakeMachine
// СИМУЛИРУЕТ то же протокольное поведение (получил cancel → опубликовал
// agent_progress с предупреждением, ПРЕЖДЕ чем реально прекратить
// активность), чтобы подтвердить, что этот сигнал целиком доходит до
// истории задачи (task_events) через настоящий стек — то, что
// orchestrator/features/08_task_cancellation.feature прямо помечает как
// НЕ покрытое в BDD-харнессе (@wip на второй сценарий, см. её комментарий:
// "агент... на момент написания... тегом @wip").
import { randomUUID } from "node:crypto";
import { expect, test } from "@playwright/test";

import { FakeMachine } from "./support/fakeMachine";
import { machineWsUrl, registrationToken } from "./support/env";
import {
  createIntegration,
  createTaskAndOpen,
  expectStatus,
  fetchTaskEvents,
  registerAndLogin,
  TASK_STATUS_LABELS,
} from "./support/ui";

test("отмена доходит до машины, останавливает работу безопасно, статус — «отменена»", async ({
  page,
}) => {
  await registerAndLogin(page, {
    registrationToken: registrationToken(),
    usernamePrefix: "e2e-cancel",
  });

  const integrationName = `e2e-cancel-${randomUUID().slice(0, 8)}`;
  const secret = await createIntegration(page, integrationName);

  const machine = await FakeMachine.connect(machineWsUrl(), secret);
  try {
    const taskText = `E2E safe-cancellation задача ${randomUUID()}`;
    const { taskId } = await createTaskAndOpen(page, integrationName, taskText);

    await machine.waitForFrame("task_assigned");
    machine.sendTaskAccepted(taskId);
    await expectStatus(page, TASK_STATUS_LABELS.running);

    // Отмена из web (FR E6, Gherkin §8 «Отмена доходит до машины»):
    // подтверждаем нативный window.confirm (см.
    // web/src/pages/TaskDetailPage.tsx, TaskActions.handleDelete-подобный
    // паттерн cancelMutation).
    page.once("dialog", (dialog) => {
      void dialog.accept();
    });
    await page.getByRole("button", { name: "Отменить" }).click();

    // Статус меняется СИНХРОННО с ответом POST /tasks/{id}/cancel (тикет
    // 8.4: Transition в cancelled происходит ДО публикации команды машине,
    // см. orchestrator/internal/api/tasks.go PostTasksIdCancel) — не нужно
    // ждать реакции машины, чтобы увидеть "Отменена" в web.
    await expectStatus(page, TASK_STATUS_LABELS.cancelled);

    // Тем не менее команда `cancel` РЕАЛЬНО доходит до машины через
    // Redpanda-мост (не только "опубликована", как в
    // orchestrator/internal/bddsteps) — это и есть предмет проверки этого
    // сценария, за рамками BDD-харнесса.
    const cancel = await machine.waitForFrame("cancel");
    expect(cancel.task_id).toBe(taskId);

    // Симулируем безопасную остановку (тикет 8.5, FR E6): предупреждение о
    // том, что мгновенная остановка не гарантируется, публикуется ДО того,
    // как машина реально прекращает активность (см. годок файла — то же
    // протокольное поведение, что claudecode.Provider.warnCancelDeferred).
    machine.sendAgentProgress(
      taskId,
      "Довожу критическую операцию до безопасного состояния перед остановкой — мгновенная остановка не гарантируется",
    );

    // agent_progress НЕ рассылается живым WS-уведомлением (см. годок файла
    // support/ui.ts, fetchTaskEvents) — ждём его появления в истории через
    // прямой GET /tasks/{id}/events, подтверждая, что предупреждение о
    // сохранности данных реально дошло и записано, а не потеряно вместе с
    // отменённой задачей.
    await expect(async () => {
      const events = await fetchTaskEvents(page, taskId);
      const progress = events.find((e) => e.type === "agent_progress");
      expect(progress).toBeTruthy();
      expect(String(progress?.payload?.text ?? "")).toContain("безопасного состояния");
    }).toPass({ timeout: 20_000 });
  } finally {
    await machine.close();
  }
});
