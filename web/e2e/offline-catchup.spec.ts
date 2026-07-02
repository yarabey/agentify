// web/e2e/offline-catchup.spec.ts — сценарий "оффлайн-догон" тикета 9.8
// (критерий выхода MVP, deps 5.6). Trace: docs/MVP_TICKETS.md строка 38
// «оффлайн-машина догоняет (5.6)»; тикет 5.6 «Оффлайн-постановка» (FR E4);
// docs/User_stories_Gherkin.md §9 «Задача поставлена при оффлайн-машине»;
// docs/protocol.md §3/§6 «Адресация команд конкретной машине... Если машина
// оффлайн — сообщение остаётся в топике (offset не коммитится), мост
// дочитывает его при реконнекте».
//
// В отличие от orchestrator/internal/bddsteps/steps_offline_test.go (тикет
// 11.2), который покрывает ТОЛЬКО ветку "зависание уже бегущей задачи"
// (StaleWorker, целиком на Postgres, БЕЗ Redpanda — см.
// orchestrator/features/README.md, тег @redpanda), этот сценарий проверяет
// именно оффлайн-ПОСТАНОВКУ (задача создаётся, когда машина ещё не
// подключена вовсе) через настоящий Redpanda-мост — ветку, которую
// BDD-харнесс сознательно не покрывает (нет тестового Redpanda-стенда в
// той песочнице, см. её README).
import { randomUUID } from "node:crypto";
import { expect, test } from "@playwright/test";

import { FakeMachine } from "./support/fakeMachine";
import { machineWsUrl, registrationToken } from "./support/env";
import {
  createIntegration,
  createTaskAndOpen,
  expectStatus,
  registerAndLogin,
  TASK_STATUS_LABELS,
} from "./support/ui";

test("оффлайн-постановка: задача ждёт в очереди, затем доставляется без потерь после реконнекта машины", async ({
  page,
}) => {
  await registerAndLogin(page, {
    registrationToken: registrationToken(),
    usernamePrefix: "e2e-offline",
  });

  const integrationName = `e2e-offline-${randomUUID().slice(0, 8)}`;
  // Секрет получен, но машина СОЗНАТЕЛЬНО ещё не подключается — интеграция
  // оффлайн с точки зрения оркестратора (last_seen_at никогда не
  // обновлялся).
  const secret = await createIntegration(page, integrationName);

  const taskText = `E2E offline-catchup задача ${randomUUID()}`;
  const { taskId } = await createTaskAndOpen(page, integrationName, taskText);

  // Команда `task_assigned` уже опубликована в machine.commands (POST
  // /tasks публикует её безусловно, см. orchestrator/internal/api/tasks.go),
  // но доставить её некому — задача должна остаться в очереди, а не потеряться
  // и не перейти в running сама по себе.
  await expectStatus(page, TASK_STATUS_LABELS.queued);
  await page.waitForTimeout(2_000);
  await expectStatus(page, TASK_STATUS_LABELS.queued);

  // Машина "включается" — реконнект должен догнать пропущенную команду
  // (мост дочитывает с незакоммиченного offset, docs/protocol.md §3/§6).
  const machine = await FakeMachine.connect(machineWsUrl(), secret);
  try {
    const assigned = await machine.waitForFrame("task_assigned", 30_000);
    expect(assigned.task_id).toBe(taskId);
    expect((assigned.payload as { text: string }).text).toBe(taskText);

    machine.sendTaskAccepted(taskId);
    await expectStatus(page, TASK_STATUS_LABELS.running);

    // Доводим до конца, чтобы подтвердить "выполнено после онлайна, без
    // потери" (тикет 5.6, приёмка) — не только "доставлено".
    machine.sendAgentCompleted(taskId, "Готово после реконнекта");
    await expectStatus(page, TASK_STATUS_LABELS.awaiting_confirm);

    await page.getByRole("button", { name: "Подтвердить завершение" }).click();
    await expectStatus(page, TASK_STATUS_LABELS.completed);
  } finally {
    await machine.close();
  }
});
