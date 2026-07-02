// web/e2e/happy-path.spec.ts — основной сценарий тикета 9.8 (критерий
// выхода MVP): постановка задачи из web → машина принимает и задаёт вопрос
// → пользователь видит вопрос в живом диалоге (тикет 9.5) и отвечает →
// машина сообщает о завершении → пользователь видит и подтверждает
// завершение. Trace: docs/MVP_TICKETS.md строка 38 «Критерий выхода MVP»;
// docs/User_stories_Gherkin.md §4 «Постановка задачи», §5 «Агент задаёт
// вопрос и получает ответ», §7 «Агент сообщил о завершении»/«Пользователь
// подтверждает завершение».
//
// Гоняется против ПОЛНОГО стека (`make e2e`, см. web/e2e/README.md) —
// машина эмулируется FakeMachine (web/e2e/support/fakeMachine.ts) реальным
// WS-подключением к `/machine/ws`, доставка `task_assigned`/`user_answer`
// идёт через настоящий Redpanda-мост (orchestrator/internal/bridge, тикет
// 3.4), не через фейковый publisher, как в orchestrator/internal/bddsteps.
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

test("web → вопрос агента → ответ → завершение → подтверждение", async ({ page }) => {
  await registerAndLogin(page, {
    registrationToken: registrationToken(),
    usernamePrefix: "e2e-happy",
  });

  const integrationName = `e2e-happy-${randomUUID().slice(0, 8)}`;
  const secret = await createIntegration(page, integrationName);

  const machine = await FakeMachine.connect(machineWsUrl(), secret);
  try {
    const taskText = `E2E happy-path задача ${randomUUID()}`;
    const { taskId } = await createTaskAndOpen(page, integrationName, taskText);

    // Доставка на машину (FR E1, Gherkin §4 «Доставка на машину») — РЕАЛЬНО
    // через POST /tasks → machine.commands (Redpanda) → мост → WS, не через
    // прямой вызов внутри процесса теста.
    const assigned = await machine.waitForFrame("task_assigned");
    expect(assigned.task_id).toBe(taskId);

    machine.sendTaskAccepted(taskId);
    await expectStatus(page, TASK_STATUS_LABELS.running);

    // Агент задаёт вопрос (FR F1, Gherkin §5) — пользователь должен увидеть
    // его БЕЗ перезагрузки страницы (живой диалог, тикет 9.5).
    const questionId = randomUUID();
    machine.sendAgentQuestion(taskId, questionId, "Какой порт использовать для сервиса?");

    await expect(page.getByRole("heading", { name: "Вопрос агента" })).toBeVisible({
      timeout: 20_000,
    });
    await expect(page.getByText("Какой порт использовать для сервиса?")).toBeVisible();

    await page.locator("#task-answer-text").fill("Используй порт 8080");
    await page.getByRole("button", { name: "Ответить" }).click();

    // Ответ пользователя реально доходит до машины (FR F2) — не только
    // фиксируется в БД оркестратора.
    const userAnswer = await machine.waitForFrame("user_answer");
    expect(userAnswer.task_id).toBe(taskId);
    expect((userAnswer.payload as { question_id: string; text: string }).question_id).toBe(
      questionId,
    );
    expect((userAnswer.payload as { question_id: string; text: string }).text).toBe(
      "Используй порт 8080",
    );

    await expectStatus(page, TASK_STATUS_LABELS.running);

    // Агент сообщил о завершении (FR E2, Gherkin §7) — задача НЕ закрыта
    // автоматически, ждёт явного подтверждения пользователя.
    machine.sendAgentCompleted(taskId, "Готово: сервис настроен на порту 8080");

    await expectStatus(page, TASK_STATUS_LABELS.awaiting_confirm);
    const confirmButton = page.getByRole("button", { name: "Подтвердить завершение" });
    await expect(confirmButton).toBeVisible({ timeout: 20_000 });
    await confirmButton.click();

    await expectStatus(page, TASK_STATUS_LABELS.completed);
  } finally {
    await machine.close();
  }
});
