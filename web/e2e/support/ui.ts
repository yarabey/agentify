// web/e2e/support/ui.ts — переиспользуемые UI-действия web-канала для E2E
// сценариев тикета 9.8 (критерий выхода MVP): регистрация/вход, создание
// интеграции, постановка задачи. Вынесены из спеков, т.к. все три сценария
// (happy-path/offline-catchup/safe-cancellation) начинаются с одной и той же
// последовательности «зарегистрироваться → войти → завести интеграцию».
//
// Селекторы намеренно завязаны на видимый пользователю текст/label (не на
// data-testid — в web/src/pages/*.tsx их нет, а вводить их только ради
// тестов вне объёма этого тикета, см. AGENTS.md §8 "не пишем фичи вне
// объёма без явного решения"): это те же метки, что видит реальный
// пользователь (Gherkin-сценарии docs/User_stories_Gherkin.md описывают
// бизнес-действия в тех же терминах).
import { randomUUID } from "node:crypto";
import { expect, type Page } from "@playwright/test";

/** Человекочитаемые статусы задачи — дословная копия `TASK_STATUS_LABELS` (web/src/lib/taskStatus.ts), т.к. e2e/ вне TS-проекта src/ (свой tsconfig, см. web/e2e/tsconfig.json) и просто импортировать оттуда не стоит риска потащить браузерный код в Node-раннер Playwright. */
export const TASK_STATUS_LABELS = {
  queued: "В очереди",
  running: "Выполняется",
  waiting_user: "Ожидает ответа пользователя",
  awaiting_confirm: "Ожидает подтверждения",
  completed: "Завершена",
  cancelled: "Отменена",
} as const;

/** Заводит нового пользователя через реальный экран `/register` (тикет 9.2) и логинится (тикет 9.2, FR A3) — общий первый шаг всех сценариев 9.8 ("постановка из web" начинается с реального входа, не с подмены localStorage). Возвращает выбранный username (для логов/отладки упавшего теста). */
export async function registerAndLogin(
  page: Page,
  opts: { registrationToken: string; usernamePrefix: string },
): Promise<string> {
  const username = `${opts.usernamePrefix}-${randomUUID().slice(0, 8)}`;
  const password = "correct-horse-battery-staple-1!";

  await page.goto("/register");
  await page.getByLabel("Токен регистрации").fill(opts.registrationToken);
  await page.getByLabel("Имя пользователя").fill(username);
  await page.getByLabel("Пароль", { exact: true }).fill(password);
  await page.getByRole("button", { name: "Зарегистрироваться" }).click();

  await expect(page).toHaveURL(/\/login$/, { timeout: 15_000 });

  await page.getByLabel("Имя пользователя").fill(username);
  await page.getByLabel("Пароль").fill(password);
  await page.getByRole("button", { name: "Войти" }).click();

  await expect(page).toHaveURL(/\/tasks$/, { timeout: 15_000 });
  return username;
}

/** Создаёт интеграцию через `/integrations` (тикет 9.3, FR B1/B2) и возвращает её секрет (UUID), показанный один раз в баннере сразу после создания — тот же секрет, что нужен FakeMachine.connect (hello, docs/protocol.md §4). */
export async function createIntegration(page: Page, name: string): Promise<string> {
  await page.goto("/integrations");
  await page.getByRole("button", { name: "Создать интеграцию" }).click();
  await page.getByLabel("Название").fill(name);
  await page.getByRole("button", { name: "Создать", exact: true }).click();

  const banner = page.getByRole("status");
  await expect(banner).toBeVisible({ timeout: 15_000 });
  const secret = (await banner.locator("p.font-mono").innerText()).trim();
  expect(secret).toMatch(/^[0-9a-f-]{36}$/i);

  await banner.getByRole("button", { name: "Я сохранил UUID" }).click();
  return secret;
}

/** Ставит задачу через `/tasks` (тикет 9.4, FR E1) на интеграцию с именем integrationName, открывает созданную задачу и возвращает её id (из URL `/tasks/{id}`). */
export async function createTaskAndOpen(
  page: Page,
  integrationName: string,
  text: string,
): Promise<{ taskId: string }> {
  await page.goto("/tasks");
  await page.getByRole("button", { name: "Новая задача" }).click();
  await page.getByLabel("Машина").selectOption({ label: integrationName });
  await page.getByLabel("Текст запроса").fill(text);
  await page.getByRole("button", { name: "Поставить задачу" }).click();

  const row = page.getByRole("link").filter({ hasText: text });
  await expect(row).toBeVisible({ timeout: 15_000 });
  await row.click();

  await expect(page).toHaveURL(/\/tasks\/[0-9a-f-]{36}$/, { timeout: 15_000 });
  const taskId = page.url().split("/tasks/")[1];
  return { taskId };
}

/** dd-элемент со статусом задачи на `/tasks/{id}` (первая пара `dt`/`dd` карточки, см. web/src/pages/TaskDetailPage.tsx) — общая точка для `expect(...).toHaveText(...)` во всех трёх сценариях. */
export function statusLocator(page: Page) {
  return page.locator("dl dd").first();
}

/** Ждёт, пока статус задачи (см. statusLocator) станет ожидаемой меткой — как правило, следствие живого обновления по WS (тикет 9.5) после кадра от FakeMachine. */
export async function expectStatus(page: Page, label: string, timeoutMs = 20_000): Promise<void> {
  await expect(statusLocator(page)).toHaveText(label, { timeout: timeoutMs });
}

/** Текущий access-токен вошедшего пользователя из `localStorage` (см. web/src/lib/tokenStore.ts) — нужен safe-cancellation.spec.ts, чтобы проверить журнал событий задачи (`GET /tasks/{id}/events`) напрямую через API: `agent_progress` (тикет 8.5) не рассылается живым WS-уведомлением (см. orchestrator/internal/api/machine_ws.go, handleAgentProgress — только запись в БД, без Notify), поэтому UI сам по себе его не покажет без ручного рефетча. */
export async function getAccessToken(page: Page): Promise<string> {
  const token = await page.evaluate(() => localStorage.getItem("agentify.accessToken"));
  if (!token) {
    throw new Error("getAccessToken: agentify.accessToken отсутствует в localStorage — пользователь не вошёл?");
  }
  return token;
}

/** Один элемент `GET /tasks/{id}/events` (тикет 8.6) — только поля, нужные тестам. */
export interface TaskEventDTO {
  type: string;
  payload: Record<string, unknown> | null;
}

/** Журнал событий задачи через реальный `GET /tasks/{id}/events` (owner-scoped, тот же bearer-токен, что и у UI) — используется, когда нужно проверить событие, не доходящее до UI живым WS-пушем (см. годок getAccessToken). */
export async function fetchTaskEvents(page: Page, taskId: string): Promise<TaskEventDTO[]> {
  const token = await getAccessToken(page);
  const response = await page.request.get(`/api/tasks/${taskId}/events`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  expect(response.ok()).toBeTruthy();
  return (await response.json()) as TaskEventDTO[];
}
