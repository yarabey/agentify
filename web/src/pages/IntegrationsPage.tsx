import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";

import { apiClient } from "@/api/client";
import type { components } from "@/api/schema";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";

type Integration = components["schemas"]["Integration"];
type IntegrationWithSecret = components["schemas"]["IntegrationWithSecret"];

/** React Query ключ списка интеграций — единая точка инвалидации после мутаций. */
const INTEGRATIONS_QUERY_KEY = ["integrations"] as const;

/**
 * Схема формы создания/редактирования интеграции (тикет 9.3, FR B1).
 * `name` обязателен, `ip_hint` опционален (проверка IP на бэкенде — FR B3,
 * дублировать её на клиенте незачем, см. `IntegrationCreate`/`IntegrationUpdate`
 * в `src/api/schema.ts`). Пустая строка `ip_hint` трактуется как «не указан» —
 * в API уходит `undefined`, а не `""`.
 */
const integrationFormSchema = z.object({
  name: z.string().min(1, "Введите название интеграции"),
  ip_hint: z.string().optional(),
});

type IntegrationFormValues = z.infer<typeof integrationFormSchema>;

function toIpHintPayload(ipHint: string | undefined): string | undefined {
  return ipHint && ipHint.trim().length > 0 ? ipHint.trim() : undefined;
}

/**
 * Общие поля формы создания/редактирования — вынесены отдельно, чтобы не
 * дублировать разметку между `CreateIntegrationForm` и `EditIntegrationForm`
 * (единственное различие между ними — какой запрос уходит по сабмиту).
 */
function IntegrationFormFields({
  idPrefix,
  register,
  errors,
}: {
  idPrefix: string;
  register: ReturnType<typeof useForm<IntegrationFormValues>>["register"];
  errors: ReturnType<typeof useForm<IntegrationFormValues>>["formState"]["errors"];
}): JSX.Element {
  return (
    <>
      <div className="flex flex-col gap-2">
        <Label htmlFor={`${idPrefix}-name`}>Название</Label>
        <Input id={`${idPrefix}-name`} {...register("name")} />
        {errors.name && (
          <p className="text-sm text-destructive">{errors.name.message}</p>
        )}
      </div>
      <div className="flex flex-col gap-2">
        <Label htmlFor={`${idPrefix}-ip-hint`}>IP (необязательно)</Label>
        <Input id={`${idPrefix}-ip-hint`} {...register("ip_hint")} />
        {errors.ip_hint && (
          <p className="text-sm text-destructive">{errors.ip_hint.message}</p>
        )}
      </div>
    </>
  );
}

/**
 * Форма создания интеграции (тикет 9.3, FR B1, B2; Gherkin §2 «Создание
 * интеграции выдаёт UUID»). При успехе поднимает `IntegrationWithSecret`
 * наверх через `onCreated` — сам экран показывает UUID и инвалидирует список,
 * форма этим не занимается (разделение ответственности).
 */
function CreateIntegrationForm({
  onCreated,
  onCancel,
}: {
  onCreated: (created: IntegrationWithSecret) => void;
  onCancel: () => void;
}): JSX.Element {
  const [formError, setFormError] = useState<string | null>(null);
  const {
    register,
    handleSubmit,
    formState: { errors, isSubmitting },
  } = useForm<IntegrationFormValues>({
    resolver: zodResolver(integrationFormSchema),
  });

  const onSubmit = handleSubmit(async (values) => {
    setFormError(null);
    const { data, error } = await apiClient.POST("/integrations", {
      body: { name: values.name, ip_hint: toIpHintPayload(values.ip_hint) },
    });
    if (error || !data) {
      setFormError("Не удалось создать интеграцию. Попробуйте ещё раз.");
      return;
    }
    onCreated(data);
  });

  return (
    <form
      className="flex flex-col gap-4 rounded-md border border-border p-4"
      onSubmit={onSubmit}
      noValidate
    >
      <h2 className="text-lg font-semibold">Новая интеграция</h2>
      <IntegrationFormFields
        idPrefix="create-integration"
        register={register}
        errors={errors}
      />
      {formError && (
        <p role="alert" className="text-sm text-destructive">
          {formError}
        </p>
      )}
      <div className="flex gap-2">
        <Button type="submit" disabled={isSubmitting}>
          {isSubmitting ? "Создаём…" : "Создать"}
        </Button>
        <Button type="button" variant="outline" onClick={onCancel}>
          Отмена
        </Button>
      </div>
    </form>
  );
}

/**
 * Форма редактирования интеграции (тикет 9.3, FR B5; Gherkin §2
 * «Редактирование интеграции не рвёт активные задачи» — сам инвариант уже
 * гарантирован бэкендом, тикет 2.5, здесь только вызов `PATCH`).
 */
function EditIntegrationForm({
  integration,
  onSaved,
  onCancel,
}: {
  integration: Integration;
  onSaved: () => void;
  onCancel: () => void;
}): JSX.Element {
  const [formError, setFormError] = useState<string | null>(null);
  const {
    register,
    handleSubmit,
    formState: { errors, isSubmitting },
  } = useForm<IntegrationFormValues>({
    resolver: zodResolver(integrationFormSchema),
    defaultValues: {
      name: integration.name ?? "",
      ip_hint: integration.ip_hint ?? "",
    },
  });

  const onSubmit = handleSubmit(async (values) => {
    if (!integration.id) {
      return;
    }
    setFormError(null);
    const { error } = await apiClient.PATCH("/integrations/{id}", {
      params: { path: { id: integration.id } },
      body: { name: values.name, ip_hint: toIpHintPayload(values.ip_hint) },
    });
    if (error) {
      setFormError("Не удалось сохранить изменения. Попробуйте ещё раз.");
      return;
    }
    onSaved();
  });

  return (
    <form
      className="flex flex-col gap-4 rounded-md border border-border p-4"
      onSubmit={onSubmit}
      noValidate
    >
      <h2 className="text-lg font-semibold">
        Редактирование: {integration.name}
      </h2>
      <IntegrationFormFields
        idPrefix={`edit-integration-${integration.id}`}
        register={register}
        errors={errors}
      />
      {formError && (
        <p role="alert" className="text-sm text-destructive">
          {formError}
        </p>
      )}
      <div className="flex gap-2">
        <Button type="submit" disabled={isSubmitting}>
          {isSubmitting ? "Сохраняем…" : "Сохранить"}
        </Button>
        <Button type="button" variant="outline" onClick={onCancel}>
          Отмена
        </Button>
      </div>
    </form>
  );
}

/**
 * Плашка с UUID сразу после создания интеграции (тикет 9.3, FR B2; Gherkin §2
 * «Создание интеграции выдаёт UUID»). Это единственный момент, когда бэкенд
 * гарантированно отдаёт секрет вместе со списочными полями в ответ на
 * действие пользователя «прямо сейчас» — поэтому UUID показывается явно и
 * не прячется по умолчанию; закрывается только явным действием
 * пользователя ("Я сохранил UUID"), а не автоматически по таймеру — секрет
 * машине нужно скопировать/куда-то сохранить, и это не должно быть гонкой
 * со временем.
 */
function CreatedSecretBanner({
  integration,
  onDismiss,
}: {
  integration: IntegrationWithSecret;
  onDismiss: () => void;
}): JSX.Element {
  return (
    <div
      role="status"
      className="flex flex-col gap-3 rounded-md border border-primary bg-primary/10 p-4"
    >
      <p className="font-medium">
        Интеграция «{integration.name}» создана. Сохраните UUID — он нужен для
        настройки агента на машине и больше не будет показан так явно.
      </p>
      <p className="break-all font-mono text-sm">{integration.uuid}</p>
      <div>
        <Button size="sm" onClick={onDismiss}>
          Я сохранил UUID
        </Button>
      </div>
    </div>
  );
}

/**
 * Строка списка интеграций (тикет 9.3). Инкапсулирует локальные действия
 * над одной интеграцией: раскрытие UUID по требованию (FR B2, повторный
 * показ через `GET /integrations/{id}` — не одноразовый), запуск
 * редактирования и удаление с подтверждением/эскалацией (FR B5).
 */
function IntegrationRow({
  integration,
  onEdit,
  onDeleted,
}: {
  integration: Integration;
  onEdit: () => void;
  onDeleted: () => void;
}): JSX.Element {
  const [revealedUuid, setRevealedUuid] = useState<string | null>(null);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [isDeleting, setIsDeleting] = useState(false);

  const revealMutation = useMutation({
    mutationFn: async () => {
      if (!integration.id) {
        throw new Error("integration без id");
      }
      const { data, error } = await apiClient.GET("/integrations/{id}", {
        params: { path: { id: integration.id } },
      });
      if (error || !data) {
        throw error ?? new Error("не найдено");
      }
      return data;
    },
    onSuccess: (data) => {
      setRevealedUuid(data.uuid ?? null);
    },
  });

  /**
   * Удаление с подтверждением (FR B5, Gherkin §2 «Удаление интеграции с
   * активной задачей требует подтверждения»). Двухступенчато:
   *   1. Обычное подтверждение действия ("точно удалить интеграцию?").
   *   2. Если бэкенд отвечает 409 (есть активные задачи, тикет 2.6) — второе,
   *      более явное предупреждение о том, что активные задачи будут
   *      отменены/завершены; только после повторного согласия уходит
   *      `DELETE ?confirm=true`.
   * Реализовано как обычный async-обработчик, а не `useMutation`: ветвление
   * зависит от HTTP-статуса ПРОМЕЖУТОЧНОГО ответа (204 vs 409), а не просто
   * от success/error, и второй запрос отправляется только при явном согласии
   * пользователя между двумя вызовами — это последовательный сценарий, а не
   * одна атомарная мутация.
   */
  async function handleDelete() {
    if (!integration.id) {
      return;
    }
    const confirmed = window.confirm(
      `Удалить интеграцию «${integration.name ?? integration.id}»?`,
    );
    if (!confirmed) {
      return;
    }

    setDeleteError(null);
    setIsDeleting(true);
    try {
      const first = await apiClient.DELETE("/integrations/{id}", {
        params: { path: { id: integration.id } },
      });
      if (first.response.status === 204) {
        onDeleted();
        return;
      }
      if (first.response.status === 409) {
        const confirmedWithActiveTasks = window.confirm(
          "У интеграции есть активные задачи. При удалении они будут " +
            "отменены/завершены. Удалить интеграцию всё равно?",
        );
        if (!confirmedWithActiveTasks) {
          return;
        }
        const second = await apiClient.DELETE("/integrations/{id}", {
          params: {
            path: { id: integration.id },
            query: { confirm: true },
          },
        });
        if (second.response.status === 204) {
          onDeleted();
          return;
        }
        setDeleteError("Не удалось удалить интеграцию. Попробуйте ещё раз.");
        return;
      }
      setDeleteError("Не удалось удалить интеграцию. Попробуйте ещё раз.");
    } finally {
      setIsDeleting(false);
    }
  }

  const isOnline = integration.status === "online";

  return (
    <li className="flex flex-col gap-3 rounded-md border border-border p-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <p className="font-medium">{integration.name}</p>
          {integration.ip_hint && (
            <p className="text-sm text-muted-foreground">
              IP: {integration.ip_hint}
            </p>
          )}
          <p className="mt-1 text-sm text-muted-foreground">
            Последний раз на связи:{" "}
            {integration.last_seen_at
              ? new Date(integration.last_seen_at).toLocaleString()
              : "никогда"}
          </p>
        </div>
        <span
          className={cn(
            "inline-flex items-center gap-2 text-sm font-medium",
            isOnline ? "text-primary" : "text-muted-foreground",
          )}
        >
          <span
            aria-hidden="true"
            className={cn(
              "inline-block h-2.5 w-2.5 rounded-full",
              isOnline ? "bg-primary" : "bg-muted-foreground",
            )}
          />
          {isOnline ? "Онлайн" : "Оффлайн"}
        </span>
      </div>

      {revealedUuid && (
        <p className="break-all rounded-md bg-muted p-2 font-mono text-sm">
          UUID: {revealedUuid}
        </p>
      )}
      {revealMutation.isError && (
        <p className="text-sm text-destructive">
          Не удалось получить UUID. Попробуйте ещё раз.
        </p>
      )}
      {deleteError && (
        <p className="text-sm text-destructive">{deleteError}</p>
      )}

      <div className="flex flex-wrap gap-2">
        {revealedUuid ? (
          <Button size="sm" variant="outline" onClick={() => setRevealedUuid(null)}>
            Скрыть UUID
          </Button>
        ) : (
          <Button
            size="sm"
            variant="outline"
            disabled={revealMutation.isPending}
            onClick={() => revealMutation.mutate()}
          >
            {revealMutation.isPending ? "Показываем…" : "Показать UUID"}
          </Button>
        )}
        <Button size="sm" variant="outline" onClick={onEdit}>
          Редактировать
        </Button>
        <Button
          size="sm"
          variant="destructive"
          disabled={isDeleting}
          onClick={() => {
            void handleDelete();
          }}
        >
          {isDeleting ? "Удаляем…" : "Удалить"}
        </Button>
      </div>
    </li>
  );
}

/**
 * Экран интеграций (`/integrations`, тикет 9.3, FR B1–B5; Gherkin §2).
 *
 * Назначение (бизнес): единственное место, где пользователь заводит,
 * настраивает и снимает "машины" (интеграции), в которые потом можно
 * направлять задачи (FR B1); видит, на связи ли машина, чтобы понимать,
 * дойдёт ли задача (FR B4); и получает UUID — секрет для настройки агента на
 * машине (FR B2), причём как в момент создания, так и повторно в любой
 * момент (FR B2, `GET /integrations/{id}` — не одноразовый показ, см.
 * `IntegrationRow`).
 *
 * Как устроено (тех): `useQuery` — список (`GET /integrations`);
 * `useMutation`/прямые вызовы `apiClient` — создание/редактирование/удаление,
 * с инвалидацией списка через `queryClient.invalidateQueries` при успехе
 * (единый источник правды о серверном состоянии — TanStack Query, без
 * дублирующего локального стейта списка). Секрет (`uuid`) никогда не
 * приходит в `GET /integrations` (только в ответах на создание и на
 * `GET /integrations/{id}`) — экран и не хранит его в состоянии списка,
 * только в состоянии конкретной строки после явного запроса пользователя.
 */
export function IntegrationsPage(): JSX.Element {
  const queryClient = useQueryClient();
  const [isCreateFormOpen, setIsCreateFormOpen] = useState(false);
  const [editingIntegration, setEditingIntegration] =
    useState<Integration | null>(null);
  const [createdSecret, setCreatedSecret] =
    useState<IntegrationWithSecret | null>(null);

  const integrationsQuery = useQuery({
    queryKey: INTEGRATIONS_QUERY_KEY,
    queryFn: async () => {
      const { data, error } = await apiClient.GET("/integrations");
      if (error) {
        throw error;
      }
      return data ?? [];
    },
  });

  function invalidateList() {
    void queryClient.invalidateQueries({ queryKey: INTEGRATIONS_QUERY_KEY });
  }

  return (
    <section className="flex flex-col gap-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Интеграции</h1>
          <p className="mt-2 text-muted-foreground">
            Машины, в которые можно направлять задачи.
          </p>
        </div>
        {!isCreateFormOpen && (
          <Button
            onClick={() => {
              setCreatedSecret(null);
              setIsCreateFormOpen(true);
            }}
          >
            Создать интеграцию
          </Button>
        )}
      </div>

      {createdSecret && (
        <CreatedSecretBanner
          integration={createdSecret}
          onDismiss={() => setCreatedSecret(null)}
        />
      )}

      {isCreateFormOpen && (
        <CreateIntegrationForm
          onCreated={(created) => {
            setCreatedSecret(created);
            setIsCreateFormOpen(false);
            invalidateList();
          }}
          onCancel={() => setIsCreateFormOpen(false)}
        />
      )}

      {editingIntegration && (
        <EditIntegrationForm
          integration={editingIntegration}
          onSaved={() => {
            setEditingIntegration(null);
            invalidateList();
          }}
          onCancel={() => setEditingIntegration(null)}
        />
      )}

      {integrationsQuery.isLoading && (
        <p className="text-muted-foreground">Загрузка…</p>
      )}
      {integrationsQuery.isError && (
        <p className="text-sm text-destructive">
          Не удалось загрузить список интеграций.
        </p>
      )}
      {integrationsQuery.data && integrationsQuery.data.length === 0 && (
        <p className="text-muted-foreground">
          Интеграций пока нет — создайте первую.
        </p>
      )}

      {integrationsQuery.data && integrationsQuery.data.length > 0 && (
        <ul className="flex flex-col gap-3">
          {integrationsQuery.data.map((integration) => (
            <IntegrationRow
              key={integration.id}
              integration={integration}
              onEdit={() => setEditingIntegration(integration)}
              onDeleted={invalidateList}
            />
          ))}
        </ul>
      )}
    </section>
  );
}
