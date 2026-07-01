import { describe, expect, it } from "vitest";

import { pwaManifest } from "@/pwaManifest";

/**
 * Тест валидности PWA-манифеста (тикет 9.1, DoD "PWA-манифест валиден"):
 * проверяет обязательные поля installable-манифеста напрямую на объекте
 * `pwaManifest` (без реальной Vite-сборки/manifest.webmanifest) — тот же
 * объект передаётся в `VitePWA({ manifest: pwaManifest })` в vite.config.ts.
 */
describe("pwaManifest", () => {
  it("содержит обязательные поля installable-манифеста", () => {
    expect(pwaManifest.name).toBeTruthy();
    expect(pwaManifest.short_name).toBeTruthy();
    expect(pwaManifest.start_url).toBeTruthy();
    expect(pwaManifest.display).toBe("standalone");
    expect(pwaManifest.theme_color).toBeTruthy();
    expect(pwaManifest.background_color).toBeTruthy();
  });

  it("содержит хотя бы одну валидную иконку >= 192px (или sizes: any)", () => {
    expect(pwaManifest.icons?.length).toBeGreaterThanOrEqual(1);

    const icon = pwaManifest.icons?.[0];
    expect(icon?.src).toBeTruthy();
    expect(icon?.type).toBeTruthy();
    expect(icon?.sizes).toBeTruthy();

    const isAnySize = icon?.sizes === "any";
    const meetsMinSize = (icon?.sizes ?? "")
      .split(" ")
      .some((size) => {
        const [width, height] = size.split("x").map(Number);
        return Number.isFinite(width) && width >= 192 && height >= 192;
      });

    expect(isAnySize || meetsMinSize).toBe(true);
  });
});
