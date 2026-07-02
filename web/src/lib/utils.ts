import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

/**
 * Слияние Tailwind-классов с разрешением конфликтов (shadcn/ui конвенция).
 * Назначение: используется всеми UI-компонентами (`src/components/ui/*`) для
 * объединения базовых и переданных через `className` классов без дублей
 * (напр. `p-2 p-4` -> `p-4`).
 */
export function cn(...inputs: ClassValue[]): string {
  return twMerge(clsx(inputs));
}
