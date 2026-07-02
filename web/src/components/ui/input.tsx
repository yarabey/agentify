import * as React from "react";

import { cn } from "@/lib/utils";

/**
 * shadcn/ui Input (тикет 9.2): та же ручная схема добавления примитива, что
 * `components/ui/button.tsx` в тикете 9.1 (стандартная реализация shadcn/ui,
 * скопированная руками — CLI недоступен без сети). Используется в формах
 * логина/регистрации (`LoginPage`, `RegisterPage`) через `react-hook-form`.
 */
export type InputProps = React.InputHTMLAttributes<HTMLInputElement>;

const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className, type, ...props }, ref) => {
    return (
      <input
        type={type}
        className={cn(
          "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50",
          className,
        )}
        ref={ref}
        {...props}
      />
    );
  },
);
Input.displayName = "Input";

export { Input };
