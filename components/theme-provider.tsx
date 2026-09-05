"use client";

import * as React from "react";

type Theme = "light" | "dark" | "system";

type ThemeProviderProps = {
  children: React.ReactNode;
  attribute?: "class" | "data-theme";
  defaultTheme?: Theme;
  disableTransitionOnChange?: boolean;
  enableSystem?: boolean;
  storageKey?: string;
};

type ThemeContextValue = {
  theme: Theme;
  resolvedTheme: "light" | "dark";
  setTheme: (theme: Theme) => void;
};

const ThemeContext = React.createContext<ThemeContextValue>({
  theme: "system",
  resolvedTheme: "light",
  setTheme: () => undefined
});

function systemTheme() {
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function storedTheme(storageKey: string, fallback: Theme) {
  try {
    const value = window.localStorage.getItem(storageKey);
    return value === "light" || value === "dark" || value === "system" ? value : fallback;
  } catch {
    return fallback;
  }
}

function suppressThemeTransition() {
  const style = document.createElement("style");
  style.appendChild(
    document.createTextNode("*{transition:none!important}*::before{transition:none!important}*::after{transition:none!important}")
  );
  document.head.appendChild(style);
  return () => {
    window.getComputedStyle(document.body);
    window.setTimeout(() => style.remove(), 1);
  };
}

export function ThemeProvider({
  children,
  attribute = "class",
  defaultTheme = "system",
  disableTransitionOnChange = false,
  enableSystem = true,
  storageKey = "theme"
}: ThemeProviderProps) {
  const loadedStoredThemeRef = React.useRef(typeof window !== "undefined");
  const [theme, setThemeState] = React.useState<Theme>(() =>
    typeof window === "undefined" ? defaultTheme : storedTheme(storageKey, defaultTheme)
  );
  const [resolvedTheme, setResolvedTheme] = React.useState<"light" | "dark">(() =>
    typeof window === "undefined" ? "light" : systemTheme()
  );

  React.useEffect(() => {
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const updateFromSystem = () => setResolvedTheme(systemTheme());

    media.addEventListener("change", updateFromSystem);
    return () => media.removeEventListener("change", updateFromSystem);
  }, []);

  React.useEffect(() => {
    const root = document.documentElement;
    const nextTheme = theme === "system" && enableSystem ? resolvedTheme : theme === "system" ? "light" : theme;
    const restoreTransitions = disableTransitionOnChange ? suppressThemeTransition() : undefined;

    if (attribute === "class") {
      root.classList.remove("light", "dark");
      root.classList.add(nextTheme);
    } else {
      root.setAttribute(attribute, nextTheme);
    }
    root.style.colorScheme = nextTheme;
    if (loadedStoredThemeRef.current) {
      try {
        window.localStorage.setItem(storageKey, theme);
      } catch {
        // The visual theme still works when storage is unavailable.
      }
    }
    restoreTransitions?.();
  }, [attribute, disableTransitionOnChange, enableSystem, resolvedTheme, storageKey, theme]);

  const setTheme = React.useCallback((nextTheme: Theme) => {
    loadedStoredThemeRef.current = true;
    setThemeState(nextTheme);
  }, []);

  const value = React.useMemo<ThemeContextValue>(
    () => ({
      theme,
      resolvedTheme: theme === "system" && enableSystem ? resolvedTheme : theme === "system" ? "light" : theme,
      setTheme
    }),
    [enableSystem, resolvedTheme, setTheme, theme]
  );

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme() {
  return React.useContext(ThemeContext);
}
