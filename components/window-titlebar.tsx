"use client";

import { Maximize2, Minimize2, Minus, X } from "lucide-react";
import { usePathname } from "next/navigation";
import * as React from "react";
import { isTauriRuntime } from "@/lib/tauri-api";
import { cn } from "@/lib/utils";

function windowTitle(pathname: string) {
  if (pathname.startsWith("/add")) return "Add torrent";
  if (pathname.startsWith("/media")) return "Media player";
  return "NovaTorrent";
}

type CurrentWindow = ReturnType<typeof import("@tauri-apps/api/window")["getCurrentWindow"]>;

export function WindowTitlebar() {
  const pathname = usePathname();
  const [maximized, setMaximized] = React.useState(false);
  const windowRef = React.useRef<CurrentWindow | null>(null);

  React.useEffect(() => {
    if (!isTauriRuntime()) return;

    let disposed = false;
    let unlisten: (() => void) | undefined;

    void import("@tauri-apps/api/window").then(async ({ getCurrentWindow }) => {
      if (disposed) return;
      const currentWindow = getCurrentWindow();
      windowRef.current = currentWindow;
      document.documentElement.classList.add("tauri-runtime");
      setMaximized(await currentWindow.isMaximized());
      unlisten = await currentWindow.onResized(async () => {
        if (!disposed) setMaximized(await currentWindow.isMaximized());
      });
      window.requestAnimationFrame(() => {
        if (!disposed) void currentWindow.show();
      });
    });

    return () => {
      disposed = true;
      unlisten?.();
      windowRef.current = null;
    };
  }, []);

  async function toggleMaximize() {
    const currentWindow = windowRef.current;
    if (!currentWindow) return;
    await currentWindow.toggleMaximize();
    setMaximized(await currentWindow.isMaximized());
  }

  return (
    <div
      className={cn("window-titlebar", pathname.startsWith("/media") && "window-titlebar-media")}
      data-tauri-drag-region
      onDoubleClick={() => void toggleMaximize()}
    >
      <div className="window-titlebar-controls" onDoubleClick={(event) => event.stopPropagation()}>
        <TitlebarButton control="close" label="Close" onClick={() => void windowRef.current?.close()}>
          <X />
        </TitlebarButton>
        <TitlebarButton control="minimize" label="Minimize" onClick={() => void windowRef.current?.minimize()}>
          <Minus />
        </TitlebarButton>
        <TitlebarButton control="maximize" label={maximized ? "Restore" : "Maximize"} onClick={() => void toggleMaximize()}>
          {maximized ? <Minimize2 /> : <Maximize2 />}
        </TitlebarButton>
      </div>
      <div className="window-titlebar-title" data-tauri-drag-region>
        <span
          className={cn("truncate text-[11px] font-medium", pathname.startsWith("/media") ? "text-zinc-400" : "text-muted-foreground")}
          data-tauri-drag-region
        >
          {windowTitle(pathname)}
        </span>
      </div>
    </div>
  );
}

function TitlebarButton({
  children,
  control,
  label,
  onClick
}: {
  children: React.ReactNode;
  control: "close" | "minimize" | "maximize";
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      className={cn("window-titlebar-button", `window-titlebar-${control}`)}
      aria-label={label}
      title={label}
      onClick={onClick}
    >
      <span className="window-titlebar-dot">{children}</span>
    </button>
  );
}
