"use client";

import Image from "next/image";
import { Copy, Minus, Square, X } from "lucide-react";
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
      <div className="pointer-events-none flex min-w-0 items-center gap-2 px-3" data-tauri-drag-region>
        <Image src="/novatorrent-logo.png" alt="" width={18} height={18} className="h-[18px] w-[18px] object-contain" priority />
        <span
          className={cn("truncate text-xs font-medium", pathname.startsWith("/media") ? "text-zinc-200" : "text-foreground/80")}
          data-tauri-drag-region
        >
          {windowTitle(pathname)}
        </span>
      </div>
      <div className="ml-auto flex h-full" onDoubleClick={(event) => event.stopPropagation()}>
        <TitlebarButton label="Minimize" onClick={() => void windowRef.current?.minimize()}>
          <Minus className="h-4 w-4" strokeWidth={1.5} />
        </TitlebarButton>
        <TitlebarButton label={maximized ? "Restore" : "Maximize"} onClick={() => void toggleMaximize()}>
          {maximized ? <Copy className="h-3.5 w-3.5" strokeWidth={1.5} /> : <Square className="h-3.5 w-3.5" strokeWidth={1.5} />}
        </TitlebarButton>
        <TitlebarButton label="Close" close onClick={() => void windowRef.current?.close()}>
          <X className="h-4 w-4" strokeWidth={1.5} />
        </TitlebarButton>
      </div>
    </div>
  );
}

function TitlebarButton({
  children,
  close = false,
  label,
  onClick
}: {
  children: React.ReactNode;
  close?: boolean;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      className={cn("window-titlebar-button", close && "window-titlebar-close")}
      aria-label={label}
      title={label}
      onClick={onClick}
    >
      {children}
    </button>
  );
}
