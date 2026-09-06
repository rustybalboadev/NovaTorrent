"use client";

import * as React from "react";
import {
  AlertCircle,
  CheckCircle2,
  ChevronDown,
  FileCheck2,
  FileUp,
  FolderOpen,
  Link2,
  Loader2,
  Plus,
  Settings2,
  X
} from "lucide-react";
import { FileTree } from "@/components/file-tree";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { addTorrent, closeAddTorrentWindow, defaultDownloadDir, isTauriRuntime, previewTorrent } from "@/lib/tauri-api";
import type { AddTorrentRequest, TorrentDetails } from "@/lib/torrent-types";
import { cn, formatBytes } from "@/lib/utils";

type AddTorrentPanelProps = {
  windowMode?: boolean;
  initialSource?: string | null;
  onAdded?: () => void;
  onCancel?: () => void;
};

export function AddTorrentPanel({ windowMode, initialSource, onAdded, onCancel }: AddTorrentPanelProps) {
  const [initialParsedSource] = React.useState(() => parseInitialSource(initialSource));
  const [sourceType, setSourceType] = React.useState<"file" | "magnet">(initialParsedSource?.kind ?? "file");
  const [torrentPath, setTorrentPath] = React.useState(initialParsedSource?.kind === "file" ? initialParsedSource.value : "");
  const [magnet, setMagnet] = React.useState(initialParsedSource?.kind === "magnet" ? initialParsedSource.value : "");
  const [destination, setDestination] = React.useState("");
  const [paused, setPaused] = React.useState(false);
  const [overwrite, setOverwrite] = React.useState(false);
  const [disableTrackers, setDisableTrackers] = React.useState(false);
  const [advancedOpen, setAdvancedOpen] = React.useState(false);
  const [preview, setPreview] = React.useState<TorrentDetails | null>(null);
  const [selectedFileIds, setSelectedFileIds] = React.useState<Set<number>>(new Set());
  const [busy, setBusy] = React.useState<"preview" | "add" | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [success, setSuccess] = React.useState<string | null>(null);
  const previewRequestId = React.useRef(0);

  const clearPreviewState = React.useCallback(() => {
    previewRequestId.current += 1;
    setPreview(null);
    setSelectedFileIds(new Set());
    setBusy((current) => (current === "preview" ? null : current));
  }, []);

  const applyIncomingSource = React.useCallback(
    (value: string) => {
      const parsed = parseDeepLinkSource(value);
      clearPreviewState();
      setError(null);
      setSuccess(null);
      if (parsed.kind === "magnet") {
        setSourceType("magnet");
        setMagnet(parsed.value);
      } else {
        setSourceType("file");
        setTorrentPath(parsed.value);
      }
    },
    [clearPreviewState]
  );

  React.useEffect(() => {
    if (initialSource || typeof window === "undefined") return;
    const params = new URLSearchParams(window.location.search);
    const source = params.get("source") || params.get("magnet") || params.get("file");
    if (!source) return;
    let cancelled = false;
    window.queueMicrotask(() => {
      if (!cancelled) applyIncomingSource(source);
    });
    return () => {
      cancelled = true;
    };
  }, [applyIncomingSource, initialSource]);

  React.useEffect(() => {
    defaultDownloadDir().then(setDestination).catch(() => undefined);
  }, []);

  React.useEffect(() => {
    if (!isTauriRuntime()) return;
    let sourceUnlisten: (() => void) | undefined;

    import("@tauri-apps/api/event")
      .then(async ({ listen }) => {
        sourceUnlisten = await listen<string>("add-torrent-source", (event) => {
          applyIncomingSource(event.payload);
        });
      })
      .catch(() => undefined);

    return () => {
      sourceUnlisten?.();
    };
  }, [applyIncomingSource]);

  const sourceValue = sourceType === "file" ? torrentPath : magnet;
  const files = preview?.files ?? [];
  const totalSize = files.reduce((sum, file) => sum + file.length, 0);
  const selectedSize = files.reduce((sum, file, index) => (selectedFileIds.has(index) ? sum + file.length : sum), 0);
  const selectedCount = files.reduce((count, _, index) => count + Number(selectedFileIds.has(index)), 0);
  const previewFiles = filesWithTorrentRoot(files, preview?.name ?? undefined);
  const hasSource = Boolean(sourceValue.trim());
  const selectionIsEmpty = files.length > 0 && selectedFileIds.size === 0;

  const previewSelectedSource = React.useCallback(async (kind: "file" | "magnet", value: string) => {
    if (!value.trim()) return;
    const requestId = ++previewRequestId.current;
    setBusy("preview");
    setError(null);
    setSuccess(null);
    try {
      const response = await previewTorrent({
        source: { kind, value: value.trim() },
        destination: null,
        paused: false,
        overwrite: false,
        disableTrackers: false,
        onlyFiles: null,
        subFolder: null
      });
      if (previewRequestId.current !== requestId) return;
      setPreview(response.details);
      setSelectedFileIds(
        new Set((response.details.files ?? []).map((file, index) => (file.included ? index : -1)).filter((index) => index >= 0))
      );
    } catch (err) {
      if (previewRequestId.current !== requestId) return;
      setPreview(null);
      setSelectedFileIds(new Set());
      setError(err instanceof Error ? err.message : "NovaTorrent could not read this torrent source.");
    } finally {
      if (previewRequestId.current === requestId) setBusy(null);
    }
  }, []);

  React.useEffect(() => {
    const value = sourceValue.trim();
    if (!value) return;
    const timer = window.setTimeout(() => {
      void previewSelectedSource(sourceType, value);
    }, 400);
    return () => window.clearTimeout(timer);
  }, [previewSelectedSource, sourceType, sourceValue]);

  async function chooseTorrentFile() {
    setError(null);
    try {
      const { open } = await import("@tauri-apps/plugin-dialog");
      const picked = await open({
        multiple: false,
        directory: false,
        filters: [{ name: "Torrent", extensions: ["torrent"] }]
      });
      if (typeof picked === "string") {
        clearPreviewState();
        setSourceType("file");
        setTorrentPath(picked);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "NovaTorrent could not open the file picker.");
    }
  }

  async function chooseDestination() {
    setError(null);
    try {
      const { open } = await import("@tauri-apps/plugin-dialog");
      const picked = await open({ multiple: false, directory: true });
      if (typeof picked === "string") setDestination(picked);
    } catch (err) {
      setError(err instanceof Error ? err.message : "NovaTorrent could not open the folder picker.");
    }
  }

  async function handleAdd() {
    if (!sourceValue.trim()) {
      setError("Choose a torrent file or paste a magnet link first.");
      return;
    }
    if (selectionIsEmpty) {
      setError("Select at least one file to download.");
      return;
    }
    previewRequestId.current += 1;
    setBusy("add");
    setError(null);
    setSuccess(null);
    try {
      await addTorrent(buildRequest(sourceType, sourceValue, selectedFileIds));
    } catch (err) {
      const message = err instanceof Error ? err.message : "NovaTorrent could not add this torrent.";
      if (message.includes("Replace existing files")) setAdvancedOpen(true);
      setError(message);
      setBusy(null);
      return;
    }

    setSuccess("Torrent added to the download queue.");
    try {
      onAdded?.();
      if (windowMode && isTauriRuntime()) await closeAddWindowMode();
    } catch {
      setSuccess("Torrent added. You can close this window.");
    } finally {
      setBusy(null);
    }
  }

  async function handleCancel() {
    if (onCancel) {
      onCancel();
      return;
    }
    await closeAddWindowMode();
  }

  async function closeAddWindowMode() {
    if (!windowMode || !isTauriRuntime()) return;
    try {
      await closeAddTorrentWindow();
    } catch {
      const { getCurrentWindow } = await import("@tauri-apps/api/window");
      await getCurrentWindow().close();
    }
  }

  function buildRequest(kind: "file" | "magnet", value: string, selectedFiles: Set<number>): AddTorrentRequest {
    const onlyFiles = files.length > 0 && selectedFiles.size < files.length ? Array.from(selectedFiles).sort((a, b) => a - b) : null;
    return {
      source: { kind, value: value.trim() },
      destination: destination.trim() || null,
      paused,
      overwrite,
      disableTrackers,
      onlyFiles,
      subFolder: null
    };
  }

  return (
    <section className={cn("panel flex h-full min-h-0 min-w-0 flex-col overflow-hidden", windowMode && "min-h-[560px]")}>
      <header className="flex items-center justify-between gap-4 border-b px-5 py-4">
        <div className="min-w-0">
          <h1 className="text-lg font-semibold tracking-tight">Add a torrent</h1>
          <p className="truncate text-sm text-muted-foreground">
            {preview?.name || "Choose a file or paste a magnet link to begin."}
          </p>
        </div>
        {!windowMode ? (
          <Button variant="ghost" size="icon" type="button" aria-label="Close add torrent" onClick={handleCancel}>
            <X />
          </Button>
        ) : null}
      </header>

      <div className="grid min-h-0 min-w-0 flex-1 overflow-y-auto md:grid-cols-[minmax(320px,380px)_1fr] md:overflow-hidden">
        <div className="min-w-0 space-y-5 border-b p-5 md:overflow-y-auto md:border-b-0 md:border-r">
          <div className="grid grid-cols-2 rounded-lg bg-muted p-1">
            <SourceButton
              active={sourceType === "file"}
              icon={<FileUp />}
              label="Torrent file"
              onClick={() => {
                setSourceType("file");
                clearPreviewState();
              }}
            />
            <SourceButton
              active={sourceType === "magnet"}
              icon={<Link2 />}
              label="Magnet link"
              onClick={() => {
                setSourceType("magnet");
                clearPreviewState();
              }}
            />
          </div>

          {sourceType === "file" ? (
            <div className="space-y-2">
              <Label htmlFor="torrent-file">Torrent file</Label>
              <div className="relative">
                <Input
                  id="torrent-file"
                  className="min-w-0 cursor-pointer pr-10"
                  value={torrentPath}
                  readOnly
                  onClick={chooseTorrentFile}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      void chooseTorrentFile();
                    }
                  }}
                  placeholder="Choose a .torrent file"
                  title={torrentPath || "Choose a .torrent file"}
                />
                <FileUp className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              </div>
            </div>
          ) : (
            <div className="space-y-2">
              <Label htmlFor="magnet-link">Magnet link</Label>
              <Textarea
                id="magnet-link"
                value={magnet}
                onChange={(event) => {
                  setMagnet(event.target.value);
                  clearPreviewState();
                }}
                placeholder="magnet:?xt=urn:btih:…"
                className="min-h-24 resize-none"
              />
            </div>
          )}

          <div className="space-y-2">
            <div className="flex items-end justify-between gap-3">
              <div>
                <Label htmlFor="destination">Save to</Label>
                <p className="mt-0.5 text-xs text-muted-foreground">Choose the base download folder.</p>
              </div>
            </div>
            <div className="relative">
              <Input
                className="min-w-0 cursor-pointer pr-10"
                id="destination"
                value={destination}
                readOnly
                onClick={chooseDestination}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    void chooseDestination();
                  }
                }}
                placeholder="Choose a destination folder"
                title={destination || "Choose a destination folder"}
              />
              <FolderOpen className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            </div>
          </div>

          <div className="overflow-hidden rounded-xl border">
            <button
              type="button"
              className="flex w-full items-center justify-between px-3.5 py-3 text-sm font-medium transition-colors hover:bg-secondary/60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
              aria-expanded={advancedOpen}
              onClick={() => setAdvancedOpen((value) => !value)}
            >
              <span className="flex items-center gap-2">
                <Settings2 className="h-4 w-4 text-muted-foreground" />
                Download options
              </span>
              <ChevronDown className={cn("h-4 w-4 text-muted-foreground transition-transform", advancedOpen && "rotate-180")} />
            </button>
            {advancedOpen ? (
              <div className="space-y-4 border-t bg-muted/35 p-3.5">
                <ToggleRow
                  label="Start paused"
                  description="Add it to the queue without connecting yet."
                  checked={paused}
                  onCheckedChange={setPaused}
                />
                <ToggleRow
                  label="Replace existing files"
                  description="Off by default to protect your files. Enable only when you intend to replace matching paths."
                  checked={overwrite}
                  onCheckedChange={setOverwrite}
                />
                <ToggleRow
                  label="Disable trackers"
                  description="Use peer discovery methods other than trackers."
                  checked={disableTrackers}
                  onCheckedChange={setDisableTrackers}
                />
              </div>
            ) : null}
          </div>

          {error ? (
            <StatusLine tone="error" icon={<AlertCircle className="h-4 w-4" />}>
              {error}
            </StatusLine>
          ) : null}
          {success ? (
            <StatusLine tone="success" icon={<CheckCircle2 className="h-4 w-4" />}>
              {success}
            </StatusLine>
          ) : null}
        </div>

        <div className="flex min-h-[380px] min-w-0 flex-col md:min-h-0">
          <div className="flex min-h-16 items-center justify-between gap-4 border-b px-5 py-3">
            <div>
              <h2 className="text-sm font-semibold">Files to download</h2>
              <p className="text-xs text-muted-foreground">
                {files.length > 0
                  ? `${selectedCount} of ${files.length} selected · ${formatBytes(selectedSize)} of ${formatBytes(totalSize)}`
                  : busy === "preview"
                    ? "Reading torrent metadata…"
                    : sourceType === "magnet" && preview
                      ? "File metadata will arrive after peer discovery."
                      : "File details appear automatically."}
              </p>
            </div>
            {files.length > 0 ? (
              <div className="flex items-center gap-1">
                <Button type="button" variant="ghost" size="sm" onClick={() => setSelectedFileIds(new Set(files.map((_, index) => index)))}>
                  Select all
                </Button>
                <Button type="button" variant="ghost" size="sm" onClick={() => setSelectedFileIds(new Set())}>
                  Clear
                </Button>
              </div>
            ) : null}
          </div>
          <div className="min-h-0 flex-1 overflow-y-auto p-4">
            {files.length > 0 ? (
              <FileTree files={previewFiles} selectedFileIds={selectedFileIds} onSelectionChange={setSelectedFileIds} />
            ) : (
              <div className="flex h-full min-h-72 flex-col items-center justify-center rounded-xl border border-dashed px-8 py-12 text-center">
                <div className="mb-3 flex h-10 w-10 items-center justify-center rounded-full bg-secondary text-muted-foreground">
                  {busy === "preview" ? <Loader2 className="h-5 w-5 animate-spin" /> : sourceType === "magnet" ? <Link2 className="h-5 w-5" /> : <FileCheck2 className="h-5 w-5" />}
                </div>
                <p className="text-sm font-medium">
                  {busy === "preview" ? "Reading torrent metadata" : sourceType === "magnet" && preview ? "Ready to fetch metadata" : "No torrent selected"}
                </p>
                <p className="mt-1 max-w-sm text-xs leading-relaxed text-muted-foreground">
                  {busy === "preview"
                    ? "The file list will appear here automatically."
                    : sourceType === "magnet" && preview
                      ? "Add the torrent to connect to peers and retrieve its file list."
                      : sourceType === "file"
                        ? "Choose a .torrent file to inspect its contents before downloading."
                        : "Paste a magnet link to inspect its available details."}
                </p>
              </div>
            )}
          </div>
        </div>
      </div>

      <Separator />
      <footer className="flex flex-col gap-3 px-5 py-4 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex min-w-0 items-center gap-2 text-xs text-muted-foreground">
          {busy === "preview" ? (
            <Loader2 className="h-3.5 w-3.5 shrink-0 animate-spin" />
          ) : preview ? (
            <CheckCircle2 className="h-3.5 w-3.5 shrink-0 text-primary" />
          ) : (
            <FileUp className="h-3.5 w-3.5 shrink-0" />
          )}
          <span className="truncate">
            {busy === "preview" ? "Previewing automatically…" : preview ? "Preview updated automatically" : "Preview starts when a source is entered"}
          </span>
        </div>
        <div className="flex justify-end gap-2">
          <Button type="button" variant="outline" onClick={handleCancel} disabled={busy === "add"}>
            Cancel
          </Button>
          <Button type="button" onClick={handleAdd} disabled={!hasSource || Boolean(busy) || selectionIsEmpty}>
            {busy === "add" ? <Loader2 className="animate-spin" /> : <Plus />}
            {paused ? "Add paused" : "Add torrent"}
          </Button>
        </div>
      </footer>
    </section>
  );
}

function SourceButton({ active, icon, label, onClick }: { active: boolean; icon: React.ReactNode; label: string; onClick: () => void }) {
  return (
    <button
      type="button"
      className={cn(
        "flex h-9 items-center justify-center gap-2 rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
        active ? "bg-background text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground"
      )}
      aria-pressed={active}
      onClick={onClick}
    >
      <span className="[&_svg]:h-4 [&_svg]:w-4">{icon}</span>
      {label}
    </button>
  );
}

function ToggleRow({
  label,
  description,
  checked,
  onCheckedChange
}: {
  label: string;
  description: string;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
}) {
  return (
    <div className="flex items-start justify-between gap-4">
      <div className="min-w-0">
        <Label className="font-normal">{label}</Label>
        <p className="mt-0.5 text-xs leading-relaxed text-muted-foreground">{description}</p>
      </div>
      <Switch className="mt-0.5" checked={checked} onCheckedChange={onCheckedChange} aria-label={label} />
    </div>
  );
}

function StatusLine({ tone, icon, children }: { tone: "error" | "success"; icon: React.ReactNode; children: React.ReactNode }) {
  return (
    <div
      className={cn(
        "flex items-start gap-2 rounded-lg px-3 py-2.5 text-sm ring-1 ring-inset",
        tone === "error"
          ? "bg-destructive/10 text-destructive ring-destructive/25"
          : "bg-primary/10 text-primary ring-primary/25"
      )}
    >
      <span className="mt-0.5 shrink-0">{icon}</span>
      <span className="leading-relaxed">{children}</span>
    </div>
  );
}

function filesWithTorrentRoot(files: NonNullable<TorrentDetails["files"]>, torrentName?: string) {
  if (!torrentName || files.length === 0) return files;
  const firstRoot = files[0]?.components.length > 1 ? files[0].components[0] : null;
  const alreadyContained = Boolean(
    firstRoot && files.every((file) => file.components.length > 1 && file.components[0] === firstRoot)
  );
  if (alreadyContained) return files;
  return files.map((file) => ({ ...file, components: [torrentName, ...file.components] }));
}

function parseInitialSource(initialSource?: string | null) {
  return initialSource ? parseDeepLinkSource(initialSource) : null;
}

function parseDeepLinkSource(value: string): { kind: "file" | "magnet"; value: string } {
  if (value.startsWith("magnet:?")) return { kind: "magnet", value };
  try {
    const url = new URL(value);
    const magnet = url.searchParams.get("magnet") || url.searchParams.get("url");
    const file = url.searchParams.get("file") || url.searchParams.get("path");
    if (magnet) return parseDeepLinkSource(magnet);
    if (file) return { kind: "file", value: file };
  } catch {
    return { kind: "file", value };
  }
  return { kind: "file", value };
}
