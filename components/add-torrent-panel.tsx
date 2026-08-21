"use client";

import * as React from "react";
import { AlertCircle, CheckCircle2, ChevronDown, FileUp, FolderOpen, Link2, Loader2, Plus, Settings2, ShieldCheck, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { FileTree } from "@/components/file-tree";
import { addTorrent, closeAddTorrentWindow, defaultDownloadDir, isTauriRuntime, previewTorrent, safeTestTorrents } from "@/lib/tauri-api";
import type { AddTorrentRequest, SafeTestTorrent, TorrentDetails } from "@/lib/torrent-types";
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
  const [subFolder, setSubFolder] = React.useState("");
  const [advancedOpen, setAdvancedOpen] = React.useState(false);
  const [preview, setPreview] = React.useState<TorrentDetails | null>(null);
  const [selectedFileIds, setSelectedFileIds] = React.useState<Set<number>>(new Set());
  const [safeSources, setSafeSources] = React.useState<SafeTestTorrent[]>([]);
  const [busy, setBusy] = React.useState<"preview" | "add" | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [success, setSuccess] = React.useState<string | null>(null);

  const applyIncomingSource = React.useCallback((value: string) => {
    const parsed = parseDeepLinkSource(value);
    if (parsed.kind === "magnet") {
      setSourceType("magnet");
      setMagnet(parsed.value);
    } else {
      setSourceType("file");
      setTorrentPath(parsed.value);
    }
  }, []);

  React.useEffect(() => {
    defaultDownloadDir().then(setDestination).catch(() => undefined);
  }, []);

  React.useEffect(() => {
    safeTestTorrents().then(setSafeSources).catch(() => undefined);
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
  const totalSize = preview?.files?.reduce((sum, file) => sum + file.length, 0) ?? 0;
  const selectedSize = preview?.files?.reduce((sum, file, index) => (selectedFileIds.has(index) ? sum + file.length : sum), 0) ?? 0;

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
        setSourceType("file");
        setTorrentPath(picked);
        void previewSelectedSource("file", picked, true);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not open the file picker.");
    }
  }

  async function chooseDestination() {
    setError(null);
    try {
      const { open } = await import("@tauri-apps/plugin-dialog");
      const picked = await open({
        multiple: false,
        directory: true
      });
      if (typeof picked === "string") setDestination(picked);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not open the folder picker.");
    }
  }

  function applySafeSource(source: SafeTestTorrent) {
    setSourceType("file");
    setTorrentPath(source.path);
    setPreview(null);
    setSelectedFileIds(new Set());
    setError(null);
    setSuccess(null);
    void previewSelectedSource("file", source.path, true);
  }

  async function handlePreview() {
    await previewSelectedSource(sourceType, sourceValue, false);
  }

  async function previewSelectedSource(kind: "file" | "magnet", value: string, automatic: boolean) {
    if (!value.trim()) {
      if (automatic) return;
      setError("Choose a torrent file or paste a magnet link.");
      return;
    }
    setBusy("preview");
    if (!automatic) setError(null);
    setSuccess(null);
    try {
      const response = await previewTorrent(buildRequest(kind, value, null));
      setPreview(response.details);
      setSelectedFileIds(
        new Set((response.details.files ?? []).map((file, index) => (file.included ? index : -1)).filter((index) => index >= 0))
      );
      if (response.output_folder) setDestination(response.output_folder);
    } catch (err) {
      if (!automatic) setError(err instanceof Error ? err.message : "Could not preview this torrent.");
    } finally {
      setBusy(null);
    }
  }

  async function handleAdd() {
    if (!sourceValue.trim()) {
      setError("Choose a torrent file or paste a magnet link.");
      return;
    }
    setBusy("add");
    setError(null);
    setSuccess(null);
    try {
      await addTorrent(buildRequest(sourceType, sourceValue, selectedFileIds));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not add this torrent.");
      setBusy(null);
      return;
    }

    setSuccess("Torrent added.");
    try {
      onAdded?.();
      if (windowMode && isTauriRuntime()) {
        await closeAddWindowMode();
      }
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

  function buildRequest(kind: "file" | "magnet", value: string, files: Set<number> | null): AddTorrentRequest {
    const fileCount = preview?.files?.length ?? 0;
    const onlyFiles = files && fileCount > 0 && files.size < fileCount ? Array.from(files).sort((a, b) => a - b) : null;
    return {
      source: {
        kind,
        value: value.trim()
      },
      destination: destination.trim() || null,
      paused,
      overwrite,
      disableTrackers,
      onlyFiles,
      subFolder: subFolder.trim() || null
    };
  }

  return (
    <section className={cn("panel flex h-full min-h-[620px] flex-col overflow-hidden", !windowMode && "max-h-[86vh]")}>
      <div className="flex items-center justify-between border-b px-4 py-3">
        <div>
          <h1 className="text-base font-semibold">Add Torrent</h1>
          <p className="text-xs text-muted-foreground">{preview?.name || "NovaTorrent"}</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="ghost" size="icon" type="button" aria-label="Close add torrent" onClick={handleCancel}>
            <X />
          </Button>
        </div>
      </div>

      <div className="grid flex-1 overflow-hidden lg:grid-cols-[390px_1fr]">
        <div className="space-y-4 overflow-y-auto border-b p-4 lg:border-b-0 lg:border-r">
          <div className="grid grid-cols-2 rounded-md border bg-muted p-1">
            <button
              type="button"
              className={cn("flex h-8 items-center justify-center gap-2 rounded text-sm font-medium", sourceType === "file" && "bg-background shadow-sm")}
              onClick={() => setSourceType("file")}
            >
              <FileUp className="h-4 w-4" />
              File
            </button>
            <button
              type="button"
              className={cn("flex h-8 items-center justify-center gap-2 rounded text-sm font-medium", sourceType === "magnet" && "bg-background shadow-sm")}
              onClick={() => setSourceType("magnet")}
            >
              <Link2 className="h-4 w-4" />
              Magnet
            </button>
          </div>

          {sourceType === "file" ? (
            <div className="space-y-2">
              <Label htmlFor="torrent-file">Torrent file</Label>
              <div className="flex gap-2">
                <Input id="torrent-file" value={torrentPath} onChange={(event) => setTorrentPath(event.target.value)} placeholder="C:/Downloads/file.torrent" />
                <Button type="button" variant="outline" size="icon" aria-label="Choose torrent file" onClick={chooseTorrentFile}>
                  <FileUp />
                </Button>
              </div>
            </div>
          ) : (
            <div className="space-y-2">
              <Label htmlFor="magnet-link">Magnet link</Label>
              <Textarea
                id="magnet-link"
                value={magnet}
                onChange={(event) => setMagnet(event.target.value)}
                placeholder="magnet:?xt=urn:btih:..."
                className="min-h-28"
              />
            </div>
          )}

          {safeSources.length ? (
            <div className="space-y-2 rounded-md border p-3">
              <Label>Safe test torrent</Label>
              {safeSources.map((source) => (
                <button
                  key={source.path}
                  type="button"
                  className="flex w-full items-center justify-between gap-3 rounded-md px-2 py-2 text-left text-sm hover:bg-secondary"
                  onClick={() => applySafeSource(source)}
                >
                  <span className="flex min-w-0 items-center gap-2">
                    <ShieldCheck className="h-4 w-4 shrink-0 text-primary" />
                    <span className="truncate">{source.label}</span>
                  </span>
                  <span className="shrink-0 text-xs tabular-nums text-muted-foreground">{formatBytes(source.payload_size)}</span>
                </button>
              ))}
            </div>
          ) : null}

          <div className="space-y-2">
            <Label htmlFor="destination">Destination</Label>
            <div className="flex gap-2">
              <Input id="destination" value={destination} onChange={(event) => setDestination(event.target.value)} />
              <Button type="button" variant="outline" size="icon" aria-label="Choose destination folder" onClick={chooseDestination}>
                <FolderOpen />
              </Button>
            </div>
          </div>

          <div className="rounded-md border">
            <button
              type="button"
              className="flex w-full items-center justify-between px-3 py-2 text-sm font-medium"
              onClick={() => setAdvancedOpen((value) => !value)}
            >
              <span className="flex items-center gap-2">
                <Settings2 className="h-4 w-4" />
                Advanced
              </span>
              <ChevronDown className={cn("h-4 w-4 transition-transform", advancedOpen && "rotate-180")} />
            </button>
            {advancedOpen ? (
              <div className="space-y-4 border-t p-3">
                <div className="space-y-2">
                  <Label htmlFor="sub-folder">Subfolder</Label>
                  <Input id="sub-folder" value={subFolder} onChange={(event) => setSubFolder(event.target.value)} placeholder="Optional folder name" />
                </div>
                <ToggleRow label="Start paused" checked={paused} onCheckedChange={setPaused} />
                <ToggleRow label="Overwrite existing files" checked={overwrite} onCheckedChange={setOverwrite} />
                <ToggleRow label="Disable trackers" checked={disableTrackers} onCheckedChange={setDisableTrackers} />
              </div>
            ) : null}
          </div>

          {preview ? (
            <div className="grid grid-cols-2 gap-3 rounded-md border p-3 text-sm">
              <div>
                <p className="text-xs text-muted-foreground">Selected</p>
                <p className="font-medium">{formatBytes(selectedSize)}</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Total</p>
                <p className="font-medium">{formatBytes(totalSize)}</p>
              </div>
            </div>
          ) : null}

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

        <div className="flex min-h-0 flex-col">
          <div className="flex items-center justify-between border-b px-4 py-3">
            <div>
              <h2 className="text-sm font-semibold">Files</h2>
              <p className="text-xs text-muted-foreground">{preview?.files?.length ? `${preview.files.length} items` : busy === "preview" ? "Loading file tree" : "Choose a source"}</p>
            </div>
            {preview?.files?.length ? (
              <div className="flex items-center gap-2">
                <Button type="button" variant="ghost" size="sm" onClick={() => setSelectedFileIds(new Set((preview.files ?? []).map((_, index) => index)))}>
                  All
                </Button>
                <Button type="button" variant="ghost" size="sm" onClick={() => setSelectedFileIds(new Set())}>
                  None
                </Button>
              </div>
            ) : null}
          </div>
          <div className="min-h-0 flex-1 overflow-y-auto p-3">
            {preview?.files ? (
              <FileTree files={preview.files} selectedFileIds={selectedFileIds} onSelectionChange={setSelectedFileIds} />
            ) : (
              <div className="flex h-full min-h-72 items-center justify-center rounded-md border border-dashed p-8 text-center text-sm text-muted-foreground">
                {busy === "preview" ? "Loading file tree..." : "Choose a torrent file or safe test torrent."}
              </div>
            )}
          </div>
        </div>
      </div>

      <Separator />
      <div className="flex flex-col gap-2 p-4 sm:flex-row sm:items-center sm:justify-between">
        <div className="text-xs text-muted-foreground">{sourceType === "magnet" ? "Magnet source" : "File source"}</div>
        <div className="flex justify-end gap-2">
          <Button type="button" variant="outline" onClick={handlePreview} disabled={Boolean(busy)}>
            {busy === "preview" ? <Loader2 className="animate-spin" /> : <FileUp />}
            Preview
          </Button>
          <Button type="button" onClick={handleAdd} disabled={Boolean(busy)}>
            {busy === "add" ? <Loader2 className="animate-spin" /> : <Plus />}
            Add Torrent
          </Button>
        </div>
      </div>
    </section>
  );
}

function ToggleRow({
  label,
  checked,
  onCheckedChange
}: {
  label: string;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
}) {
  return (
    <div className="flex items-center justify-between gap-3">
      <Label className="font-normal">{label}</Label>
      <Switch checked={checked} onCheckedChange={onCheckedChange} />
    </div>
  );
}

function StatusLine({
  tone,
  icon,
  children
}: {
  tone: "error" | "success";
  icon: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <div
      className={cn(
        "flex items-start gap-2 rounded-md border px-3 py-2 text-sm",
        tone === "error" ? "border-destructive/30 bg-destructive/10 text-destructive" : "border-primary/25 bg-primary/10 text-primary"
      )}
    >
      {icon}
      <span>{children}</span>
    </div>
  );
}

function parseUrlSource() {
  if (typeof window === "undefined") return null;
  const params = new URLSearchParams(window.location.search);
  return params.get("source") || params.get("magnet") || params.get("file");
}

function parseInitialSource(initialSource?: string | null) {
  const source = initialSource || parseUrlSource();
  return source ? parseDeepLinkSource(source) : null;
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
