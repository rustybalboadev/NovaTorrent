"use client";

import * as React from "react";
import * as ContextMenu from "@radix-ui/react-context-menu";
import * as Dialog from "@radix-ui/react-dialog";
import { useTheme } from "next-themes";
import {
  Activity,
  BarChart3,
  CheckCircle2,
  Clapperboard,
  Download,
  ExternalLink,
  FileCog,
  Magnet,
  Moon,
  Network,
  Pause,
  Play,
  Plus,
  Radar,
  RadioTower,
  Save,
  Search,
  Settings2,
  ScrollText,
  ShieldCheck,
  Sun,
  Trash2,
  Upload,
  X,
} from "lucide-react";
import { AddTorrentPanel } from "@/components/add-torrent-panel";
import { FileTree } from "@/components/file-tree";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import { Switch } from "@/components/ui/switch";
import {
  announceTorrent,
  backendLogFilePath,
  backendLogs,
  deleteTorrent,
  downloadPeerTorrent,
  downloadWebSeedTorrent,
  fetchMetadataTorrent,
  hashTorrentFile,
  listTorrents,
  openAddTorrentWindow,
  openVirusTotalReport,
  pauseTorrent,
  queryDhtTorrent,
  recheckTorrent,
  resolveMagnetTorrent,
  resumeTorrent,
  clearStreamPriority,
  setStreamPriority,
  streamFileAvailability,
  streamFileUrl,
  takePendingOpenSources,
  torrentDetails,
  updateTorrentFiles,
  updateTorrentOptions
} from "@/lib/tauri-api";
import {
  normalizeTorrent,
  type LogEntry,
  type StreamPriorityStatus,
  type TorrentFileAvailability,
  type TorrentFileHash,
  type TorrentRow,
  type UpdateTorrentOptionsRequest
} from "@/lib/torrent-types";
import { cn, formatBytes, formatEta, formatRate, percent } from "@/lib/utils";

const filters = ["All", "Downloading", "Seeding", "Paused", "Complete", "Error"] as const;
const inspectorTabs = ["Status", "General", "Peers", "Trackers", "Web Seeds", "Files", "Security", "Options", "Logs"] as const;
const completedStates = new Set(["Complete", "Seeding", "Seed Ratio Reached"]);
const playableExtensions = new Set(["mp4", "m4v", "mov", "webm", "mkv", "ogv", "avi"]);

type MediaSession = {
  torrentId: string;
  fileIndex: number;
  url: string;
  availability: TorrentFileAvailability;
  priority: StreamPriorityStatus;
};

export function TorrentDashboard() {
  const { resolvedTheme, setTheme } = useTheme();
  const [rows, setRows] = React.useState<TorrentRow[]>([]);
  const [selectedId, setSelectedId] = React.useState<string | null>(null);
  const [filter, setFilter] = React.useState<(typeof filters)[number]>("All");
  const [query, setQuery] = React.useState("");
  const [addOpen, setAddOpen] = React.useState(false);
  const [fileSelections, setFileSelections] = React.useState<Record<string, Set<number>>>({});
  const [error, setError] = React.useState<string | null>(null);
  const [bottomHeight, setBottomHeight] = React.useState(300);
  const [inspectorTab, setInspectorTab] = React.useState<(typeof inspectorTabs)[number]>("Status");
  const [logs, setLogs] = React.useState<LogEntry[]>([]);
  const [logFilePath, setLogFilePath] = React.useState<string | null>(null);
  const [mediaSession, setMediaSession] = React.useState<MediaSession | null>(null);
  const [mediaBusy, setMediaBusy] = React.useState(false);
  const [mediaError, setMediaError] = React.useState<string | null>(null);

  const selected = selectedId ? rows.find((row) => row.id === selectedId) ?? null : null;
  const selectedBackendId = typeof selected?.raw.id === "number" ? selected.raw.id : null;
  const mediaTorrentId = mediaSession?.torrentId ?? null;
  const mediaFileIndex = mediaSession?.fileIndex ?? null;
  const selectedFileIds = React.useMemo(() => {
    if (!selected) return new Set<number>();
    return fileSelections[selected.id] ?? includedFileIds(selected.files);
  }, [fileSelections, selected]);

  React.useEffect(() => {
    void refresh();
    const interval = window.setInterval(refresh, 2200);
    return () => window.clearInterval(interval);
  }, []);

  React.useEffect(() => {
    let disposed = false;

    const loadLogs = async () => {
      try {
        const nextLogs = await backendLogs(selectedBackendId);
        if (!disposed) setLogs(nextLogs);
      } catch {
        undefined;
      }
    };

    void loadLogs();
    const interval = window.setInterval(loadLogs, 3000);
    return () => {
      disposed = true;
      window.clearInterval(interval);
    };
  }, [selectedBackendId]);

  React.useEffect(() => {
    if (!selected) return;
    if (!selected.files.length) {
      void hydrateDetails(selected.id);
    }
  }, [selected]);

  React.useEffect(() => {
    if (mediaTorrentId == null || mediaFileIndex == null) return;
    let disposed = false;
    const loadAvailability = async () => {
      try {
        const availability = await streamFileAvailability(mediaTorrentId, mediaFileIndex);
        if (!disposed) {
          setMediaSession((current) =>
            current && current.torrentId === mediaTorrentId && current.fileIndex === mediaFileIndex
              ? { ...current, availability }
              : current
          );
        }
      } catch {
        undefined;
      }
    };
    const interval = window.setInterval(loadAvailability, 2500);
    return () => {
      disposed = true;
      window.clearInterval(interval);
    };
  }, [mediaTorrentId, mediaFileIndex]);

  React.useEffect(() => {
    let unlisten: (() => void) | undefined;
    import("@tauri-apps/api/event")
      .then(async ({ listen }) => {
        const pending = await takePendingOpenSources();
        pending.forEach((source) => void openAddTorrentWindow(source));
        unlisten = await listen<string>("pending-open-source", (event) => {
          void openAddTorrentWindow(event.payload);
        });
      })
      .catch(() => undefined);
    return () => unlisten?.();
  }, []);

  React.useEffect(() => {
    backendLogFilePath()
      .then(setLogFilePath)
      .catch(() => undefined);
  }, []);

  const filteredRows = rows.filter((row) => {
    const matchesFilter = filter === "All" || row.state === filter;
    const text = `${row.name} ${row.hash} ${row.outputFolder}`.toLowerCase();
    return matchesFilter && text.includes(query.toLowerCase());
  });

  const totals = rows.reduce(
    (acc, row) => {
      acc.down += row.downloadSpeed;
      acc.up += row.uploadSpeed;
      acc.active += row.state === "Downloading" || row.state === "Live" || row.state === "Seeding" ? 1 : 0;
      acc.complete += completedStates.has(row.state) ? 1 : 0;
      return acc;
    },
    { down: 0, up: 0, active: 0, complete: 0 }
  );

  async function refresh() {
    try {
      const response = await listTorrents();
      const nextRows = response.torrents.map(normalizeTorrent);
      setRows((currentRows) => mergeRows(currentRows, nextRows));
      setSelectedId((current) => (current && nextRows.some((row) => row.id === current) ? current : null));
      setMediaSession((current) =>
        current && nextRows.some((row) => row.id === current.torrentId) ? current : null
      );
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not list torrents.");
    }
  }

  async function hydrateDetails(id: string) {
    try {
      const details = await torrentDetails(id);
      const hydrated = normalizeTorrent(details);
      setRows((currentRows) => currentRows.map((row) => (row.id === id ? { ...row, ...hydrated } : row)));
    } catch {
      undefined;
    }
  }

  async function openAdd() {
    const opened = await openAddTorrentWindow();
    if (!opened) setAddOpen(true);
  }

  async function runAction(action: "pause" | "resume" | "announce" | "dht" | "resolve" | "webseed" | "peers" | "metadata" | "recheck" | "delete" | "deleteFiles", row = selected) {
    if (!row) return;
    try {
      if (action === "pause") await pauseTorrent(row.id);
      if (action === "resume") await resumeTorrent(row.id);
      if (action === "announce") await announceTorrent(row.id);
      if (action === "dht") await queryDhtTorrent(row.id);
      if (action === "resolve") await resolveMagnetTorrent(row.id);
      if (action === "webseed") await downloadWebSeedTorrent(row.id);
      if (action === "peers") await downloadPeerTorrent(row.id);
      if (action === "metadata") await fetchMetadataTorrent(row.id);
      if (action === "recheck") await recheckTorrent(row.id);
      if (action === "delete") await deleteTorrent(row.id, false);
      if (action === "deleteFiles") await deleteTorrent(row.id, true);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Torrent action failed.");
    }
  }

  async function handleFileSelection(next: Set<number>) {
    if (!selected) return;
    setFileSelections((current) => ({ ...current, [selected.id]: next }));
    setRows((currentRows) =>
      currentRows.map((row) =>
        row.id === selected.id
          ? {
              ...row,
              files: row.files.map((file, index) => ({ ...file, included: next.has(index) }))
            }
          : row
      )
    );
    try {
      await updateTorrentFiles(selected.id, Array.from(next).sort((a, b) => a - b));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not update file selection.");
    }
  }

  async function handleOptionsChange(request: UpdateTorrentOptionsRequest) {
    if (!selected) return;
    try {
      await updateTorrentOptions(selected.id, request);
      await hydrateDetails(selected.id);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not update torrent options.");
      throw err;
    }
  }

  async function handlePlayFile(row: TorrentRow, fileIndex: number, playheadOffset = 0) {
    setMediaBusy(true);
    setMediaError(null);
    try {
      const priority = await setStreamPriority(row.id, {
        fileIndex,
        playheadOffset,
        urgentBytes: null,
        lookaheadBytes: null
      });
      const url = await streamFileUrl(row.id, fileIndex);
      const availability = await streamFileAvailability(row.id, fileIndex);
      setMediaSession({ torrentId: row.id, fileIndex, url, priority, availability });
      setSelectedId(row.id);
      setInspectorTab("Files");
      setError(null);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Could not prepare this file for streaming.";
      setMediaError(message);
      setError(message);
    } finally {
      setMediaBusy(false);
    }
  }

  async function handleClearStream() {
    if (!mediaSession) return;
    const torrentId = mediaSession.torrentId;
    setMediaBusy(true);
    setMediaError(null);
    try {
      await clearStreamPriority(torrentId);
      setMediaSession(null);
    } catch (err) {
      setMediaError(err instanceof Error ? err.message : "Could not stop stream priority.");
    } finally {
      setMediaBusy(false);
    }
  }

  return (
    <main className="surface-grid min-h-screen p-3 text-foreground">
      <div className="mx-auto flex min-h-[calc(100vh-1.5rem)] flex-col gap-3">
        <header className="panel flex flex-col gap-3 px-4 py-3 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-md bg-primary text-primary-foreground">
              <Activity className="h-5 w-5" />
            </div>
            <div>
              <h1 className="text-lg font-semibold">NovaTorrent</h1>
              <p className="text-xs text-muted-foreground">{rows.length} torrents</p>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <StatPill icon={<Download />} label={formatRate(totals.down)} />
            <StatPill icon={<Upload />} label={formatRate(totals.up)} />
            <Button
              variant="outline"
              size="icon"
              aria-label="Toggle theme"
              onClick={() => setTheme(resolvedTheme === "dark" ? "light" : "dark")}
            >
              <Moon className="dark:hidden" />
              <Sun className="hidden dark:block" />
            </Button>
            <Button onClick={openAdd}>
              <Plus />
              Add Torrent
            </Button>
          </div>
        </header>

        {error ? (
          <div className="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">{error}</div>
        ) : null}

        <div className="flex min-h-0 flex-1 flex-col">
          <section className="panel flex min-h-0 flex-1 flex-col overflow-hidden">
            <div className="flex flex-col gap-3 border-b p-3 lg:flex-row lg:items-center lg:justify-between">
              <div className="flex flex-wrap gap-1">
                {filters.map((item) => (
                  <button
                    key={item}
                    type="button"
                    className={cn(
                      "h-8 rounded-md px-3 text-sm font-medium text-muted-foreground hover:bg-secondary hover:text-foreground",
                      filter === item && "bg-secondary text-foreground"
                    )}
                    onClick={() => setFilter(item)}
                  >
                    {item}
                  </button>
                ))}
              </div>
              <div className="relative w-full lg:w-72">
                <Search className="pointer-events-none absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
                <Input value={query} onChange={(event) => setQuery(event.target.value)} className="pl-8" placeholder="Search torrents" />
              </div>
            </div>

            <div className="min-h-0 flex-1 overflow-auto">
              <div className="min-w-[920px]">
                <div className="grid h-9 grid-cols-[minmax(280px,1.4fr)_110px_96px_96px_96px_86px_90px] items-center border-b px-3 text-xs font-medium uppercase text-muted-foreground">
                  <span>Name</span>
                  <span>Status</span>
                  <span>Progress</span>
                  <span>Down</span>
                  <span>Up</span>
                  <span>ETA</span>
                  <span className="text-right">Size</span>
                </div>
                {filteredRows.length ? (
                  filteredRows.map((row) => (
                    <TorrentContextMenu
                      key={row.id}
                      row={row}
                      onAction={runAction}
                      onOptions={() => {
                        setSelectedId(row.id);
                        setInspectorTab("Options");
                      }}
                    >
                      <button
                        type="button"
                        className={cn(
                          "grid min-h-16 w-full grid-cols-[minmax(280px,1.4fr)_110px_96px_96px_96px_86px_90px] items-center gap-0 border-b px-3 text-left text-sm transition-colors hover:bg-secondary/60",
                          selected?.id === row.id && "bg-primary/8"
                        )}
                        onClick={() => setSelectedId(row.id)}
                        onDoubleClick={() => void hydrateDetails(row.id)}
                      >
                        <div className="min-w-0 pr-4">
                          <div className="flex items-center gap-2">
                            <span className="truncate font-medium">{row.name}</span>
                            {completedStates.has(row.state) ? <CheckCircle2 className="h-4 w-4 shrink-0 text-primary" /> : null}
                          </div>
                          <div className="truncate-path mt-1 text-xs text-muted-foreground">{row.outputFolder}</div>
                        </div>
                        <Badge variant={statusVariant(row.state)} className="w-fit">
                          {row.state}
                        </Badge>
                        <div className="pr-4">
                          <Progress value={percent(row.progress)} />
                          <span className="mt-1 block text-xs tabular-nums text-muted-foreground">{percent(row.progress).toFixed(1)}%</span>
                        </div>
                        <span className="tabular-nums">{formatRate(row.downloadSpeed)}</span>
                        <span className="tabular-nums">{formatRate(row.uploadSpeed)}</span>
                        <span className="tabular-nums text-muted-foreground">{formatEta(row.eta)}</span>
                        <span className="text-right tabular-nums">{formatBytes(row.total)}</span>
                      </button>
                    </TorrentContextMenu>
                  ))
                ) : (
                  <div className="flex h-72 items-center justify-center text-sm text-muted-foreground">No torrents match this view.</div>
                )}
              </div>
            </div>

            {selected ? (
              <TorrentInspector
                height={bottomHeight}
                onResize={setBottomHeight}
                selected={selected}
                selectedFileIds={selectedFileIds}
                tab={inspectorTab}
                onTabChange={setInspectorTab}
                logs={logs}
                logFilePath={logFilePath}
                onFileSelectionChange={handleFileSelection}
                mediaSession={mediaSession?.torrentId === selected.id ? mediaSession : null}
                mediaBusy={mediaBusy}
                mediaError={mediaSession?.torrentId === selected.id ? mediaError : null}
                onPlayFile={(fileIndex) => void handlePlayFile(selected, fileIndex)}
                onClearStream={() => void handleClearStream()}
                onOptionsChange={handleOptionsChange}
                onRefresh={() => void hydrateDetails(selected.id)}
              />
            ) : null}
          </section>
        </div>
      </div>

      <Dialog.Root open={addOpen} onOpenChange={setAddOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-40 bg-background/70 backdrop-blur-sm" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-50 h-[88vh] w-[min(1120px,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 outline-none">
            <AddTorrentPanel
              onAdded={() => {
                setAddOpen(false);
                void refresh();
              }}
              onCancel={() => setAddOpen(false)}
            />
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
    </main>
  );
}

function TorrentContextMenu({
  row,
  children,
  onAction,
  onOptions
}: {
  row: TorrentRow;
  children: React.ReactNode;
  onAction: (action: "pause" | "resume" | "announce" | "dht" | "resolve" | "webseed" | "peers" | "metadata" | "recheck" | "delete" | "deleteFiles", row: TorrentRow) => void;
  onOptions: () => void;
}) {
  return (
    <ContextMenu.Root>
      <ContextMenu.Trigger asChild>{children}</ContextMenu.Trigger>
      <ContextMenu.Portal>
        <ContextMenu.Content className="z-50 min-w-48 overflow-hidden rounded-md border bg-popover p-1 text-popover-foreground shadow-md">
          <MenuItem icon={<Pause />} onSelect={() => onAction("pause", row)}>
            Pause
          </MenuItem>
          <MenuItem icon={<Play />} onSelect={() => onAction("resume", row)}>
            Continue
          </MenuItem>
          <MenuItem icon={<RadioTower />} onSelect={() => onAction("announce", row)}>
            Announce trackers
          </MenuItem>
          <MenuItem icon={<Radar />} onSelect={() => onAction("dht", row)}>
            Query DHT
          </MenuItem>
          <MenuItem icon={<Magnet />} onSelect={() => onAction("resolve", row)}>
            Resolve magnet
          </MenuItem>
          <MenuItem icon={<Download />} onSelect={() => onAction("webseed", row)}>
            Download webseed
          </MenuItem>
          <MenuItem icon={<Network />} onSelect={() => onAction("peers", row)}>
            Download from peers
          </MenuItem>
          <MenuItem icon={<FileCog />} onSelect={() => onAction("metadata", row)}>
            Fetch metadata
          </MenuItem>
          <MenuItem icon={<CheckCircle2 />} onSelect={() => onAction("recheck", row)}>
            Recheck files
          </MenuItem>
          <MenuItem icon={<Settings2 />} onSelect={onOptions}>
            Options
          </MenuItem>
          <ContextMenu.Separator className="my-1 h-px bg-border" />
          <MenuItem icon={<Trash2 />} tone="danger" onSelect={() => onAction("delete", row)}>
            Remove
          </MenuItem>
          <MenuItem icon={<Trash2 />} tone="danger" onSelect={() => onAction("deleteFiles", row)}>
            Delete Files
          </MenuItem>
        </ContextMenu.Content>
      </ContextMenu.Portal>
    </ContextMenu.Root>
  );
}

function MenuItem({
  icon,
  tone,
  children,
  onSelect
}: {
  icon: React.ReactNode;
  tone?: "danger";
  children: React.ReactNode;
  onSelect: () => void;
}) {
  return (
    <ContextMenu.Item
      className={cn(
        "flex h-8 cursor-default select-none items-center gap-2 rounded px-2 text-sm outline-none data-[highlighted]:bg-secondary",
        tone === "danger" && "text-destructive data-[highlighted]:bg-destructive/10"
      )}
      onSelect={onSelect}
    >
      <span className="[&_svg]:h-4 [&_svg]:w-4">{icon}</span>
      {children}
    </ContextMenu.Item>
  );
}

function TorrentInspector({
  height,
  onResize,
  selected,
  selectedFileIds,
  tab,
  onTabChange,
  logs,
  logFilePath,
  onFileSelectionChange,
  mediaSession,
  mediaBusy,
  mediaError,
  onPlayFile,
  onClearStream,
  onOptionsChange,
  onRefresh
}: {
  height: number;
  onResize: (height: number) => void;
  selected: TorrentRow;
  selectedFileIds: Set<number>;
  tab: (typeof inspectorTabs)[number];
  onTabChange: (tab: (typeof inspectorTabs)[number]) => void;
  logs: LogEntry[];
  logFilePath: string | null;
  onFileSelectionChange: (selected: Set<number>) => void;
  mediaSession: MediaSession | null;
  mediaBusy: boolean;
  mediaError: string | null;
  onPlayFile: (fileIndex: number) => void;
  onClearStream: () => void;
  onOptionsChange: (request: UpdateTorrentOptionsRequest) => Promise<void>;
  onRefresh: () => void;
}) {
  const startResize = (event: React.PointerEvent<HTMLDivElement>) => {
    event.preventDefault();
    const startY = event.clientY;
    const startHeight = height;

    const move = (moveEvent: PointerEvent) => {
      const next = Math.max(210, Math.min(560, startHeight - (moveEvent.clientY - startY)));
      onResize(next);
    };
    const up = () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  };

  return (
    <section className="flex shrink-0 flex-col overflow-hidden border-t bg-card" style={{ height }}>
      <div
        className="flex h-3 cursor-row-resize items-center justify-center border-b bg-secondary/50"
        onPointerDown={startResize}
        title="Resize details pane"
      >
        <div className="h-1 w-10 rounded-full bg-border" />
      </div>
      <div className="flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2">
        <div className="flex min-w-0 items-center gap-3">
          <div className="min-w-0">
            <h2 className="truncate text-sm font-semibold">{selected.name}</h2>
            <p className="truncate-path text-xs text-muted-foreground">{selected.hash}</p>
          </div>
        </div>
        <div className="flex flex-wrap gap-1">
          {inspectorTabs.map((item) => (
            <button
              key={item}
              type="button"
              className={cn(
                "h-8 rounded-md px-2.5 text-xs font-medium text-muted-foreground hover:bg-secondary hover:text-foreground",
                tab === item && "bg-secondary text-foreground"
              )}
              onClick={() => onTabChange(item)}
            >
              {item}
            </button>
          ))}
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-auto p-3">
        <InspectorTab
          tab={tab}
          selected={selected}
          selectedFileIds={selectedFileIds}
          logs={logs}
          logFilePath={logFilePath}
          onFileSelectionChange={onFileSelectionChange}
          mediaSession={mediaSession}
          mediaBusy={mediaBusy}
          mediaError={mediaError}
          onPlayFile={onPlayFile}
          onClearStream={onClearStream}
          onOptionsChange={onOptionsChange}
          onRefresh={onRefresh}
        />
      </div>
    </section>
  );
}

function InspectorTab({
  tab,
  selected,
  selectedFileIds,
  logs,
  logFilePath,
  onFileSelectionChange,
  mediaSession,
  mediaBusy,
  mediaError,
  onPlayFile,
  onClearStream,
  onOptionsChange,
  onRefresh
}: {
  tab: (typeof inspectorTabs)[number];
  selected: TorrentRow;
  selectedFileIds: Set<number>;
  logs: LogEntry[];
  logFilePath: string | null;
  onFileSelectionChange: (selected: Set<number>) => void;
  mediaSession: MediaSession | null;
  mediaBusy: boolean;
  mediaError: string | null;
  onPlayFile: (fileIndex: number) => void;
  onClearStream: () => void;
  onOptionsChange: (request: UpdateTorrentOptionsRequest) => Promise<void>;
  onRefresh: () => void;
}) {
  if (tab === "Status") {
    return (
      <div className="grid gap-3 lg:grid-cols-[360px_1fr]">
        <div className="space-y-3">
          <div className="grid grid-cols-3 gap-2">
            <Metric icon={<BarChart3 />} label="Progress" value={`${percent(selected.progress).toFixed(1)}%`} />
            <Metric icon={<Download />} label="Down" value={formatRate(selected.downloadSpeed)} />
            <Metric icon={<Upload />} label="Up" value={formatRate(selected.uploadSpeed)} />
          </div>
          <Progress value={percent(selected.progress)} className="h-2.5" />
          <div className="flex justify-between text-xs text-muted-foreground">
            <span>{formatBytes(selected.downloaded)}</span>
            <span>{formatBytes(selected.total)}</span>
          </div>
        </div>
        <div className="grid grid-cols-2 gap-3 text-sm md:grid-cols-4">
          <InfoLine label="Status" value={selected.state} />
          <InfoLine label="ETA" value={formatEta(selected.eta)} />
          <InfoLine label="Uploaded" value={formatBytes(selected.uploaded)} />
          <InfoLine label="Ratio" value={formatRatio(selected.general?.ratio)} />
          <InfoLine label="Active" value={formatDuration(selected.general?.active_time_seconds)} />
          <InfoLine label="Seeding" value={formatDuration(selected.general?.seeding_time_seconds)} />
          <InfoLine label="Trackers" value={String(selected.trackers.length)} />
          <InfoLine label="Connected Peers" value={String(selected.peers.length || selected.peerCount || 0)} />
        </div>
      </div>
    );
  }

  if (tab === "General") {
    return (
      <div className="grid gap-3 text-sm md:grid-cols-3">
        <InfoLine label="Name" value={selected.name} />
        <InfoLine label="Hash" value={selected.hash} />
        <InfoLine label="Save Path" value={selected.general?.save_path || selected.outputFolder} />
        <InfoLine label="Total Size" value={formatBytes(selected.general?.total_size ?? selected.total)} />
        <InfoLine label="Files" value={String(selected.general?.file_count ?? selected.files.length)} />
        <InfoLine label="Pieces" value={`${selected.general?.piece_count ?? "-"} x ${formatBytes(selected.general?.piece_size ?? 0)}`} />
        <InfoLine label="Private" value={selected.general?.private ? "Yes" : "No"} />
        <InfoLine label="Created By" value={selected.general?.created_by || "-"} />
        <InfoLine label="Created" value={formatUnixDate(selected.general?.creation_date)} />
        <InfoLine label="Comment" value={selected.general?.comment || "-"} wide />
      </div>
    );
  }

  if (tab === "Peers") {
    return selected.peers.length ? (
      <DataTable
        columns={["Address", "Client", "Progress", "Down", "Up", "Connection"]}
        rows={selected.peers.map((peer) => [
          `${peer.address}:${peer.port}`,
          peer.client || "-",
          `${percent(peer.progress * 100).toFixed(1)}%`,
          formatRate(peer.download_speed),
          formatRate(peer.upload_speed),
          peer.connection
        ])}
      />
    ) : (
      <EmptyTab icon={<Network />} text="No connected peers yet." />
    );
  }

  if (tab === "Trackers") {
    return selected.trackers.length ? (
      <DataTable
        columns={["Tracker", "Status", "Seeders", "Leechers", "Next Announce", "Message"]}
        rows={selected.trackers.map((tracker) => [
          tracker.url,
          tracker.state,
          tracker.seeders == null ? "-" : String(tracker.seeders),
          tracker.leechers == null ? "-" : String(tracker.leechers),
          tracker.next_announce_seconds == null ? "-" : formatDuration(tracker.next_announce_seconds),
          tracker.message || "-"
        ])}
      />
    ) : (
      <EmptyTab icon={<RadioTower />} text="No trackers are configured for this torrent." />
    );
  }

  if (tab === "Web Seeds") {
    return selected.webSeeds.length ? (
      <DataTable
        columns={["Web Seed", "Status", "Downloaded", "Message"]}
        rows={selected.webSeeds.map((seed) => [
          seed.url,
          seed.state,
          formatBytes(seed.bytes_downloaded),
          seed.message || "-"
        ])}
      />
    ) : (
      <EmptyTab icon={<Download />} text="No web seeds are configured for this torrent." />
    );
  }

  if (tab === "Files") {
    return (
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-sm font-semibold">
            <FileCog className="h-4 w-4" />
            Content
          </div>
          <Button type="button" variant="outline" size="sm" onClick={onRefresh}>
            Refresh
          </Button>
        </div>
        <TorrentMediaPanel
          selected={selected}
          mediaSession={mediaSession}
          mediaBusy={mediaBusy}
          mediaError={mediaError}
          onClearStream={onClearStream}
        />
        <FileTree
          files={selected.files}
          selectedFileIds={selectedFileIds}
          onSelectionChange={onFileSelectionChange}
          onPlayFile={onPlayFile}
          activeMediaFileIndex={mediaSession?.fileIndex ?? null}
          compact
        />
      </div>
    );
  }

  if (tab === "Security") {
    return <TorrentSecurityPanel key={selected.id} selected={selected} />;
  }

  if (tab === "Options") {
    return <TorrentOptionsEditor key={selected.id} selected={selected} onSave={onOptionsChange} />;
  }

  return (
    <div className="space-y-2">
      {logFilePath ? (
        <div className="flex items-center gap-2 rounded-md border bg-background px-2 py-1.5 text-xs text-muted-foreground">
          <ScrollText className="h-4 w-4 shrink-0" />
          <span className="truncate">{logFilePath}</span>
        </div>
      ) : null}
      {logs.length ? (
        <div className="space-y-1 font-mono text-xs">
          {logs.map((entry) => (
            <div key={entry.id} className="grid grid-cols-[86px_64px_120px_1fr] gap-2 rounded px-2 py-1 hover:bg-secondary/70">
              <span className="text-muted-foreground">{formatLogTime(entry.timestamp_ms)}</span>
              <span className={cn("font-semibold", logTone(entry.level))}>{entry.level}</span>
              <span className="truncate text-muted-foreground">{entry.scope}</span>
              <span>{entry.message}</span>
            </div>
          ))}
        </div>
      ) : (
        <EmptyTab icon={<ScrollText />} text="No backend logs yet." />
      )}
    </div>
  );
}

function TorrentMediaPanel({
  selected,
  mediaSession,
  mediaBusy,
  mediaError,
  onClearStream
}: {
  selected: TorrentRow;
  mediaSession: MediaSession | null;
  mediaBusy: boolean;
  mediaError: string | null;
  onClearStream: () => void;
}) {
  const playableCount = selected.files.filter(isPlayableMedia).length;
  if (!playableCount && !mediaSession && !mediaError) return null;

  const availability = mediaSession?.availability;
  const bufferPercent = availability && availability.length > 0 ? (availability.verified_bytes / availability.length) * 100 : 0;

  return (
    <div className="rounded-md border bg-background">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2">
        <div className="flex min-w-0 items-center gap-2">
          <Clapperboard className="h-4 w-4 shrink-0 text-primary" />
          <div className="min-w-0">
            <div className="truncate text-sm font-semibold">
              {availability ? availability.name : `${playableCount} playable ${playableCount === 1 ? "file" : "files"}`}
            </div>
            <div className="text-xs text-muted-foreground">
              {availability
                ? `${formatBytes(availability.verified_bytes)} verified of ${formatBytes(availability.length)}`
                : "No active media file"}
            </div>
          </div>
        </div>
        {mediaSession ? (
          <Button type="button" variant="outline" size="sm" disabled={mediaBusy} onClick={onClearStream}>
            <X />
            Stop
          </Button>
        ) : null}
      </div>
      {availability ? (
        <div className="space-y-2 px-3 py-2">
          {mediaSession.url ? (
            <video
              key={mediaSession.url}
              className="aspect-video w-full rounded-md border bg-black"
              controls
              preload="metadata"
              src={mediaSession.url}
            />
          ) : null}
          <Progress value={percent(bufferPercent)} className="h-2" />
          <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
            <span>{availability.ranges.length} verified range{availability.ranges.length === 1 ? "" : "s"}</span>
            <span>{mediaSession.priority.total_priority_pieces} priority piece{mediaSession.priority.total_priority_pieces === 1 ? "" : "s"}</span>
            <span>{availability.complete ? "Ready" : mediaBusy ? "Updating" : "Buffering"}</span>
          </div>
        </div>
      ) : null}
      {mediaError ? <div className="border-t px-3 py-2 text-sm text-destructive">{mediaError}</div> : null}
    </div>
  );
}

function TorrentSecurityPanel({ selected }: { selected: TorrentRow }) {
  const [reports, setReports] = React.useState<Record<number, TorrentFileHash>>({});
  const [hashing, setHashing] = React.useState<number | null>(null);
  const [securityError, setSecurityError] = React.useState<string | null>(null);

  async function checkFile(fileIndex: number) {
    setHashing(fileIndex);
    setSecurityError(null);
    try {
      const report = await hashTorrentFile(selected.id, fileIndex);
      setReports((current) => ({ ...current, [fileIndex]: report }));
    } catch (err) {
      setSecurityError(err instanceof Error ? err.message : "Could not hash torrent file.");
    } finally {
      setHashing(null);
    }
  }

  async function openReport(report: TorrentFileHash) {
    setSecurityError(null);
    try {
      await openVirusTotalReport(report.sha256);
    } catch (err) {
      setSecurityError(err instanceof Error ? err.message : "Could not open VirusTotal report.");
    }
  }

  const includedFiles = selected.files
    .map((file, fileIndex) => ({ file, fileIndex }))
    .filter(({ file }) => file.included);

  return (
    <div className="space-y-3">
      <div className="flex items-start gap-2 border-b pb-3 text-sm text-muted-foreground">
        <ShieldCheck className="mt-0.5 h-4 w-4 shrink-0 text-primary" />
        <p>SHA-256 is computed locally. Opening a report shares only the hash with VirusTotal; NovaTorrent never uploads the file.</p>
      </div>
      {securityError ? <p className="text-sm text-destructive">{securityError}</p> : null}
      {includedFiles.length ? (
        <div className="divide-y rounded-md border bg-background">
          {includedFiles.map(({ file, fileIndex }) => {
            const report = reports[fileIndex];
            return (
              <div key={fileIndex} className="flex flex-wrap items-center gap-3 p-3">
                <div className="min-w-48 flex-1">
                  <div className="truncate text-sm font-medium">{file.name}</div>
                  <div className="text-xs text-muted-foreground">{formatBytes(file.length)}</div>
                  {report ? <div className="mt-1 break-all font-mono text-[11px] text-muted-foreground">{report.sha256}</div> : null}
                </div>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  disabled={hashing !== null || !selected.raw.stats?.finished}
                  onClick={() => void checkFile(fileIndex)}
                >
                  <ShieldCheck />
                  {hashing === fileIndex ? "Hashing..." : report ? "Hash again" : "Check hash"}
                </Button>
                {report ? (
                  <Button type="button" size="sm" onClick={() => void openReport(report)}>
                    <ExternalLink />
                    Open report
                  </Button>
                ) : null}
              </div>
            );
          })}
        </div>
      ) : (
        <EmptyTab icon={<ShieldCheck />} text="No selected files are available for reputation checks." />
      )}
    </div>
  );
}

function TorrentOptionsEditor({
  selected,
  onSave
}: {
  selected: TorrentRow;
  onSave: (request: UpdateTorrentOptionsRequest) => Promise<void>;
}) {
  const options = selected.options;
  const [connections, setConnections] = React.useState(
    options?.max_connections == null ? "" : String(options.max_connections)
  );
  const [downloadLimit, setDownloadLimit] = React.useState(
    options?.max_download_speed == null ? "" : String(Math.round(options.max_download_speed / 1024))
  );
  const [uploadLimit, setUploadLimit] = React.useState(
    options?.max_upload_speed == null ? "" : String(Math.round(options.max_upload_speed / 1024))
  );
  const [seedRatio, setSeedRatio] = React.useState(
    options?.seed_ratio_limit == null ? "" : String(options.seed_ratio_limit)
  );
  const [sequential, setSequential] = React.useState(Boolean(options?.sequential_download));
  const [saving, setSaving] = React.useState(false);
  const [formError, setFormError] = React.useState<string | null>(null);

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSaving(true);
    setFormError(null);
    try {
      await onSave({
        maxConnections: parseOptionalLimit(connections, "Max connections", 500),
        maxDownloadSpeed: scaleOptionalLimit(downloadLimit, "Download limit", 1024),
        maxUploadSpeed: scaleOptionalLimit(uploadLimit, "Upload limit", 1024),
        sequentialDownload: sequential,
        seedRatioLimit: parseOptionalRatio(seedRatio)
      });
    } catch (err) {
      setFormError(err instanceof Error ? err.message : "Could not save torrent options.");
    } finally {
      setSaving(false);
    }
  }

  return (
    <form className="space-y-4" onSubmit={submit}>
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <OptionField
          id="max-connections"
          label="Max connections"
          value={connections}
          onChange={setConnections}
          placeholder="Automatic"
          min={1}
          max={500}
          step={1}
        />
        <OptionField
          id="download-limit"
          label="Download limit (KiB/s)"
          value={downloadLimit}
          onChange={setDownloadLimit}
          placeholder="Unlimited"
          min={1}
          step={1}
        />
        <OptionField
          id="upload-limit"
          label="Upload limit (KiB/s)"
          value={uploadLimit}
          onChange={setUploadLimit}
          placeholder="Unlimited"
          min={1}
          step={1}
        />
        <OptionField
          id="seed-ratio"
          label="Seed ratio"
          value={seedRatio}
          onChange={setSeedRatio}
          placeholder="Unlimited"
          min={0}
          max={1000}
          step={0.1}
        />
      </div>
      <div className="flex flex-wrap items-center justify-between gap-3 border-t pt-3">
        <div className="flex flex-wrap items-center gap-x-5 gap-y-2">
          <label className="flex items-center gap-2 text-sm font-medium" htmlFor="sequential-download">
            <Switch id="sequential-download" checked={sequential} onCheckedChange={setSequential} />
            Sequential download
          </label>
          <span className="text-xs text-muted-foreground">
            Storage: {options?.overwrite ? "overwrite" : "protect existing"} | Trackers: {options?.disable_trackers ? "off" : "on"}
          </span>
        </div>
        <Button type="submit" size="sm" disabled={saving}>
          <Save className="h-4 w-4" />
          {saving ? "Saving" : "Save changes"}
        </Button>
      </div>
      {formError ? <p className="text-sm text-destructive">{formError}</p> : null}
    </form>
  );
}

function OptionField({
  id,
  label,
  value,
  onChange,
  placeholder,
  min,
  max,
  step
}: {
  id: string;
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
  min: number;
  max?: number;
  step: number;
}) {
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id} className="text-xs text-muted-foreground">
        {label}
      </Label>
      <Input
        id={id}
        type="number"
        inputMode="decimal"
        value={value}
        placeholder={placeholder}
        min={min}
        max={max}
        step={step}
        onChange={(event) => onChange(event.target.value)}
      />
    </div>
  );
}

function parseOptionalLimit(value: string, label: string, max?: number) {
  const trimmed = value.trim();
  if (!trimmed) return null;
  const parsed = Number(trimmed);
  if (!Number.isInteger(parsed) || parsed <= 0 || (max != null && parsed > max)) {
    throw new Error(`${label} must be a whole number${max == null ? " above zero" : ` from 1 to ${max}`}.`);
  }
  return parsed;
}

function scaleOptionalLimit(value: string, label: string, multiplier: number) {
  const parsed = parseOptionalLimit(value, label);
  return parsed == null ? null : parsed * multiplier;
}

function parseOptionalRatio(value: string) {
  const trimmed = value.trim();
  if (!trimmed) return null;
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1000) {
    throw new Error("Seed ratio must be between 0 and 1000.");
  }
  return parsed;
}

function DataTable({ columns, rows }: { columns: string[]; rows: string[][] }) {
  return (
    <div className="overflow-auto rounded-md border">
      <table className="w-full min-w-[720px] text-left text-sm">
        <thead className="bg-secondary text-xs uppercase text-muted-foreground">
          <tr>
            {columns.map((column) => (
              <th key={column} className="px-3 py-2 font-medium">
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={index} className="border-t">
              {row.map((cell, cellIndex) => (
                <td key={cellIndex} className="max-w-80 truncate px-3 py-2">
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function EmptyTab({ icon, text }: { icon: React.ReactNode; text: string }) {
  return (
    <div className="flex h-full min-h-32 items-center justify-center gap-2 text-sm text-muted-foreground">
      <span className="[&_svg]:h-4 [&_svg]:w-4">{icon}</span>
      {text}
    </div>
  );
}

function StatPill({ icon, label }: { icon: React.ReactNode; label: string }) {
  return (
    <div className="flex h-9 items-center gap-2 rounded-md border bg-background px-3 text-sm">
      <span className="text-primary [&_svg]:h-4 [&_svg]:w-4">{icon}</span>
      <span className="tabular-nums">{label}</span>
    </div>
  );
}

function Metric({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return (
    <div className="rounded-md border bg-background p-2">
      <div className="flex items-center gap-1 text-xs text-muted-foreground">
        <span className="[&_svg]:h-3.5 [&_svg]:w-3.5">{icon}</span>
        {label}
      </div>
      <div className="mt-1 truncate text-sm font-semibold tabular-nums">{value}</div>
    </div>
  );
}

function InfoLine({ label, value, wide }: { label: string; value: string; wide?: boolean }) {
  return (
    <div className={cn(wide && "col-span-2")}>
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="truncate text-sm font-medium">{value}</div>
    </div>
  );
}

function formatRatio(value?: number | null) {
  if (value == null || !Number.isFinite(value)) return "0.00";
  return value.toFixed(2);
}

function formatDuration(seconds?: number | null) {
  if (!seconds || seconds <= 0) return "-";
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (hours >= 24) return `${Math.floor(hours / 24)}d ${hours % 24}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

function formatUnixDate(seconds?: number | null) {
  if (!seconds) return "-";
  return new Date(seconds * 1000).toLocaleString();
}

function formatLogTime(ms: number) {
  return new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function logTone(level: LogEntry["level"]) {
  if (level === "Error") return "text-destructive";
  if (level === "Warn") return "text-accent";
  if (level === "Debug") return "text-muted-foreground";
  return "text-primary";
}

function isPlayableMedia(file: { name: string; components?: string[] }) {
  const candidate = file.components?.at(-1) ?? file.name;
  const extension = candidate.split(".").pop()?.toLowerCase();
  return Boolean(extension && playableExtensions.has(extension));
}

function statusVariant(state: string): React.ComponentProps<typeof Badge>["variant"] {
  if (state === "Complete" || state === "Seeding") return "default";
  if (state === "Error") return "destructive";
  if (state === "Paused") return "secondary";
  if (state === "Seed Ratio Reached") return "secondary";
  if (state === "Queued") return "outline";
  return "warning";
}

function mergeRows(currentRows: TorrentRow[], nextRows: TorrentRow[]) {
  const currentById = new Map(currentRows.map((row) => [row.id, row]));
  return nextRows.map((row) => {
    const current = currentById.get(row.id);
    if (!current?.files.length || row.files.length) return row;
    return { ...row, files: current.files };
  });
}

function includedFileIds(files: TorrentRow["files"]) {
  return new Set(files.map((file, index) => (file.included ? index : -1)).filter((index) => index >= 0));
}
