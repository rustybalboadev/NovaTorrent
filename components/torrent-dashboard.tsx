"use client";

import * as React from "react";
import Image from "next/image";
import * as ContextMenu from "@radix-ui/react-context-menu";
import * as Dialog from "@radix-ui/react-dialog";
import {
  BarChart3,
  CheckCircle2,
  ChevronDown,
  Clapperboard,
  Download,
  FileCog,
  FolderOpen,
  Loader2,
  Moon,
  Network,
  Pause,
  Play,
  Plus,
  RadioTower,
  Save,
  Search,
  Settings2,
  ScrollText,
  Sun,
  Trash2,
  Upload,
  X,
} from "lucide-react";
import { AddTorrentPanel } from "@/components/add-torrent-panel";
import { FileTree } from "@/components/file-tree";
import { useTheme } from "@/components/theme-provider";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import { Switch } from "@/components/ui/switch";
import {
  backendLogFilePath,
  backendLogsAfter,
  deleteTorrent,
  listTorrentSummaries,
  openAddTorrentWindow,
  openTorrentFolder,
  openMediaWindow,
  pauseTorrent,
  recheckTorrent,
  resumeTorrent,
  closeMediaWindow,
  setStreamPriority,
  streamFileAvailability,
  takePendingOpenSources,
  torrentDetails,
  updateTorrentFilePriority,
  updateTorrentFiles,
  updateTorrentOptions
} from "@/lib/tauri-api";
import {
  normalizeTorrent,
  normalizeTorrentSummary,
  type LogEntry,
  type StreamPriorityStatus,
  type TorrentFileAvailability,
  type TorrentRow,
  type UpdateTorrentOptionsRequest
} from "@/lib/torrent-types";
import { isPlayableMediaName } from "@/lib/media";
import { cn, formatBytes, formatEta, formatRate, percent } from "@/lib/utils";

const filters = ["All", "Downloading", "Seeding", "Paused", "Complete", "Error"] as const;
const inspectorTabs = ["Details", "Connections", "Files", "Options", "Logs"] as const;
const completedStates = new Set(["Complete", "Seeding", "Seed Ratio Reached"]);

type MediaSession = {
  torrentId: string;
  fileIndex: number;
  availability: TorrentFileAvailability;
  priority: StreamPriorityStatus;
};

type MediaPlayerClosedPayload = {
  torrentId: string;
  fileIndex: number;
};

type TorrentAction = "pause" | "resume" | "recheck" | "delete" | "deleteFiles";

const downloadingStates = new Set(["Queued", "Discovering", "Downloading", "Resuming", "Partial", "Metadata", "Fetching Metadata", "DHT"]);

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
  const [inspectorTab, setInspectorTab] = React.useState<(typeof inspectorTabs)[number]>("Details");
  const [logs, setLogs] = React.useState<LogEntry[]>([]);
  const [logsError, setLogsError] = React.useState<string | null>(null);
  const [logFilePath, setLogFilePath] = React.useState<string | null>(null);
  const [mediaSession, setMediaSession] = React.useState<MediaSession | null>(null);
  const [mediaBusy, setMediaBusy] = React.useState(false);
  const [mediaBusyTorrentId, setMediaBusyTorrentId] = React.useState<string | null>(null);
  const [mediaError, setMediaError] = React.useState<string | null>(null);
  const [busyAction, setBusyAction] = React.useState<string | null>(null);
  const [priorityBusyFileIndex, setPriorityBusyFileIndex] = React.useState<number | null>(null);
  const [detailsError, setDetailsError] = React.useState<string | null>(null);
  const refreshPending = React.useRef<Promise<void> | null>(null);
  const detailRequestId = React.useRef(0);
  const lastLogId = React.useRef<number | null>(null);

  const selected = selectedId ? rows.find((row) => row.id === selectedId) ?? null : null;
  const selectedBackendId = typeof selected?.raw.id === "number" ? selected.raw.id : null;
  const mediaTorrentId = mediaSession?.torrentId ?? null;
  const mediaFileIndex = mediaSession?.fileIndex ?? null;
  const selectedFileIds = React.useMemo(() => {
    if (!selected) return new Set<number>();
    return fileSelections[selected.id] ?? includedFileIds(selected.files);
  }, [fileSelections, selected]);

  React.useEffect(() => {
    let disposed = false;
    let timer: number | undefined;
    const poll = async () => {
      await refresh();
      if (!disposed) timer = window.setTimeout(poll, document.hidden ? 15_000 : 1_500);
    };
    const handleVisibility = () => {
      if (timer) window.clearTimeout(timer);
      if (!document.hidden) void poll();
    };
    void poll();
    document.addEventListener("visibilitychange", handleVisibility);
    return () => {
      disposed = true;
      if (timer) window.clearTimeout(timer);
      document.removeEventListener("visibilitychange", handleVisibility);
    };
  }, []);

  React.useEffect(() => {
    if (inspectorTab !== "Logs") return;
    let disposed = false;
    let initialLoad = true;
    lastLogId.current = null;

    const loadLogs = async () => {
      try {
        const nextLogs = await backendLogsAfter(selectedBackendId, lastLogId.current);
        if (!disposed) {
          setLogs((current) => initialLoad ? nextLogs.slice(-1_000) : [...current, ...nextLogs].slice(-1_000));
          lastLogId.current = nextLogs.at(-1)?.id ?? lastLogId.current;
          initialLoad = false;
          setLogsError(null);
        }
      } catch (err) {
        if (!disposed) setLogsError(err instanceof Error ? err.message : "Could not load backend logs.");
      }
    };

    void loadLogs();
    const interval = window.setInterval(loadLogs, 3000);
    return () => {
      disposed = true;
      window.clearInterval(interval);
    };
  }, [inspectorTab, selectedBackendId]);

  React.useEffect(() => {
    if (!selectedId) {
      return;
    }
    let disposed = false;
    let timer: number | undefined;
    const poll = async (initial = false) => {
      await hydrateDetails(selectedId);
      if (!disposed) timer = window.setTimeout(() => void poll(), document.hidden ? 15_000 : 2_000);
    };
    void poll(true);
    return () => {
      disposed = true;
      detailRequestId.current += 1;
      if (timer) window.clearTimeout(timer);
    };
  }, [selectedId]);

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
    // The dedicated player polls aggressively while seeking. This background summary
    // only needs a low-frequency heartbeat, avoiding duplicate storage work.
    const interval = window.setInterval(loadAvailability, 10000);
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
    let unlisten: (() => void) | undefined;
    let disposed = false;
    import("@tauri-apps/api/event")
      .then(async ({ listen }) => {
        const stopListening = await listen<MediaPlayerClosedPayload>("media-player-closed", (event) => {
          setMediaSession((current) =>
            current &&
            current.torrentId === event.payload.torrentId &&
            current.fileIndex === event.payload.fileIndex
              ? null
              : current
          );
          setMediaBusy(false);
          setMediaBusyTorrentId(null);
        });
        if (disposed) stopListening();
        else unlisten = stopListening;
      })
      .catch(() => undefined);
    return () => {
      disposed = true;
      unlisten?.();
    };
  }, []);

  React.useEffect(() => {
    backendLogFilePath()
      .then(setLogFilePath)
      .catch(() => undefined);
  }, []);

  const filteredRows = React.useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase();
    return rows.filter((row) => {
      const matchesFilter = matchesTorrentFilter(row.state, filter);
      const text = `${row.name} ${row.hash} ${row.outputFolder}`.toLowerCase();
      return matchesFilter && text.includes(normalizedQuery);
    });
  }, [filter, query, rows]);

  const totals = React.useMemo(
    () => rows.reduce(
      (acc, row) => {
        acc.down += row.downloadSpeed;
        acc.up += row.uploadSpeed;
        acc.active += downloadingStates.has(row.state) || row.state === "Seeding" ? 1 : 0;
        acc.complete += completedStates.has(row.state) ? 1 : 0;
        return acc;
      },
      { down: 0, up: 0, active: 0, complete: 0 }
    ),
    [rows]
  );

  async function refresh() {
    if (refreshPending.current) return refreshPending.current;
    const pending = (async () => {
      try {
        const response = await listTorrentSummaries();
        const nextRows = response.torrents.map(normalizeTorrentSummary);
        setRows((currentRows) => mergeRows(currentRows, nextRows));
        setSelectedId((current) => (current && nextRows.some((row) => row.id === current) ? current : null));
        setMediaSession((current) =>
          current && nextRows.some((row) => row.id === current.torrentId) ? current : null
        );
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Could not list torrents.");
      }
    })();
    refreshPending.current = pending;
    try {
      await pending;
    } finally {
      refreshPending.current = null;
    }
  }

  async function hydrateDetails(id: string) {
    const requestId = ++detailRequestId.current;
    try {
      const details = await torrentDetails(id);
      if (requestId !== detailRequestId.current) return;
      const hydrated = normalizeTorrent(details);
      setRows((currentRows) => currentRows.map((row) => (row.id === id ? { ...row, ...hydrated } : row)));
      setDetailsError(null);
    } catch (err) {
      if (requestId !== detailRequestId.current) return;
      setDetailsError(err instanceof Error ? err.message : "Could not refresh torrent details.");
    }
  }

  async function openAdd() {
    const opened = await openAddTorrentWindow();
    if (!opened) setAddOpen(true);
  }

  async function runAction(action: TorrentAction, row = selected) {
    if (!row || busyAction) return;
    const actionKey = `${row.id}:${action}`;
    setBusyAction(actionKey);
    try {
      if (action === "pause") await pauseTorrent(row.id);
      if (action === "resume") await resumeTorrent(row.id);
      if (action === "recheck") await recheckTorrent(row.id);
      if (action === "delete") await deleteTorrent(row.id, false);
      if (action === "deleteFiles") await deleteTorrent(row.id, true);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Torrent action failed.");
    } finally {
      setBusyAction(null);
    }
  }

  async function handleOpenFolder(row = selected) {
    if (!row || busyAction) return;
    const actionKey = row.id + ":folder";
    setBusyAction(actionKey);
    try {
      await openTorrentFolder(row.id);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not open the torrent download folder.");
    } finally {
      setBusyAction(null);
    }
  }

  async function handleFileSelection(next: Set<number>) {
    if (!selected) return;
    const torrentId = selected.id;
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
      await updateTorrentFiles(torrentId, Array.from(next).sort((a, b) => a - b));
      setError(null);
    } catch (err) {
      setFileSelections((current) => {
        const restored = { ...current };
        delete restored[torrentId];
        return restored;
      });
      await hydrateDetails(torrentId);
      setError(err instanceof Error ? err.message : "Could not update file selection.");
    }
  }

  async function handleFilePriority(fileIndex: number, priority: number) {
    if (!selected || priorityBusyFileIndex != null) return;
    const torrentId = selected.id;
    const previousPriority = selected.files[fileIndex]?.priority ?? 1;
    setPriorityBusyFileIndex(fileIndex);
    setRows((currentRows) => currentRows.map((row) => row.id === torrentId ? {
      ...row,
      files: row.files.map((file, index) => index === fileIndex ? { ...file, priority } : file)
    } : row));
    try {
      await updateTorrentFilePriority(torrentId, fileIndex, priority);
      await hydrateDetails(torrentId);
      setError(null);
    } catch (err) {
      setRows((currentRows) => currentRows.map((row) => row.id === torrentId ? {
        ...row,
        files: row.files.map((file, index) => index === fileIndex ? { ...file, priority: previousPriority } : file)
      } : row));
      setError(err instanceof Error ? err.message : "Could not update file priority.");
    } finally {
      setPriorityBusyFileIndex(null);
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
    setMediaBusyTorrentId(row.id);
    setMediaError(null);
    try {
      const priority = await setStreamPriority(row.id, {
        fileIndex,
        playheadOffset,
        urgentBytes: null,
        lookaheadBytes: null
      });
      await openMediaWindow(row.id, fileIndex);
      const availability = await streamFileAvailability(row.id, fileIndex);
      setMediaSession({ torrentId: row.id, fileIndex, priority, availability });
      setSelectedId(row.id);
      setError(null);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Could not prepare this file for streaming.";
      setMediaError(message);
      setError(message);
    } finally {
      setMediaBusy(false);
      setMediaBusyTorrentId(null);
    }
  }

  function handlePlayTorrent(row: TorrentRow) {
    if (row.firstPlayableFileIndex == null) return;
    void handlePlayFile(row, row.firstPlayableFileIndex);
  }

  async function handleClearStream() {
    if (!mediaSession) return;
    const torrentId = mediaSession.torrentId;
    const fileIndex = mediaSession.fileIndex;
    setMediaBusy(true);
    setMediaError(null);
    try {
      await closeMediaWindow(torrentId, fileIndex);
      setMediaSession(null);
    } catch (err) {
      setMediaError(err instanceof Error ? err.message : "Could not close the media player.");
    } finally {
      setMediaBusy(false);
    }
  }

  return (
    <main className="h-full min-h-0 bg-background p-3 text-foreground">
      <div className="mx-auto flex min-h-full flex-col gap-3">
        <header className="panel flex min-w-0 items-center gap-3 px-4 py-3">
          <div className="flex shrink-0 items-center gap-3">
            <Image
              src="/novatorrent-logo.png"
              alt="NovaTorrent"
              width={40}
              height={40}
              className="h-10 w-10 object-contain"
            />
            <div>
              <h1 className="text-lg font-semibold">NovaTorrent</h1>
              <p className="text-xs text-muted-foreground">{rows.length} torrents</p>
            </div>
          </div>
          <div className="ml-auto flex shrink-0 items-center gap-2">
            <StatPill icon={<Download />} label={formatRate(totals.down)} />
            <StatPill icon={<Upload />} label={formatRate(totals.up)} />
          </div>
          <div className="flex shrink-0 items-center gap-2">
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
              <span className="hidden sm:inline">Add Torrent</span>
            </Button>
          </div>
        </header>

        {error ? (
          <div className="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">{error}</div>
        ) : null}

        <div className="flex min-h-0 flex-1 flex-col">
          <section className="panel flex min-h-0 flex-1 flex-col overflow-hidden">
            <div className="flex flex-col gap-2 border-b p-3 sm:flex-row sm:items-center sm:justify-between">
              <div className="flex items-center gap-2">
                <Label htmlFor="torrent-status-filter" className="sr-only">Filter torrents by status</Label>
                <div className="relative">
                  <select
                    id="torrent-status-filter"
                    className="h-9 appearance-none rounded-md border bg-background py-1 pl-2.5 pr-9 text-sm font-medium outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
                    value={filter}
                    onChange={(event) => setFilter(event.target.value as (typeof filters)[number])}
                  >
                    {filters.map((item) => <option key={item} value={item}>{item === "All" ? "All torrents" : item}</option>)}
                  </select>
                  <ChevronDown className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                </div>
                <span className="text-xs tabular-nums text-muted-foreground">{filteredRows.length} shown</span>
              </div>
              <div className="relative w-full sm:w-72">
                <Search className="pointer-events-none absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
                <Input value={query} onChange={(event) => setQuery(event.target.value)} className="pl-8" placeholder="Search torrents" />
              </div>
            </div>

            <div className="min-h-0 flex-1 overflow-auto">
              <div className="min-w-[1160px]">
                <div className="grid h-8 grid-cols-[76px_minmax(250px,1.6fr)_104px_164px_88px_88px_72px_64px_64px_82px] items-center border-b px-3 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
                  <span>Media</span>
                  <span>Name</span>
                  <span>Status</span>
                  <span>Progress</span>
                  <span>Down</span>
                  <span>Up</span>
                  <span>ETA</span>
                  <span>Peers</span>
                  <span>Ratio</span>
                  <span className="text-right">Size</span>
                </div>
                {filteredRows.length ? (
                  filteredRows.map((row) => (
                    <TorrentContextMenu
                      key={row.id}
                      row={row}
                      onAction={runAction}
                      onOpenFolder={() => void handleOpenFolder(row)}
                      busy={Boolean(busyAction)}
                    >
                      <div
                        role="button"
                        tabIndex={0}
                        className={cn(
                          "grid min-h-14 w-full grid-cols-[76px_minmax(250px,1.6fr)_104px_164px_88px_88px_72px_64px_64px_82px] items-center gap-0 border-b px-3 text-left text-xs outline-none transition-colors hover:bg-secondary/60 focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring",
                          selected?.id === row.id && "bg-primary/8"
                        )}
                        onClick={() => setSelectedId(row.id)}
                        onDoubleClick={() => void hydrateDetails(row.id)}
                        onKeyDown={(event) => {
                          if (event.key === "Enter" || event.key === " ") {
                            event.preventDefault();
                            setSelectedId(row.id);
                          }
                        }}
                      >
                        <div className="pr-2">
                          {row.firstPlayableFileIndex != null ? (
                            <Button
                              type="button"
                              variant="outline"
                              size="sm"
                              className="h-8 border-primary/35 px-2 text-primary hover:bg-primary/10 hover:text-primary"
                              disabled={mediaBusy}
                              title={row.playableFileCount > 1 ? `Play media (${row.playableFileCount} files)` : "Play media"}
                              aria-label={row.playableFileCount > 1 ? `Play media from ${row.name}; ${row.playableFileCount} files available` : `Play media from ${row.name}`}
                              onClick={(event) => {
                                event.stopPropagation();
                                handlePlayTorrent(row);
                              }}
                              onDoubleClick={(event) => event.stopPropagation()}
                            >
                              {mediaBusyTorrentId === row.id ? <Loader2 className="animate-spin" /> : <Play />}
                              Play
                            </Button>
                          ) : <span className="text-[11px] text-muted-foreground">—</span>}
                        </div>
                        <div className="min-w-0 pr-3">
                          <div className="flex items-center gap-2">
                            <span className="truncate text-sm font-medium">{row.name}</span>
                            {completedStates.has(row.state) ? <CheckCircle2 className="h-4 w-4 shrink-0 text-primary" /> : null}
                          </div>
                          <div className="truncate-path mt-0.5 text-[11px] text-muted-foreground">{row.outputFolder}</div>
                        </div>
                        <Badge variant={statusVariant(row.state)} className="w-fit">
                          {row.state}
                        </Badge>
                        <div className="pr-3">
                          <Progress value={percent(row.progress)} className="h-1.5" />
                          <span className="mt-1 block whitespace-nowrap text-[11px] tabular-nums text-muted-foreground">
                            {percent(row.progress).toFixed(1)}% · {formatBytes(row.downloaded)}
                          </span>
                        </div>
                        <span className="tabular-nums">{formatRate(row.downloadSpeed)}</span>
                        <span className="tabular-nums">{formatRate(row.uploadSpeed)}</span>
                        <span className="tabular-nums text-muted-foreground">{formatEta(row.eta)}</span>
                        <span className="tabular-nums">{row.peerCount ?? row.peers.length}</span>
                        <span className="tabular-nums">{formatRatio(torrentRatio(row))}</span>
                        <span className="text-right tabular-nums">{formatBytes(row.total)}</span>
                      </div>
                    </TorrentContextMenu>
                  ))
                ) : (
                  <div className="flex h-72 flex-col items-center justify-center gap-2 px-6 text-center text-sm text-muted-foreground">
                    <p>{rows.length ? "No torrents match this filter." : "No torrents yet."}</p>
                    {!rows.length ? <Button size="sm" onClick={openAdd}><Plus />Add your first torrent</Button> : null}
                  </div>
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
                logsError={logsError}
                logFilePath={logFilePath}
                onFileSelectionChange={handleFileSelection}
                onFilePriorityChange={handleFilePriority}
                priorityBusyFileIndex={priorityBusyFileIndex}
                mediaSession={mediaSession?.torrentId === selected.id ? mediaSession : null}
                mediaBusy={mediaBusy}
                mediaError={mediaSession?.torrentId === selected.id ? mediaError : null}
                onPlayFile={(fileIndex) => void handlePlayFile(selected, fileIndex)}
                onClearStream={() => void handleClearStream()}
                onOptionsChange={handleOptionsChange}
                detailsError={detailsError}
                actionBusy={Boolean(busyAction?.startsWith(`${selected.id}:`))}
                onTogglePause={() => void runAction(selected.state === "Paused" ? "resume" : "pause", selected)}
                onRecheck={() => void runAction("recheck", selected)}
                onOpenFolder={() => void handleOpenFolder(selected)}
              />
            ) : null}
          </section>
        </div>
      </div>

      <Dialog.Root open={addOpen} onOpenChange={setAddOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-40 bg-background/70 backdrop-blur-sm" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-50 h-[min(88vh,760px)] max-h-[calc(100vh-1rem)] w-[min(1120px,calc(100vw-1rem))] -translate-x-1/2 -translate-y-1/2 outline-none">
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
  onOpenFolder,
  busy
}: {
  row: TorrentRow;
  children: React.ReactNode;
  onAction: (action: TorrentAction, row: TorrentRow) => void;
  onOpenFolder: () => void;
  busy: boolean;
}) {
  return (
    <ContextMenu.Root>
      <ContextMenu.Trigger asChild>{children}</ContextMenu.Trigger>
      <ContextMenu.Portal>
        <ContextMenu.Content className={cn("z-50 min-w-48 overflow-hidden rounded-md border bg-popover p-1 text-popover-foreground shadow-md", busy && "pointer-events-none opacity-60")}>
          <MenuItem icon={row.state === "Paused" ? <Play /> : <Pause />} onSelect={() => onAction(row.state === "Paused" ? "resume" : "pause", row)}>
            {row.state === "Paused" ? "Continue" : "Pause"}
          </MenuItem>
          <MenuItem icon={<FolderOpen />} onSelect={onOpenFolder}>
            Open download folder
          </MenuItem>
          <ContextMenu.Separator className="my-1 h-px bg-border" />
          <MenuItem icon={<CheckCircle2 />} onSelect={() => onAction("recheck", row)}>
            Recheck files
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
  logsError,
  logFilePath,
  onFileSelectionChange,
  onFilePriorityChange,
  priorityBusyFileIndex,
  mediaSession,
  mediaBusy,
  mediaError,
  onPlayFile,
  onClearStream,
  onOptionsChange,
  detailsError,
  actionBusy,
  onTogglePause,
  onRecheck,
  onOpenFolder,
}: {
  height: number;
  onResize: (height: number) => void;
  selected: TorrentRow;
  selectedFileIds: Set<number>;
  tab: (typeof inspectorTabs)[number];
  onTabChange: (tab: (typeof inspectorTabs)[number]) => void;
  logs: LogEntry[];
  logsError: string | null;
  logFilePath: string | null;
  onFileSelectionChange: (selected: Set<number>) => void;
  onFilePriorityChange: (fileIndex: number, priority: number) => void;
  priorityBusyFileIndex: number | null;
  mediaSession: MediaSession | null;
  mediaBusy: boolean;
  mediaError: string | null;
  onPlayFile: (fileIndex: number) => void;
  onClearStream: () => void;
  onOptionsChange: (request: UpdateTorrentOptionsRequest) => Promise<void>;
  detailsError: string | null;
  actionBusy: boolean;
  onTogglePause: () => void;
  onRecheck: () => void;
  onOpenFolder: () => void;
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
      <div className="border-b">
        <div className="flex items-center justify-between gap-3 px-3 py-2">
          <div className="flex min-w-0 items-center gap-3">
          <div className="min-w-0">
            <h2 className="truncate text-sm font-semibold">{selected.name}</h2>
            <p className="truncate-path text-xs text-muted-foreground">{selected.hash}</p>
          </div>
          </div>
          <div className="flex shrink-0 items-center gap-2">
            <Button type="button" variant="outline" size="sm" className="h-8" disabled={actionBusy} onClick={onOpenFolder}>
              <FolderOpen />
              <span className="hidden md:inline">Open folder</span>
            </Button>
            <Button type="button" variant="outline" size="sm" className="h-8" disabled={actionBusy} onClick={onTogglePause}>
              {actionBusy ? <Loader2 className="animate-spin" /> : selected.state === "Paused" ? <Play /> : <Pause />}
              <span className="hidden sm:inline">{selected.state === "Paused" ? "Continue" : "Pause"}</span>
            </Button>
            <Button type="button" variant="outline" size="sm" className="h-8" disabled={actionBusy} onClick={onRecheck}>
              <CheckCircle2 />
              <span className="hidden md:inline">Recheck</span>
            </Button>
          </div>
        </div>
        <div className="flex gap-1 overflow-x-auto border-t px-3 py-1.5">
          {inspectorTabs.map((item) => (
            <button
              key={item}
              type="button"
              className={cn(
                "h-8 rounded-md px-2.5 text-xs font-medium text-muted-foreground hover:bg-secondary hover:text-foreground",
                tab === item && "bg-secondary text-foreground"
              )}
              onClick={() => onTabChange(item)}
              aria-current={tab === item ? "page" : undefined}
            >
              {item}
            </button>
          ))}
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-auto p-3">
        {detailsError ? (
          <div className="mb-3 rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
            {detailsError}
          </div>
        ) : null}
        <InspectorTab
          tab={tab}
          selected={selected}
          selectedFileIds={selectedFileIds}
          logs={logs}
          logsError={logsError}
          logFilePath={logFilePath}
          onFileSelectionChange={onFileSelectionChange}
          onFilePriorityChange={onFilePriorityChange}
          priorityBusyFileIndex={priorityBusyFileIndex}
          mediaSession={mediaSession}
          mediaBusy={mediaBusy}
          mediaError={mediaError}
          onPlayFile={onPlayFile}
          onClearStream={onClearStream}
          onOptionsChange={onOptionsChange}
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
  logsError,
  logFilePath,
  onFileSelectionChange,
  onFilePriorityChange,
  priorityBusyFileIndex,
  mediaSession,
  mediaBusy,
  mediaError,
  onPlayFile,
  onClearStream,
  onOptionsChange
}: {
  tab: (typeof inspectorTabs)[number];
  selected: TorrentRow;
  selectedFileIds: Set<number>;
  logs: LogEntry[];
  logsError: string | null;
  logFilePath: string | null;
  onFileSelectionChange: (selected: Set<number>) => void;
  onFilePriorityChange: (fileIndex: number, priority: number) => void;
  priorityBusyFileIndex: number | null;
  mediaSession: MediaSession | null;
  mediaBusy: boolean;
  mediaError: string | null;
  onPlayFile: (fileIndex: number) => void;
  onClearStream: () => void;
  onOptionsChange: (request: UpdateTorrentOptionsRequest) => Promise<void>;
}) {
  if (tab === "Details") {
    return (
      <div className="space-y-3">
        {selected.raw.stats?.error ? (
          <div className="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
            {selected.raw.stats.error}
          </div>
        ) : null}
        <div className="grid gap-3 lg:grid-cols-[360px_1fr]">
          <div className="space-y-3">
            <div className="grid grid-cols-3 gap-2">
              <Metric icon={<BarChart3 />} label="Progress" value={`${percent(selected.progress).toFixed(1)}%`} />
              <Metric icon={<Download />} label="Down" value={formatRate(selected.downloadSpeed)} />
              <Metric icon={<Upload />} label="Up" value={formatRate(selected.uploadSpeed)} />
            </div>
            <PieceMap states={selected.raw.piece_states ?? []} progress={selected.progress} />
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
            <InfoLine label="Elapsed" value={formatDuration(selected.general?.active_time_seconds)} />
            <InfoLine label="Since complete" value={formatDuration(selected.general?.seeding_time_seconds)} />
            <InfoLine label="Trackers" value={String(selected.trackers.length)} />
            <InfoLine label="Known peers" value={String(Math.max(selected.peers.length, selected.peerCount || 0))} />
          </div>
        </div>
        <section className="border-t pt-3" aria-labelledby="torrent-metadata-heading">
          <h3 id="torrent-metadata-heading" className="mb-3 text-xs font-semibold text-muted-foreground">Torrent metadata</h3>
          <div className="grid gap-3 pb-1 text-sm md:grid-cols-3">
            <InfoLine label="Name" value={selected.name} />
            <InfoLine label="Hash" value={selected.hash} wrap />
            <InfoLine label="Save Path" value={selected.general?.save_path || selected.outputFolder} wrap />
            <InfoLine label="Total Size" value={formatBytes(selected.general?.total_size ?? selected.total)} />
            <InfoLine label="Files" value={String(selected.general?.file_count ?? selected.files.length)} />
            <InfoLine label="Pieces" value={formatPieceLayout(selected.general?.piece_count, selected.general?.piece_size)} />
            <InfoLine label="Private" value={selected.general?.private ? "Yes" : "No"} />
            <InfoLine label="Created By" value={selected.general?.created_by || "-"} />
            <InfoLine label="Created" value={formatUnixDate(selected.general?.creation_date)} />
            <InfoLine label="Comment" value={selected.general?.comment || "-"} wide wrap />
          </div>
        </section>
      </div>
    );
  }

  if (tab === "Connections") {
    return (
      <div className="space-y-2">
        <ConnectionGroup title="Peers" count={selected.peers.length} icon={<Network />} defaultOpen>
          {selected.peers.length ? (
            <DataTable
              columns={["Address", "Client", "Progress", "Down", "Up", "Activity"]}
              rows={selected.peers.map((peer) => [
                `${peer.address}:${peer.port}`,
                peer.client || "-",
                `${percent(peer.progress * 100).toFixed(1)}%`,
                formatRate(peer.download_speed),
                formatRate(peer.upload_speed),
                peer.connection
              ])}
            />
          ) : <EmptyTab icon={<Network />} text="No peers have been discovered yet." />}
        </ConnectionGroup>
        <ConnectionGroup title="Trackers" count={selected.trackers.length} icon={<RadioTower />}>
          {selected.trackers.length ? (
            <DataTable
              columns={["Tracker", "Status", "Seeders", "Leechers", "Announce Interval", "Message"]}
              rows={selected.trackers.map((tracker) => [
                tracker.url,
                tracker.state,
                tracker.seeders == null ? "-" : String(tracker.seeders),
                tracker.leechers == null ? "-" : String(tracker.leechers),
                tracker.next_announce_seconds == null ? "-" : formatDuration(tracker.next_announce_seconds),
                tracker.message || "-"
              ])}
            />
          ) : <EmptyTab icon={<RadioTower />} text="No trackers are configured for this torrent." />}
        </ConnectionGroup>
        <ConnectionGroup title="Web seeds" count={selected.webSeeds.length} icon={<Download />}>
          {selected.webSeeds.length ? (
            <DataTable
              columns={["Web Seed", "Status", "Downloaded", "Message"]}
              rows={selected.webSeeds.map((seed) => [
                seed.url,
                seed.state,
                formatBytes(seed.bytes_downloaded),
                seed.message || "-"
              ])}
            />
          ) : <EmptyTab icon={<Download />} text="No web seeds are configured for this torrent." />}
        </ConnectionGroup>
      </div>
    );
  }

  if (tab === "Files") {
    return (
      <div className="space-y-2">
        <div className="flex items-center gap-2 text-sm font-semibold">
          <FileCog className="h-4 w-4" />
          Content
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
          onPriorityChange={onFilePriorityChange}
          priorityBusyFileIndex={priorityBusyFileIndex}
          onPlayFile={onPlayFile}
          activeMediaFileIndex={mediaSession?.fileIndex ?? null}
          compact
        />
      </div>
    );
  }

  if (tab === "Options") {
    return <TorrentOptionsEditor key={selected.id} selected={selected} onSave={onOptionsChange} />;
  }

  return <TorrentLogViewer logs={logs} logsError={logsError} logFilePath={logFilePath} />;
}

function PieceMap({ states, progress }: { states: number[]; progress: number }) {
  const buckets = React.useMemo(() => compressPieceStates(states, 240), [states]);
  const safeProgress = percent(progress);
  if (!buckets.length) {
    return <Progress value={safeProgress} className="h-3" />;
  }
  return (
    <div className="space-y-1.5">
      <div
        className="grid h-3 overflow-hidden rounded-sm bg-secondary"
        style={{ gridTemplateColumns: `repeat(${buckets.length}, minmax(0, 1fr))` }}
        role="img"
        aria-label={`Piece availability map: ${safeProgress.toFixed(1)} percent verified. Teal is verified, amber is downloading, muted sections are missing.`}
      >
        {buckets.map((state, index) => (
          <span
            key={index}
            className={cn(
              state === 2 && "bg-primary",
              state === 1 && "bg-accent",
              state === 3 && "bg-primary/45"
            )}
          />
        ))}
      </div>
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-muted-foreground">
        <PieceLegend className="bg-primary" label="Verified" />
        <PieceLegend className="bg-accent" label="Downloading" />
        <PieceLegend className="bg-secondary" label="Missing" />
        <span className="ml-auto tabular-nums text-foreground">{safeProgress.toFixed(1)}%</span>
      </div>
    </div>
  );
}

function PieceLegend({ className, label }: { className: string; label: string }) {
  return <span className="inline-flex items-center gap-1"><span className={cn("h-2 w-2 rounded-[2px]", className)} />{label}</span>;
}

function compressPieceStates(states: number[], maxBuckets: number) {
  if (states.length <= maxBuckets) return states;
  return Array.from({ length: maxBuckets }, (_, bucket) => {
    const start = Math.floor((bucket * states.length) / maxBuckets);
    const end = Math.max(start + 1, Math.floor(((bucket + 1) * states.length) / maxBuckets));
    const slice = states.slice(start, end);
    if (slice.some((state) => state === 1)) return 1;
    if (slice.every((state) => state === 2)) return 2;
    if (slice.some((state) => state === 2)) return 3;
    return 0;
  });
}

function TorrentLogViewer({
  logs,
  logsError,
  logFilePath
}: {
  logs: LogEntry[];
  logsError: string | null;
  logFilePath: string | null;
}) {
  const [level, setLevel] = React.useState<LogEntry["level"] | "All">("All");
  const [scope, setScope] = React.useState("All");
  const [logQuery, setLogQuery] = React.useState("");
  const scopes = React.useMemo(
    () => Array.from(new Set(logs.map((entry) => entry.scope))).sort((left, right) => left.localeCompare(right)),
    [logs]
  );
  const visibleLogs = React.useMemo(() => {
    const normalizedQuery = logQuery.trim().toLowerCase();
    return logs
      .filter((entry) => level === "All" || entry.level === level)
      .filter((entry) => scope === "All" || entry.scope === scope)
      .filter((entry) => !normalizedQuery || `${entry.scope} ${entry.message}`.toLowerCase().includes(normalizedQuery))
      .sort((left, right) => right.id - left.id);
  }, [level, logQuery, logs, scope]);

  return (
    <div className="space-y-2">
      {logsError ? (
        <div className="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">{logsError}</div>
      ) : null}
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative min-w-56 flex-1">
          <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            aria-label="Filter log messages"
            className="h-8 pl-8 text-xs"
            value={logQuery}
            onChange={(event) => setLogQuery(event.currentTarget.value)}
            placeholder="Filter log messages"
          />
        </div>
        <div className="relative">
          <select
            aria-label="Filter logs by level"
            className="h-8 appearance-none rounded-md border bg-background py-1 pl-2.5 pr-8 text-xs text-foreground"
            value={level}
            onChange={(event) => setLevel(event.currentTarget.value as LogEntry["level"] | "All")}
          >
            {(["All", "Debug", "Info", "Warn", "Error"] as const).map((item) => (
              <option key={item} value={item}>{item === "All" ? "All levels" : item}</option>
            ))}
          </select>
          <ChevronDown className="pointer-events-none absolute right-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
        </div>
        <div className="relative max-w-48">
          <select
            aria-label="Filter logs by source"
            className="h-8 w-full appearance-none rounded-md border bg-background py-1 pl-2.5 pr-8 text-xs text-foreground"
            value={scope}
            onChange={(event) => setScope(event.currentTarget.value)}
          >
            <option value="All">All sources</option>
            {scopes.map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
          <ChevronDown className="pointer-events-none absolute right-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
        </div>
        <span className="text-xs tabular-nums text-muted-foreground">{visibleLogs.length} of {logs.length}</span>
      </div>
      {logFilePath ? (
        <div className="flex items-center gap-2 rounded-md border bg-background px-2 py-1.5 text-xs text-muted-foreground" title={logFilePath}>
          <ScrollText className="h-4 w-4 shrink-0" />
          <span className="truncate">Newest first · {logFilePath}</span>
        </div>
      ) : null}
      {visibleLogs.length ? (
        <div className="space-y-1 font-mono text-xs">
          {visibleLogs.map((entry) => (
            <div key={entry.id} className="grid gap-x-2 gap-y-1 rounded px-2 py-1.5 hover:bg-secondary/70 sm:grid-cols-[86px_64px_120px_1fr]">
              <span className="text-muted-foreground">{formatLogTime(entry.timestamp_ms)}</span>
              <span className={cn("font-semibold", logTone(entry.level))}>{entry.level}</span>
              <span className="truncate text-muted-foreground" title={entry.scope}>{entry.scope}</span>
              <span className="break-words">{entry.message}</span>
            </div>
          ))}
        </div>
      ) : logs.length ? (
        <EmptyTab icon={<Search />} text="No logs match these filters." />
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
            Close player
          </Button>
        ) : null}
      </div>
      {availability ? (
        <div className="space-y-2 px-3 py-2">
          <Progress value={percent(bufferPercent)} className="h-2" />
          <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
            <span>{availability.ranges.length} verified range{availability.ranges.length === 1 ? "" : "s"}</span>
            <span>{mediaSession.priority.total_priority_pieces} priority piece{mediaSession.priority.total_priority_pieces === 1 ? "" : "s"}</span>
            <span>{availability.complete ? "Ready" : mediaBusy ? "Updating" : "Viewer open"}</span>
          </div>
        </div>
      ) : null}
      {mediaError ? <div className="border-t px-3 py-2 text-sm text-destructive">{mediaError}</div> : null}
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

function ConnectionGroup({
  title,
  count,
  icon,
  defaultOpen,
  children
}: {
  title: string;
  count: number;
  icon: React.ReactNode;
  defaultOpen?: boolean;
  children: React.ReactNode;
}) {
  const [open, setOpen] = React.useState(Boolean(defaultOpen));
  return (
    <details
      className="overflow-hidden rounded-md border bg-background"
      open={open}
      onToggle={(event) => setOpen(event.currentTarget.open)}
    >
      <summary className="flex cursor-pointer list-none items-center gap-2 px-3 py-2 text-sm font-medium hover:bg-secondary/50">
        <span className="text-primary [&_svg]:h-4 [&_svg]:w-4">{icon}</span>
        <span>{title}</span>
        <span className="ml-auto text-xs tabular-nums text-muted-foreground">{count}</span>
      </summary>
      <div className="border-t">{children}</div>
    </details>
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

function InfoLine({ label, value, wide, wrap }: { label: string; value: string; wide?: boolean; wrap?: boolean }) {
  return (
    <div className={cn(wide && "col-span-2")}>
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className={cn("text-sm font-medium", wrap ? "break-words" : "truncate")} title={value}>{value}</div>
    </div>
  );
}

function formatPieceLayout(count?: number, size?: number) {
  if (!count || !size) return "Metadata pending";
  return `${count} × ${formatBytes(size)}`;
}

function formatRatio(value?: number | null) {
  if (value == null || !Number.isFinite(value)) return "0.00";
  return value.toFixed(2);
}

function torrentRatio(row: TorrentRow) {
  if (row.general?.ratio != null && Number.isFinite(row.general.ratio)) return row.general.ratio;
  return row.downloaded > 0 ? row.uploaded / row.downloaded : 0;
}

function formatDuration(seconds?: number | null) {
  if (!seconds || seconds <= 0) return "-";
  if (seconds < 60) return "<1m";
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
  return isPlayableMediaName(candidate);
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
    if (!current) return row;
    return {
      ...row,
      files: current.files,
      general: { ...current.general, ...row.general },
      trackers: current.trackers,
      webSeeds: current.webSeeds,
      peers: current.peers,
      options: current.options,
      raw: {
        ...current.raw,
        ...row.raw,
        stats: row.raw.stats,
        general: { ...current.raw.general, ...row.raw.general }
      }
    };
  });
}

function matchesTorrentFilter(state: string, filter: (typeof filters)[number]) {
  if (filter === "All") return true;
  if (filter === "Complete") return completedStates.has(state);
  if (filter === "Downloading") return downloadingStates.has(state);
  if (filter === "Error") return state === "Error" || state.endsWith(" Error") || state === "Missing Files";
  return state === filter;
}

function includedFileIds(files: TorrentRow["files"]) {
  return new Set(files.map((file, index) => (file.included ? index : -1)).filter((index) => index >= 0));
}
