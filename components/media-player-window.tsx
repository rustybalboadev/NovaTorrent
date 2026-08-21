"use client";

import * as React from "react";
import { Clapperboard, Loader2, Pause, Play, RotateCw, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import {
  clearStreamPriority,
  setStreamPriority,
  streamFileAvailability,
  streamFileUrl,
  torrentDetails
} from "@/lib/tauri-api";
import type { StreamPriorityStatus, TorrentFileAvailability, TorrentDetails } from "@/lib/torrent-types";
import { cn, formatBytes, percent } from "@/lib/utils";

const networkRetryLimit = 12;
const streamUrgentBytes = 8 * 1024 * 1024;
const streamLookaheadBytes = 48 * 1024 * 1024;
const priorityUpdateThresholdBytes = 2 * 1024 * 1024;
const priorityUpdateThresholdSeconds = 12;
const mediaErrorSrcNotSupported = 4;

type ViewerParams = {
  id: string;
  fileIndex: number;
};

type InitialViewerState = {
  params: ViewerParams | null;
  error: string | null;
};

function initialViewerState(): InitialViewerState {
  if (typeof window === "undefined") return { params: null, error: null };
  const search = new URLSearchParams(window.location.search);
  const id = search.get("id") ?? "";
  const fileIndex = Number(search.get("fileIndex"));
  if (!id || !Number.isInteger(fileIndex) || fileIndex < 0) {
    return { params: null, error: "Could not open media file." };
  }
  return { params: { id, fileIndex }, error: null };
}

export function MediaPlayerWindow() {
  const videoRef = React.useRef<HTMLVideoElement | null>(null);
  const shouldResumeRef = React.useRef(false);
  const userWantsPlaybackRef = React.useRef(false);
  const lastTimeRef = React.useRef(0);
  const lastPriorityOffsetRef = React.useRef<number | null>(null);
  const lastPriorityTimeRef = React.useRef(0);
  const retryingForSeekRef = React.useRef(false);
  const retryTimerRef = React.useRef<number | null>(null);
  const [initialState] = React.useState(initialViewerState);
  const params = initialState.params;
  const [details, setDetails] = React.useState<TorrentDetails | null>(null);
  const [availability, setAvailability] = React.useState<TorrentFileAvailability | null>(null);
  const [priority, setPriority] = React.useState<StreamPriorityStatus | null>(null);
  const [streamUrl, setStreamUrl] = React.useState("");
  const [retryKey, setRetryKey] = React.useState(0);
  const [networkRetries, setNetworkRetries] = React.useState(0);
  const [busy, setBusy] = React.useState(Boolean(params && !initialState.error));
  const [buffering, setBuffering] = React.useState(false);
  const [fetchingSeekPoint, setFetchingSeekPoint] = React.useState(false);
  const [bufferedAheadSeconds, setBufferedAheadSeconds] = React.useState<number | null>(null);
  const [error, setError] = React.useState<string | null>(initialState.error);

  const updateBufferMetrics = React.useCallback(
    (nextAvailability: TorrentFileAvailability | null = availability) => {
      const video = videoRef.current;
      const offset = estimateByteOffset(lastTimeRef.current, video?.duration, nextAvailability?.length);
      if (!nextAvailability || offset == null) {
        setBufferedAheadSeconds(null);
        return null;
      }

      const range = verifiedRangeContaining(nextAvailability, offset);
      setBufferedAheadSeconds(secondsBufferedAhead(nextAvailability, offset, video?.duration, range));
      if (range) {
        retryingForSeekRef.current = false;
        setFetchingSeekPoint(false);
      }
      return offset;
    },
    [availability]
  );

  React.useEffect(() => {
    if (!params) return;
    const viewerParams = params;
    let disposed = false;

    async function prepare() {
      try {
        const [nextDetails, nextPriority, nextUrl, nextAvailability] = await Promise.all([
          torrentDetails(viewerParams.id),
          setStreamPriority(viewerParams.id, {
            fileIndex: viewerParams.fileIndex,
            playheadOffset: 0,
            urgentBytes: streamUrgentBytes,
            lookaheadBytes: streamLookaheadBytes
          }),
          streamFileUrl(viewerParams.id, viewerParams.fileIndex),
          streamFileAvailability(viewerParams.id, viewerParams.fileIndex)
        ]);
        if (disposed) return;
        setDetails(nextDetails);
        setPriority(nextPriority);
        setStreamUrl(nextUrl);
        setAvailability(nextAvailability);
      } catch (err) {
        if (!disposed) setError(err instanceof Error ? err.message : "Could not prepare media playback.");
      } finally {
        if (!disposed) setBusy(false);
      }
    }

    void prepare();
    return () => {
      disposed = true;
    };
  }, [params]);

  React.useEffect(() => {
    if (!params) return;
    let disposed = false;
    const refresh = async () => {
      try {
        const nextAvailability = await streamFileAvailability(params.id, params.fileIndex);
        if (!disposed) {
          setAvailability(nextAvailability);
          updateBufferMetrics(nextAvailability);
        }
      } catch {
        undefined;
      }
    };
    const interval = window.setInterval(refresh, 2500);
    return () => {
      disposed = true;
      window.clearInterval(interval);
    };
  }, [params, updateBufferMetrics]);

  React.useEffect(() => {
    return () => {
      if (retryTimerRef.current != null) window.clearTimeout(retryTimerRef.current);
    };
  }, []);

  const streamSrc = streamUrl ? `${streamUrl}${retryKey ? `?retry=${retryKey}` : ""}` : "";
  const bufferPercent =
    availability && availability.length > 0 ? percent((availability.verified_bytes / availability.length) * 100) : 0;
  const fileName = availability?.name ?? priority?.name ?? "Media";
  const torrentName = details?.name ?? "NovaTorrent";

  function rememberPosition() {
    const video = videoRef.current;
    if (!video || !Number.isFinite(video.currentTime)) return;
    lastTimeRef.current = Math.max(0, video.currentTime);
  }

  function resumeWhenReady() {
    const video = videoRef.current;
    if (!video || !shouldResumeRef.current || !streamUrl) return;
    if (lastTimeRef.current > 0 && Math.abs(video.currentTime - lastTimeRef.current) > 0.35) {
      try {
        video.currentTime = lastTimeRef.current;
      } catch {
        undefined;
      }
    }
    if (video.paused && video.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA) {
      void video.play().catch(() => undefined);
    }
  }

  async function refreshAvailability() {
    if (!params) return null;
    try {
      const nextAvailability = await streamFileAvailability(params.id, params.fileIndex);
      setAvailability(nextAvailability);
      updateBufferMetrics(nextAvailability);
      return nextAvailability;
    } catch {
      return null;
    }
  }

  async function updateStreamPriorityForTime(time: number, force = false) {
    if (!params || !availability) return;
    const video = videoRef.current;
    const offset = estimateByteOffset(time, video?.duration, availability.length);
    if (offset == null) return;
    const now = Date.now();
    const lastOffset = lastPriorityOffsetRef.current;
    if (
      !force &&
      lastOffset != null &&
      Math.abs(offset - lastOffset) < priorityUpdateThresholdBytes &&
      now - lastPriorityTimeRef.current < priorityUpdateThresholdSeconds * 1000
    ) {
      return;
    }

    lastPriorityOffsetRef.current = offset;
    lastPriorityTimeRef.current = now;
    try {
      const nextPriority = await setStreamPriority(params.id, {
        fileIndex: params.fileIndex,
        playheadOffset: offset,
        urgentBytes: streamUrgentBytes,
        lookaheadBytes: streamLookaheadBytes
      });
      setPriority(nextPriority);
      const nextAvailability = await refreshAvailability();
      if (nextAvailability && isOffsetVerified(nextAvailability, offset)) {
        setBuffering(false);
        setFetchingSeekPoint(false);
        retryingForSeekRef.current = false;
        resumeWhenReady();
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not update stream position.");
    }
  }

  function handleSeekIntent(video: HTMLVideoElement) {
    rememberPosition();
    shouldResumeRef.current = userWantsPlaybackRef.current || !video.paused;
    retryingForSeekRef.current = true;
    setFetchingSeekPoint(true);
    setBuffering(true);
    void updateStreamPriorityForTime(video.currentTime, true);
  }

  function handlePlaybackProgress() {
    rememberPosition();
    const currentOffset = updateBufferMetrics();
    void updateStreamPriorityForTime(lastTimeRef.current);
    if (currentOffset != null && availability && isOffsetVerified(availability, currentOffset)) {
      retryingForSeekRef.current = false;
      setFetchingSeekPoint(false);
    }
  }

  function scheduleNetworkRetry() {
    const video = videoRef.current;
    const mediaError = video?.error;
    if (!video || mediaError?.code === mediaErrorSrcNotSupported) return;
    if (!shouldResumeRef.current || networkRetries >= networkRetryLimit) return;
    rememberPosition();
    setBuffering(true);
    retryTimerRef.current = window.setTimeout(() => {
      setNetworkRetries((count) => count + 1);
      setRetryKey((key) => key + 1);
    }, 900);
  }

  async function closeViewer() {
    if (params) {
      try {
        await clearStreamPriority(params.id);
      } catch {
        undefined;
      }
    }
    try {
      const { getCurrentWindow } = await import("@tauri-apps/api/window");
      await getCurrentWindow().close();
    } catch {
      window.close();
    }
  }

  return (
    <main className="min-h-screen bg-zinc-950 text-zinc-100">
      <div className="flex min-h-screen flex-col">
        <header className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-3">
          <div className="flex min-w-0 items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-md bg-primary text-primary-foreground">
              <Clapperboard className="h-4 w-4" />
            </div>
            <div className="min-w-0">
              <h1 className="truncate text-sm font-semibold">{fileName}</h1>
              <p className="truncate text-xs text-zinc-400">{torrentName}</p>
            </div>
          </div>
          <Button type="button" variant="outline" size="sm" className="border-white/15 bg-white/5 text-zinc-100 hover:bg-white/10" onClick={closeViewer}>
            <X className="h-4 w-4" />
            Close
          </Button>
        </header>

        <section className="flex min-h-0 flex-1 flex-col p-3">
          <div className="relative flex min-h-0 flex-1 items-center justify-center overflow-hidden rounded-md border border-white/10 bg-black">
            {streamSrc ? (
              <video
                ref={videoRef}
                className="h-full max-h-[calc(100vh-9.5rem)] w-full bg-black object-contain"
                controls
                preload="auto"
                src={streamSrc}
                onPlay={() => {
                  userWantsPlaybackRef.current = true;
                  shouldResumeRef.current = true;
                  setBuffering(false);
                  void updateStreamPriorityForTime(lastTimeRef.current, true);
                }}
                onPause={(event) => {
                  const video = event.currentTarget;
                  rememberPosition();
                  if (!video.ended && video.readyState < HTMLMediaElement.HAVE_FUTURE_DATA) {
                    shouldResumeRef.current = true;
                    setBuffering(true);
                    return;
                  }
                  userWantsPlaybackRef.current = false;
                  shouldResumeRef.current = false;
                }}
                onSeeking={(event) => handleSeekIntent(event.currentTarget)}
                onSeeked={resumeWhenReady}
                onWaiting={() => {
                  rememberPosition();
                  shouldResumeRef.current = true;
                  setBuffering(true);
                  void updateStreamPriorityForTime(lastTimeRef.current, true);
                }}
                onStalled={() => {
                  rememberPosition();
                  shouldResumeRef.current = true;
                  setBuffering(true);
                  void updateStreamPriorityForTime(lastTimeRef.current, true);
                }}
                onTimeUpdate={handlePlaybackProgress}
                onLoadedMetadata={resumeWhenReady}
                onCanPlay={() => {
                  setBuffering(false);
                  resumeWhenReady();
                }}
                onProgress={resumeWhenReady}
                onError={scheduleNetworkRetry}
              />
            ) : (
              <div className="flex items-center gap-2 text-sm text-zinc-400">
                <Loader2 className="h-4 w-4 animate-spin" />
                Opening media
              </div>
            )}
            {busy || buffering ? (
              <div className="pointer-events-none absolute left-3 top-3 flex items-center gap-2 rounded-md bg-black/70 px-2.5 py-1.5 text-xs text-zinc-100">
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                {busy ? "Opening" : "Buffering"}
              </div>
            ) : null}
          </div>

          <footer className="mt-3 grid gap-3 rounded-md border border-white/10 bg-white/[0.04] p-3 text-xs text-zinc-300 md:grid-cols-[1fr_auto] md:items-center">
            <div className="min-w-0 space-y-2">
              <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
                <span>{availability ? `${formatBytes(availability.verified_bytes)} verified` : "Checking buffer"}</span>
                <span>{availability ? `${availability.ranges.length} range${availability.ranges.length === 1 ? "" : "s"}` : "0 ranges"}</span>
                <span>{priority ? `${priority.total_priority_pieces} priority pieces` : "Priority pending"}</span>
                <span>
                  {bufferedAheadSeconds != null
                    ? `${Math.floor(bufferedAheadSeconds)}s ready here`
                    : "Seek point pending"}
                </span>
              </div>
              <Progress value={bufferPercent} className="h-2 bg-white/10" />
            </div>
            <div className="flex items-center gap-2 text-zinc-400">
              {buffering ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
              <span className={cn(error && "text-red-300")}>
                {error ??
                  (fetchingSeekPoint
                    ? "Fetching seek point"
                    : networkRetries > 0
                      ? `Retried ${networkRetries} time${networkRetries === 1 ? "" : "s"}`
                      : availability?.complete
                        ? "Ready"
                        : "Streaming")}
              </span>
              {networkRetries > 0 ? <RotateCw className="h-4 w-4" /> : null}
            </div>
          </footer>
        </section>
      </div>
    </main>
  );
}

function estimateByteOffset(time: number, duration: number | undefined, length: number | undefined) {
  if (!length || length <= 0 || !Number.isFinite(time) || time < 0) return null;
  if (!duration || !Number.isFinite(duration) || duration <= 0) return 0;
  const ratio = Math.max(0, Math.min(1, time / duration));
  return Math.min(length - 1, Math.floor(length * ratio));
}

function verifiedRangeContaining(availability: TorrentFileAvailability, offset: number) {
  return availability.ranges.find((range) => {
    const end = range.offset + range.length;
    return range.offset <= offset && offset < end;
  });
}

function isOffsetVerified(availability: TorrentFileAvailability, offset: number) {
  return Boolean(verifiedRangeContaining(availability, offset));
}

function secondsBufferedAhead(
  availability: TorrentFileAvailability | null,
  offset: number | null,
  duration: number | undefined,
  range = availability && offset != null ? verifiedRangeContaining(availability, offset) : undefined
) {
  if (!availability || offset == null || !range || !duration || !Number.isFinite(duration) || duration <= 0) {
    return null;
  }
  const bytesAhead = Math.max(0, range.offset + range.length - offset);
  return (bytesAhead / availability.length) * duration;
}
