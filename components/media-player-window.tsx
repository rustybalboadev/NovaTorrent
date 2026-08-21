"use client";

import * as React from "react";
import { Clapperboard, Crosshair, Loader2, Pause, Play, RotateCw, SkipForward, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import {
  closeMediaWindow,
  clearStreamPriority,
  mediaPlayerLog,
  setStreamPriority,
  streamFileAvailability,
  streamFileUrl,
  torrentDetails
} from "@/lib/tauri-api";
import type { StreamPriorityStatus, TorrentFileAvailability, TorrentDetails } from "@/lib/torrent-types";
import { cn, formatBytes, percent } from "@/lib/utils";

const networkRetryLimit = 12;
const streamUrgentBytes = 16 * 1024 * 1024;
const streamLookaheadBytes = 192 * 1024 * 1024;
const mediaErrorSrcNotSupported = 4;

type ViewerParams = {
  id: string;
  fileIndex: number;
};

type InitialViewerState = {
  params: ViewerParams | null;
  error: string | null;
};

function parseViewerParams(search: URLSearchParams): InitialViewerState {
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
  const pendingResumeTimeRef = React.useRef<number | null>(null);
  const retryingForSeekRef = React.useRef(false);
  const retryTimerRef = React.useRef<number | null>(null);
  const [params, setParams] = React.useState<ViewerParams | null>(null);
  const [details, setDetails] = React.useState<TorrentDetails | null>(null);
  const [availability, setAvailability] = React.useState<TorrentFileAvailability | null>(null);
  const [priority, setPriority] = React.useState<StreamPriorityStatus | null>(null);
  const [streamUrl, setStreamUrl] = React.useState("");
  const [retryKey, setRetryKey] = React.useState(0);
  const [networkRetries, setNetworkRetries] = React.useState(0);
  const [busy, setBusy] = React.useState(false);
  const [buffering, setBuffering] = React.useState(false);
  const [fetchingSeekPoint, setFetchingSeekPoint] = React.useState(false);
  const [bufferedAheadSeconds, setBufferedAheadSeconds] = React.useState<number | null>(null);
  const [targetOffset, setTargetOffset] = React.useState<number | null>(null);
  const [targetReady, setTargetReady] = React.useState(false);
  const [targetTime, setTargetTime] = React.useState(0);
  const [nearestReadyTime, setNearestReadyTime] = React.useState<number | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  const updateBufferMetrics = React.useCallback(
    (nextAvailability: TorrentFileAvailability | null = availability) => {
      const video = videoRef.current;
      const targetTime = pendingResumeTimeRef.current ?? lastTimeRef.current;
      const offset = estimateByteOffset(targetTime, video?.duration, nextAvailability?.length);
      setTargetTime(targetTime);
      setTargetOffset(offset);
      if (!nextAvailability || offset == null) {
        setBufferedAheadSeconds(null);
        setTargetReady(false);
        setNearestReadyTime(null);
        return null;
      }

      const range = verifiedRangeContaining(nextAvailability, offset);
      setTargetReady(Boolean(range));
      setBufferedAheadSeconds(secondsBufferedAhead(nextAvailability, offset, video?.duration, range));
      setNearestReadyTime(range ? null : nearestReadyPlaybackTime(nextAvailability, offset, video?.duration));
      const hasMediaBuffer =
        video &&
        (isPlaybackTimeBuffered(video, targetTime) ||
          (!video.seeking &&
            Math.abs(video.currentTime - targetTime) <= 0.75 &&
            video.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA));
      if (range && hasMediaBuffer) {
        retryingForSeekRef.current = false;
        setFetchingSeekPoint(false);
        setBuffering(false);
      }
      return offset;
    },
    [availability]
  );

  React.useEffect(() => {
    let disposed = false;
    window.queueMicrotask(() => {
      if (disposed) return;
      const initialState = parseViewerParams(new URLSearchParams(window.location.search));
      setParams(initialState.params);
      setError(initialState.error);
      setBusy(Boolean(initialState.params && !initialState.error));
    });
    return () => {
      disposed = true;
    };
  }, []);

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
        void mediaPlayerLog({
          id: viewerParams.id,
          fileIndex: viewerParams.fileIndex,
          event: "prepare-ready",
          targetTime: 0,
          targetOffset: 0,
          targetReady: nextAvailability.ranges.some((range) => range.offset === 0 && range.length > 0),
          message: `${nextAvailability.verified_bytes} verified byte(s) across ${nextAvailability.ranges.length} range(s)`
        }).catch(() => undefined);
      } catch (err) {
        const message = err instanceof Error ? err.message : "Could not prepare media playback.";
        if (!disposed) {
          setError(message);
          void mediaPlayerLog({
            id: viewerParams.id,
            fileIndex: viewerParams.fileIndex,
            event: "prepare-error",
            message
          }).catch(() => undefined);
        }
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
  const targetOffsetLabel =
    targetOffset != null && availability
      ? `${formatBytes(targetOffset)} / ${formatBytes(availability.length)}`
      : "Target pending";

  function mediaNumber(value: number | undefined) {
    return value != null && Number.isFinite(value) ? value : null;
  }

  function logPlayerEvent(event: string, message?: string) {
    if (!params) return;
    const video = videoRef.current;
    const currentTime = mediaNumber(video?.currentTime);
    const duration = mediaNumber(video?.duration);
    const playbackTarget = targetPlaybackTime();
    const playbackTargetOffset =
      estimateByteOffset(playbackTarget, duration ?? undefined, availability?.length) ?? targetOffset;
    const playbackTargetReady =
      availability && playbackTargetOffset != null ? isOffsetVerified(availability, playbackTargetOffset) : targetReady;
    void mediaPlayerLog({
      id: params.id,
      fileIndex: params.fileIndex,
      event,
      currentTime,
      duration,
      readyState: video?.readyState ?? null,
      networkState: video?.networkState ?? null,
      paused: video?.paused ?? null,
      seeking: video?.seeking ?? null,
      targetTime: playbackTarget,
      targetOffset: playbackTargetOffset,
      targetReady: playbackTargetReady,
      bufferedAheadSeconds,
      retryKey,
      networkRetries,
      message: message ?? null
    }).catch(() => undefined);
  }

  function estimatedOffsetForTime(time: number) {
    const video = videoRef.current;
    return estimateByteOffset(time, video?.duration, availability?.length);
  }

  function isPlaybackTimeBuffered(video: HTMLVideoElement, time: number) {
    const buffered = video.buffered;
    for (let index = 0; index < buffered.length; index += 1) {
      if (buffered.start(index) <= time + 0.2 && time <= buffered.end(index) + 0.2) return true;
    }
    return false;
  }

  function markTargetReady(offset: number | null, message?: string) {
    retryingForSeekRef.current = false;
    setFetchingSeekPoint(false);
    setBuffering(false);
    setTargetReady(true);
    if (offset != null) setTargetOffset(offset);
    setNearestReadyTime(null);
    setNetworkRetries(0);
    if (message) logPlayerEvent("target-ready", message);
  }

  function clearBufferingIfTargetReady(message: string) {
    const video = videoRef.current;
    if (!video) return false;
    const targetTime = targetPlaybackTime();
    const hasMediaBuffer =
      isPlaybackTimeBuffered(video, targetTime) ||
      (!video.seeking &&
        Math.abs(video.currentTime - targetTime) <= 0.75 &&
        video.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA);
    if (!hasMediaBuffer) return false;
    markTargetReady(estimatedOffsetForTime(targetTime), message);
    return true;
  }

  function rememberPosition() {
    const video = videoRef.current;
    if (!video || !Number.isFinite(video.currentTime)) return;
    const time = Math.max(0, video.currentTime);
    const pendingTime = pendingResumeTimeRef.current;
    if (pendingTime != null && time + 1 < pendingTime && (retryingForSeekRef.current || video.error)) {
      return;
    }
    lastTimeRef.current = time;
  }

  function rememberTargetTime(time: number) {
    if (!Number.isFinite(time) || time < 0) return;
    lastTimeRef.current = time;
    pendingResumeTimeRef.current = time;
  }

  function targetPlaybackTime() {
    return pendingResumeTimeRef.current ?? lastTimeRef.current;
  }

  function resumeWhenReady() {
    const video = videoRef.current;
    if (!video || !streamUrl) return;
    const targetTime = pendingResumeTimeRef.current ?? lastTimeRef.current;
    if (targetTime > 0 && Math.abs(video.currentTime - targetTime) > 0.35) {
      try {
        video.currentTime = targetTime;
      } catch {
        undefined;
      }
    }
    if (shouldResumeRef.current && video.paused && video.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA) {
      void video.play().catch(() => undefined);
    }
    if (Math.abs(video.currentTime - targetTime) <= 1 && video.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA) {
      pendingResumeTimeRef.current = null;
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

  function jumpToNearestReadyPoint() {
    const video = videoRef.current;
    if (!video || nearestReadyTime == null) return;
    rememberTargetTime(nearestReadyTime);
    retryingForSeekRef.current = false;
    setFetchingSeekPoint(false);
    setBuffering(false);
    try {
      video.currentTime = nearestReadyTime;
    } catch {
      undefined;
    }
    logPlayerEvent("jump-nearest-ready", "jumped to a browser-ready point");
  }

  function handleSeekIntent(video: HTMLVideoElement) {
    rememberTargetTime(video.currentTime);
    shouldResumeRef.current = userWantsPlaybackRef.current || !video.paused;
    const offset = estimateByteOffset(video.currentTime, video.duration, availability?.length);
    if (isPlaybackTimeBuffered(video, video.currentTime)) {
      markTargetReady(offset, "seek target is already buffered by the media element");
      return;
    }
    retryingForSeekRef.current = true;
    setFetchingSeekPoint(true);
    setBuffering(true);
    logPlayerEvent("seek-fetch", "seek target is not verified yet");
  }

  function handlePlaybackProgress() {
    rememberPosition();
    const currentOffset = updateBufferMetrics();
    if (currentOffset != null && availability && isOffsetVerified(availability, currentOffset)) {
      markTargetReady(currentOffset);
    }
  }

  function scheduleNetworkRetry() {
    const video = videoRef.current;
    const mediaError = video?.error;
    if (!video || mediaError?.code === mediaErrorSrcNotSupported) return;
    if ((!shouldResumeRef.current && !retryingForSeekRef.current) || networkRetries >= networkRetryLimit) return;
    if (pendingResumeTimeRef.current == null) {
      rememberPosition();
      pendingResumeTimeRef.current = lastTimeRef.current;
    }
    setBuffering(true);
    setFetchingSeekPoint(true);
    logPlayerEvent("network-retry", mediaError ? `media error ${mediaError.code}` : "retrying media source");
    if (retryTimerRef.current != null) {
      window.clearTimeout(retryTimerRef.current);
    }
    retryTimerRef.current = window.setTimeout(() => {
      setNetworkRetries((count) => count + 1);
      setRetryKey((key) => key + 1);
    }, 900);
  }

  async function closeViewer() {
    if (params) {
      try {
        logPlayerEvent("close");
        await closeMediaWindow(params.id, params.fileIndex);
        return;
      } catch {
        try {
          await clearStreamPriority(params.id);
        } catch {
          undefined;
        }
      }
    }
    window.close();
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
                  logPlayerEvent("play");
                }}
                onPause={(event) => {
                  const video = event.currentTarget;
                  rememberPosition();
                  if (
                    !video.ended &&
                    (video.seeking ||
                      retryingForSeekRef.current ||
                      fetchingSeekPoint ||
                      pendingResumeTimeRef.current != null ||
                      video.readyState < HTMLMediaElement.HAVE_FUTURE_DATA)
                  ) {
                    shouldResumeRef.current = shouldResumeRef.current || userWantsPlaybackRef.current;
                    setBuffering(true);
                    logPlayerEvent("pause-buffering", "media element paused while waiting for stream data");
                    return;
                  }
                  userWantsPlaybackRef.current = false;
                  shouldResumeRef.current = false;
                  logPlayerEvent("pause");
                }}
                onSeeking={(event) => handleSeekIntent(event.currentTarget)}
                onSeeked={() => {
                  logPlayerEvent("seeked");
                  updateBufferMetrics();
                  resumeWhenReady();
                }}
                onWaiting={() => {
                  rememberPosition();
                  if (clearBufferingIfTargetReady("waiting event ignored because target is verified")) return;
                  shouldResumeRef.current = true;
                  setBuffering(true);
                  logPlayerEvent("waiting", "media element is waiting for data");
                }}
                onStalled={() => {
                  rememberPosition();
                  if (clearBufferingIfTargetReady("stalled event ignored because target is verified")) return;
                  shouldResumeRef.current = true;
                  setBuffering(true);
                  logPlayerEvent("stalled", "media element reported a stalled network load");
                }}
                onTimeUpdate={handlePlaybackProgress}
                onLoadedMetadata={() => {
                  logPlayerEvent("loaded-metadata");
                  resumeWhenReady();
                }}
                onCanPlay={() => {
                  if (!clearBufferingIfTargetReady("can-play reached verified target")) {
                    setBuffering(false);
                    if (!retryingForSeekRef.current) setFetchingSeekPoint(false);
                    logPlayerEvent("can-play");
                  }
                  resumeWhenReady();
                }}
                onProgress={() => {
                  clearBufferingIfTargetReady("progress event reached verified target");
                  resumeWhenReady();
                }}
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
            {fetchingSeekPoint && !targetReady ? (
              <div className="absolute bottom-4 left-4 right-4 rounded-md border border-white/10 bg-zinc-950/88 p-3 shadow-2xl backdrop-blur md:left-auto md:w-[25rem]">
                <div className="flex items-start gap-3">
                  <div className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-md bg-white/10 text-zinc-100">
                    <Crosshair className="h-4 w-4" />
                  </div>
                  <div className="min-w-0 flex-1 space-y-2">
                    <div>
                      <p className="text-sm font-medium text-zinc-100">Fetching selected point</p>
                      <p className="text-xs text-zinc-400">
                        {formatPlaybackTime(targetTime)} · {targetOffsetLabel}
                      </p>
                    </div>
                    <Progress value={bufferPercent} className="h-1.5 bg-white/10" />
                    <div className="flex flex-wrap items-center gap-2 text-xs text-zinc-400">
                      <span>{availability ? `${availability.ranges.length} verified ranges` : "Checking ranges"}</span>
                      {nearestReadyTime != null ? (
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          className="h-7 border-white/15 bg-white/5 px-2 text-zinc-100 hover:bg-white/10"
                          onClick={jumpToNearestReadyPoint}
                        >
                          <SkipForward className="h-3.5 w-3.5" />
                          Jump to {formatPlaybackTime(nearestReadyTime)}
                        </Button>
                      ) : null}
                    </div>
                  </div>
                </div>
              </div>
            ) : null}
          </div>

          <footer className="mt-3 grid gap-3 rounded-md border border-white/10 bg-white/[0.04] p-3 text-xs text-zinc-300 md:grid-cols-[1fr_auto] md:items-center">
            <div className="min-w-0 space-y-2">
              <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
                <span>{availability ? `${formatBytes(availability.verified_bytes)} verified` : "Checking buffer"}</span>
                <span>{availability ? `${availability.ranges.length} range${availability.ranges.length === 1 ? "" : "s"}` : "0 ranges"}</span>
                <span>{priority ? `${priority.total_priority_pieces} priority pieces` : "Priority pending"}</span>
                <span>{targetReady ? "Current point ready" : targetOffsetLabel}</span>
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

function estimatePlaybackTime(offset: number, duration: number | undefined, length: number | undefined) {
  if (!length || length <= 0 || !duration || !Number.isFinite(duration) || duration <= 0) return null;
  const ratio = Math.max(0, Math.min(1, offset / length));
  return duration * ratio;
}

function nearestReadyPlaybackTime(
  availability: TorrentFileAvailability,
  offset: number,
  duration: number | undefined
) {
  const nearestRange = availability.ranges
    .filter((range) => range.length > 0)
    .map((range) => {
      const end = range.offset + range.length - 1;
      const nearestOffset = offset < range.offset ? range.offset : Math.min(offset, end);
      return {
        offset: nearestOffset,
        distance: Math.abs(nearestOffset - offset)
      };
    })
    .sort((left, right) => left.distance - right.distance)[0];
  return nearestRange ? estimatePlaybackTime(nearestRange.offset, duration, availability.length) : null;
}

function formatPlaybackTime(seconds: number) {
  if (!Number.isFinite(seconds) || seconds < 0) return "0:00";
  const totalSeconds = Math.floor(seconds);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const remainder = totalSeconds % 60;
  if (hours > 0) {
    return `${hours}:${minutes.toString().padStart(2, "0")}:${remainder.toString().padStart(2, "0")}`;
  }
  return `${minutes}:${remainder.toString().padStart(2, "0")}`;
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
