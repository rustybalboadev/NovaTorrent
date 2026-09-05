"use client";

import * as React from "react";
import {
  Captions,
  Check,
  ChevronDown,
  Clapperboard,
  Crosshair,
  GripVertical,
  ListVideo,
  Loader2,
  Maximize,
  Minimize,
  Pause,
  PanelRightClose,
  PanelRightOpen,
  Play,
  SkipBack,
  SkipForward,
  Volume2,
  VolumeX,
  X
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import {
  closeMediaWindow,
  clearStreamPriority,
  mediaPlayerLog,
  setStreamPriority,
  streamFileAvailability,
  streamFileUrl,
  subtitleFileText,
  torrentDetails
} from "@/lib/tauri-api";
import type { StreamPriorityRequest, TorrentFileAvailability, TorrentDetails } from "@/lib/torrent-types";
import { isPlayableMediaName } from "@/lib/media";
import { cn, percent } from "@/lib/utils";

const networkRetryLimit = 12;
const streamUrgentBytes = 16 * 1024 * 1024;
const streamLookaheadBytes = 192 * 1024 * 1024;
const mediaErrorSrcNotSupported = 4;
const playbackPositionStoragePrefix = "novatorrent.playbackPosition";
const captionPreferenceStorageKey = "novatorrent.captionLanguage";

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
  const rawFileIndex = search.get("fileIndex");
  const fileIndex = rawFileIndex == null || rawFileIndex.trim() === "" ? Number.NaN : Number(rawFileIndex);
  if (!id || !Number.isInteger(fileIndex) || fileIndex < 0) {
    return { params: null, error: "Could not open media file." };
  }
  return { params: { id, fileIndex }, error: null };
}

function playableEntries(details: TorrentDetails | null) {
  return (details?.files ?? [])
    .map((file, fileIndex) => ({ file, fileIndex }))
    .filter(({ file }) => file.included && isPlayableMediaName(file.name));
}

const subtitleLanguages: Record<string, { code: string; label: string }> = {
  en: { code: "en", label: "English" }, eng: { code: "en", label: "English" }, english: { code: "en", label: "English" },
  es: { code: "es", label: "Spanish" }, spa: { code: "es", label: "Spanish" }, spanish: { code: "es", label: "Spanish" },
  fr: { code: "fr", label: "French" }, fra: { code: "fr", label: "French" }, fre: { code: "fr", label: "French" }, french: { code: "fr", label: "French" },
  de: { code: "de", label: "German" }, deu: { code: "de", label: "German" }, ger: { code: "de", label: "German" }, german: { code: "de", label: "German" },
  it: { code: "it", label: "Italian" }, ita: { code: "it", label: "Italian" }, italian: { code: "it", label: "Italian" },
  pt: { code: "pt", label: "Portuguese" }, por: { code: "pt", label: "Portuguese" }, portuguese: { code: "pt", label: "Portuguese" },
  ru: { code: "ru", label: "Russian" }, rus: { code: "ru", label: "Russian" }, russian: { code: "ru", label: "Russian" },
  ja: { code: "ja", label: "Japanese" }, jpn: { code: "ja", label: "Japanese" }, japanese: { code: "ja", label: "Japanese" },
  ko: { code: "ko", label: "Korean" }, kor: { code: "ko", label: "Korean" }, korean: { code: "ko", label: "Korean" },
  zh: { code: "zh", label: "Chinese" }, zho: { code: "zh", label: "Chinese" }, chi: { code: "zh", label: "Chinese" }, chinese: { code: "zh", label: "Chinese" },
  ar: { code: "ar", label: "Arabic" }, ara: { code: "ar", label: "Arabic" }, arabic: { code: "ar", label: "Arabic" },
  hi: { code: "hi", label: "Hindi" }, hin: { code: "hi", label: "Hindi" }, hindi: { code: "hi", label: "Hindi" }
};

function subtitleEntries(details: TorrentDetails | null) {
  return (details?.files ?? []).flatMap((file, fileIndex) => {
    if (!file.included || !/\.(srt|vtt)$/i.test(file.name)) return [];
    const tokens = file.name.toLowerCase().split(/[\\/._\-\s()[\]]+/).filter(Boolean);
    const language = tokens.map((token) => subtitleLanguages[token]).find(Boolean);
    return [{
      file,
      fileIndex,
      languageCode: language?.code ?? "und",
      languageLabel: language?.label ?? "Unknown language"
    }];
  });
}

function subtitleTextToVtt(name: string, text: string) {
  const normalized = text.replace(/^\uFEFF/, "").replace(/\r\n?/g, "\n").replace(/<[^>]*>/g, "");
  if (/\.vtt$/i.test(name) && normalized.trimStart().startsWith("WEBVTT")) return normalized;
  return `WEBVTT\n\n${normalized.replace(/(\d{2}:\d{2}:\d{2}),(\d{3})/g, "$1.$2")}`;
}

function reconcileQueueOrder(current: number[], entries: ReturnType<typeof playableEntries>) {
  const available = entries.map((entry) => entry.fileIndex);
  const availableSet = new Set(available);
  const retained = current.filter((fileIndex) => availableSet.has(fileIndex));
  const retainedSet = new Set(retained);
  const next = [...retained, ...available.filter((fileIndex) => !retainedSet.has(fileIndex))];
  return next.length === current.length && next.every((fileIndex, index) => fileIndex === current[index])
    ? current
    : next;
}

export function MediaPlayerWindow() {
  const videoRef = React.useRef<HTMLVideoElement | null>(null);
  const playerFrameRef = React.useRef<HTMLDivElement | null>(null);
  const shouldResumeRef = React.useRef(false);
  const userWantsPlaybackRef = React.useRef(false);
  const lastTimeRef = React.useRef(0);
  const pendingResumeTimeRef = React.useRef<number | null>(null);
  const retryingForSeekRef = React.useRef(false);
  const retryTimerRef = React.useRef<number | null>(null);
  const controlsHideTimerRef = React.useRef<number | null>(null);
  const lastLoggedEventRef = React.useRef<{ event: string; at: number } | null>(null);
  const lastPositionSaveRef = React.useRef(0);
  const restoredPositionRef = React.useRef(false);
  const availabilityRef = React.useRef<TorrentFileAvailability | null>(null);
  const subtitleUrlRef = React.useRef("");
  const originalWindowParamsRef = React.useRef<ViewerParams | null>(null);
  const sourceEpochRef = React.useRef(0);
  const priorityMutationRef = React.useRef<Promise<void>>(Promise.resolve());
  const [params, setParams] = React.useState<ViewerParams | null>(null);
  const [details, setDetails] = React.useState<TorrentDetails | null>(null);
  const [availability, setAvailability] = React.useState<TorrentFileAvailability | null>(null);
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
  const [currentTime, setCurrentTime] = React.useState(0);
  const [duration, setDuration] = React.useState(0);
  const [scrubTime, setScrubTime] = React.useState<number | null>(null);
  const [playing, setPlaying] = React.useState(false);
  const [volume, setVolume] = React.useState(1);
  const [muted, setMuted] = React.useState(false);
  const [playbackRate, setPlaybackRate] = React.useState(1);
  const [fullscreen, setFullscreen] = React.useState(false);
  const [browserBuffered, setBrowserBuffered] = React.useState<Array<{ start: number; end: number }>>([]);
  const [queueOpen, setQueueOpen] = React.useState(true);
  const [autoAdvance, setAutoAdvance] = React.useState(true);
  const [queueOrder, setQueueOrder] = React.useState<number[]>([]);
  const [draggedFileIndex, setDraggedFileIndex] = React.useState<number | null>(null);
  const [captionsMenuOpen, setCaptionsMenuOpen] = React.useState(false);
  const [selectedSubtitleFileIndex, setSelectedSubtitleFileIndex] = React.useState<number | null>(null);
  const [subtitleUrl, setSubtitleUrl] = React.useState("");
  const [subtitleLoading, setSubtitleLoading] = React.useState(false);
  const [subtitleError, setSubtitleError] = React.useState<string | null>(null);
  const [controlsVisible, setControlsVisible] = React.useState(true);
  const detectedPlayableFiles = React.useMemo(
    () => playableEntries(details),
    [details]
  );
  const playableFiles = React.useMemo(() => {
    const byIndex = new Map(detectedPlayableFiles.map((entry) => [entry.fileIndex, entry]));
    const ordered = queueOrder.flatMap((fileIndex) => {
      const entry = byIndex.get(fileIndex);
      return entry ? [entry] : [];
    });
    const queued = new Set(ordered.map((entry) => entry.fileIndex));
    return [...ordered, ...detectedPlayableFiles.filter((entry) => !queued.has(entry.fileIndex))];
  }, [detectedPlayableFiles, queueOrder]);
  const subtitles = React.useMemo(() => subtitleEntries(details), [details]);

  const clearControlsHideTimer = React.useCallback(() => {
    if (controlsHideTimerRef.current != null) {
      window.clearTimeout(controlsHideTimerRef.current);
      controlsHideTimerRef.current = null;
    }
  }, []);

  const keepControlsVisible = React.useCallback(() => {
    clearControlsHideTimer();
    setControlsVisible(true);
  }, [clearControlsHideTimer]);

  const revealControls = React.useCallback(() => {
    keepControlsVisible();
    if (!playing || captionsMenuOpen || scrubTime != null) return;
    controlsHideTimerRef.current = window.setTimeout(() => {
      setControlsVisible(false);
      controlsHideTimerRef.current = null;
    }, 2500);
  }, [captionsMenuOpen, keepControlsVisible, playing, scrubTime]);

  React.useEffect(() => {
    clearControlsHideTimer();
    if (playing && !captionsMenuOpen && scrubTime == null) {
      controlsHideTimerRef.current = window.setTimeout(() => {
        setControlsVisible(false);
        controlsHideTimerRef.current = null;
      }, 2500);
    }
    return clearControlsHideTimer;
  }, [captionsMenuOpen, clearControlsHideTimer, playing, scrubTime]);

  function queueStreamPriority(
    id: string,
    request: StreamPriorityRequest,
    sourceEpoch = sourceEpochRef.current
  ) {
    const mutation = priorityMutationRef.current.then(async () => {
      if (sourceEpoch !== sourceEpochRef.current) return null;
      return setStreamPriority(id, request);
    });
    priorityMutationRef.current = mutation.then(() => undefined, () => undefined);
    return mutation;
  }

  React.useEffect(() => {
    availabilityRef.current = availability;
  }, [availability]);

  React.useEffect(() => {
    let disposed = false;
    window.queueMicrotask(() => {
      if (disposed) return;
      if (!params || !subtitles.length) {
        setSelectedSubtitleFileIndex(null);
        return;
      }
      const storedPreference = window.localStorage.getItem(captionPreferenceStorageKey);
      if (storedPreference === "off") {
        setSelectedSubtitleFileIndex(null);
        return;
      }
      const browserLanguage = window.navigator.language.split("-")[0]?.toLowerCase();
      const preferredLanguage = storedPreference || browserLanguage;
      const preferred = subtitles.find((entry) => entry.languageCode === preferredLanguage);
      setSelectedSubtitleFileIndex(preferred?.fileIndex ?? (subtitles.length === 1 ? subtitles[0].fileIndex : null));
    });
    return () => {
      disposed = true;
    };
  }, [params, subtitles]);

  React.useEffect(() => {
    let disposed = false;
    let retryTimer: number | null = null;
    const replaceSubtitleUrl = (nextUrl: string) => {
      if (subtitleUrlRef.current) URL.revokeObjectURL(subtitleUrlRef.current);
      subtitleUrlRef.current = nextUrl;
      setSubtitleUrl(nextUrl);
    };
    window.queueMicrotask(() => {
      if (disposed) return;
      replaceSubtitleUrl("");
      setSubtitleError(null);
      if (!params || selectedSubtitleFileIndex == null) setSubtitleLoading(false);
    });
    if (!params || selectedSubtitleFileIndex == null) {
      return () => {
        disposed = true;
      };
    }

    const loadSubtitle = async () => {
      setSubtitleLoading(true);
      try {
        const subtitle = await subtitleFileText(params.id, selectedSubtitleFileIndex);
        if (disposed) return;
        const blob = new Blob([subtitleTextToVtt(subtitle.name, subtitle.text)], { type: "text/vtt" });
        replaceSubtitleUrl(URL.createObjectURL(blob));
        setSubtitleLoading(false);
        setSubtitleError(null);
      } catch (err) {
        if (disposed) return;
        const message = err instanceof Error ? err.message : "Could not load captions.";
        if (message.toLowerCase().includes("still downloading")) {
          retryTimer = window.setTimeout(loadSubtitle, 1_000);
        } else {
          setSubtitleLoading(false);
          setSubtitleError(message);
        }
      }
    };
    void loadSubtitle();
    return () => {
      disposed = true;
      if (retryTimer != null) window.clearTimeout(retryTimer);
    };
  }, [params, selectedSubtitleFileIndex]);

  React.useEffect(() => {
    return () => {
      if (subtitleUrlRef.current) URL.revokeObjectURL(subtitleUrlRef.current);
    };
  }, []);

  const updateBufferMetrics = React.useCallback(
    (nextAvailability: TorrentFileAvailability | null = availabilityRef.current) => {
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
    []
  );

  React.useEffect(() => {
    let disposed = false;
    window.queueMicrotask(() => {
      if (disposed) return;
      const initialState = parseViewerParams(new URLSearchParams(window.location.search));
      originalWindowParamsRef.current = initialState.params;
      setParams(initialState.params);
      setError(initialState.error);
      setBusy(Boolean(initialState.params && !initialState.error));
    });
    return () => {
      disposed = true;
    };
  }, []);

  React.useEffect(() => {
    const handleFullscreenChange = () => setFullscreen(document.fullscreenElement === playerFrameRef.current);
    document.addEventListener("fullscreenchange", handleFullscreenChange);
    return () => document.removeEventListener("fullscreenchange", handleFullscreenChange);
  }, []);

  React.useEffect(() => {
    if (!params) return;
    const viewerParams = params;
    const sourceEpoch = ++sourceEpochRef.current;
    let disposed = false;

    async function prepare() {
      try {
        const nextDetails = await torrentDetails(viewerParams.id);
        if (disposed || sourceEpoch !== sourceEpochRef.current) return;
        const supplementalFileIndices = subtitleEntries(nextDetails).map((entry) => entry.fileIndex);
        const [, nextUrl, nextAvailability] = await Promise.all([
          queueStreamPriority(viewerParams.id, {
            fileIndex: viewerParams.fileIndex,
            playheadOffset: 0,
            urgentBytes: streamUrgentBytes,
            lookaheadBytes: streamLookaheadBytes,
            supplementalFileIndices
          }, sourceEpoch),
          streamFileUrl(viewerParams.id, viewerParams.fileIndex),
          streamFileAvailability(viewerParams.id, viewerParams.fileIndex)
        ]);
        if (disposed || sourceEpoch !== sourceEpochRef.current) return;
        setError(null);
        setDetails(nextDetails);
        setQueueOrder((current) => reconcileQueueOrder(current, playableEntries(nextDetails)));
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
        if (!disposed && sourceEpoch === sourceEpochRef.current) {
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
    let refreshTimer: number | null = null;
    const scheduleRefresh = () => {
      const video = videoRef.current;
      const delay = retryingForSeekRef.current ? 750 : video && !video.paused ? 2_500 : 5_000;
      refreshTimer = window.setTimeout(async () => {
        await refresh();
        if (!disposed) scheduleRefresh();
      }, delay);
    };
    void refresh().finally(() => {
      if (!disposed) scheduleRefresh();
    });
    return () => {
      disposed = true;
      if (refreshTimer != null) window.clearTimeout(refreshTimer);
    };
  }, [params, updateBufferMetrics]);

  React.useEffect(() => {
    return () => {
      if (retryTimerRef.current != null) window.clearTimeout(retryTimerRef.current);
    };
  }, []);

  const streamSrc = streamUrl ? `${streamUrl}${retryKey ? `?retry=${retryKey}` : ""}` : "";
  const fileName =
    (params ? details?.files?.[params.fileIndex]?.name : undefined) ?? availability?.name ?? "Media";
  const torrentName = details?.name ?? "NovaTorrent";
  const selectedSubtitle = subtitles.find((entry) => entry.fileIndex === selectedSubtitleFileIndex);

  function mediaNumber(value: number | undefined) {
    return value != null && Number.isFinite(value) ? value : null;
  }

  function logPlayerEvent(event: string, message?: string) {
    if (!params) return;
    const now = Date.now();
    const lastLogged = lastLoggedEventRef.current;
    if (lastLogged?.event === event && now - lastLogged.at < 2_000) return;
    lastLoggedEventRef.current = { event, at: now };
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
    if (params && Date.now() - lastPositionSaveRef.current >= 5_000) {
      window.localStorage.setItem(`${playbackPositionStoragePrefix}.${params.id}.${params.fileIndex}`, String(time));
      lastPositionSaveRef.current = Date.now();
    }
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
    if (
      isPlaybackTimeBuffered(video, video.currentTime) ||
      (offset != null && availability && isOffsetVerified(availability, offset))
    ) {
      markTargetReady(offset, "seek target is already downloaded");
      return;
    }
    retryingForSeekRef.current = true;
    setFetchingSeekPoint(true);
    setBuffering(true);
    logPlayerEvent("seek-fetch", "seek target is not verified yet");
    if (params && offset != null) {
      const sourceEpoch = sourceEpochRef.current;
      void queueStreamPriority(params.id, {
        fileIndex: params.fileIndex,
        playheadOffset: offset,
        urgentBytes: streamUrgentBytes,
        lookaheadBytes: streamLookaheadBytes,
        supplementalFileIndices: subtitles.map((entry) => entry.fileIndex)
      }, sourceEpoch).catch(() => undefined);
    }
  }

  function handlePlaybackProgress() {
    rememberPosition();
    syncPlayerUi();
    const currentOffset = updateBufferMetrics();
    if (currentOffset != null && availability && isOffsetVerified(availability, currentOffset)) {
      markTargetReady(currentOffset);
    }
  }

  function syncPlayerUi(video = videoRef.current) {
    if (!video) return;
    setCurrentTime(Number.isFinite(video.currentTime) ? video.currentTime : 0);
    setDuration(Number.isFinite(video.duration) ? video.duration : 0);
    setPlaying(!video.paused && !video.ended);
    setVolume(video.volume);
    setMuted(video.muted);
    setPlaybackRate(video.playbackRate);
    setBrowserBuffered(
      Array.from({ length: video.buffered.length }, (_, index) => ({
        start: video.buffered.start(index),
        end: video.buffered.end(index)
      }))
    );
  }

  function togglePlayback() {
    const video = videoRef.current;
    if (!video) return;
    if (video.paused) {
      userWantsPlaybackRef.current = true;
      shouldResumeRef.current = true;
      void video.play().catch(() => undefined);
    } else {
      userWantsPlaybackRef.current = false;
      shouldResumeRef.current = false;
      video.pause();
    }
  }

  function commitSeek(time: number) {
    const video = videoRef.current;
    if (!video || !Number.isFinite(time)) return;
    const nextTime = Math.max(0, Math.min(video.duration || 0, time));
    rememberTargetTime(nextTime);
    setScrubTime(null);
    setCurrentTime(nextTime);
    video.currentTime = nextTime;
  }

  function skipBy(seconds: number) {
    const video = videoRef.current;
    if (!video) return;
    commitSeek(video.currentTime + seconds);
  }

  function changeVolume(nextVolume: number) {
    const video = videoRef.current;
    if (!video) return;
    video.volume = nextVolume;
    video.muted = nextVolume === 0;
    syncPlayerUi(video);
  }

  function toggleMute() {
    const video = videoRef.current;
    if (!video) return;
    video.muted = !video.muted;
    syncPlayerUi(video);
  }

  function changePlaybackRate(nextRate: number) {
    const video = videoRef.current;
    if (!video) return;
    video.playbackRate = nextRate;
    syncPlayerUi(video);
  }

  async function toggleFullscreen() {
    const frame = playerFrameRef.current;
    if (!frame) return;
    if (document.fullscreenElement) await document.exitFullscreen();
    else await frame.requestFullscreen();
  }

  function selectPlayableFile(fileIndex: number, forcePlayback = false) {
    if (!params || params.fileIndex === fileIndex) return;
    const nextFile = details?.files?.[fileIndex];
    if (!nextFile?.included || !isPlayableMediaName(nextFile.name)) return;

    const video = videoRef.current;
    const resumePlayback = forcePlayback || Boolean(video && (!video.paused || userWantsPlaybackRef.current));
    rememberPosition();
    if (params && lastTimeRef.current > 0) {
      window.localStorage.setItem(
        `${playbackPositionStoragePrefix}.${params.id}.${params.fileIndex}`,
        String(lastTimeRef.current)
      );
    }
    video?.pause();
    if (retryTimerRef.current != null) {
      window.clearTimeout(retryTimerRef.current);
      retryTimerRef.current = null;
    }

    availabilityRef.current = null;
    sourceEpochRef.current += 1;
    lastTimeRef.current = 0;
    pendingResumeTimeRef.current = null;
    retryingForSeekRef.current = false;
    restoredPositionRef.current = false;
    lastPositionSaveRef.current = 0;
    lastLoggedEventRef.current = null;
    userWantsPlaybackRef.current = resumePlayback;
    shouldResumeRef.current = resumePlayback;
    setAvailability(null);
    setStreamUrl("");
    setRetryKey(0);
    setNetworkRetries(0);
    setBusy(true);
    setBuffering(false);
    setFetchingSeekPoint(false);
    setBufferedAheadSeconds(null);
    setTargetOffset(null);
    setTargetReady(false);
    setTargetTime(0);
    setNearestReadyTime(null);
    setError(null);
    setCurrentTime(0);
    setDuration(0);
    setScrubTime(null);
    setPlaying(false);
    setBrowserBuffered([]);

    const nextParams = { id: params.id, fileIndex };
    const nextUrl = new URL(window.location.href);
    nextUrl.searchParams.set("id", nextParams.id);
    nextUrl.searchParams.set("fileIndex", String(nextParams.fileIndex));
    window.history.replaceState(null, "", nextUrl);
    setParams(nextParams);
  }

  function moveQueueItem(sourceFileIndex: number, targetFileIndex: number) {
    if (sourceFileIndex === targetFileIndex) return;
    setQueueOrder((current) => {
      const completeOrder = playableFiles.map((entry) => entry.fileIndex);
      const sourceIndex = completeOrder.indexOf(sourceFileIndex);
      const targetIndex = completeOrder.indexOf(targetFileIndex);
      if (sourceIndex < 0 || targetIndex < 0) return current;
      const next = [...completeOrder];
      const [moved] = next.splice(sourceIndex, 1);
      next.splice(targetIndex, 0, moved);
      return next;
    });
  }

  function selectSubtitle(fileIndex: number | null) {
    setSelectedSubtitleFileIndex(fileIndex);
    setCaptionsMenuOpen(false);
    if (fileIndex == null) {
      window.localStorage.setItem(captionPreferenceStorageKey, "off");
      return;
    }
    const selected = subtitles.find((entry) => entry.fileIndex === fileIndex);
    if (selected?.languageCode && selected.languageCode !== "und") {
      window.localStorage.setItem(captionPreferenceStorageKey, selected.languageCode);
    }
  }

  function handleMediaEnded() {
    syncPlayerUi();
    if (params) {
      window.localStorage.removeItem(`${playbackPositionStoragePrefix}.${params.id}.${params.fileIndex}`);
    }
    const currentQueueIndex = playableFiles.findIndex((entry) => entry.fileIndex === params?.fileIndex);
    const nextEntry = currentQueueIndex >= 0 ? playableFiles[currentQueueIndex + 1] : undefined;
    if (autoAdvance && nextEntry) {
      selectPlayableFile(nextEntry.fileIndex, true);
      return;
    }
    userWantsPlaybackRef.current = false;
    shouldResumeRef.current = false;
    setPlaying(false);
    setControlsVisible(true);
  }

  function restoreSavedPosition(video = videoRef.current) {
    if (!video || !params || restoredPositionRef.current || !Number.isFinite(video.duration)) return;
    restoredPositionRef.current = true;
    const saved = Number(
      window.localStorage.getItem(`${playbackPositionStoragePrefix}.${params.id}.${params.fileIndex}`)
    );
    if (!Number.isFinite(saved) || saved < 5 || saved >= video.duration - 10) return;
    rememberTargetTime(saved);
    setCurrentTime(saved);
    video.currentTime = saved;
  }

  function scheduleNetworkRetry() {
    const video = videoRef.current;
    const mediaError = video?.error;
    if (!video) return;
    if (mediaError?.code === mediaErrorSrcNotSupported) {
      const message = "This container or video codec is not supported by the Windows media engine.";
      setError(message);
      logPlayerEvent("unsupported-media", message);
      return;
    }
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
    const windowParams = originalWindowParamsRef.current ?? params;
    if (windowParams) {
      try {
        logPlayerEvent("close");
        await closeMediaWindow(windowParams.id, windowParams.fileIndex);
        return;
      } catch {
        try {
          await clearStreamPriority(windowParams.id);
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
          <div className="flex min-w-0 flex-1 items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-md bg-primary text-primary-foreground">
              <Clapperboard className="h-4 w-4" />
            </div>
            <div className="min-w-0 flex-1">
              <h1 className="truncate text-sm font-semibold">{fileName}</h1>
              <p className="truncate text-xs text-zinc-400">{torrentName}</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="border-white/15 bg-white/5 text-zinc-100 hover:bg-white/10"
              aria-expanded={queueOpen}
              aria-controls="media-queue"
              onClick={() => setQueueOpen((open) => !open)}
            >
              {queueOpen ? <PanelRightClose className="h-4 w-4" /> : <PanelRightOpen className="h-4 w-4" />}
              {queueOpen ? "Hide queue" : `Queue (${playableFiles.length})`}
            </Button>
            <Button type="button" variant="outline" size="sm" className="shrink-0 border-white/15 bg-white/5 text-zinc-100 hover:bg-white/10" onClick={closeViewer}>
              <X className="h-4 w-4" />
              Close
            </Button>
          </div>
        </header>

        <section className="flex min-h-0 flex-1 p-3">
          <div className={cn("grid min-h-0 flex-1 gap-3", queueOpen ? "grid-cols-[minmax(0,1fr)_minmax(240px,300px)] max-[680px]:grid-cols-1" : "grid-cols-1")}>
            <div className="flex min-h-0 flex-col">
              <div
                ref={playerFrameRef}
                className={cn(
                  "group relative flex min-h-0 flex-1 items-center justify-center overflow-hidden rounded-md border border-white/10 bg-black",
                  !controlsVisible && playing && "cursor-none"
                )}
                onPointerMove={revealControls}
                onPointerDown={revealControls}
                onFocusCapture={keepControlsVisible}
                onBlurCapture={revealControls}
              >
            {streamSrc ? (
              <video
                key={params ? `${params.id}:${params.fileIndex}` : "media"}
                ref={videoRef}
                className="h-full max-h-[calc(100vh-9.5rem)] w-full bg-black object-contain"
                preload="auto"
                src={streamSrc}
                onClick={togglePlayback}
                onPlay={() => {
                  userWantsPlaybackRef.current = true;
                  shouldResumeRef.current = true;
                  setBuffering(false);
                  setPlaying(true);
                  setControlsVisible(true);
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
                  setPlaying(false);
                  setControlsVisible(true);
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
                onLoadedMetadata={(event) => {
                  event.currentTarget.volume = volume;
                  event.currentTarget.muted = muted;
                  event.currentTarget.playbackRate = playbackRate;
                  syncPlayerUi(event.currentTarget);
                  restoreSavedPosition();
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
                  syncPlayerUi();
                  clearBufferingIfTargetReady("progress event reached verified target");
                  resumeWhenReady();
                }}
                onError={scheduleNetworkRetry}
                onVolumeChange={() => syncPlayerUi()}
                onRateChange={() => syncPlayerUi()}
                onEnded={handleMediaEnded}
              >
                {subtitleUrl && selectedSubtitle ? (
                  <track
                    key={subtitleUrl}
                    kind="subtitles"
                    src={subtitleUrl}
                    srcLang={selectedSubtitle.languageCode}
                    label={selectedSubtitle.languageLabel}
                    default
                  />
                ) : null}
              </video>
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
              <div className="absolute bottom-24 left-4 right-4 rounded-md border border-white/10 bg-zinc-950/88 p-3 shadow-2xl backdrop-blur md:left-auto md:w-[25rem]">
                <div className="flex items-start gap-3">
                  <div className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-md bg-white/10 text-zinc-100">
                    <Crosshair className="h-4 w-4" />
                  </div>
                  <div className="min-w-0 flex-1">
                    <div>
                      <p className="text-sm font-medium text-zinc-100">Fetching selected point</p>
                      <p className="text-xs text-zinc-400">{formatPlaybackTime(targetTime)} is not downloaded yet.</p>
                    </div>
                    {nearestReadyTime != null ? (
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        className="mt-2 h-7 border-white/15 bg-white/5 px-2 text-zinc-100 hover:bg-white/10"
                        onClick={jumpToNearestReadyPoint}
                      >
                        <SkipForward className="h-3.5 w-3.5" />
                        Play from {formatPlaybackTime(nearestReadyTime)}
                      </Button>
                    ) : null}
                  </div>
                </div>
              </div>
            ) : null}
            {streamSrc ? (
              <div
                className={cn(
                  "absolute inset-x-0 bottom-0 bg-gradient-to-t from-black via-black/85 to-transparent px-3 pb-3 pt-10 text-white transition-opacity duration-200 motion-reduce:transition-none",
                  controlsVisible ? "opacity-100" : "pointer-events-none opacity-0"
                )}
              >
                <div className="relative mb-2 h-5">
                  <div className="absolute inset-x-0 top-2 h-1 rounded-full bg-white/20">
                    {duration > 0
                      ? browserBuffered.map((range, index) => (
                          <span
                            key={`${range.start}-${range.end}-${index}`}
                            className="absolute h-full rounded-full bg-white/45"
                            style={{ left: `${(range.start / duration) * 100}%`, width: `${((range.end - range.start) / duration) * 100}%` }}
                          />
                        ))
                      : null}
                    <span
                      className="absolute h-full rounded-full bg-primary"
                      style={{ width: `${duration > 0 ? ((scrubTime ?? currentTime) / duration) * 100 : 0}%` }}
                    />
                  </div>
                  <input
                    aria-label="Seek"
                    className="absolute inset-0 h-5 w-full cursor-pointer opacity-0"
                    type="range"
                    min={0}
                    max={duration || 0}
                    step={0.1}
                    value={scrubTime ?? currentTime}
                    onChange={(event) => setScrubTime(Number(event.currentTarget.value))}
                    onPointerUp={(event) => commitSeek(Number(event.currentTarget.value))}
                    onKeyUp={(event) => commitSeek(Number(event.currentTarget.value))}
                  />
                </div>
                <div className="flex items-center gap-1.5">
                  <button type="button" className="rounded p-2 hover:bg-white/15" aria-label={playing ? "Pause" : "Play"} onClick={togglePlayback}>
                    {playing ? <Pause className="h-5 w-5" /> : <Play className="h-5 w-5" />}
                  </button>
                  <button type="button" className="rounded p-2 hover:bg-white/15" aria-label="Back 10 seconds" onClick={() => skipBy(-10)}>
                    <SkipBack className="h-4 w-4" />
                  </button>
                  <button type="button" className="rounded p-2 hover:bg-white/15" aria-label="Forward 10 seconds" onClick={() => skipBy(10)}>
                    <SkipForward className="h-4 w-4" />
                  </button>
                  <button type="button" className="rounded p-2 hover:bg-white/15" aria-label={muted ? "Unmute" : "Mute"} onClick={toggleMute}>
                    {muted || volume === 0 ? <VolumeX className="h-4 w-4" /> : <Volume2 className="h-4 w-4" />}
                  </button>
                  <input
                    aria-label="Volume"
                    className="hidden w-20 accent-white sm:block"
                    type="range"
                    min={0}
                    max={1}
                    step={0.05}
                    value={muted ? 0 : volume}
                    onChange={(event) => changeVolume(Number(event.currentTarget.value))}
                  />
                  <span className="ml-1 whitespace-nowrap text-xs tabular-nums text-white/85">
                    {formatPlaybackTime(scrubTime ?? currentTime)} / {formatPlaybackTime(duration)}
                  </span>
                  <div className="ml-auto flex items-center gap-1.5">
                    {subtitles.length ? (
                      <div className="relative">
                        <button
                          type="button"
                          className={cn(
                            "rounded p-2 hover:bg-white/15",
                            selectedSubtitleFileIndex != null && "bg-white/15 text-primary"
                          )}
                          aria-label="Captions"
                          aria-expanded={captionsMenuOpen}
                          onClick={() => setCaptionsMenuOpen((open) => !open)}
                        >
                          <Captions className="h-4 w-4" />
                        </button>
                        {captionsMenuOpen ? (
                          <div className="absolute bottom-11 right-0 z-20 w-72 overflow-hidden rounded-md border border-white/15 bg-zinc-950/95 text-left shadow-2xl backdrop-blur">
                            <div className="border-b border-white/10 px-3 py-2.5">
                              <p className="text-sm font-semibold text-white">Captions</p>
                              <p className="text-[11px] text-zinc-400">
                                {subtitleLoading ? "Preparing selected captions…" : `${subtitles.length} subtitle file${subtitles.length === 1 ? "" : "s"}`}
                              </p>
                            </div>
                            <div className="max-h-64 overflow-y-auto p-1.5">
                              <button
                                type="button"
                                className="flex w-full items-center gap-2 rounded px-2 py-2 text-sm text-zinc-200 hover:bg-white/10"
                                onClick={() => selectSubtitle(null)}
                              >
                                <span className="flex h-4 w-4 items-center justify-center">
                                  {selectedSubtitleFileIndex == null ? <Check className="h-4 w-4" /> : null}
                                </span>
                                Off
                              </button>
                              {subtitles.map((subtitle) => (
                                <button
                                  key={subtitle.fileIndex}
                                  type="button"
                                  className="flex w-full items-start gap-2 rounded px-2 py-2 text-left hover:bg-white/10"
                                  onClick={() => selectSubtitle(subtitle.fileIndex)}
                                >
                                  <span className="mt-0.5 flex h-4 w-4 shrink-0 items-center justify-center">
                                    {selectedSubtitleFileIndex === subtitle.fileIndex ? <Check className="h-4 w-4" /> : null}
                                  </span>
                                  <span className="min-w-0">
                                    <span className="block text-sm text-zinc-100">{subtitle.languageLabel}</span>
                                    <span className="block truncate text-[11px] text-zinc-500">{mediaLeafName(subtitle.file.name)}</span>
                                  </span>
                                </button>
                              ))}
                            </div>
                            {subtitleError ? <p className="border-t border-white/10 px-3 py-2 text-xs text-red-300">{subtitleError}</p> : null}
                          </div>
                        ) : null}
                      </div>
                    ) : null}
                    <div className="relative">
                      <select
                        aria-label="Playback speed"
                        className="h-8 appearance-none rounded border border-white/15 bg-black/50 py-1.5 pl-2.5 pr-7 text-xs text-white outline-none focus:ring-2 focus:ring-white/40"
                        value={playbackRate}
                        onChange={(event) => changePlaybackRate(Number(event.currentTarget.value))}
                      >
                        {[0.5, 0.75, 1, 1.25, 1.5, 2].map((rate) => <option key={rate} value={rate}>{rate}×</option>)}
                      </select>
                      <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3 w-3 -translate-y-1/2 text-white/65" />
                    </div>
                    <button type="button" className="rounded p-2 hover:bg-white/15" aria-label={fullscreen ? "Exit fullscreen" : "Fullscreen"} onClick={() => void toggleFullscreen()}>
                      {fullscreen ? <Minimize className="h-4 w-4" /> : <Maximize className="h-4 w-4" />}
                    </button>
                  </div>
                </div>
              </div>
            ) : null}
              </div>

            </div>

            {queueOpen ? (
              <aside id="media-queue" className="flex min-h-0 flex-col overflow-hidden rounded-md border border-white/10 bg-white/[0.035]">
                <div className="flex items-center gap-2 border-b border-white/10 px-3 py-3">
                  <ListVideo className="h-4 w-4 text-primary" />
                  <div className="min-w-0 flex-1">
                    <h2 className="text-sm font-semibold">Play queue</h2>
                    <p className="text-xs text-zinc-400">Drag files to change the order</p>
                  </div>
                  <span className="rounded bg-white/10 px-1.5 py-0.5 text-xs tabular-nums text-zinc-300">{playableFiles.length}</span>
                  <Button type="button" variant="ghost" size="icon" className="h-8 w-8 text-zinc-300 hover:bg-white/10 hover:text-white" aria-label="Collapse play queue" onClick={() => setQueueOpen(false)}>
                    <PanelRightClose className="h-4 w-4" />
                  </Button>
                </div>

                <div className="flex items-center justify-between gap-3 border-b border-white/10 px-3 py-2.5">
                  <div>
                    <p className="text-xs font-medium text-zinc-200">Play next automatically</p>
                    <p className="text-[11px] text-zinc-500">Continues in queue order</p>
                  </div>
                  <Switch checked={autoAdvance} onCheckedChange={setAutoAdvance} aria-label="Play next file automatically" />
                </div>

                <ol className="min-h-0 flex-1 space-y-1 overflow-y-auto p-2">
                  {playableFiles.length ? playableFiles.map(({ file, fileIndex }, queueIndex) => {
                    const isCurrent = params?.fileIndex === fileIndex;
                    const fileProgress = file.length > 0 ? percent(((file.downloaded ?? 0) / file.length) * 100) : 0;
                    return (
                      <li key={fileIndex}>
                        <div
                          draggable={playableFiles.length > 1}
                          className={cn(
                            "group/queue flex items-center gap-1 rounded-md border border-transparent transition-colors",
                            isCurrent ? "border-primary/35 bg-primary/10" : "hover:bg-white/[0.06]",
                            draggedFileIndex === fileIndex && "opacity-45"
                          )}
                          onDragStart={(event) => {
                            setDraggedFileIndex(fileIndex);
                            event.dataTransfer.effectAllowed = "move";
                            event.dataTransfer.setData("text/plain", String(fileIndex));
                          }}
                          onDragEnd={() => setDraggedFileIndex(null)}
                          onDragOver={(event) => {
                            event.preventDefault();
                            event.dataTransfer.dropEffect = "move";
                          }}
                          onDrop={(event) => {
                            event.preventDefault();
                            const sourceFileIndex = draggedFileIndex ?? Number(event.dataTransfer.getData("text/plain"));
                            if (Number.isInteger(sourceFileIndex)) moveQueueItem(sourceFileIndex, fileIndex);
                            setDraggedFileIndex(null);
                          }}
                        >
                          <span className={cn("flex h-11 w-7 shrink-0 cursor-grab items-center justify-center text-zinc-600 active:cursor-grabbing", playableFiles.length <= 1 && "cursor-default opacity-30")} aria-hidden="true">
                            <GripVertical className="h-4 w-4" />
                          </span>
                          <button
                            type="button"
                            className="min-w-0 flex-1 px-1 py-2 text-left outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-primary"
                            onClick={() => selectPlayableFile(fileIndex, true)}
                            disabled={busy && !isCurrent}
                            aria-current={isCurrent ? "true" : undefined}
                          >
                            <span className="flex items-center gap-2">
                              <span className={cn("w-5 shrink-0 text-[11px] tabular-nums", isCurrent ? "text-primary" : "text-zinc-500")}>
                                {isCurrent ? <Play className="h-3.5 w-3.5 fill-current" /> : queueIndex + 1}
                              </span>
                              <span className={cn("truncate text-xs font-medium", isCurrent ? "text-white" : "text-zinc-200")}>{mediaLeafName(file.name)}</span>
                            </span>
                            <span className="mt-1 flex items-center gap-2 pl-7 text-[11px] text-zinc-500">
                              <span className="truncate">{mediaParentPath(file.name) || "Torrent root"}</span>
                              <span className="ml-auto shrink-0 pr-2 tabular-nums">{fileProgress.toFixed(0)}%</span>
                            </span>
                          </button>
                        </div>
                      </li>
                    );
                  }) : (
                    <li className="flex min-h-32 items-center justify-center px-4 text-center text-xs text-zinc-500">
                      Loading playable files…
                    </li>
                  )}
                </ol>
              </aside>
            ) : null}
          </div>
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

function mediaPathParts(name: string) {
  return name.split(/[\\/]/).filter(Boolean);
}

function mediaLeafName(name: string) {
  return mediaPathParts(name).at(-1) ?? name;
}

function mediaParentPath(name: string) {
  return mediaPathParts(name).slice(0, -1).join(" / ");
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
