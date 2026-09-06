"use client";

import { invoke } from "@tauri-apps/api/core";
import { isPlayableMediaName } from "@/lib/media";
import type {
  AddTorrentRequest,
  AddTorrentResponse,
  LogEntry,
  MediaPlayerLogRequest,
  StreamPriorityRequest,
  StreamPriorityStatus,
  SubtitleFileText,
  TorrentDetails,
  TorrentFileAvailability,
  TorrentListResponse,
  TorrentSummaryListResponse,
  UpdateTorrentOptionsRequest
} from "@/lib/torrent-types";
import { mockPreview, mockTorrents } from "@/lib/mock-data";

declare global {
  interface Window {
    __TAURI_INTERNALS__?: unknown;
  }
}

export function isTauriRuntime() {
  return typeof window !== "undefined" && Boolean(window.__TAURI_INTERNALS__);
}

function mockTorrentById(id: string) {
  return mockTorrents.find((torrent) => String(torrent.id) === id || torrent.info_hash === id) ?? mockTorrents[0];
}

export async function listTorrents(): Promise<TorrentListResponse> {
  if (!isTauriRuntime()) return { torrents: mockTorrents };
  return invoke<TorrentListResponse>("list_torrents");
}

export async function listTorrentSummaries(): Promise<TorrentSummaryListResponse> {
  if (!isTauriRuntime()) {
    return {
      torrents: mockTorrents.map((torrent) => ({
        id: torrent.id,
        info_hash: torrent.info_hash,
        name: torrent.name,
        output_folder: torrent.output_folder,
        stats: torrent.stats ?? {},
        peer_count: torrent.peers?.length ?? 0,
        file_count: torrent.files?.length ?? 0,
        playable_file_count: (torrent.files ?? []).filter((file) => file.included && isPlayableMediaName(file.name)).length,
        first_playable_file_index: firstPlayableMediaIndex(torrent.files ?? [])
      }))
    };
  }
  return invoke<TorrentSummaryListResponse>("list_torrent_summaries");
}

function firstPlayableMediaIndex(files: NonNullable<TorrentDetails["files"]>) {
  const index = files.findIndex((file) => file.included && isPlayableMediaName(file.name));
  return index >= 0 ? index : null;
}

export async function torrentDetails(id: string): Promise<TorrentDetails> {
  if (!isTauriRuntime()) {
    return mockTorrentById(id);
  }
  return invoke<TorrentDetails>("torrent_details", { id });
}

export async function defaultDownloadDir(): Promise<string> {
  if (!isTauriRuntime()) return "C:\\Users\\Example\\Downloads";
  return invoke<string>("default_download_dir");
}

export async function backendLogFilePath(): Promise<string | null> {
  if (!isTauriRuntime()) return "C:\\Users\\Example\\AppData\\Local\\com.novatorrent.desktop\\logs\\novatorrent.log";
  return invoke<string>("backend_log_file_path");
}

export async function openAddTorrentWindow(source?: string) {
  if (!isTauriRuntime()) return false;
  await invoke("open_add_torrent_window", { source: source ?? null });
  return true;
}

export async function closeAddTorrentWindow() {
  if (!isTauriRuntime()) return;
  await invoke("close_add_torrent_window");
}

export async function takePendingOpenSources(): Promise<string[]> {
  if (!isTauriRuntime()) return [];
  return invoke<string[]>("take_pending_open_sources");
}

export async function previewTorrent(request: AddTorrentRequest): Promise<AddTorrentResponse> {
  if (!isTauriRuntime()) {
    const outputFolder = [request.destination || mockPreview.output_folder, request.subFolder]
      .filter(Boolean)
      .join("\\");
    return {
      id: null,
      details: {
        ...mockPreview,
        output_folder: outputFolder
      },
      output_folder: outputFolder,
      seen_peers: []
    };
  }
  return invoke<AddTorrentResponse>("preview_torrent", { request });
}

export async function addTorrent(request: AddTorrentRequest): Promise<AddTorrentResponse> {
  if (!isTauriRuntime()) {
    const outputFolder = [request.destination || mockPreview.output_folder, request.subFolder]
      .filter(Boolean)
      .join("\\");
    return {
      id: Date.now(),
      details: {
        ...mockPreview,
        id: Date.now(),
        output_folder: outputFolder
      },
      output_folder: outputFolder,
      seen_peers: []
    };
  }
  return invoke<AddTorrentResponse>("add_torrent", { request });
}

export async function pauseTorrent(id: string) {
  if (!isTauriRuntime()) return;
  await invoke("pause_torrent", { id });
}

export async function resumeTorrent(id: string) {
  if (!isTauriRuntime()) return;
  await invoke("resume_torrent", { id });
}

export async function announceTorrent(id: string) {
  if (!isTauriRuntime()) return;
  await invoke("announce_torrent", { id });
}

export async function downloadWebSeedTorrent(id: string) {
  if (!isTauriRuntime()) return;
  await invoke("download_webseed_torrent", { id });
}

export async function downloadPeerTorrent(id: string) {
  if (!isTauriRuntime()) return;
  await invoke("download_peer_torrent", { id });
}

export async function recheckTorrent(id: string) {
  if (!isTauriRuntime()) return;
  await invoke("recheck_torrent", { id });
}

export async function openTorrentFolder(id: string) {
  if (!isTauriRuntime()) return;
  await invoke("open_torrent_folder", { id });
}

export async function queryDhtTorrent(id: string) {
  if (!isTauriRuntime()) return;
  await invoke("query_dht_torrent", { id });
}

export async function resolveMagnetTorrent(id: string) {
  if (!isTauriRuntime()) return;
  await invoke("resolve_magnet_torrent", { id });
}

export async function fetchMetadataTorrent(id: string) {
  if (!isTauriRuntime()) return;
  await invoke("fetch_metadata_torrent", { id });
}

export async function deleteTorrent(id: string, deleteFiles: boolean) {
  if (!isTauriRuntime()) return;
  await invoke("delete_torrent", { id, deleteFiles });
}

export async function updateTorrentFiles(id: string, onlyFiles: number[]) {
  if (!isTauriRuntime()) return;
  await invoke("update_torrent_files", { id, onlyFiles });
}

export async function updateTorrentFilePriority(id: string, fileIndex: number, priority: number) {
  if (!isTauriRuntime()) return;
  await invoke("update_torrent_file_priority", { id, fileIndex, priority });
}

export async function updateTorrentOptions(id: string, request: UpdateTorrentOptionsRequest) {
  if (!isTauriRuntime()) return;
  await invoke("update_torrent_options", { id, request });
}

export async function streamFileAvailability(id: string, fileIndex: number): Promise<TorrentFileAvailability> {
  if (!isTauriRuntime()) {
    const file = mockTorrentById(id)?.files?.[fileIndex];
    const length = file?.length ?? 0;
    const verified = Math.floor(length * 0.35);
    return {
      file_index: fileIndex,
      name: file?.name ?? "preview-video.mp4",
      length,
      verified_bytes: verified,
      complete: verified === length,
      partial_store_present: verified > 0,
      ranges: verified > 0 ? [{ offset: 0, length: verified }] : []
    };
  }
  return invoke<TorrentFileAvailability>("stream_file_availability", { id, fileIndex });
}

export async function streamFileUrl(id: string, fileIndex: number): Promise<string> {
  if (!isTauriRuntime()) return "";
  return invoke<string>("stream_file_url", { id, fileIndex });
}

export async function subtitleFileText(id: string, fileIndex: number): Promise<SubtitleFileText> {
  if (!isTauriRuntime()) {
    const file = mockTorrentById(id)?.files?.[fileIndex];
    return {
      file_index: fileIndex,
      name: file?.name ?? "captions.en.srt",
      text: "1\n00:00:01,000 --> 00:00:04,000\nNovaTorrent captions are ready.\n"
    };
  }
  return invoke<SubtitleFileText>("subtitle_file_text", { id, fileIndex });
}

export async function openMediaWindow(id: string, fileIndex: number) {
  if (!isTauriRuntime()) {
    window.open(`/media/?id=${encodeURIComponent(id)}&fileIndex=${fileIndex}`, "_blank", "noopener,noreferrer");
    return;
  }
  await invoke("open_media_window", { id, fileIndex });
}

export async function closeMediaWindow(id: string, fileIndex: number) {
  if (!isTauriRuntime()) {
    window.close();
    return;
  }
  await invoke("close_media_window", { id, fileIndex });
}

export async function mediaPlayerLog(request: MediaPlayerLogRequest) {
  if (!isTauriRuntime()) return;
  await invoke("media_player_log", { request });
}

export async function setStreamPriority(id: string, request: StreamPriorityRequest): Promise<StreamPriorityStatus> {
  if (!isTauriRuntime()) {
    const file = mockTorrentById(id)?.files?.[request.fileIndex];
    return {
      file_index: request.fileIndex,
      name: file?.name ?? "preview-video.mp4",
      playhead_offset: request.playheadOffset,
      urgent_pieces: 3,
      lookahead_pieces: 8,
      total_priority_pieces: 11
    };
  }
  return invoke<StreamPriorityStatus>("set_stream_priority", { id, request });
}

export async function clearStreamPriority(id: string) {
  if (!isTauriRuntime()) return;
  await invoke("clear_stream_priority", { id });
}

export async function backendLogs(torrentId?: number | null): Promise<LogEntry[]> {
  if (!isTauriRuntime()) {
    return [
      {
        id: 1,
        timestamp_ms: Date.now() - 14_000,
        level: "Info",
        scope: "app",
        message: "NovaTorrent preview mode using mock backend data",
        torrent_id: null
      },
      {
        id: 2,
        timestamp_ms: Date.now() - 4_000,
        level: "Debug",
        scope: "metainfo",
        message: "manual parser ready; bencode spans preserve info-hash bytes",
        torrent_id: torrentId ?? null
      }
    ];
  }
  return invoke<LogEntry[]>("backend_logs", { torrentId: torrentId ?? null });
}

export async function backendLogsAfter(torrentId?: number | null, afterId?: number | null): Promise<LogEntry[]> {
  if (!isTauriRuntime()) {
    const entries = await backendLogs(torrentId);
    return afterId == null ? entries : entries.filter((entry) => entry.id > afterId);
  }
  return invoke<LogEntry[]>("backend_logs_after", { torrentId: torrentId ?? null, afterId: afterId ?? null });
}
