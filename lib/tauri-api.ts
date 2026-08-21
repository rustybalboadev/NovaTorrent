"use client";

import { invoke } from "@tauri-apps/api/core";
import type {
  AddTorrentRequest,
  AddTorrentResponse,
  LogEntry,
  SafeTestTorrent,
  TorrentDetails,
  TorrentFileAvailability,
  TorrentFileHash,
  TorrentListResponse,
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

export async function listTorrents(): Promise<TorrentListResponse> {
  if (!isTauriRuntime()) return { torrents: mockTorrents };
  return invoke<TorrentListResponse>("list_torrents");
}

export async function torrentDetails(id: string): Promise<TorrentDetails> {
  if (!isTauriRuntime()) {
    return mockTorrents.find((torrent) => String(torrent.id) === id || torrent.info_hash === id) ?? mockTorrents[0];
  }
  return invoke<TorrentDetails>("torrent_details", { id });
}

export async function defaultDownloadDir(): Promise<string> {
  if (!isTauriRuntime()) return "C:\\Users\\rusty\\Downloads\\NovaTorrent";
  return invoke<string>("default_download_dir");
}

export async function backendLogFilePath(): Promise<string | null> {
  if (!isTauriRuntime()) return "C:\\Users\\rusty\\Downloads\\NovaTorrent\\novatorrent.log";
  return invoke<string>("backend_log_file_path");
}

export async function safeTestTorrents(): Promise<SafeTestTorrent[]> {
  if (!isTauriRuntime()) return [];
  return invoke<SafeTestTorrent[]>("safe_test_torrents");
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
    return {
      id: null,
      details: {
        ...mockPreview,
        output_folder: request.destination || mockPreview.output_folder
      },
      output_folder: request.destination || mockPreview.output_folder,
      seen_peers: []
    };
  }
  return invoke<AddTorrentResponse>("preview_torrent", { request });
}

export async function addTorrent(request: AddTorrentRequest): Promise<AddTorrentResponse> {
  if (!isTauriRuntime()) {
    return {
      id: Date.now(),
      details: {
        ...mockPreview,
        id: Date.now(),
        output_folder: request.destination || mockPreview.output_folder
      },
      output_folder: request.destination || mockPreview.output_folder,
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

export async function updateTorrentOptions(id: string, request: UpdateTorrentOptionsRequest) {
  if (!isTauriRuntime()) return;
  await invoke("update_torrent_options", { id, request });
}

export async function hashTorrentFile(id: string, fileIndex: number): Promise<TorrentFileHash> {
  if (!isTauriRuntime()) {
    const sha256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    return {
      file_index: fileIndex,
      name: mockTorrents[0]?.files?.[fileIndex]?.name ?? "preview-file.bin",
      path: "C:\\Users\\rusty\\Downloads\\NovaTorrent\\preview-file.bin",
      size: mockTorrents[0]?.files?.[fileIndex]?.length ?? 0,
      sha256,
      virustotal_url: `https://www.virustotal.com/gui/file/${sha256}`
    };
  }
  return invoke<TorrentFileHash>("hash_torrent_file", { id, fileIndex });
}

export async function streamFileAvailability(id: string, fileIndex: number): Promise<TorrentFileAvailability> {
  if (!isTauriRuntime()) {
    const file = mockTorrents[0]?.files?.[fileIndex];
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

export async function openVirusTotalReport(sha256: string) {
  if (!isTauriRuntime()) {
    window.open(`https://www.virustotal.com/gui/file/${sha256}`, "_blank", "noopener,noreferrer");
    return;
  }
  await invoke("open_virustotal_report", { sha256 });
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
