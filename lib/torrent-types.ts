export type TorrentSource =
  | {
      kind: "magnet";
      value: string;
    }
  | {
      kind: "file";
      value: string;
    };

export type TorrentFile = {
  name: string;
  components: string[];
  length: number;
  included: boolean;
  attributes?: Record<string, unknown>;
};

export type TorrentStats = {
  state?: unknown;
  file_progress?: number[];
  error?: string | null;
  progress_bytes?: number;
  uploaded_bytes?: number;
  total_bytes?: number;
  finished?: boolean;
  live?: {
    download_speed?: unknown;
    upload_speed?: unknown;
    time_remaining?: unknown;
  } | null;
};

export type TorrentGeneral = {
  save_path?: string;
  total_size?: number;
  downloaded?: number;
  uploaded?: number;
  ratio?: number;
  piece_size?: number;
  piece_count?: number;
  file_count?: number;
  private?: boolean;
  comment?: string | null;
  created_by?: string | null;
  creation_date?: number | null;
  active_time_seconds?: number;
  seeding_time_seconds?: number;
};

export type TorrentTracker = {
  url: string;
  state: string;
  seeders?: number | null;
  leechers?: number | null;
  next_announce_seconds?: number | null;
  message?: string | null;
};

export type TorrentPeer = {
  address: string;
  port: number;
  client?: string | null;
  progress: number;
  download_speed: number;
  upload_speed: number;
  connection: string;
};

export type WebSeedStatus = {
  url: string;
  state: string;
  message?: string | null;
  bytes_downloaded: number;
};

export type TorrentOptions = {
  paused?: boolean;
  overwrite?: boolean;
  disable_trackers?: boolean;
  sub_folder?: string | null;
  max_connections?: number | null;
  max_download_speed?: number | null;
  max_upload_speed?: number | null;
  sequential_download?: boolean;
  seed_ratio_limit?: number | null;
};

export type UpdateTorrentOptionsRequest = {
  maxConnections: number | null;
  maxDownloadSpeed: number | null;
  maxUploadSpeed: number | null;
  sequentialDownload: boolean;
  seedRatioLimit: number | null;
};

export type TorrentDetails = {
  id?: number | null;
  info_hash: string;
  name?: string | null;
  output_folder: string;
  files?: TorrentFile[] | null;
  stats?: TorrentStats | null;
  general?: TorrentGeneral | null;
  trackers?: TorrentTracker[];
  web_seeds?: WebSeedStatus[];
  peers?: TorrentPeer[];
  options?: TorrentOptions | null;
};

export type TorrentListResponse = {
  torrents: TorrentDetails[];
};

export type AddTorrentRequest = {
  source: TorrentSource;
  destination?: string | null;
  paused: boolean;
  overwrite: boolean;
  disableTrackers: boolean;
  onlyFiles?: number[] | null;
  subFolder?: string | null;
};

export type AddTorrentResponse = {
  id?: number | null;
  details: TorrentDetails;
  output_folder: string;
  seen_peers?: string[] | null;
};

export type SafeTestTorrent = {
  label: string;
  path: string;
  source_url: string;
  sha256: string;
  payload_size: number;
};

export type TorrentFileHash = {
  file_index: number;
  name: string;
  path: string;
  size: number;
  sha256: string;
  virustotal_url: string;
};

export type TorrentRow = {
  id: string;
  name: string;
  hash: string;
  outputFolder: string;
  progress: number;
  downloaded: number;
  uploaded: number;
  total: number;
  state: string;
  downloadSpeed: number;
  uploadSpeed: number;
  eta: number | null;
  peerCount: number | null;
  files: TorrentFile[];
  general: TorrentGeneral | null;
  trackers: TorrentTracker[];
  webSeeds: WebSeedStatus[];
  peers: TorrentPeer[];
  options: TorrentOptions | null;
  raw: TorrentDetails;
};

export type LogEntry = {
  id: number;
  timestamp_ms: number;
  level: "Debug" | "Info" | "Warn" | "Error";
  scope: string;
  message: string;
  torrent_id?: number | null;
};

export function normalizeTorrent(torrent: TorrentDetails): TorrentRow {
  const stats = torrent.stats;
  const total = stats?.total_bytes ?? torrent.files?.reduce((sum, file) => sum + file.length, 0) ?? 0;
  const downloaded = stats?.progress_bytes ?? 0;
  const progress = total > 0 ? (downloaded / total) * 100 : stats?.finished ? 100 : 0;

  return {
    id: String(torrent.id ?? torrent.info_hash),
    name: torrent.name || "Unnamed torrent",
    hash: torrent.info_hash,
    outputFolder: torrent.output_folder,
    progress,
    downloaded,
    uploaded: stats?.uploaded_bytes ?? 0,
    total,
    state: normalizeState(stats),
    downloadSpeed: normalizeSpeed(stats?.live?.download_speed),
    uploadSpeed: normalizeSpeed(stats?.live?.upload_speed),
    eta: normalizeDuration(stats?.live?.time_remaining),
    peerCount: normalizePeers(stats?.live),
    files: torrent.files ?? [],
    general: torrent.general ?? null,
    trackers: torrent.trackers ?? [],
    webSeeds: torrent.web_seeds ?? [],
    peers: torrent.peers ?? [],
    options: torrent.options ?? null,
    raw: torrent
  };
}

function normalizeState(stats?: TorrentStats | null) {
  if (!stats) return "Queued";
  if (stats.error) return "Error";
  const raw = stats.state;
  if (typeof raw === "string") {
    const normalized = titleCase(raw);
    if (normalized === "Downloading From Peers") return "Downloading";
    if (normalized === "Resuming Partial Data") return "Resuming";
    if (normalized === "Fetching Metadata") return "Metadata";
    if (normalized === "Querying DHT") return "DHT";
    return normalized;
  }
  if (raw && typeof raw === "object") {
    const firstKey = Object.keys(raw as Record<string, unknown>)[0];
    if (firstKey) return titleCase(firstKey);
  }
  if (stats.finished) return "Complete";
  return stats.live ? "Downloading" : "Paused";
}

function normalizeSpeed(value: unknown) {
  if (typeof value === "number") return value;
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    const candidates = ["bytes_per_second", "bytesPerSecond", "value", "bytes"];
    for (const key of candidates) {
      if (typeof record[key] === "number") return record[key] as number;
    }
  }
  return 0;
}

function normalizeDuration(value: unknown) {
  if (typeof value === "number") return value;
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    const candidates = ["secs", "seconds", "value"];
    for (const key of candidates) {
      if (typeof record[key] === "number") return record[key] as number;
    }
  }
  return null;
}

function normalizePeers(value: unknown) {
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    const snapshot = record.snapshot;
    if (snapshot && typeof snapshot === "object") {
      const snapshotRecord = snapshot as Record<string, unknown>;
      for (const key of ["live", "connected", "peers", "num_peers"]) {
        if (typeof snapshotRecord[key] === "number") return snapshotRecord[key] as number;
      }
    }
  }
  return null;
}

function titleCase(value: string) {
  return value
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase())
    .trim();
}
