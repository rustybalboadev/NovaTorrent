use std::{
    collections::{HashMap, HashSet},
    fs::{self, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize, Serializer};

use crate::torrent::{
    dht::{
        self, DhtAnnounceTarget, DhtContact, DhtLookupOptions, DhtNode, DhtQueryKind,
        DhtRoutingTable,
    },
    lsd::{self, LsdAnnounce},
    magnet::MagnetLink,
    metainfo::{Metainfo, TorrentFile},
    peer::PeerInfo,
    peerwire::{self, MetadataFetchPlan, PeerDownloadPlan},
    sha1, sha256,
    storage,
    tracker::{self, TrackerAnnounceResponse, TrackerStatus, UdpAnnounceEvent, UdpAnnounceRequest},
    webseed::{self, WebSeedStatus},
};

const RUN_PAUSED: &str = "torrent was paused";
const MAX_PARALLEL_PEERS: usize = 16;
const DEFAULT_CONNECTION_LIMIT: usize = 50;
const MAX_CONNECTION_LIMIT: usize = 500;
const MAX_UPLOAD_SLOTS: usize = 4;
const MAX_INBOUND_PEER_CONNECTIONS: usize = 64;
const MAX_ENDGAME_PIECES: usize = 8;
const MAX_PIECES_PER_PEER_ROUND: usize = 16;
const MIN_RATE_LIMIT: u64 = 1024;
const MAX_RATE_LIMIT: u64 = 10 * 1024 * 1024 * 1024;
const TRACKER_ANNOUNCE_RESPONSE_TIMEOUT: Duration = Duration::from_secs(16);
const TRACKER_EARLY_PEER_GRACE: Duration = Duration::from_millis(450);
const MAX_SWARM_REFRESH_ROUNDS: usize = 8;
const MAX_RUNTIME_RETRY_DELAY: Duration = Duration::from_secs(60);
const DEFAULT_STREAM_URGENT_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_STREAM_LOOKAHEAD_BYTES: u64 = 192 * 1024 * 1024;
const MAX_STREAM_LOOKAHEAD_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_MEDIA_STREAM_READ_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddTorrentRequest {
    pub source: TorrentSource,
    pub destination: Option<String>,
    pub paused: bool,
    pub overwrite: bool,
    pub disable_trackers: bool,
    pub only_files: Option<Vec<usize>>,
    pub sub_folder: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum TorrentSource {
    File(String),
    Magnet(String),
}

#[derive(Debug, Clone, Serialize)]
pub struct TorrentListResponse {
    pub torrents: Vec<TorrentDetails>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AddTorrentResponse {
    pub id: Option<u64>,
    pub details: TorrentDetails,
    pub output_folder: String,
    pub seen_peers: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EmptyJsonResponse {}

#[derive(Debug, Clone, Serialize)]
pub struct TorrentFileHash {
    pub file_index: usize,
    pub name: String,
    pub path: String,
    pub size: u64,
    pub sha256: String,
    pub virustotal_url: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TorrentFileAvailability {
    pub file_index: usize,
    pub name: String,
    pub length: u64,
    pub verified_bytes: u64,
    pub complete: bool,
    pub partial_store_present: bool,
    pub ranges: Vec<storage::VerifiedByteRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamFileRead {
    pub bytes: Vec<u8>,
    pub total_length: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DhtMaintenanceSummary {
    pub pinged: usize,
    pub verified: usize,
    pub evicted: usize,
    pub refreshes: usize,
    pub candidates: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct TorrentDetails {
    pub id: Option<u64>,
    pub info_hash: String,
    pub name: Option<String>,
    pub output_folder: String,
    pub files: Option<Vec<TorrentFile>>,
    pub stats: Option<TorrentStats>,
    pub general: TorrentGeneral,
    pub trackers: Vec<TrackerStatus>,
    pub web_seeds: Vec<WebSeedStatus>,
    pub peers: Vec<PeerInfo>,
    pub options: TorrentOptions,
}

#[derive(Debug, Clone, Serialize)]
pub struct TorrentStats {
    pub state: TorrentState,
    pub file_progress: Vec<u64>,
    pub error: Option<String>,
    pub progress_bytes: u64,
    pub uploaded_bytes: u64,
    pub total_bytes: u64,
    pub finished: bool,
    pub live: Option<LiveStats>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentState {
    Preview,
    Queued,
    Paused,
    Discovering,
    Downloading,
    Resuming,
    Partial,
    Seeding,
    SeedRatioReached,
    Error,
    MissingFiles,
    Metadata,
    FetchingMetadata,
    MetadataError,
    Dht,
    DhtError,
}

impl TorrentState {
    fn as_str(self) -> &'static str {
        match self {
            TorrentState::Preview => "Preview",
            TorrentState::Queued => "Queued",
            TorrentState::Paused => "Paused",
            TorrentState::Discovering => "Discovering",
            TorrentState::Downloading => "Downloading from peers",
            TorrentState::Resuming => "Resuming partial data",
            TorrentState::Partial => "Partial",
            TorrentState::Seeding => "Seeding",
            TorrentState::SeedRatioReached => "Seed ratio reached",
            TorrentState::Error => "Error",
            TorrentState::MissingFiles => "Missing files",
            TorrentState::Metadata => "Metadata",
            TorrentState::FetchingMetadata => "Fetching metadata",
            TorrentState::MetadataError => "Metadata error",
            TorrentState::Dht => "Querying DHT",
            TorrentState::DhtError => "DHT error",
        }
    }
}

impl Serialize for TorrentState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl PartialEq<&str> for TorrentState {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl std::fmt::Display for TorrentState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LiveStats {
    pub download_speed: u64,
    pub upload_speed: u64,
    pub time_remaining: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TorrentGeneral {
    pub save_path: String,
    pub total_size: u64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub ratio: f64,
    pub piece_size: u64,
    pub piece_count: usize,
    pub file_count: usize,
    pub private: bool,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    pub creation_date: Option<i64>,
    pub active_time_seconds: u64,
    pub seeding_time_seconds: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TorrentOptions {
    pub paused: bool,
    pub overwrite: bool,
    pub disable_trackers: bool,
    pub sub_folder: Option<String>,
    pub max_connections: Option<u32>,
    pub max_download_speed: Option<u64>,
    pub max_upload_speed: Option<u64>,
    pub sequential_download: bool,
    pub seed_ratio_limit: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateTorrentOptionsRequest {
    pub max_connections: Option<u32>,
    pub max_download_speed: Option<u64>,
    pub max_upload_speed: Option<u64>,
    pub sequential_download: bool,
    pub seed_ratio_limit: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamPriorityRequest {
    pub file_index: usize,
    pub playhead_offset: u64,
    pub urgent_bytes: Option<u64>,
    pub lookahead_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamPriorityStatus {
    pub file_index: usize,
    pub name: String,
    pub playhead_offset: u64,
    pub urgent_pieces: usize,
    pub lookahead_pieces: usize,
    pub total_priority_pieces: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    pub id: u64,
    pub timestamp_ms: u128,
    pub level: LogLevel,
    pub scope: String,
    pub message: String,
    pub torrent_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone)]
pub struct DeletedTorrentCleanup {
    pub id: u64,
    pub info_hash: [u8; 20],
    pub name: String,
    pub output_folder: PathBuf,
    pub files: Vec<TorrentFile>,
    pub delete_files: bool,
}

#[derive(Debug, Clone, Default)]
struct PeerHealth {
    connection_attempts: u32,
    consecutive_failures: u32,
    last_failure_ms: Option<u128>,
    last_success_ms: Option<u128>,
    bytes_downloaded: u64,
    pieces_downloaded: u64,
    recent_bytes_per_second: u64,
    unavailable_pieces: u64,
}

#[derive(Debug, Clone)]
struct TorrentTask {
    id: u64,
    source: TorrentSource,
    info_hash: [u8; 20],
    name: String,
    output_folder: PathBuf,
    files: Vec<TorrentFile>,
    trackers: Vec<TrackerStatus>,
    web_seeds: Vec<WebSeedStatus>,
    peers: Vec<PeerInfo>,
    piece_hashes: Vec<[u8; 20]>,
    stats: TorrentStats,
    general: TorrentGeneral,
    options: TorrentOptions,
    cancelled: Arc<AtomicBool>,
    dht_announce_targets: Vec<DhtAnnounceTarget>,
    tracker_started: bool,
    tracker_completed: bool,
    next_announce_at_ms: Option<u128>,
    added_at_ms: u128,
    upload_gate: peerwire::UploadGate,
    download_limiter: peerwire::BandwidthLimiter,
    upload_limiter: peerwire::BandwidthLimiter,
    peer_health: HashMap<String, PeerHealth>,
    stream_priority: Option<StreamPriorityState>,
}

#[derive(Debug, Clone)]
struct AnnounceSnapshot {
    id: u64,
    info_hash: [u8; 20],
    uploaded: u64,
    downloaded: u64,
    left: u64,
    port: u16,
    event: UdpAnnounceEvent,
    trackers: Vec<String>,
}

#[derive(Debug, Clone)]
struct WebSeedSnapshot {
    id: u64,
    name: String,
    output_folder: PathBuf,
    files: Vec<TorrentFile>,
    piece_length: u64,
    piece_hashes: Vec<[u8; 20]>,
    web_seeds: Vec<String>,
    overwrite: bool,
    cancelled: Arc<AtomicBool>,
    download_limiter: peerwire::BandwidthLimiter,
}

#[derive(Debug, Clone)]
struct PeerDownloadSnapshot {
    id: u64,
    name: String,
    info_hash: [u8; 20],
    output_folder: PathBuf,
    files: Vec<TorrentFile>,
    total_length: u64,
    piece_length: u64,
    piece_hashes: Vec<[u8; 20]>,
    peers: Vec<PeerInfo>,
    max_connections: usize,
    sequential_download: bool,
    download_limiter: peerwire::BandwidthLimiter,
    private: bool,
    overwrite: bool,
    cancelled: Arc<AtomicBool>,
    candidate_peers: usize,
    deferred_for_backoff: usize,
    deferred_for_duplicate_ip: usize,
    stream_priority: Option<StreamPriorityState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamPriorityState {
    file_index: usize,
    playhead_offset: u64,
    urgent_bytes: u64,
    lookahead_bytes: u64,
    updated_at_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamPiecePriorityPlan {
    urgent: Vec<u32>,
    lookahead: Vec<u32>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PeerSchedulingHint {
    success_score: u128,
    piece_score: u64,
    byte_score: u64,
    rate_score: u64,
    unavailable_score: u64,
    failure_count: u32,
    attempt_count: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SwarmCoverageSummary {
    missing_pieces: usize,
    coverable_pieces: usize,
    unavailable_pieces: usize,
    single_peer_pieces: usize,
    connected_peers: usize,
}

struct ConnectedPeer {
    peer: PeerInfo,
    connection: peerwire::PeerDownloadConnection,
}

type PeerConnectionResult = (
    PeerInfo,
    Result<peerwire::PeerDownloadConnection, String>,
    Duration,
);

struct EndgameRaceResult {
    winner: Option<(PeerInfo, peerwire::DownloadedPiece)>,
    cancelled: Vec<PeerInfo>,
    duplicates: Vec<PeerInfo>,
    errors: Vec<(PeerInfo, String)>,
    worker_panics: Vec<String>,
    reusable: Vec<ConnectedPeer>,
}

#[derive(Debug, Clone)]
struct StoredDhtPeer {
    peer: PeerInfo,
    last_seen_ms: u128,
}

#[derive(Debug, Clone)]
struct RecheckSnapshot {
    id: u64,
    name: String,
    output_folder: PathBuf,
    files: Vec<TorrentFile>,
    piece_length: u64,
    piece_hashes: Vec<[u8; 20]>,
}

#[derive(Debug, Clone)]
struct DhtLookupSnapshot {
    id: u64,
    info_hash: [u8; 20],
    private: bool,
    finished: bool,
    paused: bool,
}

#[derive(Debug, Clone)]
struct MetadataFetchSnapshot {
    id: u64,
    info_hash: [u8; 20],
    peers: Vec<PeerInfo>,
    metadata_available: bool,
    private: bool,
}

#[derive(Debug, Clone)]
struct IncomingSeedSnapshot {
    id: u64,
    name: String,
    info_hash: [u8; 20],
    output_folder: PathBuf,
    files: Vec<TorrentFile>,
    total_length: u64,
    piece_length: u64,
    piece_hashes: Vec<[u8; 20]>,
    peers: Vec<PeerInfo>,
    private: bool,
    paused: bool,
    finished: bool,
    upload_gate: peerwire::UploadGate,
    upload_limiter: peerwire::BandwidthLimiter,
    ratio: f64,
    seed_ratio_limit: Option<f64>,
}

#[derive(Debug, Clone)]
struct RuntimeSnapshot {
    id: u64,
    name: String,
    output_folder: PathBuf,
    files: Vec<TorrentFile>,
    paused: bool,
    finished: bool,
    seed_ratio_reached: bool,
    metadata_available: bool,
    private: bool,
    trackers_disabled: bool,
    tracker_count: usize,
    peer_count: usize,
    webseed_count: usize,
    overwrite: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSession {
    version: u32,
    torrents: Vec<PersistedTorrent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedDhtState {
    version: u32,
    node_id: [u8; 20],
    nodes: Vec<DhtNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedTorrent {
    id: u64,
    source: TorrentSource,
    destination: String,
    paused: bool,
    overwrite: bool,
    disable_trackers: bool,
    only_files: Option<Vec<usize>>,
    sub_folder: Option<String>,
    max_connections: Option<u32>,
    max_download_speed: Option<u64>,
    max_upload_speed: Option<u64>,
    #[serde(default)]
    sequential_download: bool,
    seed_ratio_limit: Option<f64>,
}

pub struct TorrentSession {
    default_output_dir: PathBuf,
    log_file_path: PathBuf,
    session_file_path: PathBuf,
    dht_state_file_path: PathBuf,
    dht_node_id: [u8; 20],
    dht_token_secret: [u8; 20],
    dht_port: AtomicU16,
    dht_routing: Mutex<DhtRoutingTable>,
    dht_peers: Mutex<HashMap<[u8; 20], Vec<StoredDhtPeer>>>,
    dht_state_write: Mutex<()>,
    listen_port: AtomicU16,
    torrents: Mutex<Vec<TorrentTask>>,
    logs: Mutex<Vec<LogEntry>>,
    active_runs: Mutex<HashSet<u64>>,
    active_announces: Mutex<HashSet<u64>>,
    active_incoming_peers: Arc<AtomicUsize>,
    next_id: AtomicU64,
    next_log_id: AtomicU64,
}

struct ActiveAnnounceGuard<'a> {
    active_announces: &'a Mutex<HashSet<u64>>,
    torrent_id: u64,
}

pub(crate) struct IncomingPeerPermit {
    active: Arc<AtomicUsize>,
}

impl Drop for ActiveAnnounceGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active_announces.lock() {
            active.remove(&self.torrent_id);
        }
    }
}

impl Drop for IncomingPeerPermit {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Release);
    }
}

impl TorrentSession {
    pub fn new(default_output_dir: PathBuf) -> Self {
        let log_file_path = default_output_dir.join("novatorrent.log");
        let session_file_path = default_output_dir.join("novatorrent-session.json");
        let dht_state_file_path = default_output_dir.join("novatorrent-dht.json");
        let (dht_node_id, persisted_nodes, dht_state_error, write_new_dht_state) =
            match load_dht_state(&dht_state_file_path) {
                Ok(Some(state)) if state.version == 1 && state.node_id != [0u8; 20] => {
                    (state.node_id, state.nodes, None, false)
                }
                Ok(Some(state)) => (
                    session_node_id(&default_output_dir),
                    Vec::new(),
                    Some(format!(
                        "persisted DHT state version {} or node ID is invalid; generated a new identity",
                        state.version
                    )),
                    true,
                ),
                Ok(None) => (session_node_id(&default_output_dir), Vec::new(), None, true),
                Err(err) => (
                    session_node_id(&default_output_dir),
                    Vec::new(),
                    Some(format!("could not restore DHT state: {err}")),
                    true,
                ),
            };
        let dht_token_secret = session_token_secret(&default_output_dir);
        let mut dht_routing = DhtRoutingTable::new(dht_node_id);
        let mut restored_nodes = 0usize;
        for node in persisted_nodes.into_iter().take(256) {
            if node.port != 0
                && node.address.parse::<std::net::Ipv4Addr>().is_ok()
                && dht_routing.insert_persisted_candidate(node)
            {
                restored_nodes += 1;
            }
        }
        let session = Self {
            default_output_dir,
            log_file_path,
            session_file_path,
            dht_state_file_path,
            dht_node_id,
            dht_token_secret,
            dht_port: AtomicU16::new(0),
            dht_routing: Mutex::new(dht_routing),
            dht_peers: Mutex::new(HashMap::new()),
            dht_state_write: Mutex::new(()),
            listen_port: AtomicU16::new(0),
            torrents: Mutex::new(Vec::new()),
            logs: Mutex::new(Vec::new()),
            active_runs: Mutex::new(HashSet::new()),
            active_announces: Mutex::new(HashSet::new()),
            active_incoming_peers: Arc::new(AtomicUsize::new(0)),
            next_id: AtomicU64::new(1),
            next_log_id: AtomicU64::new(1),
        };
        if let Some(error) = dht_state_error {
            session.log(LogLevel::Warn, "dht", error, None);
        }
        if restored_nodes > 0 {
            session.log(
                LogLevel::Info,
                "dht",
                format!(
                    "restored {restored_nodes} DHT contacts as questionable pending verification"
                ),
                None,
            );
        }
        if write_new_dht_state {
            session.persist_dht_state_or_log();
        }
        session.restore_session();
        session
    }

    pub fn default_output_dir(&self) -> &Path {
        &self.default_output_dir
    }

    pub fn log_file_path(&self) -> &Path {
        &self.log_file_path
    }

    #[cfg(test)]
    fn session_file_path(&self) -> &Path {
        &self.session_file_path
    }

    #[cfg(test)]
    fn dht_state_file_path(&self) -> &Path {
        &self.dht_state_file_path
    }

    fn persist_dht_state(&self) -> Result<(), String> {
        let _write_guard = self
            .dht_state_write
            .lock()
            .map_err(|_| "DHT state write lock poisoned".to_string())?;
        let state = PersistedDhtState {
            version: 1,
            node_id: self.dht_node_id,
            nodes: self
                .dht_routing
                .lock()
                .map_err(|_| "DHT routing lock poisoned".to_string())?
                .persistable_nodes(256),
        };
        if let Some(parent) = self.dht_state_file_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("could not create DHT state directory: {err}"))?;
        }
        let bytes = serde_json::to_vec_pretty(&state)
            .map_err(|err| format!("could not encode DHT state: {err}"))?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.dht_state_file_path)
            .map_err(|err| format!("could not open DHT state: {err}"))?;
        file.write_all(&bytes)
            .map_err(|err| format!("could not write DHT state: {err}"))?;
        file.sync_all()
            .map_err(|err| format!("could not flush DHT state: {err}"))
    }

    fn persist_dht_state_or_log(&self) {
        if let Err(err) = self.persist_dht_state() {
            self.log(LogLevel::Error, "dht", err, None);
        }
    }

    pub fn runnable_ids(&self) -> Vec<u64> {
        self.torrents
            .lock()
            .map(|torrents| {
                torrents
                    .iter()
                    .filter(|torrent| {
                        !torrent.options.paused
                            && (!torrent.stats.finished || !seed_ratio_reached(torrent))
                    })
                    .map(|torrent| torrent.id)
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn set_listen_port(&self, port: u16) {
        self.listen_port.store(port, Ordering::Relaxed);
    }

    pub fn listen_port(&self) -> u16 {
        self.listen_port.load(Ordering::Relaxed)
    }

    pub fn lsd_public_info_hashes(&self) -> Vec<[u8; 20]> {
        if self.listen_port() == 0 {
            return Vec::new();
        }
        self.torrents
            .lock()
            .map(|torrents| {
                torrents
                    .iter()
                    .filter(|torrent| {
                        !torrent.options.paused
                            && !torrent.general.private
                            && !seed_ratio_reached(torrent)
                    })
                    .map(|torrent| torrent.info_hash)
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn handle_lsd_announce(
        &self,
        announce: &LsdAnnounce,
        source: SocketAddr,
    ) -> Result<usize, String> {
        if !lsd::contactable_lsd_source(source.ip()) {
            return Ok(0);
        }
        let listen_port = self.listen_port();
        if listen_port != 0 && source.ip().is_loopback() && announce.port == listen_port {
            return Ok(0);
        }
        let peer = PeerInfo {
            address: source.ip().to_string(),
            port: announce.port,
            client: None,
            progress: 0.0,
            download_speed: 0,
            upload_speed: 0,
            connection: "LSD discovered".to_string(),
        };
        let mut added = 0usize;
        let mut matched_ids = Vec::new();
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        for info_hash in &announce.info_hashes {
            let Some(torrent) = torrents.iter_mut().find(|torrent| {
                torrent.info_hash == *info_hash
                    && !torrent.general.private
                    && !torrent.options.paused
                    && !seed_ratio_reached(torrent)
            }) else {
                continue;
            };
            let before = torrent.peers.len();
            merge_peers(&mut torrent.peers, vec![peer.clone()]);
            if torrent.peers.len() > before {
                added += 1;
                matched_ids.push(torrent.id);
            }
        }
        drop(torrents);
        for id in matched_ids {
            self.log(
                LogLevel::Debug,
                "lsd",
                format!(
                    "accepted LAN peer candidate {}:{} from BEP 14 announce",
                    peer.address, peer.port
                ),
                Some(id),
            );
        }
        Ok(added)
    }

    pub fn set_dht_port(&self, port: u16) {
        self.dht_port.store(port, Ordering::Relaxed);
    }

    pub fn dht_port(&self) -> u16 {
        self.dht_port.load(Ordering::Relaxed)
    }

    pub fn handle_dht_packet(&self, packet: &[u8], source: SocketAddr) -> Vec<u8> {
        let query = match dht::parse_dht_query(packet) {
            Ok(query) => query,
            Err(err) => {
                let transaction_id = dht::transaction_id_from_message(packet).unwrap_or_default();
                let code = if err.starts_with("unknown DHT method") { 204 } else { 203 };
                return dht::build_krpc_error(&transaction_id, code, &single_line(&err));
            }
        };
        if source.is_ipv4() {
            if let Ok(mut routing) = self.dht_routing.lock() {
                routing.observe_query(dht::DhtNode {
                    id: query.node_id,
                    address: source.ip().to_string(),
                    port: source.port(),
                });
            }
        }

        match query.kind {
            DhtQueryKind::Ping => dht::build_id_response(&query.transaction_id, self.dht_node_id),
            DhtQueryKind::FindNode { target } => {
                let nodes = self
                    .dht_routing
                    .lock()
                    .map(|routing| routing.closest_nodes(target, 8))
                    .unwrap_or_default();
                dht::build_find_node_response(&query.transaction_id, self.dht_node_id, &nodes)
                    .unwrap_or_else(|err| dht::build_krpc_error(&query.transaction_id, 202, &err))
            }
            DhtQueryKind::GetPeers { info_hash } => {
                if self.is_private_info_hash(info_hash) {
                    return dht::build_krpc_error(
                        &query.transaction_id,
                        203,
                        "private torrent is not available through DHT",
                    );
                }
                let token = self.dht_token(source.ip(), dht_token_bucket());
                let peers = self.stored_dht_peers(info_hash);
                let nodes = if peers.is_empty() {
                    self.dht_routing
                        .lock()
                        .map(|routing| routing.closest_nodes(info_hash, 8))
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };
                dht::build_get_peers_response(
                    &query.transaction_id,
                    self.dht_node_id,
                    &token,
                    &peers,
                    &nodes,
                )
                .unwrap_or_else(|err| dht::build_krpc_error(&query.transaction_id, 202, &err))
            }
            DhtQueryKind::AnnouncePeer {
                info_hash,
                port,
                token,
                implied_port,
            } => {
                if self.is_private_info_hash(info_hash) {
                    return dht::build_krpc_error(
                        &query.transaction_id,
                        203,
                        "private torrent is not available through DHT",
                    );
                }
                if !self.valid_dht_token(source.ip(), &token) {
                    return dht::build_krpc_error(&query.transaction_id, 203, "invalid token");
                }
                let peer = PeerInfo {
                    address: source.ip().to_string(),
                    port: if implied_port { source.port() } else { port },
                    client: None,
                    progress: 0.0,
                    download_speed: 0,
                    upload_speed: 0,
                    connection: "DHT announced".to_string(),
                };
                if let Err(err) = self.store_dht_peer(info_hash, peer) {
                    return dht::build_krpc_error(&query.transaction_id, 202, &err);
                }
                dht::build_id_response(&query.transaction_id, self.dht_node_id)
            }
        }
    }

    fn learn_peer_dht_node(
        &self,
        address: &str,
        port: u16,
        torrent_id: u64,
    ) -> Result<(), String> {
        if port == 0 {
            return Err("peer advertised DHT port zero".to_string());
        }
        let ip: std::net::IpAddr = address
            .parse()
            .map_err(|err| format!("peer DHT address is not an IP address: {err}"))?;
        if !ip.is_ipv4() {
            return Err("IPv6 DHT contacts are not supported yet".to_string());
        }
        let endpoint = SocketAddr::new(ip, port).to_string();
        let transaction_id = ((timestamp_ms() as u64 ^ torrent_id) as u16).to_be_bytes();
        let response = dht::ping_node_with_timeout(
            &endpoint,
            &transaction_id,
            self.dht_node_id,
            Duration::from_secs(2),
        )?;
        if response.transaction_id != transaction_id {
            return Err("peer DHT ping transaction ID mismatch".to_string());
        }
        let node_id = response
            .node_id
            .ok_or_else(|| "peer DHT ping response is missing its node ID".to_string())?;
        let inserted = self
            .dht_routing
            .lock()
            .map_err(|_| "DHT routing lock poisoned".to_string())?
            .insert(DhtNode {
                id: node_id,
                address: ip.to_string(),
                port,
            });
        self.log(
            LogLevel::Debug,
            "dht",
            format!(
                "verified peer-advertised DHT node {endpoint}{}",
                if inserted { " and added it to routing" } else { "" }
            ),
            Some(torrent_id),
        );
        self.persist_dht_state_or_log();
        Ok(())
    }

    fn verify_peer_dht_nodes(&self, contacts: Vec<(String, u16)>, torrent_id: u64) {
        let contacts = contacts.into_iter().collect::<HashSet<_>>();
        std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for (address, port) in contacts {
                workers.push(scope.spawn(move || {
                    let result = self.learn_peer_dht_node(&address, port, torrent_id);
                    (address, port, result)
                }));
            }
            for worker in workers {
                match worker.join() {
                    Ok((address, port, Err(err))) => self.log(
                        LogLevel::Debug,
                        "dht",
                        format!("could not verify DHT port {port} from peer {address}: {err}"),
                        Some(torrent_id),
                    ),
                    Err(_) => self.log(
                        LogLevel::Debug,
                        "dht",
                        "peer DHT verification worker panicked",
                        Some(torrent_id),
                    ),
                    Ok((_, _, Ok(()))) => {}
                }
            }
        });
    }

    pub fn bootstrap_dht_routing(&self) -> Result<DhtMaintenanceSummary, String> {
        let mut summary = self.maintain_dht_routing()?;
        let bootstrap_nodes = self
            .dht_routing
            .lock()
            .map_err(|_| "DHT routing lock poisoned".to_string())?
            .closest_nodes(self.dht_node_id, 3);
        if bootstrap_nodes.is_empty() {
            self.log(
                LogLevel::Debug,
                "dht",
                "startup self-lookup skipped because no DHT contacts passed revalidation",
                None,
            );
            return Ok(summary);
        }

        let bootstrap_query_count = bootstrap_nodes.len();
        summary.refreshes += bootstrap_query_count;
        let lookup_results = std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for (index, node) in bootstrap_nodes.into_iter().enumerate() {
                workers.push(scope.spawn(move || {
                    let contact = DhtContact {
                        address: node.address,
                        port: node.port,
                    };
                    let transaction_id = dht_maintenance_transaction(0x53, index);
                    let result = dht_contact_endpoint(&contact).and_then(|endpoint| {
                        dht::find_node(
                            &endpoint,
                            &transaction_id,
                            self.dht_node_id,
                            self.dht_node_id,
                            Duration::from_secs(2),
                        )
                    });
                    (contact, result)
                }));
            }
            workers
                .into_iter()
                .filter_map(|worker| worker.join().ok())
                .collect::<Vec<_>>()
        });

        {
            let mut routing = self
                .dht_routing
                .lock()
                .map_err(|_| "DHT routing lock poisoned".to_string())?;
            for (contact, result) in lookup_results {
                match result {
                    Ok(response) => {
                        if let Some(node_id) = response.node_id {
                            routing.insert(DhtNode {
                                id: node_id,
                                address: contact.address,
                                port: contact.port,
                            });
                            summary.verified += 1;
                        }
                        for node in response.nodes {
                            summary.candidates += usize::from(routing.insert_candidate(node));
                        }
                    }
                    Err(err) => {
                        summary.evicted += usize::from(routing.record_failure(&contact));
                        self.log(
                            LogLevel::Debug,
                            "dht",
                            format!(
                                "startup self-lookup through {}:{} failed: {err}",
                                contact.address, contact.port
                            ),
                            None,
                        );
                    }
                }
            }
        }
        self.log(
            LogLevel::Info,
            "dht",
            format!(
                "startup self-lookup queried {} validated nodes and learned {} candidates",
                bootstrap_query_count, summary.candidates
            ),
            None,
        );
        if summary.verified > 0 || summary.evicted > 0 {
            self.persist_dht_state_or_log();
        }
        Ok(summary)
    }

    pub fn maintain_dht_routing(&self) -> Result<DhtMaintenanceSummary, String> {
        let questionable = self
            .dht_routing
            .lock()
            .map_err(|_| "DHT routing lock poisoned".to_string())?
            .questionable_contacts(8);
        let ping_results = std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for (index, contact) in questionable.iter().cloned().enumerate() {
                workers.push(scope.spawn(move || {
                    let transaction_id = dht_maintenance_transaction(0x50, index);
                    let result = dht_contact_endpoint(&contact).and_then(|endpoint| {
                        dht::ping_node_with_timeout(
                            &endpoint,
                            &transaction_id,
                            self.dht_node_id,
                            Duration::from_secs(2),
                        )
                    });
                    (contact, result)
                }));
            }
            workers
                .into_iter()
                .filter_map(|worker| worker.join().ok())
                .collect::<Vec<_>>()
        });

        let mut summary = DhtMaintenanceSummary {
            pinged: questionable.len(),
            ..DhtMaintenanceSummary::default()
        };
        {
            let mut routing = self
                .dht_routing
                .lock()
                .map_err(|_| "DHT routing lock poisoned".to_string())?;
            for (contact, result) in ping_results {
                match result.and_then(|response| {
                    response
                        .node_id
                        .ok_or_else(|| "DHT ping response is missing its node ID".to_string())
                }) {
                    Ok(node_id) => {
                        routing.insert(DhtNode {
                            id: node_id,
                            address: contact.address,
                            port: contact.port,
                        });
                        summary.verified += 1;
                    }
                    Err(err) => {
                        let evicted = routing.record_failure(&contact);
                        summary.evicted += usize::from(evicted);
                        self.log(
                            if evicted { LogLevel::Warn } else { LogLevel::Debug },
                            "dht",
                            format!(
                                "DHT node {}:{} failed a liveness check{}: {err}",
                                contact.address,
                                contact.port,
                                if evicted { " and was evicted" } else { "" }
                            ),
                            None,
                        );
                    }
                }
            }
        }

        let refresh_jobs = self
            .dht_routing
            .lock()
            .map_err(|_| "DHT routing lock poisoned".to_string())?
            .take_refresh_jobs(2);
        summary.refreshes = refresh_jobs.len();
        let refresh_results = std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for (index, job) in refresh_jobs.into_iter().enumerate() {
                workers.push(scope.spawn(move || {
                    let transaction_id = dht_maintenance_transaction(0x46, index);
                    let result = dht_contact_endpoint(&job.contact).and_then(|endpoint| {
                        dht::find_node(
                            &endpoint,
                            &transaction_id,
                            self.dht_node_id,
                            job.target,
                            Duration::from_secs(2),
                        )
                    });
                    (job.contact, result)
                }));
            }
            workers
                .into_iter()
                .filter_map(|worker| worker.join().ok())
                .collect::<Vec<_>>()
        });
        {
            let mut routing = self
                .dht_routing
                .lock()
                .map_err(|_| "DHT routing lock poisoned".to_string())?;
            for (contact, result) in refresh_results {
                match result {
                    Ok(response) => {
                        if let Some(node_id) = response.node_id {
                            routing.insert(DhtNode {
                                id: node_id,
                                address: contact.address.clone(),
                                port: contact.port,
                            });
                            summary.verified += 1;
                        }
                        for node in response.nodes {
                            summary.candidates += usize::from(routing.insert_candidate(node));
                        }
                    }
                    Err(err) => {
                        let evicted = routing.record_failure(&contact);
                        summary.evicted += usize::from(evicted);
                        self.log(
                            if evicted { LogLevel::Warn } else { LogLevel::Debug },
                            "dht",
                            format!(
                                "DHT refresh through {}:{} failed{}: {err}",
                                contact.address,
                                contact.port,
                                if evicted { " and the node was evicted" } else { "" }
                            ),
                            None,
                        );
                    }
                }
            }
        }
        if summary.pinged > 0 || summary.refreshes > 0 {
            self.log(
                LogLevel::Debug,
                "dht",
                format!(
                    "DHT maintenance checked {} nodes, verified {}, evicted {}, ran {} refreshes, and learned {} candidates",
                    summary.pinged,
                    summary.verified,
                    summary.evicted,
                    summary.refreshes,
                    summary.candidates
                ),
                None,
            );
        }
        if summary.verified > 0 || summary.evicted > 0 {
            self.persist_dht_state_or_log();
        }
        Ok(summary)
    }

    fn is_private_info_hash(&self, info_hash: [u8; 20]) -> bool {
        self.torrents
            .lock()
            .map(|torrents| {
                torrents
                    .iter()
                    .any(|torrent| torrent.info_hash == info_hash && torrent.general.private)
            })
            .unwrap_or(false)
    }

    fn dht_token(&self, address: std::net::IpAddr, bucket: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.dht_token_secret);
        match address {
            std::net::IpAddr::V4(address) => bytes.extend_from_slice(&address.octets()),
            std::net::IpAddr::V6(address) => bytes.extend_from_slice(&address.octets()),
        }
        bytes.extend_from_slice(&bucket.to_be_bytes());
        sha1::digest(&bytes)[..8].to_vec()
    }

    fn valid_dht_token(&self, address: std::net::IpAddr, token: &[u8]) -> bool {
        let current = dht_token_bucket();
        token == self.dht_token(address, current)
            || current
                .checked_sub(1)
                .is_some_and(|previous| token == self.dht_token(address, previous))
    }

    fn stored_dht_peers(&self, info_hash: [u8; 20]) -> Vec<PeerInfo> {
        let cutoff = timestamp_ms().saturating_sub(30 * 60 * 1_000);
        let Ok(mut store) = self.dht_peers.lock() else {
            return Vec::new();
        };
        let Some(peers) = store.get_mut(&info_hash) else {
            return Vec::new();
        };
        peers.retain(|peer| peer.last_seen_ms >= cutoff);
        peers
            .iter()
            .filter(|peer| peer.peer.address.parse::<std::net::Ipv4Addr>().is_ok())
            .take(50)
            .map(|peer| peer.peer.clone())
            .collect()
    }

    fn store_dht_peer(&self, info_hash: [u8; 20], peer: PeerInfo) -> Result<(), String> {
        let mut store = self.dht_peers.lock().map_err(|_| "DHT peer store lock poisoned")?;
        if !store.contains_key(&info_hash) && store.len() >= 1_024 {
            return Err("DHT peer store is full".to_string());
        }
        let peers = store.entry(info_hash).or_default();
        if let Some(existing) = peers
            .iter_mut()
            .find(|existing| existing.peer.address == peer.address && existing.peer.port == peer.port)
        {
            existing.last_seen_ms = timestamp_ms();
        } else {
            if peers.len() >= 200 {
                peers.sort_by_key(|peer| peer.last_seen_ms);
                peers.remove(0);
            }
            peers.push(StoredDhtPeer {
                peer: peer.clone(),
                last_seen_ms: timestamp_ms(),
            });
        }
        drop(store);

        if let Ok(mut torrents) = self.torrents.lock() {
            if let Some(torrent) = torrents
                .iter_mut()
                .find(|torrent| torrent.info_hash == info_hash && !torrent.general.private)
            {
                merge_peers(&mut torrent.peers, vec![peer]);
            }
        }
        Ok(())
    }

    pub fn due_tracker_ids(&self) -> Vec<u64> {
        let now = timestamp_ms();
        self.torrents
            .lock()
            .map(|torrents| {
                torrents
                    .iter()
                    .filter(|torrent| {
                        !torrent.options.paused
                            && !torrent.options.disable_trackers
                            && !torrent.trackers.is_empty()
                            && !(torrent.stats.finished && seed_ratio_reached(torrent))
                            && torrent
                                .next_announce_at_ms
                                .map_or(true, |next_announce| next_announce <= now)
                    })
                    .map(|torrent| torrent.id)
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn list(&self) -> TorrentListResponse {
        let torrents = self
            .torrents
            .lock()
            .map(|torrents| torrents.iter().map(TorrentTask::details).collect())
            .unwrap_or_default();
        TorrentListResponse { torrents }
    }

    pub fn details(&self, id: &str) -> Result<TorrentDetails, String> {
        let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        torrents
            .iter()
            .find(|torrent| torrent.matches_id(id))
            .map(TorrentTask::details)
            .ok_or_else(|| format!("torrent not found: {id}"))
    }

    pub fn preview(&self, request: AddTorrentRequest) -> Result<AddTorrentResponse, String> {
        let mut task = self.build_task(request, None)?;
        task.stats.state = TorrentState::Preview;
        self.log(LogLevel::Info, "metainfo", "previewed torrent metadata", None);
        Ok(AddTorrentResponse {
            id: None,
            output_folder: task.output_folder.to_string_lossy().into_owned(),
            seen_peers: Some(Vec::new()),
            details: task.details(),
        })
    }

    pub fn add(&self, request: AddTorrentRequest) -> Result<AddTorrentResponse, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let task = self.build_task(request, Some(id))?;
        let output_folder = task.output_folder.to_string_lossy().into_owned();
        let details = task.details();
        let name = task.name.clone();
        let tracker_count = task.trackers.len();
        self.torrents
            .lock()
            .map_err(|_| "torrent lock poisoned")?
            .push(task);
        self.log(
            LogLevel::Info,
            "session",
            format!("added torrent '{name}' with {tracker_count} trackers"),
            Some(id),
        );
        self.persist_session_or_log(Some(id));
        Ok(AddTorrentResponse {
            id: Some(id),
            details,
            output_folder,
            seen_peers: Some(Vec::new()),
        })
    }

    pub fn pause(&self, id: &str) -> Result<EmptyJsonResponse, String> {
        self.update_task(id, |torrent| {
            torrent.cancelled.store(true, Ordering::Relaxed);
            torrent.options.paused = true;
            torrent.stats.state = TorrentState::Paused;
        })?;
        self.log(LogLevel::Info, "runtime", "pause requested", id.parse().ok());
        self.persist_session_or_log(id.parse().ok());
        Ok(EmptyJsonResponse {})
    }

    pub fn resume(&self, id: &str) -> Result<EmptyJsonResponse, String> {
        self.update_task(id, |torrent| {
            torrent.cancelled.store(false, Ordering::Relaxed);
            torrent.options.paused = false;
            torrent.stats.state = if torrent.stats.finished {
                completed_torrent_state(torrent)
            } else {
                TorrentState::Queued
            };
            torrent.stats.error = None;
        })?;
        self.log(LogLevel::Info, "runtime", "continue requested", id.parse().ok());
        self.persist_session_or_log(id.parse().ok());
        Ok(EmptyJsonResponse {})
    }

    pub(crate) fn try_acquire_incoming_peer(&self) -> Result<IncomingPeerPermit, String> {
        self.active_incoming_peers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < MAX_INBOUND_PEER_CONNECTIONS).then_some(active + 1)
            })
            .map(|_| IncomingPeerPermit {
                active: Arc::clone(&self.active_incoming_peers),
            })
            .map_err(|active| {
                format!(
                    "inbound peer connection limit reached ({active}/{MAX_INBOUND_PEER_CONNECTIONS})"
                )
            })
    }

    pub fn serve_incoming_peer(&self, stream: TcpStream) -> Result<peerwire::PeerSeedResult, String> {
        let permit = self.try_acquire_incoming_peer()?;
        self.serve_incoming_peer_with_permit(stream, permit)
    }

    pub(crate) fn serve_incoming_peer_with_permit(
        &self,
        mut stream: TcpStream,
        _permit: IncomingPeerPermit,
    ) -> Result<peerwire::PeerSeedResult, String> {
        let remote = stream.peer_addr().ok();
        let handshake = peerwire::read_incoming_handshake(&mut stream).map_err(|err| {
            self.log(LogLevel::Warn, "seed", format!("incoming handshake failed: {err}"), None);
            err
        })?;
        let snapshot = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.info_hash == handshake.info_hash)
                .ok_or_else(|| "incoming peer requested an unknown info hash".to_string())?;
            IncomingSeedSnapshot {
                id: torrent.id,
                name: torrent.name.clone(),
                info_hash: torrent.info_hash,
                output_folder: torrent.output_folder.clone(),
                files: torrent.files.clone(),
                total_length: torrent.stats.total_bytes,
                piece_length: torrent.general.piece_size,
                piece_hashes: torrent.piece_hashes.clone(),
                peers: torrent.peers.clone(),
                private: torrent.general.private,
                paused: torrent.options.paused,
                finished: torrent.stats.finished,
                upload_gate: torrent.upload_gate.clone(),
                upload_limiter: torrent.upload_limiter.clone(),
                ratio: torrent.general.ratio,
                seed_ratio_limit: torrent.options.seed_ratio_limit,
            }
        };
        if snapshot.paused {
            return Err("incoming peer requested a paused torrent".to_string());
        }
        if !snapshot.finished || snapshot.piece_hashes.is_empty() {
            return Err("incoming peer requested torrent data that is not complete".to_string());
        }
        if snapshot
            .seed_ratio_limit
            .is_some_and(|limit| snapshot.ratio >= limit)
        {
            return Err(format!(
                "seed ratio limit reached ({:.2}/{:.2})",
                snapshot.ratio,
                snapshot.seed_ratio_limit.unwrap_or_default()
            ));
        }

        let read_block = self.seed_block_reader(&snapshot)?;
        self.log(
            LogLevel::Info,
            "seed",
            format!(
                "serving incoming peer {} for '{}'",
                remote
                    .map(|address| address.to_string())
                    .unwrap_or_else(|| "unknown address".to_string()),
                snapshot.name
            ),
            Some(snapshot.id),
        );
        let result = peerwire::seed_connected_peer_with_reader_and_gate(
            stream,
            handshake,
            peerwire::PeerSeedReadPlan {
                info_hash: snapshot.info_hash,
                peer_id: peer_id_for(snapshot.id),
                dht_port: (!snapshot.private)
                    .then(|| self.dht_port())
                    .filter(|port| *port != 0),
                enable_pex: !snapshot.private,
                pex_peers: pex_seed_candidates(&snapshot.peers, remote),
                piece_length: snapshot.piece_length,
                total_length: snapshot.total_length,
                read_block,
                available_pieces: None,
                disconnect_after_blocks: None,
                block_response_delay: None,
            },
            Some(&snapshot.upload_gate),
            Some(&snapshot.upload_limiter),
        )?;
        if !snapshot.private {
            if let (Some(remote), Some(dht_port)) = (remote, result.remote_dht_port) {
                if let Err(err) =
                    self.learn_peer_dht_node(&remote.ip().to_string(), dht_port, snapshot.id)
                {
                    self.log(
                        LogLevel::Debug,
                        "dht",
                        format!(
                            "could not verify DHT port {dht_port} from incoming peer {}: {err}",
                            remote.ip()
                        ),
                        Some(snapshot.id),
                    );
                }
            }
        }
        self.record_upload(snapshot.id, remote, result.bytes_uploaded)?;
        self.log(
            LogLevel::Info,
            "seed",
            format!(
                "uploaded {} bytes in {} blocks to incoming peer; upload slot rotated {} times",
                result.bytes_uploaded, result.blocks_served, result.upload_rotations
            ),
            Some(snapshot.id),
        );
        Ok(result)
    }

    fn seed_block_reader(
        &self,
        snapshot: &IncomingSeedSnapshot,
    ) -> Result<peerwire::SeedBlockReader, String> {
        let output_folder = snapshot.output_folder.clone();
        let torrent_name = snapshot.name.clone();
        let files = snapshot.files.clone();
        let piece_length = snapshot.piece_length;
        let total_length = snapshot.total_length;
        let piece_hashes = snapshot.piece_hashes.clone();
        if files.iter().all(|file| file.included) {
            return Ok(Arc::new(move |offset, length| {
                read_verified_seed_block_from_files(
                    &output_folder,
                    &torrent_name,
                    &files,
                    piece_length,
                    total_length,
                    &piece_hashes,
                    offset,
                    length,
                )
            }));
        }

        {
            let key = sha1::hex(&snapshot.info_hash);
            let state_path = snapshot
                .output_folder
                .join(".novatorrent")
                .join(format!("{key}.json"));
            if !state_path.is_file() {
                return Err("complete partial store is unavailable for unchecked files".to_string());
            }
        }
        let key = sha1::hex(&snapshot.info_hash);
        Ok(Arc::new(move |offset, length| {
            read_verified_seed_block_from_partial_store(
                &output_folder,
                &key,
                piece_length,
                total_length,
                &piece_hashes,
                offset,
                length,
            )
        }))
    }

    fn record_upload(
        &self,
        id: u64,
        remote: Option<std::net::SocketAddr>,
        bytes_uploaded: u64,
    ) -> Result<(), String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.id == id)
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        let was_seed_limited = seed_ratio_reached(torrent);
        torrent.stats.uploaded_bytes = torrent.stats.uploaded_bytes.saturating_add(bytes_uploaded);
        torrent.general.uploaded = torrent.stats.uploaded_bytes;
        torrent.general.ratio = if torrent.general.downloaded == 0 {
            0.0
        } else {
            torrent.general.uploaded as f64 / torrent.general.downloaded as f64
        };
        torrent.general.seeding_time_seconds = ((timestamp_ms() - torrent.added_at_ms) / 1_000) as u64;
        if let Some(live) = torrent.stats.live.as_mut() {
            live.upload_speed = bytes_uploaded;
        }
        if let Some(remote) = remote {
            let address = remote.ip().to_string();
            let port = remote.port();
            if let Some(peer) = torrent
                .peers
                .iter_mut()
                .find(|peer| peer.address == address && peer.port == port)
            {
                peer.upload_speed = bytes_uploaded;
                peer.connection = "Uploaded requested blocks".to_string();
            } else {
                torrent.peers.push(PeerInfo {
                    address,
                    port,
                    client: None,
                    progress: 0.0,
                    download_speed: 0,
                    upload_speed: bytes_uploaded,
                    connection: "Uploaded requested blocks".to_string(),
                });
            }
        }
        if torrent.stats.finished {
            update_completed_torrent_state(torrent);
        }
        let limit_reached = !was_seed_limited && seed_ratio_reached(torrent);
        let ratio = torrent.general.ratio;
        let limit = torrent.options.seed_ratio_limit;
        drop(torrents);
        if limit_reached {
            self.log(
                LogLevel::Info,
                "seed",
                format!(
                    "seed ratio limit reached after upload ({ratio:.2}/{:.2}); future incoming uploads will be rejected",
                    limit.unwrap_or_default()
                ),
                Some(id),
            );
            if let Err(err) = self.announce_stopped(&id.to_string()) {
                self.log(
                    LogLevel::Warn,
                    "tracker",
                    format!("could not send stopped announce after seed ratio limit was reached: {err}"),
                    Some(id),
                );
            }
        }
        Ok(())
    }

    pub fn run_torrent(&self, id: &str) -> Result<EmptyJsonResponse, String> {
        let snapshot = self.runtime_snapshot(id)?;
        if snapshot.paused {
            return Err(RUN_PAUSED.to_string());
        }

        {
            let mut active = self.active_runs.lock().map_err(|_| "runtime lock poisoned")?;
            if !active.insert(snapshot.id) {
                self.log(
                    LogLevel::Debug,
                    "runtime",
                    "torrent already has an active background worker",
                    Some(snapshot.id),
                );
                return Ok(EmptyJsonResponse {});
            }
        }

        self.log(
            LogLevel::Info,
            "runtime",
            "background torrent worker started",
            Some(snapshot.id),
        );
        let result = self.run_torrent_inner(id);
        if let Ok(mut active) = self.active_runs.lock() {
            active.remove(&snapshot.id);
        }

        match result {
            Ok(()) => {
                if let Ok(completed) = self.runtime_snapshot(id) {
                    if completed.finished
                        && !completed.seed_ratio_reached
                        && !completed.trackers_disabled
                    {
                        if let Err(err) = self.announce_completion_after_verification(id) {
                            self.log(
                                LogLevel::Warn,
                                "tracker",
                                format!(
                                    "could not finish post-download tracker announce sequence: {err}"
                                ),
                                Some(completed.id),
                            );
                        }
                    }
                    if completed.finished && !completed.seed_ratio_reached && !completed.private {
                        let _ = self.announce_dht_targets(id, Duration::from_secs(4));
                    }
                }
                self.log(
                    LogLevel::Info,
                    "runtime",
                    "background torrent worker finished",
                    Some(snapshot.id),
                );
                Ok(EmptyJsonResponse {})
            }
            Err(err) if err == RUN_PAUSED => {
                let _ = self.set_torrent_state(id, TorrentState::Paused, None);
                self.log(
                    LogLevel::Info,
                    "runtime",
                    "background torrent worker stopped after pause",
                    Some(snapshot.id),
                );
                Ok(EmptyJsonResponse {})
            }
            Err(err) => {
                let still_exists = self.runtime_snapshot(id).is_ok();
                if still_exists {
                    let _ = self.set_torrent_state(id, TorrentState::Error, Some(err.clone()));
                }
                self.log(
                    LogLevel::Error,
                    "runtime",
                    format!("background torrent worker failed: {err}"),
                    Some(snapshot.id),
                );
                Err(err)
            }
        }
    }

    pub fn retry_delay_after_runtime_failure(
        &self,
        id: &str,
        error: &str,
        failure_count: u32,
    ) -> Option<Duration> {
        if error == RUN_PAUSED || !recoverable_runtime_error(error) {
            return None;
        }
        let snapshot = self.runtime_snapshot(id).ok()?;
        if snapshot.paused || snapshot.finished {
            return None;
        }
        let has_retry_source = snapshot.peer_count > 0
            || snapshot.webseed_count > 0
            || (!snapshot.trackers_disabled && snapshot.tracker_count > 0)
            || !snapshot.private;
        if !has_retry_source {
            return None;
        }
        let seconds = 5u64
            .saturating_mul(2u64.saturating_pow(failure_count.min(4)))
            .min(MAX_RUNTIME_RETRY_DELAY.as_secs());
        Some(Duration::from_secs(seconds))
    }

    pub fn mark_runtime_retry_scheduled(
        &self,
        id: &str,
        error: &str,
        delay: Duration,
    ) -> Result<(), String> {
        self.set_torrent_state(
            id,
            TorrentState::Queued,
            Some(format!(
                "recoverable issue; retrying in {} seconds: {error}",
                delay.as_secs()
            )),
        )
    }

    fn run_torrent_inner(&self, id: &str) -> Result<(), String> {
        let runtime_started = Instant::now();
        let mut snapshot = self.runtime_snapshot(id)?;
        self.ensure_running(&snapshot)?;
        if snapshot.finished {
            self.set_completed_torrent_state(id)?;
            return Ok(());
        }

        self.set_torrent_state(id, TorrentState::Discovering, None)?;
        self.log(
            LogLevel::Info,
            "runtime",
            format!(
                "download plan: metadata={}, trackers={}, known peers={}, webseeds={}, private={}, overwrite={}",
                snapshot.metadata_available,
                snapshot.tracker_count,
                snapshot.peer_count,
                snapshot.webseed_count,
                snapshot.private,
                snapshot.overwrite
            ),
            Some(snapshot.id),
        );

        if snapshot.metadata_available
            && !snapshot.files.is_empty()
            && storage::torrent_files_exist(
                &snapshot.output_folder,
                &snapshot.name,
                &snapshot.files,
            )?
        {
            self.log(
                LogLevel::Info,
                "runtime",
                "existing torrent files found; verifying before network activity",
                Some(snapshot.id),
            );
            self.recheck(id)?;
            snapshot = self.runtime_snapshot(id)?;
            if snapshot.finished {
                return Ok(());
            }
            if !snapshot.overwrite {
                return Err(
                    "existing torrent files are incomplete or corrupt and overwrite is disabled"
                        .to_string(),
                );
            }
        }

        let mut discovery = self.discover_sources_for_runtime(id, &snapshot);
        snapshot = self.runtime_snapshot(id)?;
        self.ensure_running(&snapshot)?;

        if !snapshot.metadata_available {
            if snapshot.peer_count == 0 && !snapshot.private && !discovery.dht_ran {
                let dht_started = Instant::now();
                if let Err(err) = self.query_dht(id) {
                    discovery.dht_error = Some(err);
                }
                discovery.dht_ran = true;
                snapshot = self.runtime_snapshot(id)?;
                self.log_discovery_finished(
                    id,
                    snapshot.id,
                    "dht",
                    "metadata DHT discovery",
                    dht_started.elapsed(),
                );
            }

            snapshot = self.runtime_snapshot(id)?;
            self.ensure_running(&snapshot)?;
            if snapshot.peer_count == 0 {
                let metadata_errors = discovery.errors();
                let detail = if metadata_errors.is_empty() {
                    "no peers were discovered".to_string()
                } else {
                    metadata_errors.join("; ")
                };
                return Err(format!("could not resolve magnet metadata: {detail}"));
            }
            self.fetch_metadata(id)?;
            snapshot = self.runtime_snapshot(id)?;
        }

        self.ensure_running(&snapshot)?;
        let mut download_errors = Vec::new();
        let mut webseed_attempted = false;
        if snapshot.peer_count == 0 && snapshot.webseed_count > 0 {
            webseed_attempted = true;
            self.log(
                LogLevel::Info,
                "runtime",
                "no peers discovered yet; trying HTTP webseed fallback",
                Some(snapshot.id),
            );
            match self.download_webseed(id) {
                Ok(_) => return Ok(()),
                Err(err) => download_errors.push(format!("webseed download: {err}")),
            }
            snapshot = self.runtime_snapshot(id)?;
            self.ensure_running(&snapshot)?;
        }

        if snapshot.peer_count == 0 {
            if snapshot.peer_count == 0 && !snapshot.private && !discovery.dht_ran {
                let dht_started = Instant::now();
                if let Err(err) = self.query_dht(id) {
                    discovery.dht_error = Some(err);
                }
                discovery.dht_ran = true;
                snapshot = self.runtime_snapshot(id)?;
                self.log_discovery_finished(
                    id,
                    snapshot.id,
                    "dht",
                    "DHT discovery",
                    dht_started.elapsed(),
                );
            }
            let discovery_errors = discovery.errors();
            if !discovery_errors.is_empty() {
                self.log(
                    LogLevel::Debug,
                    "runtime",
                    discovery_errors.join("; "),
                    Some(snapshot.id),
                );
            }
            snapshot = self.runtime_snapshot(id)?;
        }

        self.ensure_running(&snapshot)?;
        if snapshot.peer_count > 0 {
            self.log(
                LogLevel::Info,
                "runtime",
                format!("starting peer download after {} ms", runtime_started.elapsed().as_millis()),
                Some(snapshot.id),
            );
            match self.download_from_peers(id) {
                Ok(_) => return Ok(()),
                Err(err) => download_errors.push(format!("peer download: {err}")),
            }
        }

        snapshot = self.runtime_snapshot(id)?;
        self.ensure_running(&snapshot)?;
        if snapshot.webseed_count > 0 && !webseed_attempted {
            self.log(
                LogLevel::Info,
                "runtime",
                format!(
                    "peer path did not complete; trying webseed fallback after {} ms",
                    runtime_started.elapsed().as_millis()
                ),
                Some(snapshot.id),
            );
            match self.download_webseed(id) {
                Ok(_) => return Ok(()),
                Err(err) => download_errors.push(format!("webseed download: {err}")),
            }
        }

        if download_errors.is_empty() {
            download_errors.push("no peers or supported webseeds are available".to_string());
        }
        Err(download_errors.join("; "))
    }

    fn discover_sources_for_runtime(
        &self,
        id: &str,
        snapshot: &RuntimeSnapshot,
    ) -> DiscoveryOutcome {
        let tracker_enabled = !snapshot.trackers_disabled && snapshot.tracker_count > 0;
        let dht_enabled = !snapshot.private;
        let mut outcome = DiscoveryOutcome {
            dht_ran: dht_enabled,
            ..DiscoveryOutcome::default()
        };

        if tracker_enabled && dht_enabled {
            std::thread::scope(|scope| {
                let tracker = scope.spawn(|| {
                    let started = Instant::now();
                    (self.announce(id).err(), started.elapsed())
                });
                let dht = scope.spawn(|| {
                    let started = Instant::now();
                    (self.query_dht(id).err(), started.elapsed())
                });
                match tracker.join() {
                    Ok((error, elapsed)) => {
                        outcome.tracker_error = error;
                        self.log_discovery_finished(
                            id,
                            snapshot.id,
                            "tracker",
                            "tracker discovery",
                            elapsed,
                        );
                    }
                    Err(_) => {
                        outcome.tracker_error =
                            Some("tracker discovery worker panicked".to_string());
                    }
                }
                match dht.join() {
                    Ok((error, elapsed)) => {
                        outcome.dht_error = error;
                        self.log_discovery_finished(
                            id,
                            snapshot.id,
                            "dht",
                            "DHT discovery",
                            elapsed,
                        );
                    }
                    Err(_) => {
                        outcome.dht_error = Some("DHT discovery worker panicked".to_string());
                    }
                }
            });
        } else if tracker_enabled {
            let started = Instant::now();
            outcome.tracker_error = self.announce(id).err();
            self.log_discovery_finished(
                id,
                snapshot.id,
                "tracker",
                "tracker discovery",
                started.elapsed(),
            );
        } else if dht_enabled {
            let started = Instant::now();
            outcome.dht_error = self.query_dht(id).err();
            self.log_discovery_finished(
                id,
                snapshot.id,
                "dht",
                "DHT discovery",
                started.elapsed(),
            );
        }

        outcome
    }

    fn log_discovery_finished(
        &self,
        id: &str,
        torrent_id: u64,
        scope: &'static str,
        label: &'static str,
        elapsed: Duration,
    ) {
        let peer_count = self
            .runtime_snapshot(id)
            .map(|snapshot| snapshot.peer_count)
            .unwrap_or_default();
        self.log(
            LogLevel::Info,
            scope,
            format!(
                "{label} finished in {} ms with {peer_count} known peers",
                elapsed.as_millis()
            ),
            Some(torrent_id),
        );
    }

    pub fn announce(&self, id: &str) -> Result<EmptyJsonResponse, String> {
        let event = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            if !torrent.tracker_started {
                UdpAnnounceEvent::Started
            } else if torrent.stats.finished && !torrent.tracker_completed {
                UdpAnnounceEvent::Completed
            } else {
                UdpAnnounceEvent::None
            }
        };
        self.announce_with_event(id, event)
    }

    pub fn announce_stopped(&self, id: &str) -> Result<EmptyJsonResponse, String> {
        let tracker_started = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?
                .tracker_started
        };
        if !tracker_started {
            return Ok(EmptyJsonResponse {});
        }
        self.announce_with_event(id, UdpAnnounceEvent::Stopped)
    }

    fn announce_completion_after_verification(&self, id: &str) -> Result<(), String> {
        let needs_started = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            if !torrent.stats.finished
                || torrent.options.disable_trackers
                || torrent.trackers.is_empty()
            {
                return Ok(());
            }
            !torrent.tracker_started
        };
        if needs_started {
            self.announce(id)?;
        }

        let needs_completed = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            torrent.stats.finished && torrent.tracker_started && !torrent.tracker_completed
        };
        if needs_completed {
            self.announce(id)?;
        }
        Ok(())
    }

    fn announce_with_event(
        &self,
        id: &str,
        event: UdpAnnounceEvent,
    ) -> Result<EmptyJsonResponse, String> {
        let snapshot = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            AnnounceSnapshot {
                id: torrent.id,
                info_hash: torrent.info_hash,
                uploaded: torrent.stats.uploaded_bytes,
                downloaded: torrent.stats.progress_bytes,
                left: torrent.stats.total_bytes.saturating_sub(torrent.stats.progress_bytes),
                port: self.listen_port(),
                event,
                trackers: torrent.trackers.iter().map(|tracker| tracker.url.clone()).collect(),
            }
        };

        if snapshot.trackers.is_empty() {
            self.log(LogLevel::Warn, "tracker", "announce skipped: no trackers configured", Some(snapshot.id));
            return Ok(EmptyJsonResponse {});
        }
        if snapshot.port == 0 {
            self.log(
                LogLevel::Warn,
                "tracker",
                "announce skipped: inbound peer listener is not active",
                Some(snapshot.id),
            );
            return Err("inbound peer listener is not active".to_string());
        }
        let wait_started = std::time::Instant::now();
        loop {
            let acquired = self
                .active_announces
                .lock()
                .map_err(|_| "tracker runtime lock poisoned")?
                .insert(snapshot.id);
            if acquired {
                break;
            }
            if !matches!(snapshot.event, UdpAnnounceEvent::Stopped) {
                self.log(
                    LogLevel::Debug,
                    "tracker",
                    "tracker announce already in progress",
                    Some(snapshot.id),
                );
                return Ok(EmptyJsonResponse {});
            }
            if wait_started.elapsed() >= Duration::from_secs(30) {
                return Err("timed out waiting to send the stopped tracker announce".to_string());
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let _active_announce = ActiveAnnounceGuard {
            active_announces: &self.active_announces,
            torrent_id: snapshot.id,
        };

        self.log(
            LogLevel::Info,
            "tracker",
            format!(
                "sending {} announce to {} trackers",
                tracker_event_label(snapshot.event),
                snapshot.trackers.len()
            ),
            Some(snapshot.id),
        );
        let peer_id = peer_id_for(snapshot.id);
        let mut results = Vec::new();
        let (tracker_sender, tracker_receiver) = std::sync::mpsc::channel();
        let mut tracker_workers = Vec::new();
        for (index, url) in snapshot.trackers.iter().cloned().enumerate() {
            let sender = tracker_sender.clone();
            let http_event = tracker_http_event(snapshot.event).map(str::to_string);
            let request = UdpAnnounceRequest {
                connection_id: 0,
                transaction_id: tracker_transaction_id(snapshot.id, index),
                info_hash: snapshot.info_hash,
                peer_id,
                downloaded: snapshot.downloaded,
                left: snapshot.left,
                uploaded: snapshot.uploaded,
                event: snapshot.event,
                key: tracker_transaction_id(snapshot.id, index + 10_000),
                num_want: 50,
                port: snapshot.port,
            };
            tracker_workers.push(std::thread::spawn(move || {
                let tracker_started = Instant::now();
                let result = if url.starts_with("http://") || url.starts_with("https://") {
                    tracker::announce_http(
                        &url,
                        request.info_hash,
                        request.peer_id,
                        request.port,
                        request.uploaded,
                        request.downloaded,
                        request.left,
                        http_event.as_deref(),
                    )
                } else if url.starts_with("udp://") {
                    tracker::announce_udp(&url, request)
                } else {
                    Err("unsupported tracker scheme".to_string())
                };
                let _ = sender.send((url, result, tracker_started.elapsed()));
            }));
        }
        drop(tracker_sender);

        let can_use_early_peers = matches!(
            snapshot.event,
            UdpAnnounceEvent::Started | UdpAnnounceEvent::None
        );
        let mut first_peer_result_at = None::<Instant>;
        while results.len() < snapshot.trackers.len() {
            let receive_timeout = if let Some(first_peer_result_at) = first_peer_result_at {
                let elapsed = first_peer_result_at.elapsed();
                if elapsed >= TRACKER_EARLY_PEER_GRACE {
                    break;
                }
                TRACKER_EARLY_PEER_GRACE.saturating_sub(elapsed)
            } else {
                TRACKER_ANNOUNCE_RESPONSE_TIMEOUT
            };
            let Ok((url, result, elapsed)) = tracker_receiver.recv_timeout(receive_timeout) else {
                if first_peer_result_at.is_some() {
                    break;
                }
                self.log(
                    LogLevel::Warn,
                    "tracker",
                    "tracker worker timed out before reporting a result",
                    Some(snapshot.id),
                );
                break;
            };
            let usable_peer_response = matches!(&result, Ok(response) if !response.peers.is_empty());
            self.log(
                match &result {
                    Ok(_) => LogLevel::Debug,
                    Err(_) => LogLevel::Warn,
                },
                "tracker",
                match &result {
                    Ok(response) => format!(
                        "{} answered in {} ms with {} peers",
                        url,
                        elapsed.as_millis(),
                        response.peers.len()
                    ),
                    Err(err) => format!(
                        "{} failed in {} ms: {}",
                        url,
                        elapsed.as_millis(),
                        err
                    ),
                },
                Some(snapshot.id),
            );
            if can_use_early_peers && usable_peer_response && first_peer_result_at.is_none() {
                first_peer_result_at = Some(Instant::now());
            }
            results.push((url, result));
        }
        let pending_workers = snapshot.trackers.len().saturating_sub(results.len());
        let using_early_peers = can_use_early_peers && first_peer_result_at.is_some() && pending_workers > 0;
        if using_early_peers {
            self.log(
                LogLevel::Info,
                "tracker",
                format!(
                    "using early tracker peers; {pending_workers} tracker workers still pending"
                ),
                Some(snapshot.id),
            );
        } else {
            for worker in tracker_workers {
                if worker.join().is_err() {
                    self.log(
                        LogLevel::Warn,
                        "tracker",
                        "tracker announce worker panicked",
                        Some(snapshot.id),
                    );
                }
            }
        }

        let mut discovered = 0usize;
        let mut shortest_interval = None::<u64>;
        let mut successful = false;
        let mut failures = Vec::new();
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        for (url, result) in results {
            let Some(status_index) = torrent.trackers.iter().position(|tracker| tracker.url == url) else {
                continue;
            };
            match result {
                Ok(response) => {
                    successful = true;
                    shortest_interval = Some(
                        shortest_interval
                            .map(|interval| interval.min(response.interval_seconds))
                            .unwrap_or(response.interval_seconds),
                    );
                    discovered += response.peers.len();
                    update_tracker_status(&mut torrent.trackers[status_index], &response);
                    merge_peers(&mut torrent.peers, response.peers);
                }
                Err(err) => {
                    let status = &mut torrent.trackers[status_index];
                    status.state = "Error".to_string();
                    status.message = Some(err.clone());
                    failures.push(format!("{url}: {err}"));
                    self.log(LogLevel::Warn, "tracker", format!("{url}: {err}"), Some(snapshot.id));
                }
            }
        }
        if discovered > 0 && torrent.stats.state != TorrentState::Paused && !torrent.stats.finished {
            torrent.stats.state = TorrentState::Queued;
        }
        if successful {
            match snapshot.event {
                UdpAnnounceEvent::Started => torrent.tracker_started = true,
                UdpAnnounceEvent::Completed => torrent.tracker_completed = true,
                UdpAnnounceEvent::Stopped => {
                    torrent.tracker_started = false;
                    torrent.tracker_completed = false;
                }
                UdpAnnounceEvent::None => {}
            }
        }
        torrent.next_announce_at_ms = if matches!(snapshot.event, UdpAnnounceEvent::Stopped) {
            None
        } else {
            Some(
                timestamp_ms()
                    .saturating_add(shortest_interval.unwrap_or(60).saturating_mul(1_000) as u128),
            )
        };
        drop(torrents);
        self.log(
            LogLevel::Info,
            "tracker",
            format!("announce finished; discovered {discovered} peer entries"),
            Some(snapshot.id),
        );
        if !successful {
            return Err(format!("all tracker announces failed: {}", failures.join("; ")));
        }
        Ok(EmptyJsonResponse {})
    }

    pub fn download_webseed(&self, id: &str) -> Result<EmptyJsonResponse, String> {
        let snapshot = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            WebSeedSnapshot {
                id: torrent.id,
                name: torrent.name.clone(),
                output_folder: torrent.output_folder.clone(),
                files: torrent.files.clone(),
                piece_length: torrent.general.piece_size,
                piece_hashes: torrent.piece_hashes.clone(),
                web_seeds: torrent.web_seeds.iter().map(|seed| seed.url.clone()).collect(),
                overwrite: torrent.options.overwrite,
                cancelled: Arc::clone(&torrent.cancelled),
                download_limiter: torrent.download_limiter.clone(),
            }
        };

        if snapshot.files.is_empty() {
            return Err("torrent metadata has no files".to_string());
        }
        if snapshot.web_seeds.is_empty() {
            self.log(LogLevel::Warn, "webseed", "webseed download skipped: no web seeds configured", Some(snapshot.id));
            return Err("torrent has no web seeds".to_string());
        }

        self.log(
            LogLevel::Info,
            "webseed",
            format!("trying {} webseed URLs", snapshot.web_seeds.len()),
            Some(snapshot.id),
        );
        let multi_file = snapshot.files.len() > 1
            || snapshot
                .files
                .first()
                .is_some_and(|file| file.components.len() > 1);
        let mut last_error = None;
        for seed_url in snapshot
            .web_seeds
            .iter()
            .filter(|url| url.starts_with("http://") || url.starts_with("https://"))
        {
            self.set_webseed_state(id, seed_url, "Downloading", None, 0)?;
            match webseed::download_torrent_cancellable_with_limiter(
                seed_url,
                &snapshot.name,
                &snapshot.output_folder,
                &snapshot.files,
                snapshot.piece_length,
                &snapshot.piece_hashes,
                multi_file,
                Arc::clone(&snapshot.cancelled),
                snapshot.overwrite,
                Some(&snapshot.download_limiter),
            ) {
                Ok(result) => {
                    self.mark_webseed_complete(id, seed_url, result.bytes_written, result.pieces_verified)?;
                    self.log(
                        LogLevel::Info,
                        "webseed",
                        format!(
                            "downloaded {} bytes from webseed, verified {} pieces, and wrote {} selected files at {}",
                            result.bytes_written,
                            result.pieces_verified,
                            result.files_written,
                            result.output_path.to_string_lossy()
                        ),
                        Some(snapshot.id),
                    );
                    return Ok(EmptyJsonResponse {});
                }
                Err(err) => {
                    self.set_webseed_state(id, seed_url, "Error", Some(err.clone()), 0)?;
                    self.log(LogLevel::Warn, "webseed", format!("{seed_url}: {err}"), Some(snapshot.id));
                    last_error = Some(err);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| "no HTTP(S) webseed URLs are available".to_string()))
    }

    fn accept_peer_connection_result(
        &self,
        id: &str,
        torrent_id: u64,
        peer: PeerInfo,
        result: Result<peerwire::PeerDownloadConnection, String>,
        elapsed: Duration,
        connected: &mut Vec<ConnectedPeer>,
        pending_dht_nodes: &mut Vec<(String, u16)>,
        errors: &mut Vec<String>,
    ) -> Result<usize, String> {
        match result {
            Ok(mut connection) => {
                self.record_peer_connect_success(id, &peer.address, peer.port)?;
                if let Some(dht_port) = connection.take_remote_dht_port() {
                    pending_dht_nodes.push((peer.address.clone(), dht_port));
                }
                let pex_peers = connection.take_pex_peers();
                let pex_added =
                    self.merge_pex_discovered_peers(id, torrent_id, &peer, pex_peers)?;
                if let Some(client) =
                    self.set_peer_client(id, &peer.address, peer.port, &connection.peer_id())?
                {
                    self.log(
                        LogLevel::Debug,
                        "peer",
                        format!("{}:{} identified as {client}", peer.address, peer.port),
                        Some(torrent_id),
                    );
                }
                let available = connection.availability().iter().filter(|piece| **piece).count();
                self.set_peer_connection(
                    id,
                    &peer.address,
                    peer.port,
                    &format!(
                        "Ready in {} ms; {available} pieces available",
                        elapsed.as_millis()
                    ),
                    None,
                )?;
                self.log(
                    LogLevel::Info,
                    "peer",
                    format!(
                        "{}:{} connected in {} ms with {available} available pieces",
                        peer.address,
                        peer.port,
                        elapsed.as_millis()
                    ),
                    Some(torrent_id),
                );
                connected.push(ConnectedPeer { peer, connection });
                Ok(pex_added)
            }
            Err(err) => {
                self.record_peer_failure(id, &peer.address, peer.port)?;
                self.set_peer_connection(id, &peer.address, peer.port, "Error", Some(err.clone()))?;
                self.log(
                    LogLevel::Warn,
                    "peer",
                    format!(
                        "{}:{} failed after {} ms: {err}",
                        peer.address,
                        peer.port,
                        elapsed.as_millis()
                    ),
                    Some(torrent_id),
                );
                errors.push(format!("{}:{}: {err}", peer.address, peer.port));
                Ok(0)
            }
        }
    }

    fn drain_peer_connection_results(
        &self,
        id: &str,
        torrent_id: u64,
        receiver: &std::sync::mpsc::Receiver<PeerConnectionResult>,
        connected: &mut Vec<ConnectedPeer>,
        pending_dht_nodes: &mut Vec<(String, u16)>,
        errors: &mut Vec<String>,
    ) -> Result<(usize, usize), String> {
        let mut drained = 0usize;
        let mut pex_added = 0usize;
        while let Ok((peer, result, elapsed)) = receiver.try_recv() {
            drained += 1;
            pex_added += self.accept_peer_connection_result(
                id,
                torrent_id,
                peer,
                result,
                elapsed,
                connected,
                pending_dht_nodes,
                errors,
            )?;
        }
        Ok((drained, pex_added))
    }

    fn merge_pex_discovered_peers(
        &self,
        id: &str,
        torrent_id: u64,
        source: &PeerInfo,
        peers: Vec<PeerInfo>,
    ) -> Result<usize, String> {
        if peers.is_empty() {
            return Ok(0);
        }
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        if torrent.general.private {
            return Ok(0);
        }
        let mut known_addresses = torrent
            .peers
            .iter()
            .map(|peer| peer.address.clone())
            .collect::<HashSet<_>>();
        let before = torrent.peers.len();
        let mut accepted = Vec::new();
        for peer in peers {
            if peer.address == source.address || peer.port == 0 {
                continue;
            }
            if !known_addresses.insert(peer.address.clone()) {
                continue;
            }
            accepted.push(peer);
        }
        merge_peers(&mut torrent.peers, accepted);
        let added = torrent.peers.len().saturating_sub(before);
        drop(torrents);
        if added > 0 {
            self.log(
                LogLevel::Debug,
                "pex",
                format!(
                    "accepted {added} PEX peer candidates from {}:{}",
                    source.address, source.port
                ),
                Some(torrent_id),
            );
        }
        Ok(added)
    }

    pub fn download_from_peers(&self, id: &str) -> Result<EmptyJsonResponse, String> {
        let mut attempted = HashSet::new();
        self.download_from_peers_refreshing(id, &mut attempted, 0)
    }

    fn download_from_peers_refreshing(
        &self,
        id: &str,
        attempted: &mut HashSet<(String, u16)>,
        refresh_round: usize,
    ) -> Result<EmptyJsonResponse, String> {
        let snapshot = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            let max_connections = normalized_connection_limit(torrent.options.max_connections);
            let PeerSchedule {
                mut peers,
                candidate_peers,
                deferred_for_backoff,
                deferred_for_duplicate_ip,
            } = schedule_peers_for_download(torrent, max_connections);
            peers.retain(|peer| !attempted.contains(&(peer.address.clone(), peer.port)));
            PeerDownloadSnapshot {
                id: torrent.id,
                name: torrent.name.clone(),
                info_hash: torrent.info_hash,
                output_folder: torrent.output_folder.clone(),
                files: torrent.files.clone(),
                total_length: torrent.stats.total_bytes,
                piece_length: torrent.general.piece_size,
                piece_hashes: torrent.piece_hashes.clone(),
                peers,
                max_connections,
                sequential_download: torrent.options.sequential_download,
                download_limiter: torrent.download_limiter.clone(),
                private: torrent.general.private,
                overwrite: torrent.options.overwrite,
                cancelled: Arc::clone(&torrent.cancelled),
                candidate_peers,
                deferred_for_backoff,
                deferred_for_duplicate_ip,
                stream_priority: torrent.stream_priority.clone(),
            }
        };

        if snapshot.files.is_empty() || snapshot.piece_hashes.is_empty() {
            self.log(
                LogLevel::Warn,
                "peer",
                "peer download skipped: torrent metadata is not available yet",
                Some(snapshot.id),
            );
            return Err("torrent metadata is not available yet".to_string());
        }
        if snapshot.peers.is_empty() {
            if attempted.is_empty() {
                self.log(
                    LogLevel::Warn,
                    "peer",
                    "peer download skipped: announce trackers first to discover peers",
                    Some(snapshot.id),
                );
                return Err("torrent has no discovered peers; announce trackers first".to_string());
            }
            self.log(
                LogLevel::Warn,
                "peer",
                format!(
                    "peer download skipped: all {} known peer endpoint(s) have already been tried",
                    snapshot.candidate_peers
                ),
                Some(snapshot.id),
            );
            return Err("all known peer endpoints have already been tried".to_string());
        }

        self.set_torrent_state(id, TorrentState::Downloading, None)?;
        self.log(
            LogLevel::Info,
            "peer",
            format!(
                "trying up to {} discovered peers with at most {} concurrent connections",
                snapshot.peers.len(),
                snapshot.max_connections.min(MAX_PARALLEL_PEERS)
            ),
            Some(snapshot.id),
        );
        if snapshot.deferred_for_backoff > 0 {
            self.log(
                LogLevel::Debug,
                "peer",
                format!(
                    "peer scheduler prioritized {} of {} candidates; {} recently failed peers were deferred by backoff",
                    snapshot.peers.len(),
                    snapshot.candidate_peers,
                    snapshot.deferred_for_backoff
                ),
                Some(snapshot.id),
            );
        }
        if snapshot.deferred_for_duplicate_ip > 0 {
            self.log(
                LogLevel::Debug,
                "peer",
                format!(
                    "peer scheduler deferred {} duplicate-IP candidates to preserve swarm diversity",
                    snapshot.deferred_for_duplicate_ip
                ),
                Some(snapshot.id),
            );
        }

        let store_key = sha1::hex(&snapshot.info_hash);
        let mut partial_store = storage::PartialPieceStore::open(
            &snapshot.output_folder,
            &store_key,
            snapshot.total_length,
            snapshot.piece_length,
            &snapshot.piece_hashes,
        )?;
        let mut verified = partial_store.verified_pieces().to_vec();
        let resumed_pieces = verified.iter().filter(|piece| **piece).count();
        if resumed_pieces > 0 {
            self.mark_resumed_piece_progress(
                id,
                &verified,
                snapshot.total_length,
                snapshot.piece_length,
            )?;
            self.log(
                LogLevel::Info,
                "storage",
                format!("resumed {resumed_pieces} verified pieces from the partial store"),
                Some(snapshot.id),
            );
        }
        let mut errors = Vec::new();
        let mut pending_dht_nodes = Vec::new();
        let mut pex_candidates_added = 0usize;
        let mut last_coverage = SwarmCoverageSummary::default();
        for peer_batch in snapshot
            .peers
            .chunks(snapshot.max_connections.min(MAX_PARALLEL_PEERS))
        {
            let batch_started = Instant::now();
            if snapshot.cancelled.load(Ordering::Relaxed) {
                return Err("peer download cancelled".to_string());
            }
            if verified.iter().all(|piece| *piece) {
                break;
            }

            let (connect_sender, connect_receiver) = std::sync::mpsc::channel();
            for peer in peer_batch.iter().cloned() {
                attempted.insert((peer.address.clone(), peer.port));
                self.set_peer_connection(id, &peer.address, peer.port, "Connecting", None)?;
                self.record_peer_attempt(id, &peer.address, peer.port)?;
                let address = peer.address.clone();
                let plan = PeerDownloadPlan {
                    info_hash: snapshot.info_hash,
                    peer_id: peer_id_for(snapshot.id),
                    dht_port: (!snapshot.private)
                        .then(|| self.dht_port())
                        .filter(|port| *port != 0),
                    enable_pex: !snapshot.private,
                    total_length: snapshot.total_length,
                    piece_length: snapshot.piece_length,
                    piece_hashes: snapshot.piece_hashes.clone(),
                    cancelled: Some(Arc::clone(&snapshot.cancelled)),
                };
                let download_limiter = snapshot.download_limiter.clone();
                let sender = connect_sender.clone();
                std::thread::spawn(move || {
                    let started = Instant::now();
                    let result = peerwire::connect_peer_for_download_with_limiter(
                        &address,
                        peer.port,
                        plan,
                        Some(download_limiter),
                    );
                    let _ = sender.send((peer, result, started.elapsed()));
                });
            }
            drop(connect_sender);

            let mut connected = Vec::new();
            let mut advertised_dht_nodes = Vec::new();
            let mut settled_peers = HashSet::new();
            let mut received = 0usize;
            let mut first_connected_at = None::<Instant>;
            let batch_deadline = Instant::now() + Duration::from_secs(6);
            while received < peer_batch.len() {
                if snapshot.cancelled.load(Ordering::Relaxed) {
                    return Err("peer download cancelled".to_string());
                }
                let timeout = if let Some(first_connected_at) = first_connected_at {
                    let grace = Duration::from_millis(900);
                    if first_connected_at.elapsed() >= grace {
                        break;
                    }
                    grace.saturating_sub(first_connected_at.elapsed())
                } else {
                    let remaining = batch_deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    remaining.min(Duration::from_millis(500))
                };
                let (peer, result, elapsed) = match connect_receiver.recv_timeout(timeout) {
                    Ok(result) => result,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if first_connected_at.is_some() || Instant::now() >= batch_deadline {
                            break;
                        }
                        continue;
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                };
                received += 1;
                settled_peers.insert((peer.address.clone(), peer.port));
                let connected_before = connected.len();
                pex_candidates_added += self.accept_peer_connection_result(
                    id,
                    snapshot.id,
                    peer,
                    result,
                    elapsed,
                    &mut connected,
                    &mut advertised_dht_nodes,
                    &mut errors,
                )?;
                if connected.len() > connected_before && first_connected_at.is_none() {
                    first_connected_at = Some(Instant::now());
                }
            }
            for peer in peer_batch {
                if !settled_peers.contains(&(peer.address.clone(), peer.port)) {
                    self.set_peer_connection(
                        id,
                        &peer.address,
                        peer.port,
                        "Deferred; downloading started",
                        None,
                    )?;
                }
            }
            if received < peer_batch.len() {
                self.log(
                    LogLevel::Debug,
                    "peer",
                    format!(
                        "starting piece assignment with {} peers while {} connection attempts continue in the background",
                        connected.len(),
                        peer_batch.len().saturating_sub(received)
                    ),
                    Some(snapshot.id),
                );
            }
            pending_dht_nodes.extend(advertised_dht_nodes);
            self.log(
                LogLevel::Info,
                "peer",
                format!(
                    "peer batch connected {}/{} peers in {} ms",
                    connected.len(),
                    peer_batch.len(),
                    batch_started.elapsed().as_millis()
                ),
                Some(snapshot.id),
            );

            if connected.is_empty() {
                let (drained, pex_added) = self.drain_peer_connection_results(
                    id,
                    snapshot.id,
                    &connect_receiver,
                    &mut connected,
                    &mut pending_dht_nodes,
                    &mut errors,
                )?;
                received += drained;
                pex_candidates_added += pex_added;
            }

            if connected.is_empty() {
                continue;
            }
            self.log(
                LogLevel::Info,
                "peer",
                format!(
                    "connected to {} peers; assigning {} missing pieces rarest-first",
                    connected.len(),
                    verified.iter().filter(|piece| !**piece).count()
                ),
                Some(snapshot.id),
            );

            loop {
                if snapshot.cancelled.load(Ordering::Relaxed) {
                    return Err("peer download cancelled".to_string());
                }
                if verified.iter().all(|piece| *piece) || connected.is_empty() {
                    break;
                }
                let (late_connections, pex_added) = self.drain_peer_connection_results(
                    id,
                    snapshot.id,
                    &connect_receiver,
                    &mut connected,
                    &mut pending_dht_nodes,
                    &mut errors,
                )?;
                received += late_connections;
                pex_candidates_added += pex_added;
                if late_connections > 0 {
                    self.log(
                        LogLevel::Debug,
                        "peer",
                        format!(
                            "admitted {late_connections} late peer connection results into the active batch"
                        ),
                        Some(snapshot.id),
                    );
                }
                let missing = verified
                    .iter()
                    .enumerate()
                    .filter_map(|(index, complete)| (!*complete).then_some(index as u32))
                    .collect::<Vec<_>>();
                let endgame_threshold = connected
                    .len()
                    .clamp(2, MAX_ENDGAME_PIECES);
                let endgame_candidate = (missing.len() <= endgame_threshold)
                    .then(|| {
                        missing
                            .iter()
                            .filter_map(|piece_index| {
                                let peer_count = connected
                                    .iter()
                                    .filter(|peer| {
                                        peer.connection
                                            .availability()
                                            .get(*piece_index as usize)
                                            .copied()
                                            .unwrap_or(false)
                                    })
                                    .count();
                                (peer_count >= 2).then_some((*piece_index, peer_count))
                            })
                            .min_by_key(|(piece_index, peer_count)| (*peer_count, *piece_index))
                    })
                    .flatten();
                if let Some((piece_index, endgame_peer_count)) = endgame_candidate {
                        self.log(
                            LogLevel::Info,
                            "peer",
                            format!(
                                "endgame mode: racing piece {piece_index} across {endgame_peer_count} peers; {} pieces remain",
                                missing.len()
                            ),
                            Some(snapshot.id),
                        );
                        let mut endgame_peers = Vec::with_capacity(endgame_peer_count);
                        let mut remaining_connections = Vec::new();
                        for peer in connected {
                            if peer
                                .connection
                                .availability()
                                .get(piece_index as usize)
                                .copied()
                                .unwrap_or(false)
                            {
                                endgame_peers.push(peer);
                            } else {
                                remaining_connections.push(peer);
                            }
                        }
                        let EndgameRaceResult {
                            winner,
                            cancelled,
                            duplicates,
                            errors: race_errors,
                            worker_panics,
                            reusable,
                        } = race_endgame_piece(
                            endgame_peers,
                            piece_index,
                            snapshot.cancelled.as_ref(),
                        );
                        remaining_connections.extend(reusable);
                        connected = remaining_connections;
                        for peer in cancelled {
                            self.set_peer_connection(
                                id,
                                &peer.address,
                                peer.port,
                                "Endgame duplicate cancelled",
                                None,
                            )?;
                        }
                        for peer in duplicates {
                            self.set_peer_connection(
                                id,
                                &peer.address,
                                peer.port,
                                "Endgame duplicate discarded",
                                None,
                            )?;
                        }
                        for (peer, error) in race_errors {
                            self.record_peer_failure(id, &peer.address, peer.port)?;
                            self.set_peer_connection(
                                id,
                                &peer.address,
                                peer.port,
                                "Error",
                                Some(error.clone()),
                            )?;
                            errors.push(format!("{}:{}: {error}", peer.address, peer.port));
                        }
                        errors.extend(worker_panics);
                        if snapshot.cancelled.load(Ordering::Acquire) {
                            return Err("peer download cancelled".to_string());
                        }
                        if let Some((peer, downloaded)) = winner {
                            let downloaded_bytes = downloaded.bytes.len() as u64;
                            partial_store.write_piece(downloaded.index, &downloaded.bytes)?;
                            verified[downloaded.index as usize] = true;
                            self.mark_peer_piece_progress(
                                id,
                                &peer.address,
                                peer.port,
                                downloaded_bytes,
                                1,
                                &verified,
                                snapshot.total_length,
                                snapshot.piece_length,
                                0,
                                None,
                                None,
                            )?;
                            self.log(
                                LogLevel::Info,
                                "peer",
                                format!(
                                    "{}:{} won endgame piece {piece_index} ({} bytes); duplicate requests were cancelled",
                                    peer.address, peer.port, downloaded_bytes
                                ),
                                Some(snapshot.id),
                            );
                            continue;
                        }
                        if connected.is_empty() {
                            break;
                        }
                }
                let availability = connected
                    .iter()
                    .map(|peer| peer.connection.availability().to_vec())
                    .collect::<Vec<_>>();
                let coverage = swarm_coverage_summary(&verified, &availability);
                last_coverage = coverage;
                let stream_priority = self
                    .current_stream_priority(id)?
                    .or_else(|| snapshot.stream_priority.clone());
                let mut assignments = if let Some(stream_priority) = stream_priority.as_ref() {
                    let peer_hints = self.peer_scheduling_hints(id, &connected)?;
                    assign_streaming_pieces(
                        &verified,
                        &availability,
                        &snapshot.files,
                        snapshot.piece_length,
                        stream_priority,
                        &peer_hints,
                        snapshot.sequential_download,
                    )?
                } else if snapshot.sequential_download {
                    assign_sequential_pieces(&verified, &availability)?
                } else {
                    assign_rarest_pieces(&verified, &availability)?
                };
                let assigned_before_limit = assignments.iter().map(Vec::len).sum::<usize>();
                limit_piece_assignments(&mut assignments, MAX_PIECES_PER_PEER_ROUND);
                let assigned_after_limit = assignments.iter().map(Vec::len).sum::<usize>();
                if assigned_after_limit < assigned_before_limit {
                    self.log(
                        LogLevel::Debug,
                        "peer",
                        format!(
                            "bounded peer assignment round to {assigned_after_limit}/{assigned_before_limit} pieces across {} peers",
                            assignments.len()
                        ),
                        Some(snapshot.id),
                    );
                }
                if assignments.iter().all(Vec::is_empty) {
                    let fresh_candidates = self.untried_peer_count(id, attempted)?;
                    if fresh_candidates > 0 && refresh_round < MAX_SWARM_REFRESH_ROUNDS {
                        self.log(
                            LogLevel::Info,
                            "peer",
                            format!(
                                "piece assignment stalled: {}; refreshing schedule with {fresh_candidates} untried candidate(s)",
                                format_swarm_coverage(coverage)
                            ),
                            Some(snapshot.id),
                        );
                        if !pending_dht_nodes.is_empty() {
                            self.verify_peer_dht_nodes(pending_dht_nodes, snapshot.id);
                        }
                        drop(partial_store);
                        return self.download_from_peers_refreshing(
                            id,
                            attempted,
                            refresh_round + 1,
                        );
                    }
                    if received < peer_batch.len() {
                        match connect_receiver.recv_timeout(Duration::from_millis(900)) {
                            Ok((peer, result, elapsed)) => {
                                received += 1;
                                let connected_before = connected.len();
                                pex_candidates_added += self.accept_peer_connection_result(
                                    id,
                                    snapshot.id,
                                    peer,
                                    result,
                                    elapsed,
                                    &mut connected,
                                    &mut pending_dht_nodes,
                                    &mut errors,
                                )?;
                                if connected.len() > connected_before {
                                    self.log(
                                        LogLevel::Debug,
                                        "peer",
                                        "late peer connection can cover stalled piece assignment",
                                        Some(snapshot.id),
                                    );
                                }
                                continue;
                            }
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {}
                        }
                    }
                    self.log(
                        LogLevel::Warn,
                        "peer",
                        format!(
                            "piece assignment stopped: {}; no immediately usable peer candidates remain in this batch",
                            format_swarm_coverage(coverage)
                        ),
                        Some(snapshot.id),
                    );
                    break;
                }

                let mut idle = Vec::new();
                let mut download_workers = Vec::new();
                for (mut connected_peer, wanted) in connected.into_iter().zip(assignments) {
                    if wanted.is_empty() {
                        idle.push(connected_peer);
                        continue;
                    }
                    let label = format!("{}:{}", connected_peer.peer.address, connected_peer.peer.port);
                    let worker = std::thread::spawn(move || {
                        let started = Instant::now();
                        let result = connected_peer.connection.download_pieces(&wanted);
                        (connected_peer, result, started.elapsed())
                    });
                    download_workers.push((label, worker));
                }

                let mut round_progress = 0usize;
                let mut peer_failed = false;
                let mut advertised_dht_nodes = Vec::new();
                for (label, worker) in download_workers {
                    let (mut connected_peer, result, download_elapsed) = match worker.join() {
                        Ok(value) => value,
                        Err(_) => {
                            peer_failed = true;
                            errors.push(format!("{label}: peer download worker panicked"));
                            continue;
                        }
                    };
                    if let Some(dht_port) = connected_peer.connection.take_remote_dht_port() {
                        advertised_dht_nodes
                            .push((connected_peer.peer.address.clone(), dht_port));
                    }
                    let pex_peers = connected_peer.connection.take_pex_peers();
                    pex_candidates_added += self.merge_pex_discovered_peers(
                        id,
                        snapshot.id,
                        &connected_peer.peer,
                        pex_peers,
                    )?;
                    let peer = &connected_peer.peer;
                    let mut contributed_bytes = 0u64;
                    let mut contributed_pieces = 0usize;
                    for downloaded in result.pieces {
                        let index = downloaded.index as usize;
                        if verified.get(index).copied().unwrap_or(false) {
                            continue;
                        }
                        partial_store.write_piece(downloaded.index, &downloaded.bytes)?;
                        verified[index] = true;
                        contributed_bytes += downloaded.bytes.len() as u64;
                        contributed_pieces += 1;
                    }
                    round_progress += contributed_pieces;
                    self.mark_peer_piece_progress(
                        id,
                        &peer.address,
                        peer.port,
                        contributed_bytes,
                        contributed_pieces,
                        &verified,
                        snapshot.total_length,
                        snapshot.piece_length,
                        result.unavailable.len(),
                        Some(download_elapsed),
                        result.error.as_deref(),
                    )?;
                    let download_rate = transfer_rate_bytes_per_second(contributed_bytes, download_elapsed);
                    self.log(
                        LogLevel::Info,
                        "peer",
                        format!(
                            "{}:{} contributed {} verified pieces ({} bytes at {download_rate} B/s, {} unavailable); {} pieces remain",
                            peer.address,
                            peer.port,
                            contributed_pieces,
                            contributed_bytes,
                            result.unavailable.len(),
                            verified.iter().filter(|piece| !**piece).count()
                        ),
                        Some(snapshot.id),
                    );
                    if let Some(err) = result.error {
                        peer_failed = true;
                        self.log(
                            LogLevel::Warn,
                            "peer",
                            format!("{}:{} stopped after partial progress: {err}", peer.address, peer.port),
                            Some(snapshot.id),
                        );
                        errors.push(format!("{}:{}: {err}", peer.address, peer.port));
                    } else {
                        idle.push(connected_peer);
                    }
                }
                pending_dht_nodes.extend(advertised_dht_nodes);
                connected = idle;
                if round_progress == 0 && !peer_failed {
                    self.log(
                        LogLevel::Warn,
                        "peer",
                        format!(
                            "peer round made no verified progress after assigning pieces; {}",
                            format_swarm_coverage(last_coverage)
                        ),
                        Some(snapshot.id),
                    );
                    break;
                }
            }
        }

        if verified.iter().all(|piece| *piece) {
            let summary = partial_store.write_complete_to_files(
                &snapshot.output_folder,
                &snapshot.name,
                &snapshot.files,
                snapshot.overwrite,
            )?;
            if snapshot.files.iter().all(|file| file.included) {
                partial_store.clear()?;
            }
            self.mark_swarm_download_complete(id, verified.len())?;
            self.log(
                LogLevel::Info,
                "peer",
                format!(
                    "swarm download verified all {} pieces and wrote {} files ({} bytes)",
                    verified.len(), summary.files_written, summary.bytes_written
                ),
                Some(snapshot.id),
            );
            if !pending_dht_nodes.is_empty() {
                self.verify_peer_dht_nodes(pending_dht_nodes, snapshot.id);
            }
            return Ok(EmptyJsonResponse {});
        }

        if pex_candidates_added > 0 && refresh_round < MAX_SWARM_REFRESH_ROUNDS {
            self.log(
                LogLevel::Info,
                "pex",
                format!(
                    "peer path learned {pex_candidates_added} PEX candidates; retrying swarm scheduling before failing"
                ),
                Some(snapshot.id),
            );
            if !pending_dht_nodes.is_empty() {
                self.verify_peer_dht_nodes(pending_dht_nodes, snapshot.id);
            }
            drop(partial_store);
            return self.download_from_peers_refreshing(id, attempted, refresh_round + 1);
        }

        let fresh_candidates = self.untried_peer_count(id, attempted)?;
        if fresh_candidates > 0 && refresh_round < MAX_SWARM_REFRESH_ROUNDS {
            self.log(
                LogLevel::Info,
                "peer",
                format!(
                    "swarm stalled with {fresh_candidates} newly discovered peer candidate(s); refreshing peer schedule"
                ),
                Some(snapshot.id),
            );
            if !pending_dht_nodes.is_empty() {
                self.verify_peer_dht_nodes(pending_dht_nodes, snapshot.id);
            }
            drop(partial_store);
            return self.download_from_peers_refreshing(id, attempted, refresh_round + 1);
        }

        let missing = verified.iter().filter(|piece| !**piece).count();
        let coverage_detail = if last_coverage.connected_peers > 0 || last_coverage.missing_pieces > 0 {
            format!("; {}", format_swarm_coverage(last_coverage))
        } else {
            String::new()
        };
        let detail = if errors.is_empty() {
            format!("swarm is missing {missing} pieces{coverage_detail}")
        } else {
            format!(
                "swarm is missing {missing} pieces{coverage_detail}; {}",
                errors.join("; ")
            )
        };
        self.set_torrent_state(
            id,
            TorrentState::Error,
            Some(detail.clone()),
        )?;
        if !pending_dht_nodes.is_empty() {
            self.verify_peer_dht_nodes(pending_dht_nodes, snapshot.id);
        }
        Err(detail)
    }

    fn untried_peer_count(
        &self,
        id: &str,
        attempted: &HashSet<(String, u16)>,
    ) -> Result<usize, String> {
        let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        Ok(torrent
            .peers
            .iter()
            .filter(|peer| !attempted.contains(&(peer.address.clone(), peer.port)))
            .count())
    }

    pub fn recheck(&self, id: &str) -> Result<EmptyJsonResponse, String> {
        let snapshot = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            RecheckSnapshot {
                id: torrent.id,
                name: torrent.name.clone(),
                output_folder: torrent.output_folder.clone(),
                files: torrent.files.clone(),
                piece_length: torrent.general.piece_size,
                piece_hashes: torrent.piece_hashes.clone(),
            }
        };

        if snapshot.files.is_empty() || snapshot.piece_hashes.is_empty() {
            self.log(
                LogLevel::Warn,
                "storage",
                "recheck skipped: torrent metadata is not available yet",
                Some(snapshot.id),
            );
            return Err("torrent metadata is not available yet".to_string());
        }

        self.log(LogLevel::Info, "storage", "rechecking stored torrent data", Some(snapshot.id));
        match storage::verify_stored_torrent(
            &snapshot.output_folder,
            &snapshot.name,
            &snapshot.files,
            snapshot.piece_length,
            &snapshot.piece_hashes,
        ) {
            Ok(check) => {
                let file_progress =
                    storage::file_progress_from_pieces(&snapshot.files, snapshot.piece_length, &check.pieces)?;
                let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
                let torrent = torrents
                    .iter_mut()
                    .find(|torrent| torrent.matches_id(id))
                    .ok_or_else(|| format!("torrent not found: {id}"))?;
                torrent.stats.progress_bytes = check.bytes_verified;
                torrent.stats.file_progress = file_progress;
                torrent.stats.finished = check.complete;
                torrent.general.downloaded = check.bytes_verified;
                torrent.stats.state = if check.complete {
                    completed_torrent_state(torrent)
                } else {
                    TorrentState::Partial
                };
                torrent.stats.error = None;
                self.log(
                    LogLevel::Info,
                    "storage",
                    format!(
                        "recheck finished: {}/{} pieces verified ({} bytes)",
                        check.pieces.iter().filter(|verified| **verified).count(),
                        check.pieces.len(),
                        check.bytes_verified
                    ),
                    Some(snapshot.id),
                );
                Ok(EmptyJsonResponse {})
            }
            Err(err) => {
                self.set_torrent_state(id, TorrentState::MissingFiles, Some(err.clone()))?;
                self.log(LogLevel::Warn, "storage", format!("recheck failed: {err}"), Some(snapshot.id));
                Err(err)
            }
        }
    }

    pub fn hash_torrent_file(&self, id: &str, file_index: usize) -> Result<TorrentFileHash, String> {
        let (torrent_id, torrent_name, output_folder, files, file, finished) = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            let file = torrent
                .files
                .get(file_index)
                .cloned()
                .ok_or_else(|| format!("torrent file index is out of range: {file_index}"))?;
            (
                torrent.id,
                torrent.name.clone(),
                torrent.output_folder.clone(),
                torrent.files.clone(),
                file,
                torrent.stats.finished,
            )
        };
        if !finished {
            return Err("file reputation lookup requires a completed, verified torrent".to_string());
        }
        if !file.included {
            return Err("unchecked torrent files are not available for hashing".to_string());
        }

        let multi_file = files.len() > 1
            || files
                .first()
                .is_some_and(|candidate| candidate.components.len() > 1);
        let path = storage::output_path_for_file(
            &output_folder,
            &torrent_name,
            &file,
            multi_file,
        )?;
        let link_metadata = fs::symlink_metadata(&path)
            .map_err(|err| format!("could not inspect torrent file for hashing: {err}"))?;
        if link_metadata.file_type().is_symlink() || !link_metadata.file_type().is_file() {
            return Err("torrent reputation checks require a regular, non-symlink file".to_string());
        }
        let canonical_root = fs::canonicalize(&output_folder)
            .map_err(|err| format!("could not resolve torrent output folder: {err}"))?;
        let canonical_path = fs::canonicalize(&path)
            .map_err(|err| format!("could not resolve torrent file: {err}"))?;
        if !canonical_path.starts_with(&canonical_root) {
            return Err("torrent file resolves outside its output folder".to_string());
        }
        if link_metadata.len() != file.length {
            return Err(format!(
                "torrent file length changed: expected {}, found {}",
                file.length,
                link_metadata.len()
            ));
        }

        self.log(
            LogLevel::Info,
            "security",
            format!("computing local SHA-256 for '{}'", file.name),
            Some(torrent_id),
        );
        let mut input = fs::File::open(&canonical_path)
            .map_err(|err| format!("could not open torrent file for hashing: {err}"))?;
        let mut hasher = sha256::Sha256::new();
        let mut buffer = [0u8; 64 * 1024];
        let mut bytes_read = 0u64;
        loop {
            let length = input
                .read(&mut buffer)
                .map_err(|err| format!("could not read torrent file for hashing: {err}"))?;
            if length == 0 {
                break;
            }
            bytes_read = bytes_read
                .checked_add(length as u64)
                .ok_or_else(|| "torrent file hash byte count overflowed".to_string())?;
            hasher.update(&buffer[..length]);
        }
        if bytes_read != file.length {
            return Err(format!(
                "torrent file changed while hashing: expected {}, read {bytes_read}",
                file.length
            ));
        }
        let sha256 = sha256::hex(&hasher.finalize());
        let virustotal_url = format!("https://www.virustotal.com/gui/file/{sha256}");
        self.log(
            LogLevel::Info,
            "security",
            format!(
                "computed SHA-256 {} for '{}'; no file bytes were uploaded",
                sha256, file.name
            ),
            Some(torrent_id),
        );
        Ok(TorrentFileHash {
            file_index,
            name: file.name,
            path: canonical_path.to_string_lossy().into_owned(),
            size: bytes_read,
            sha256,
            virustotal_url,
        })
    }

    pub fn stream_file_availability(
        &self,
        id: &str,
        file_index: usize,
    ) -> Result<TorrentFileAvailability, String> {
        let (torrent_id, torrent_name, output_folder, files, file, info_hash, piece_length, piece_hashes, finished) = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            let file = torrent
                .files
                .get(file_index)
                .cloned()
                .ok_or_else(|| format!("torrent file index is out of range: {file_index}"))?;
            (
                torrent.id,
                torrent.name.clone(),
                torrent.output_folder.clone(),
                torrent.files.clone(),
                file,
                torrent.info_hash,
                torrent.general.piece_size,
                torrent.piece_hashes.clone(),
                torrent.stats.finished,
            )
        };
        if files.is_empty() || piece_hashes.is_empty() {
            return Err("torrent metadata is not available yet".to_string());
        }
        if !file.included {
            return Err("unchecked torrent files are not available for streaming".to_string());
        }

        if finished {
            let ranges = if file.length == 0 {
                Vec::new()
            } else {
                vec![storage::VerifiedByteRange {
                    offset: 0,
                    length: file.length,
                }]
            };
            return Ok(TorrentFileAvailability {
                file_index,
                name: file.name,
                length: file.length,
                verified_bytes: file.length,
                complete: true,
                partial_store_present: false,
                ranges,
            });
        }

        let store_key = sha1::hex(&info_hash);
        let partial_store = storage::PartialPieceStore::open_existing(
            &output_folder,
            &store_key,
            storage::total_file_length(&files)?,
            piece_length,
            &piece_hashes,
        )?;
        let pieces = partial_store
            .as_ref()
            .map(|store| store.verified_pieces().to_vec())
            .unwrap_or_else(|| vec![false; piece_hashes.len()]);
        let ranges = storage::verified_file_ranges_from_pieces(
            &files,
            file_index,
            piece_length,
            &pieces,
        )?;
        let verified_bytes = ranges.iter().map(|range| range.length).sum::<u64>();
        self.log(
            LogLevel::Debug,
            "stream",
            format!(
                "file availability for '{}' in '{}': {} verified bytes across {} range(s)",
                file.name,
                torrent_name,
                verified_bytes,
                ranges.len()
            ),
            Some(torrent_id),
        );

        Ok(TorrentFileAvailability {
            file_index,
            name: file.name,
            length: file.length,
            verified_bytes,
            complete: verified_bytes == file.length,
            partial_store_present: partial_store.is_some(),
            ranges,
        })
    }

    pub fn read_stream_file_range(
        &self,
        id: &str,
        file_index: usize,
        offset: u64,
        length: u64,
    ) -> Result<StreamFileRead, String> {
        if length > MAX_MEDIA_STREAM_READ_BYTES {
            return Err(format!(
                "stream byte range is too large: maximum is {MAX_MEDIA_STREAM_READ_BYTES} bytes"
            ));
        }
        let (
            torrent_id,
            torrent_name,
            output_folder,
            files,
            file,
            info_hash,
            piece_length,
            piece_hashes,
            finished,
        ) = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            let file = torrent
                .files
                .get(file_index)
                .cloned()
                .ok_or_else(|| format!("torrent file index is out of range: {file_index}"))?;
            (
                torrent.id,
                torrent.name.clone(),
                torrent.output_folder.clone(),
                torrent.files.clone(),
                file,
                torrent.info_hash,
                torrent.general.piece_size,
                torrent.piece_hashes.clone(),
                torrent.stats.finished,
            )
        };
        if files.is_empty() || piece_hashes.is_empty() {
            return Err("torrent metadata is not available yet".to_string());
        }
        if !file.included {
            return Err("unchecked torrent files are not available for streaming".to_string());
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| "stream byte range overflow".to_string())?;
        if end > file.length {
            return Err("stream byte range exceeds file length".to_string());
        }

        let bytes = if finished {
            let multi_file = files.len() > 1
                || files
                    .first()
                    .is_some_and(|candidate| candidate.components.len() > 1);
            let path = storage::output_path_for_file(
                &output_folder,
                &torrent_name,
                &file,
                multi_file,
            )?;
            let link_metadata = fs::symlink_metadata(&path)
                .map_err(|err| format!("could not inspect torrent stream file: {err}"))?;
            if link_metadata.file_type().is_symlink() || !link_metadata.file_type().is_file() {
                return Err("torrent streaming requires a regular, non-symlink file".to_string());
            }
            let canonical_root = fs::canonicalize(&output_folder)
                .map_err(|err| format!("could not resolve torrent output folder: {err}"))?;
            let canonical_path = fs::canonicalize(&path)
                .map_err(|err| format!("could not resolve torrent stream file: {err}"))?;
            if !canonical_path.starts_with(&canonical_root) {
                return Err("torrent stream file resolves outside its output folder".to_string());
            }
            if link_metadata.len() != file.length {
                return Err(format!(
                    "torrent stream file length changed: expected {}, found {}",
                    file.length,
                    link_metadata.len()
                ));
            }
            let read_len = usize::try_from(length)
                .map_err(|_| "stream byte range is too large for this platform".to_string())?;
            let mut bytes = vec![0u8; read_len];
            let mut input = fs::File::open(&canonical_path)
                .map_err(|err| format!("could not open torrent stream file: {err}"))?;
            input
                .seek(SeekFrom::Start(offset))
                .map_err(|err| format!("could not seek torrent stream file: {err}"))?;
            input
                .read_exact(&mut bytes)
                .map_err(|err| format!("could not read torrent stream file: {err}"))?;
            bytes
        } else {
            let store_key = sha1::hex(&info_hash);
            let mut partial_store = storage::PartialPieceStore::open_existing(
                &output_folder,
                &store_key,
                storage::total_file_length(&files)?,
                piece_length,
                &piece_hashes,
            )?
            .ok_or_else(|| "stream data is not buffered yet".to_string())?;
            partial_store.read_verified_file_range(&files, file_index, offset, length)?
        };

        self.log(
            LogLevel::Debug,
            "stream",
            format!(
                "served {} byte(s) from '{}' at offset {}",
                bytes.len(),
                file.name,
                offset
            ),
            Some(torrent_id),
        );
        Ok(StreamFileRead {
            bytes,
            total_length: file.length,
        })
    }

    pub fn set_stream_priority(
        &self,
        id: &str,
        request: StreamPriorityRequest,
    ) -> Result<StreamPriorityStatus, String> {
        self.set_stream_priority_inner(id, request, true)
    }

    pub fn update_stream_priority_quietly(
        &self,
        id: &str,
        request: StreamPriorityRequest,
    ) -> Result<StreamPriorityStatus, String> {
        self.set_stream_priority_inner(id, request, false)
    }

    fn set_stream_priority_inner(
        &self,
        id: &str,
        request: StreamPriorityRequest,
        log_priority: bool,
    ) -> Result<StreamPriorityStatus, String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        if torrent.files.is_empty() || torrent.piece_hashes.is_empty() {
            return Err("torrent metadata is not available yet".to_string());
        }
        let file = torrent
            .files
            .get(request.file_index)
            .ok_or_else(|| format!("torrent file index is out of range: {}", request.file_index))?;
        if !file.included {
            return Err("unchecked torrent files cannot be streamed".to_string());
        }
        let file_name = file.name.clone();
        let file_length = file.length;
        let playhead_offset = if file_length == 0 {
            0
        } else {
            request.playhead_offset.min(file_length - 1)
        };
        let state = StreamPriorityState {
            file_index: request.file_index,
            playhead_offset,
            urgent_bytes: normalize_stream_window_bytes(
                request.urgent_bytes,
                DEFAULT_STREAM_URGENT_BYTES,
            ),
            lookahead_bytes: normalize_stream_window_bytes(
                request.lookahead_bytes,
                DEFAULT_STREAM_LOOKAHEAD_BYTES,
            ),
            updated_at_ms: timestamp_ms(),
        };
        let plan = stream_priority_piece_plan(&torrent.files, torrent.general.piece_size, &state)?;
        torrent.stream_priority = Some(state);
        let status = StreamPriorityStatus {
            file_index: request.file_index,
            name: file_name,
            playhead_offset,
            urgent_pieces: plan.urgent.len(),
            lookahead_pieces: plan.lookahead.len(),
            total_priority_pieces: plan.urgent.len() + plan.lookahead.len(),
        };
        if log_priority {
            self.log(
                LogLevel::Info,
                "stream",
                format!(
                    "prioritizing '{}' at byte {} ({} urgent piece(s), {} lookahead piece(s))",
                    status.name, status.playhead_offset, status.urgent_pieces, status.lookahead_pieces
                ),
                Some(torrent.id),
            );
        }
        Ok(status)
    }

    pub fn clear_stream_priority(&self, id: &str) -> Result<EmptyJsonResponse, String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        torrent.stream_priority = None;
        self.log(
            LogLevel::Info,
            "stream",
            "cleared stream playback priority; normal piece scheduling resumes",
            Some(torrent.id),
        );
        Ok(EmptyJsonResponse {})
    }

    pub fn stream_priority_window(
        &self,
        id: &str,
        file_index: usize,
    ) -> Result<Option<(u64, u64)>, String> {
        let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        Ok(torrent
            .stream_priority
            .as_ref()
            .filter(|priority| priority.file_index == file_index)
            .map(|priority| (priority.urgent_bytes, priority.lookahead_bytes)))
    }

    fn current_stream_priority(&self, id: &str) -> Result<Option<StreamPriorityState>, String> {
        let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        Ok(torrent.stream_priority.clone())
    }

    fn peer_scheduling_hints(
        &self,
        id: &str,
        connected: &[ConnectedPeer],
    ) -> Result<Vec<PeerSchedulingHint>, String> {
        let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        Ok(connected
            .iter()
            .map(|connected| {
                let health = torrent
                    .peer_health
                    .get(&peer_key(&connected.peer.address, connected.peer.port));
                PeerSchedulingHint {
                    success_score: peer_success_score(health),
                    piece_score: peer_piece_score(health),
                    byte_score: peer_byte_score(health),
                    rate_score: peer_rate_score(health),
                    unavailable_score: peer_unavailable_count(health),
                    failure_count: peer_failure_count(health),
                    attempt_count: peer_attempt_count(health),
                }
            })
            .collect())
    }

    pub fn query_dht(&self, id: &str) -> Result<EmptyJsonResponse, String> {
        let seeds = dht::default_bootstrap_nodes();
        self.query_dht_with_seeds(id, &seeds, Duration::from_secs(4), 24)
    }

    pub fn resolve_magnet(&self, id: &str) -> Result<EmptyJsonResponse, String> {
        let seeds = dht::default_bootstrap_nodes();
        self.resolve_magnet_with_seeds(id, &seeds, Duration::from_secs(4), 24)
    }

    fn resolve_magnet_with_seeds(
        &self,
        id: &str,
        seeds: &[DhtContact],
        timeout: Duration,
        max_queries: usize,
    ) -> Result<EmptyJsonResponse, String> {
        let snapshot = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            MetadataFetchSnapshot {
                id: torrent.id,
                info_hash: torrent.info_hash,
                peers: torrent.peers.clone(),
                metadata_available: !torrent.piece_hashes.is_empty(),
                private: torrent.general.private,
            }
        };

        if snapshot.metadata_available {
            self.log(
                LogLevel::Info,
                "metadata",
                "magnet resolve skipped: metadata is already available",
                Some(snapshot.id),
            );
            return Ok(EmptyJsonResponse {});
        }

        self.log(
            LogLevel::Info,
            "metadata",
            "resolving magnet through DHT peer lookup and BEP 9 metadata fetch",
            Some(snapshot.id),
        );
        if snapshot.peers.is_empty() {
            self.query_dht_with_seeds(id, seeds, timeout, max_queries)?;
        } else {
            self.log(
                LogLevel::Info,
                "metadata",
                format!("using {} existing peers before metadata fetch", snapshot.peers.len()),
                Some(snapshot.id),
            );
        }
        self.fetch_metadata(id)
    }

    fn query_dht_with_seeds(
        &self,
        id: &str,
        seeds: &[DhtContact],
        timeout: Duration,
        max_queries: usize,
    ) -> Result<EmptyJsonResponse, String> {
        let snapshot = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            DhtLookupSnapshot {
                id: torrent.id,
                info_hash: torrent.info_hash,
                private: torrent.general.private,
                finished: torrent.stats.finished,
                paused: torrent.options.paused,
            }
        };

        if snapshot.private {
            self.log(
                LogLevel::Warn,
                "dht",
                "DHT lookup skipped: private torrents must not use public peer discovery",
                Some(snapshot.id),
            );
            return Err("private torrents must not use DHT peer discovery".to_string());
        }
        if seeds.is_empty() {
            self.log(
                LogLevel::Warn,
                "dht",
                "DHT lookup skipped: no bootstrap nodes configured",
                Some(snapshot.id),
            );
            return Err("DHT lookup needs at least one bootstrap node".to_string());
        }

        self.set_torrent_state(id, TorrentState::Dht, None)?;
        self.log(
            LogLevel::Info,
            "dht",
            format!("querying DHT from {} bootstrap nodes", seeds.len()),
            Some(snapshot.id),
        );

        let result = match dht::lookup_peers(
            seeds,
            self.dht_node_id,
            snapshot.info_hash,
            DhtLookupOptions { timeout, max_queries },
        ) {
            Ok(result) => result,
            Err(err) => {
                self.set_torrent_state(id, TorrentState::DhtError, Some(err.clone()))?;
                self.log(LogLevel::Warn, "dht", format!("DHT lookup failed: {err}"), Some(snapshot.id));
                return Err(err);
            }
        };

        for error in result.errors.iter().take(5) {
            self.log(LogLevel::Debug, "dht", error.clone(), Some(snapshot.id));
        }

        let queried_nodes = result.queried_nodes;
        let discovered_nodes = result.discovered_nodes;
        let announce_target_count = result.announce_targets.len();
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        let before = torrent.peers.len();
        merge_peers(&mut torrent.peers, result.peers);
        merge_dht_announce_targets(&mut torrent.dht_announce_targets, result.announce_targets);
        let added = torrent.peers.len().saturating_sub(before);
        if torrent.stats.state != TorrentState::Paused {
            torrent.stats.state = if torrent.stats.finished {
                completed_torrent_state(torrent)
            } else if torrent.piece_hashes.is_empty() {
                TorrentState::Metadata
            } else {
                TorrentState::Queued
            };
            torrent.stats.error = None;
        }
        drop(torrents);
        self.log(
            LogLevel::Info,
            "dht",
            format!(
                "DHT lookup finished: queried {queried_nodes} nodes, learned {discovered_nodes} closer nodes, added {added} peer entries, retained {announce_target_count} announce tokens"
            ),
            Some(snapshot.id),
        );
        if snapshot.finished && !snapshot.paused && announce_target_count > 0 {
            let _ = self.announce_dht_targets(id, timeout);
        }
        Ok(EmptyJsonResponse {})
    }

    fn announce_dht_targets(&self, id: &str, timeout: Duration) -> Result<usize, String> {
        let (torrent_id, info_hash, targets) = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            if torrent.general.private {
                return Err("private torrents must not use DHT announce_peer".to_string());
            }
            if torrent.options.paused || !torrent.stats.finished {
                return Ok(0);
            }
            (torrent.id, torrent.info_hash, torrent.dht_announce_targets.clone())
        };
        if targets.is_empty() {
            return Ok(0);
        }
        let port = self.listen_port();
        if port == 0 {
            return Err("DHT announce_peer requires an active inbound listener".to_string());
        }

        let mut announced = 0usize;
        let mut errors = Vec::new();
        for (index, target) in targets.iter().enumerate() {
            let transaction_id = dht_announce_transaction_id(torrent_id, index);
            match dht::announce_peer(
                target,
                &transaction_id,
                self.dht_node_id,
                info_hash,
                port,
                timeout,
            ) {
                Ok(_) => announced += 1,
                Err(err) => errors.push(format!("{}:{}: {err}", target.contact.address, target.contact.port)),
            }
        }
        self.log(
            if announced > 0 { LogLevel::Info } else { LogLevel::Warn },
            "dht",
            format!(
                "announce_peer finished: advertised to {announced}/{} token-issuing nodes",
                targets.len()
            ),
            Some(torrent_id),
        );
        for error in errors.iter().take(5) {
            self.log(LogLevel::Debug, "dht", error.clone(), Some(torrent_id));
        }
        if announced == 0 {
            return Err(format!("all DHT announce_peer queries failed: {}", errors.join("; ")));
        }
        Ok(announced)
    }

    pub fn fetch_metadata(&self, id: &str) -> Result<EmptyJsonResponse, String> {
        let snapshot = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            MetadataFetchSnapshot {
                id: torrent.id,
                info_hash: torrent.info_hash,
                peers: torrent.peers.clone(),
                metadata_available: !torrent.piece_hashes.is_empty(),
                private: torrent.general.private,
            }
        };

        if snapshot.metadata_available {
            self.log(
                LogLevel::Info,
                "metadata",
                "metadata fetch skipped: metadata is already available",
                Some(snapshot.id),
            );
            return Ok(EmptyJsonResponse {});
        }
        if snapshot.peers.is_empty() {
            self.log(
                LogLevel::Warn,
                "metadata",
                "metadata fetch skipped: announce trackers or query DHT first to discover peers",
                Some(snapshot.id),
            );
            return Err("torrent has no discovered peers; announce trackers or query DHT first".to_string());
        }

        self.set_torrent_state(id, TorrentState::FetchingMetadata, None)?;
        self.log(
            LogLevel::Info,
            "metadata",
            format!("trying {} discovered peers for metadata", snapshot.peers.len()),
            Some(snapshot.id),
        );

        let mut last_error = None;
        for peer in &snapshot.peers {
            self.set_peer_connection(id, &peer.address, peer.port, "Fetching metadata", None)?;
            match peerwire::fetch_metadata_from_peer(
                &peer.address,
                peer.port,
                MetadataFetchPlan {
                    info_hash: snapshot.info_hash,
                    peer_id: peer_id_for(snapshot.id),
                    dht_port: (!snapshot.private)
                        .then(|| self.dht_port())
                        .filter(|port| *port != 0),
                },
            ) {
                Ok(result) => {
                    if let Some(client) =
                        self.set_peer_client(id, &peer.address, peer.port, &result.peer_id)?
                    {
                        self.log(
                            LogLevel::Debug,
                            "metadata",
                            format!("{}:{} identified as {client}", peer.address, peer.port),
                            Some(snapshot.id),
                        );
                    }
                    let meta = Metainfo::from_info_bytes(&result.info_bytes, None, Vec::new(), Vec::new())?;
                    if !meta.private {
                        if let Some(dht_port) = result.remote_dht_port {
                            if let Err(err) =
                                self.learn_peer_dht_node(&peer.address, dht_port, snapshot.id)
                            {
                                self.log(
                                    LogLevel::Debug,
                                    "dht",
                                    format!(
                                        "could not verify DHT port {dht_port} from metadata peer {}: {err}",
                                        peer.address
                                    ),
                                    Some(snapshot.id),
                                );
                            }
                        }
                    }
                    self.apply_fetched_metadata(id, meta)?;
                    self.set_peer_connection(id, &peer.address, peer.port, "Metadata received", None)?;
                    self.log(
                        LogLevel::Info,
                        "metadata",
                        format!(
                            "fetched {} metadata bytes from {}:{} in {} pieces",
                            result.info_bytes.len(),
                            peer.address,
                            peer.port,
                            result.pieces_received
                        ),
                        Some(snapshot.id),
                    );
                    return Ok(EmptyJsonResponse {});
                }
                Err(err) => {
                    self.set_peer_connection(id, &peer.address, peer.port, "Error", Some(err.clone()))?;
                    self.log(
                        LogLevel::Warn,
                        "metadata",
                        format!("{}:{}: {err}", peer.address, peer.port),
                        Some(snapshot.id),
                    );
                    last_error = Some(err);
                }
            }
        }

        self.set_torrent_state(
            id,
            TorrentState::MetadataError,
            Some("all discovered peers failed to provide metadata".to_string()),
        )?;
        Err(last_error.unwrap_or_else(|| "all discovered peers failed to provide metadata".to_string()))
    }

    pub fn delete(&self, id: &str, delete_files: bool) -> Result<EmptyJsonResponse, String> {
        if let Err(err) = self.announce_stopped(id) {
            self.log(
                LogLevel::Warn,
                "tracker",
                format!("could not send stopped announce before removal: {err}"),
                id.parse().ok(),
            );
        }
        let cleanup = self.remove_for_delete(id, delete_files)?;
        self.cleanup_deleted_torrent(cleanup);
        Ok(EmptyJsonResponse {})
    }

    pub fn remove_for_delete(
        &self,
        id: &str,
        delete_files: bool,
    ) -> Result<DeletedTorrentCleanup, String> {
        let started = Instant::now();
        let torrent = {
            let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let index = torrents
                .iter()
                .position(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            torrents.remove(index)
        };
        torrent.cancelled.store(true, Ordering::Relaxed);
        let cleanup = DeletedTorrentCleanup {
            id: torrent.id,
            info_hash: torrent.info_hash,
            name: torrent.name.clone(),
            output_folder: torrent.output_folder.clone(),
            files: torrent.files.clone(),
            delete_files,
        };
        self.log(
            LogLevel::Warn,
            "session",
            if delete_files {
                format!(
                    "removed torrent from session and scheduled file deletion in {} ms",
                    started.elapsed().as_millis()
                )
            } else {
                format!(
                    "removed torrent from session in {} ms",
                    started.elapsed().as_millis()
                )
            },
            Some(cleanup.id),
        );
        self.persist_session_or_log(Some(cleanup.id));
        Ok(cleanup)
    }

    pub fn cleanup_deleted_torrent(&self, cleanup: DeletedTorrentCleanup) {
        if !cleanup.delete_files {
            return;
        }
        let started = Instant::now();
        match delete_torrent_payload_files(
            &cleanup.output_folder,
            &cleanup.name,
            &cleanup.files,
            cleanup.info_hash,
        ) {
            Ok((files_deleted, bytes_deleted)) => self.log(
                LogLevel::Info,
                "storage",
                format!(
                    "deleted {files_deleted} torrent files and {bytes_deleted} bytes in {} ms",
                    started.elapsed().as_millis()
                ),
                Some(cleanup.id),
            ),
            Err(err) => self.log(
                LogLevel::Error,
                "storage",
                format!("file deletion failed after {} ms: {err}", started.elapsed().as_millis()),
                Some(cleanup.id),
            ),
        }
    }

    pub fn update_files(&self, id: &str, only_files: Vec<usize>) -> Result<EmptyJsonResponse, String> {
        self.update_task(id, |torrent| {
            for (index, file) in torrent.files.iter_mut().enumerate() {
                file.included = only_files.contains(&index);
            }
            torrent.stats.file_progress = torrent
                .files
                .iter()
                .map(|file| if file.included { 0 } else { file.length })
                .collect();
        })?;
        self.persist_session_or_log(id.parse().ok());
        Ok(EmptyJsonResponse {})
    }

    pub fn update_options(
        &self,
        id: &str,
        request: UpdateTorrentOptionsRequest,
    ) -> Result<EmptyJsonResponse, String> {
        let options = validate_runtime_options(request)?;
        let torrent_id = {
            let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            apply_runtime_options(torrent, &options);
            update_completed_torrent_state(torrent);
            torrent.id
        };
        self.log(
            LogLevel::Info,
            "options",
            format!(
                "updated runtime options: connections={}, download limit={}, upload limit={}, order={}, seed ratio={}",
                options
                    .max_connections
                    .map(|limit| limit.to_string())
                    .unwrap_or_else(|| format!("automatic ({DEFAULT_CONNECTION_LIMIT})")),
                format_optional_rate(options.max_download_speed),
                format_optional_rate(options.max_upload_speed),
                if options.sequential_download {
                    "sequential"
                } else {
                    "rarest-first"
                },
                options
                    .seed_ratio_limit
                    .map(|limit| format!("{limit:.2}"))
                    .unwrap_or_else(|| "unlimited".to_string())
            ),
            Some(torrent_id),
        );
        self.persist_session_or_log(Some(torrent_id));
        Ok(EmptyJsonResponse {})
    }

    pub fn logs(&self, torrent_id: Option<u64>) -> Vec<LogEntry> {
        self.logs
            .lock()
            .map(|logs| {
                logs.iter()
                    .filter(|entry| torrent_id.is_none() || entry.torrent_id == torrent_id)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn log(
        &self,
        level: LogLevel,
        scope: impl Into<String>,
        message: impl Into<String>,
        torrent_id: Option<u64>,
    ) {
        let entry = LogEntry {
            id: self.next_log_id.fetch_add(1, Ordering::Relaxed),
            timestamp_ms: timestamp_ms(),
            level,
            scope: scope.into(),
            message: message.into(),
            torrent_id,
        };
        self.write_log_entry(&entry);
        if let Ok(mut logs) = self.logs.lock() {
            logs.push(entry);
            if logs.len() > 1_000 {
                logs.remove(0);
            }
        }
    }

    fn write_log_entry(&self, entry: &LogEntry) {
        if let Some(parent) = self.log_file_path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_file_path)
        else {
            return;
        };
        let torrent = entry
            .torrent_id
            .map(|id| format!(" torrent={id}"))
            .unwrap_or_default();
        let _ = writeln!(
            file,
            "{} [{}] scope={}{} message={}",
            entry.timestamp_ms,
            log_level_label(&entry.level),
            single_line(&entry.scope),
            torrent,
            single_line(&entry.message)
        );
    }

    fn restore_session(&self) {
        if !self.session_file_path.is_file() {
            return;
        }
        let bytes = match fs::read(&self.session_file_path) {
            Ok(bytes) => bytes,
            Err(err) => {
                self.log(
                    LogLevel::Warn,
                    "session",
                    format!("could not read persisted session: {err}"),
                    None,
                );
                return;
            }
        };
        let manifest = match serde_json::from_slice::<PersistedSession>(&bytes) {
            Ok(manifest) if manifest.version == 1 => manifest,
            Ok(manifest) => {
                self.log(
                    LogLevel::Warn,
                    "session",
                    format!("persisted session version {} is not supported", manifest.version),
                    None,
                );
                return;
            }
            Err(err) => {
                self.log(
                    LogLevel::Warn,
                    "session",
                    format!("persisted session is not valid JSON: {err}"),
                    None,
                );
                return;
            }
        };

        let mut restored = Vec::new();
        let mut highest_id = 0u64;
        for persisted in manifest.torrents {
            if persisted.id == 0 || restored.iter().any(|torrent: &TorrentTask| torrent.id == persisted.id) {
                self.log(
                    LogLevel::Warn,
                    "session",
                    format!("skipped persisted torrent with invalid or duplicate ID {}", persisted.id),
                    None,
                );
                continue;
            }
            let runtime_options = match validate_runtime_options(UpdateTorrentOptionsRequest {
                max_connections: persisted.max_connections,
                max_download_speed: persisted.max_download_speed,
                max_upload_speed: persisted.max_upload_speed,
                sequential_download: persisted.sequential_download,
                seed_ratio_limit: persisted.seed_ratio_limit,
            }) {
                Ok(options) => options,
                Err(err) => {
                    self.log(
                        LogLevel::Warn,
                        "session",
                        format!(
                            "persisted torrent {} has invalid runtime options; defaults restored: {err}",
                            persisted.id
                        ),
                        Some(persisted.id),
                    );
                    default_runtime_options()
                }
            };
            let request = AddTorrentRequest {
                source: persisted.source,
                destination: Some(persisted.destination),
                paused: persisted.paused,
                overwrite: persisted.overwrite,
                disable_trackers: persisted.disable_trackers,
                only_files: persisted.only_files,
                sub_folder: persisted.sub_folder,
            };
            match self.build_task(request, Some(persisted.id)) {
                Ok(mut task) => {
                    apply_runtime_options(&mut task, &runtime_options);
                    highest_id = highest_id.max(task.id);
                    restored.push(task);
                }
                Err(err) => self.log(
                    LogLevel::Warn,
                    "session",
                    format!("skipped persisted torrent {}: {err}", persisted.id),
                    Some(persisted.id),
                ),
            }
        }

        let restored_count = restored.len();
        if let Ok(mut torrents) = self.torrents.lock() {
            *torrents = restored;
        }
        self.next_id
            .store(highest_id.saturating_add(1).max(1), Ordering::Relaxed);
        self.log(
            LogLevel::Info,
            "session",
            format!("restored {restored_count} torrents from persisted session"),
            None,
        );
    }

    fn persist_session(&self) -> Result<(), String> {
        let manifest = {
            let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            PersistedSession {
                version: 1,
                torrents: torrents
                    .iter()
                    .map(|torrent| {
                        let only_files = if torrent.files.is_empty()
                            || torrent.files.iter().all(|file| file.included)
                        {
                            None
                        } else {
                            Some(
                                torrent
                                    .files
                                    .iter()
                                    .enumerate()
                                    .filter(|(_, file)| file.included)
                                    .map(|(index, _)| index)
                                    .collect(),
                            )
                        };
                        PersistedTorrent {
                            id: torrent.id,
                            source: torrent.source.clone(),
                            destination: torrent.output_folder.to_string_lossy().into_owned(),
                            paused: torrent.options.paused,
                            overwrite: torrent.options.overwrite,
                            disable_trackers: torrent.options.disable_trackers,
                            only_files,
                            sub_folder: torrent.options.sub_folder.clone(),
                            max_connections: torrent.options.max_connections,
                            max_download_speed: torrent.options.max_download_speed,
                            max_upload_speed: torrent.options.max_upload_speed,
                            sequential_download: torrent.options.sequential_download,
                            seed_ratio_limit: torrent.options.seed_ratio_limit,
                        }
                    })
                    .collect(),
            }
        };
        if let Some(parent) = self.session_file_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("could not create session state directory: {err}"))?;
        }
        let bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|err| format!("could not encode persisted session: {err}"))?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.session_file_path)
            .map_err(|err| format!("could not open persisted session: {err}"))?;
        file.write_all(&bytes)
            .map_err(|err| format!("could not write persisted session: {err}"))?;
        file.sync_all()
            .map_err(|err| format!("could not flush persisted session: {err}"))
    }

    fn persist_session_or_log(&self, torrent_id: Option<u64>) {
        if let Err(err) = self.persist_session() {
            self.log(LogLevel::Error, "session", err, torrent_id);
        }
    }

    fn build_task(&self, request: AddTorrentRequest, id: Option<u64>) -> Result<TorrentTask, String> {
        let output_folder = request
            .destination
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| self.default_output_dir.clone());
        let options = TorrentOptions {
            paused: request.paused,
            overwrite: request.overwrite,
            disable_trackers: request.disable_trackers,
            sub_folder: request.sub_folder.clone(),
            max_connections: Some(50),
            max_download_speed: None,
            max_upload_speed: None,
            sequential_download: false,
            seed_ratio_limit: Some(1.0),
        };

        match &request.source {
            TorrentSource::File(path) => self.task_from_file(
                id.unwrap_or(0),
                path,
                output_folder,
                options,
                request.only_files.as_deref(),
                request.source.clone(),
            ),
            TorrentSource::Magnet(link) => self.task_from_magnet(
                id.unwrap_or(0),
                link,
                output_folder,
                options,
                request.source.clone(),
            ),
        }
    }

    fn task_from_file(
        &self,
        id: u64,
        path: &str,
        output_folder: PathBuf,
        options: TorrentOptions,
        only_files: Option<&[usize]>,
        source: TorrentSource,
    ) -> Result<TorrentTask, String> {
        let bytes = fs::read(path).map_err(|err| format!("could not read torrent file: {err}"))?;
        let meta = Metainfo::from_bytes(&bytes)?;
        let mut files = meta.files.clone();
        if let Some(only_files) = only_files {
            for (index, file) in files.iter_mut().enumerate() {
                file.included = only_files.contains(&index);
            }
        }
        let trackers = if options.disable_trackers {
            Vec::new()
        } else {
            meta.tracker_urls()
                .into_iter()
                .map(|url| TrackerStatus {
                    url,
                    state: "Not contacted".to_string(),
                    seeders: None,
                    leechers: None,
                    next_announce_seconds: None,
                    message: None,
                })
                .collect()
        };
        let web_seeds = webseed::initial_statuses(&meta.web_seeds);
        let stats = TorrentStats {
            state: if options.paused {
                TorrentState::Paused
            } else {
                TorrentState::Queued
            },
            file_progress: files.iter().map(|_| 0).collect(),
            error: None,
            progress_bytes: 0,
            uploaded_bytes: 0,
            total_bytes: meta.total_length,
            finished: false,
            live: Some(LiveStats {
                download_speed: 0,
                upload_speed: 0,
                time_remaining: None,
            }),
        };
        let general = TorrentGeneral {
            save_path: output_folder.to_string_lossy().into_owned(),
            total_size: meta.total_length,
            downloaded: 0,
            uploaded: 0,
            ratio: 0.0,
            piece_size: meta.piece_length,
            piece_count: meta.pieces.len(),
            file_count: files.len(),
            private: meta.private,
            comment: meta.comment,
            created_by: meta.created_by,
            creation_date: meta.creation_date,
            active_time_seconds: 0,
            seeding_time_seconds: 0,
        };

        let cancelled = Arc::new(AtomicBool::new(options.paused));
        let upload_gate = peerwire::UploadGate::new(upload_slot_limit(options.max_connections));
        let download_limiter = peerwire::BandwidthLimiter::new(options.max_download_speed);
        let upload_limiter = peerwire::BandwidthLimiter::new(options.max_upload_speed);
        Ok(TorrentTask {
            id,
            source,
            info_hash: meta.info_hash,
            name: meta.name,
            output_folder,
            files,
            trackers,
            web_seeds,
            peers: Vec::new(),
            piece_hashes: meta.pieces,
            stats,
            general,
            options,
                cancelled,
                tracker_started: false,
                dht_announce_targets: Vec::new(),
                tracker_completed: false,
                next_announce_at_ms: None,
            added_at_ms: timestamp_ms(),
            upload_gate,
                download_limiter,
                upload_limiter,
                peer_health: HashMap::new(),
                stream_priority: None,
            })
    }

    fn task_from_magnet(
        &self,
        id: u64,
        link: &str,
        output_folder: PathBuf,
        options: TorrentOptions,
        source: TorrentSource,
    ) -> Result<TorrentTask, String> {
        let magnet = MagnetLink::parse(link)?;
        let name = magnet
            .display_name
            .clone()
            .unwrap_or_else(|| sha1::hex(&magnet.info_hash));
        let trackers = if options.disable_trackers {
            Vec::new()
        } else {
            magnet
                .trackers
                .into_iter()
                .map(|url| TrackerStatus {
                    url,
                    state: "Not contacted".to_string(),
                    seeders: None,
                    leechers: None,
                    next_announce_seconds: None,
                    message: None,
                })
                .collect()
        };
        let web_seeds = webseed::initial_statuses(&magnet.web_seeds);
        let peers = magnet.peers;
        let stats = TorrentStats {
            state: if options.paused {
                TorrentState::Paused
            } else {
                TorrentState::Metadata
            },
            file_progress: Vec::new(),
            error: None,
            progress_bytes: 0,
            uploaded_bytes: 0,
            total_bytes: 0,
            finished: false,
            live: Some(LiveStats {
                download_speed: 0,
                upload_speed: 0,
                time_remaining: None,
            }),
        };
        let general = TorrentGeneral {
            save_path: output_folder.to_string_lossy().into_owned(),
            total_size: 0,
            downloaded: 0,
            uploaded: 0,
            ratio: 0.0,
            piece_size: 0,
            piece_count: 0,
            file_count: 0,
            private: false,
            comment: None,
            created_by: None,
            creation_date: None,
            active_time_seconds: 0,
            seeding_time_seconds: 0,
        };

        let cancelled = Arc::new(AtomicBool::new(options.paused));
        let upload_gate = peerwire::UploadGate::new(upload_slot_limit(options.max_connections));
        let download_limiter = peerwire::BandwidthLimiter::new(options.max_download_speed);
        let upload_limiter = peerwire::BandwidthLimiter::new(options.max_upload_speed);
        Ok(TorrentTask {
            id,
            source,
            info_hash: magnet.info_hash,
            name,
            output_folder,
            files: Vec::new(),
            trackers,
            web_seeds,
            peers,
            piece_hashes: Vec::new(),
            stats,
            general,
            options,
            cancelled,
            tracker_started: false,
            dht_announce_targets: Vec::new(),
            tracker_completed: false,
            next_announce_at_ms: None,
            added_at_ms: timestamp_ms(),
            upload_gate,
            download_limiter,
            upload_limiter,
            peer_health: HashMap::new(),
            stream_priority: None,
        })
    }

    fn runtime_snapshot(&self, id: &str) -> Result<RuntimeSnapshot, String> {
        let torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        Ok(RuntimeSnapshot {
            id: torrent.id,
            name: torrent.name.clone(),
            output_folder: torrent.output_folder.clone(),
            files: torrent.files.clone(),
            paused: torrent.options.paused,
            finished: torrent.stats.finished,
            seed_ratio_reached: seed_ratio_reached(torrent),
            metadata_available: !torrent.piece_hashes.is_empty(),
            private: torrent.general.private,
            trackers_disabled: torrent.options.disable_trackers,
            tracker_count: torrent.trackers.len(),
            peer_count: torrent.peers.len(),
            webseed_count: torrent.web_seeds.len(),
            overwrite: torrent.options.overwrite,
        })
    }

    fn ensure_running(&self, snapshot: &RuntimeSnapshot) -> Result<(), String> {
        if snapshot.paused {
            Err(RUN_PAUSED.to_string())
        } else {
            Ok(())
        }
    }

    fn update_task(&self, id: &str, update: impl FnOnce(&mut TorrentTask)) -> Result<(), String> {
        let torrent_id = {
            let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.matches_id(id))
                .ok_or_else(|| format!("torrent not found: {id}"))?;
            update(torrent);
            torrent.id
        };
        self.log(LogLevel::Info, "session", "updated torrent state", Some(torrent_id));
        self.persist_session_or_log(Some(torrent_id));
        Ok(())
    }

    fn set_webseed_state(
        &self,
        id: &str,
        url: &str,
        state: &str,
        message: Option<String>,
        bytes_downloaded: u64,
    ) -> Result<(), String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        if let Some(seed) = torrent.web_seeds.iter_mut().find(|seed| seed.url == url) {
            seed.state = state.to_string();
            seed.message = message;
            seed.bytes_downloaded = bytes_downloaded;
        }
        Ok(())
    }

    fn set_torrent_state(
        &self,
        id: &str,
        state: TorrentState,
        error: Option<String>,
    ) -> Result<(), String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        torrent.stats.state = state;
        torrent.stats.error = error;
        Ok(())
    }

    fn set_completed_torrent_state(&self, id: &str) -> Result<(), String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        update_completed_torrent_state(torrent);
        torrent.stats.error = None;
        Ok(())
    }

    fn set_peer_client(
        &self,
        id: &str,
        address: &str,
        port: u16,
        peer_id: &[u8],
    ) -> Result<Option<String>, String> {
        let Some(client) = crate::torrent::peer::decode_peer_client(peer_id) else {
            return Ok(None);
        };
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        if let Some(peer) = torrent
            .peers
            .iter_mut()
            .find(|peer| peer.address == address && peer.port == port)
        {
            peer.client = Some(client.clone());
        }
        Ok(Some(client))
    }

    fn set_peer_connection(
        &self,
        id: &str,
        address: &str,
        port: u16,
        connection: &str,
        message: Option<String>,
    ) -> Result<(), String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        if let Some(peer) = torrent
            .peers
            .iter_mut()
            .find(|peer| peer.address == address && peer.port == port)
        {
            peer.connection = match message {
                Some(message) => format!("{connection}: {message}"),
                None => connection.to_string(),
            };
        }
        Ok(())
    }

    fn record_peer_attempt(&self, id: &str, address: &str, port: u16) -> Result<(), String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        let health = peer_health_entry(torrent, address, port);
        health.connection_attempts = health.connection_attempts.saturating_add(1);
        Ok(())
    }

    fn record_peer_connect_success(
        &self,
        id: &str,
        address: &str,
        port: u16,
    ) -> Result<(), String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        let health = peer_health_entry(torrent, address, port);
        health.consecutive_failures = 0;
        health.last_success_ms = Some(timestamp_ms());
        Ok(())
    }

    fn record_peer_failure(&self, id: &str, address: &str, port: u16) -> Result<(), String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        let health = peer_health_entry(torrent, address, port);
        health.consecutive_failures = health.consecutive_failures.saturating_add(1);
        health.last_failure_ms = Some(timestamp_ms());
        Ok(())
    }

    fn mark_webseed_complete(
        &self,
        id: &str,
        url: &str,
        bytes_downloaded: u64,
        pieces_verified: usize,
    ) -> Result<(), String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        if let Some(seed) = torrent.web_seeds.iter_mut().find(|seed| seed.url == url) {
            seed.state = "Complete".to_string();
            seed.message = Some(format!("verified {pieces_verified} pieces"));
            seed.bytes_downloaded = bytes_downloaded;
        }
        torrent.stats.progress_bytes = torrent.stats.total_bytes;
        torrent.stats.finished = true;
        torrent.stats.file_progress = torrent.files.iter().map(|file| file.length).collect();
        torrent.general.downloaded = torrent.stats.total_bytes;
        update_completed_torrent_state(torrent);
        Ok(())
    }

    fn mark_resumed_piece_progress(
        &self,
        id: &str,
        verified: &[bool],
        total_length: u64,
        piece_length: u64,
    ) -> Result<(), String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        let progress_bytes = verified_piece_bytes(total_length, piece_length, verified)?;
        torrent.stats.file_progress =
            storage::file_progress_from_pieces(&torrent.files, piece_length, verified)?;
        torrent.stats.progress_bytes = progress_bytes;
        torrent.stats.finished = false;
        torrent.stats.state = TorrentState::Resuming;
        torrent.stats.error = None;
        torrent.general.downloaded = progress_bytes;
        Ok(())
    }

    fn mark_peer_piece_progress(
        &self,
        id: &str,
        address: &str,
        port: u16,
        contributed_bytes: u64,
        contributed_pieces: usize,
        verified: &[bool],
        total_length: u64,
        piece_length: u64,
        unavailable_pieces: usize,
        download_elapsed: Option<Duration>,
        error: Option<&str>,
    ) -> Result<(), String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        let progress_bytes = verified_piece_bytes(total_length, piece_length, verified)?;
        let file_progress = storage::file_progress_from_pieces(&torrent.files, piece_length, verified)?;
        let transfer_rate =
            download_elapsed.map(|elapsed| transfer_rate_bytes_per_second(contributed_bytes, elapsed));
        let displayed_download_speed = transfer_rate.unwrap_or(contributed_bytes);
        if let Some(peer) = torrent
            .peers
            .iter_mut()
            .find(|peer| peer.address == address && peer.port == port)
        {
            peer.connection = match error {
                Some(error) if contributed_pieces > 0 => {
                    format!("Contributed {contributed_pieces} pieces; stopped: {error}")
                }
                Some(error) => format!("Error: {error}"),
                None if contributed_pieces == 0 && unavailable_pieces > 0 => {
                    format!("No requested pieces available; {unavailable_pieces} unavailable")
                }
                None if contributed_pieces == 0 => "No needed pieces".to_string(),
                None => format!("Contributed {contributed_pieces} verified pieces"),
            };
            peer.download_speed = displayed_download_speed;
        }
        {
            let health = peer_health_entry(torrent, address, port);
            if contributed_pieces > 0 {
                health.bytes_downloaded = health.bytes_downloaded.saturating_add(contributed_bytes);
                health.pieces_downloaded = health
                    .pieces_downloaded
                    .saturating_add(contributed_pieces as u64);
                health.consecutive_failures = 0;
                health.last_success_ms = Some(timestamp_ms());
                if let Some(rate) = transfer_rate {
                    health.recent_bytes_per_second = smooth_transfer_rate(
                        health.recent_bytes_per_second,
                        rate,
                    );
                }
            }
            if unavailable_pieces > 0 {
                health.unavailable_pieces = health
                    .unavailable_pieces
                    .saturating_add(unavailable_pieces as u64);
            }
            if error.is_some() {
                health.consecutive_failures = health.consecutive_failures.saturating_add(1);
                health.last_failure_ms = Some(timestamp_ms());
            }
        }
        torrent.stats.progress_bytes = progress_bytes;
        torrent.stats.finished = false;
        torrent.stats.state = TorrentState::Downloading;
        torrent.stats.error = None;
        torrent.stats.file_progress = file_progress;
        if let Some(live) = torrent.stats.live.as_mut() {
            live.download_speed = displayed_download_speed;
            live.time_remaining = None;
        }
        torrent.general.downloaded = progress_bytes;
        Ok(())
    }

    fn mark_swarm_download_complete(&self, id: &str, pieces_verified: usize) -> Result<(), String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        for peer in &mut torrent.peers {
            if peer.connection.starts_with("Contributed") {
                peer.connection = format!("Swarm complete; {pieces_verified} pieces verified");
            }
        }
        torrent.stats.progress_bytes = torrent.stats.total_bytes;
        torrent.stats.finished = true;
        torrent.stats.error = None;
        torrent.stats.file_progress = torrent.files.iter().map(|file| file.length).collect();
        if let Some(live) = torrent.stats.live.as_mut() {
            live.download_speed = 0;
            live.time_remaining = Some(0);
        }
        torrent.general.downloaded = torrent.stats.total_bytes;
        update_completed_torrent_state(torrent);
        Ok(())
    }

    fn apply_fetched_metadata(&self, id: &str, meta: Metainfo) -> Result<(), String> {
        let mut torrents = self.torrents.lock().map_err(|_| "torrent lock poisoned")?;
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.matches_id(id))
            .ok_or_else(|| format!("torrent not found: {id}"))?;
        if torrent.info_hash != meta.info_hash {
            return Err("fetched metadata info hash does not match magnet".to_string());
        }

        torrent.name = meta.name;
        torrent.files = meta.files;
        torrent.piece_hashes = meta.pieces;
        torrent.web_seeds = webseed::initial_statuses(&meta.web_seeds);
        torrent.stats.state = if torrent.options.paused {
            TorrentState::Paused
        } else {
            TorrentState::Queued
        };
        torrent.stats.error = None;
        torrent.stats.file_progress = torrent.files.iter().map(|_| 0).collect();
        torrent.stats.progress_bytes = 0;
        torrent.stats.total_bytes = meta.total_length;
        torrent.stats.finished = false;
        torrent.general.total_size = meta.total_length;
        torrent.general.downloaded = 0;
        torrent.general.piece_size = meta.piece_length;
        torrent.general.piece_count = torrent.piece_hashes.len();
        torrent.general.file_count = torrent.files.len();
        torrent.general.private = meta.private;
        torrent.general.comment = meta.comment;
        torrent.general.created_by = meta.created_by;
        torrent.general.creation_date = meta.creation_date;
        Ok(())
    }
}

impl TorrentTask {
    fn details(&self) -> TorrentDetails {
        let mut stats = self.stats.clone();
        stats.live = stats.live.or(Some(LiveStats {
            download_speed: 0,
            upload_speed: 0,
            time_remaining: None,
        }));
        let mut general = self.general.clone();
        general.active_time_seconds = ((timestamp_ms() - self.added_at_ms) / 1_000) as u64;

        TorrentDetails {
            id: Some(self.id).filter(|id| *id != 0),
            info_hash: sha1::hex(&self.info_hash),
            name: Some(self.name.clone()),
            output_folder: self.output_folder.to_string_lossy().into_owned(),
            files: Some(self.files.clone()),
            stats: Some(stats),
            general,
            trackers: self.trackers.clone(),
            web_seeds: self.web_seeds.clone(),
            peers: self.peers.clone(),
            options: self.options.clone(),
        }
    }

    fn matches_id(&self, id: &str) -> bool {
        self.id.to_string() == id || sha1::hex(&self.info_hash) == id
    }
}

fn race_endgame_piece(
    connected: Vec<ConnectedPeer>,
    piece_index: u32,
    torrent_cancelled: &AtomicBool,
) -> EndgameRaceResult {
    let (sender, receiver) = std::sync::mpsc::channel();
    let mut round_cancellations = Vec::with_capacity(connected.len());
    let mut workers = Vec::with_capacity(connected.len());

    for mut connected_peer in connected {
        let round_cancelled = Arc::new(AtomicBool::new(false));
        round_cancellations.push(Arc::clone(&round_cancelled));
        let worker_cancelled = Arc::clone(&round_cancelled);
        let peer = connected_peer.peer.clone();
        let label = format!("{}:{}", peer.address, peer.port);
        let sender = sender.clone();
        let worker = std::thread::spawn(move || {
            let result = connected_peer
                .connection
                .download_pieces_with_round_cancel(&[piece_index], Arc::clone(&worker_cancelled));
            let was_cancelled = worker_cancelled.load(Ordering::Acquire);
            let _ = sender.send((connected_peer, peer, result, was_cancelled));
        });
        workers.push((label, worker));
    }
    drop(sender);

    let worker_count = workers.len();
    let mut received = 0usize;
    let mut winner = None;
    let mut cancelled = Vec::new();
    let mut duplicates = Vec::new();
    let mut errors = Vec::new();
    let mut reusable = Vec::new();
    while received < worker_count {
        if torrent_cancelled.load(Ordering::Acquire) {
            for cancellation in &round_cancellations {
                cancellation.store(true, Ordering::Release);
            }
        }
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok((connected_peer, peer, result, was_cancelled)) => {
                received += 1;
                let downloaded = result
                    .pieces
                    .into_iter()
                    .find(|piece| piece.index == piece_index);
                if let Some(downloaded) = downloaded {
                    if winner.is_none() {
                        winner = Some((peer, downloaded));
                        for cancellation in &round_cancellations {
                            cancellation.store(true, Ordering::Release);
                        }
                    } else {
                        duplicates.push(peer);
                    }
                    reusable.push(connected_peer);
                } else if was_cancelled || torrent_cancelled.load(Ordering::Acquire) {
                    cancelled.push(peer);
                    reusable.push(connected_peer);
                } else if let Some(error) = result.error {
                    errors.push((peer, error));
                } else {
                    reusable.push(connected_peer);
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    let mut worker_panics = Vec::new();
    for (label, worker) in workers {
        if worker.join().is_err() {
            worker_panics.push(format!("{label}: endgame worker panicked"));
        }
    }
    EndgameRaceResult {
        winner,
        cancelled,
        duplicates,
        errors,
        worker_panics,
        reusable,
    }
}

fn assign_rarest_pieces(
    verified: &[bool],
    peer_availability: &[Vec<bool>],
) -> Result<Vec<Vec<u32>>, String> {
    let mut assignments = vec![Vec::new(); peer_availability.len()];
    let mut missing = verified
        .iter()
        .enumerate()
        .filter(|(_, complete)| !**complete)
        .filter_map(|(piece_index, _)| {
            let rarity = peer_availability
                .iter()
                .filter(|availability| availability.get(piece_index).copied().unwrap_or(false))
                .count();
            (rarity > 0).then_some((piece_index, rarity))
        })
        .collect::<Vec<_>>();
    missing.sort_by_key(|(piece_index, rarity)| (*rarity, *piece_index));

    for (piece_index, _) in missing {
        let Some(peer_index) = peer_availability
            .iter()
            .enumerate()
            .filter(|(_, availability)| {
                availability.get(piece_index).copied().unwrap_or(false)
            })
            .min_by_key(|(peer_index, _)| (assignments[*peer_index].len(), *peer_index))
            .map(|(peer_index, _)| peer_index)
        else {
            continue;
        };
        assignments[peer_index].push(
            u32::try_from(piece_index).map_err(|_| "torrent has too many pieces".to_string())?,
        );
    }
    Ok(assignments)
}

fn assign_sequential_pieces(
    verified: &[bool],
    peer_availability: &[Vec<bool>],
) -> Result<Vec<Vec<u32>>, String> {
    let mut assignments = vec![Vec::new(); peer_availability.len()];
    for (piece_index, complete) in verified.iter().enumerate() {
        if *complete {
            continue;
        }
        let Some(peer_index) = peer_availability
            .iter()
            .enumerate()
            .filter(|(_, availability)| {
                availability.get(piece_index).copied().unwrap_or(false)
            })
            .min_by_key(|(peer_index, _)| (assignments[*peer_index].len(), *peer_index))
            .map(|(peer_index, _)| peer_index)
        else {
            continue;
        };
        assignments[peer_index].push(
            u32::try_from(piece_index).map_err(|_| "torrent has too many pieces".to_string())?,
        );
    }
    Ok(assignments)
}

fn swarm_coverage_summary(
    verified: &[bool],
    peer_availability: &[Vec<bool>],
) -> SwarmCoverageSummary {
    let mut summary = SwarmCoverageSummary {
        connected_peers: peer_availability.len(),
        ..SwarmCoverageSummary::default()
    };
    for (piece_index, complete) in verified.iter().enumerate() {
        if *complete {
            continue;
        }
        summary.missing_pieces += 1;
        let covering_peers = peer_availability
            .iter()
            .filter(|availability| availability.get(piece_index).copied().unwrap_or(false))
            .count();
        if covering_peers == 0 {
            summary.unavailable_pieces += 1;
        } else {
            summary.coverable_pieces += 1;
            if covering_peers == 1 {
                summary.single_peer_pieces += 1;
            }
        }
    }
    summary
}

fn format_swarm_coverage(summary: SwarmCoverageSummary) -> String {
    format!(
        "{} missing piece(s), {} coverable by {} connected peer(s), {} unavailable, {} only on one peer",
        summary.missing_pieces,
        summary.coverable_pieces,
        summary.connected_peers,
        summary.unavailable_pieces,
        summary.single_peer_pieces
    )
}

fn assign_streaming_pieces(
    verified: &[bool],
    peer_availability: &[Vec<bool>],
    files: &[TorrentFile],
    piece_length: u64,
    priority: &StreamPriorityState,
    peer_hints: &[PeerSchedulingHint],
    fallback_sequential: bool,
) -> Result<Vec<Vec<u32>>, String> {
    let mut assignments = vec![Vec::new(); peer_availability.len()];
    let mut assigned = HashSet::<usize>::new();
    let plan = stream_priority_piece_plan(files, piece_length, priority)?;

    for piece_index in plan.urgent.iter().chain(plan.lookahead.iter()) {
        let piece_index = *piece_index as usize;
        if assigned.contains(&piece_index) || verified.get(piece_index).copied().unwrap_or(true) {
            continue;
        }
        if assign_piece_to_best_stream_peer(
            &mut assignments,
            peer_availability,
            peer_hints,
            piece_index,
        )? {
            assigned.insert(piece_index);
        }
    }
    if !assigned.is_empty() {
        return Ok(assignments);
    }

    let mut remaining = verified
        .iter()
        .enumerate()
        .filter(|(piece_index, complete)| !**complete && !assigned.contains(piece_index))
        .filter_map(|(piece_index, _)| {
            let rarity = peer_availability
                .iter()
                .filter(|availability| availability.get(piece_index).copied().unwrap_or(false))
                .count();
            (rarity > 0).then_some((piece_index, rarity))
        })
        .collect::<Vec<_>>();
    if fallback_sequential {
        remaining.sort_by_key(|(piece_index, _)| *piece_index);
    } else {
        remaining.sort_by_key(|(piece_index, rarity)| (*rarity, *piece_index));
    }
    for (piece_index, _) in remaining {
        let _ = assign_piece_to_best_peer(&mut assignments, peer_availability, piece_index)?;
    }
    Ok(assignments)
}

fn assign_piece_to_best_peer(
    assignments: &mut [Vec<u32>],
    peer_availability: &[Vec<bool>],
    piece_index: usize,
) -> Result<bool, String> {
    let Some(peer_index) = peer_availability
        .iter()
        .enumerate()
        .filter(|(_, availability)| availability.get(piece_index).copied().unwrap_or(false))
        .min_by_key(|(peer_index, _)| (assignments[*peer_index].len(), *peer_index))
        .map(|(peer_index, _)| peer_index)
    else {
        return Ok(false);
    };
    assignments[peer_index].push(
        u32::try_from(piece_index).map_err(|_| "torrent has too many pieces".to_string())?,
    );
    Ok(true)
}

fn assign_piece_to_best_stream_peer(
    assignments: &mut [Vec<u32>],
    peer_availability: &[Vec<bool>],
    peer_hints: &[PeerSchedulingHint],
    piece_index: usize,
) -> Result<bool, String> {
    let Some(peer_index) = peer_availability
        .iter()
        .enumerate()
        .filter(|(_, availability)| availability.get(piece_index).copied().unwrap_or(false))
        .min_by_key(|(peer_index, _)| {
            let hint = peer_hints.get(*peer_index).copied().unwrap_or_default();
            (
                usize::from(hint.success_score == 0 && hint.piece_score == 0),
                assignments[*peer_index].len(),
                hint.failure_count,
                hint.unavailable_score,
                std::cmp::Reverse(hint.rate_score),
                std::cmp::Reverse(hint.success_score),
                std::cmp::Reverse(hint.piece_score),
                std::cmp::Reverse(hint.byte_score),
                hint.attempt_count,
                *peer_index,
            )
        })
        .map(|(peer_index, _)| peer_index)
    else {
        return Ok(false);
    };
    assignments[peer_index].push(
        u32::try_from(piece_index).map_err(|_| "torrent has too many pieces".to_string())?,
    );
    Ok(true)
}

fn stream_priority_piece_plan(
    files: &[TorrentFile],
    piece_length: u64,
    priority: &StreamPriorityState,
) -> Result<StreamPiecePriorityPlan, String> {
    if piece_length == 0 {
        return Err("piece length cannot be zero".to_string());
    }
    let file = files
        .get(priority.file_index)
        .ok_or_else(|| format!("torrent file index is out of range: {}", priority.file_index))?;
    if file.length == 0 {
        return Ok(StreamPiecePriorityPlan {
            urgent: Vec::new(),
            lookahead: Vec::new(),
        });
    }
    let file_start = files
        .iter()
        .take(priority.file_index)
        .try_fold(0u64, |total, file| {
            total
                .checked_add(file.length)
                .ok_or_else(|| "file offset overflow".to_string())
        })?;
    let file_end = file_start
        .checked_add(file.length)
        .ok_or_else(|| "file offset overflow".to_string())?;
    let playhead = file_start
        .checked_add(priority.playhead_offset.min(file.length - 1))
        .ok_or_else(|| "stream playhead offset overflow".to_string())?;
    let urgent_end = playhead
        .checked_add(priority.urgent_bytes)
        .unwrap_or(file_end)
        .min(file_end);
    let lookahead_end = urgent_end
        .checked_add(priority.lookahead_bytes)
        .unwrap_or(file_end)
        .min(file_end);

    let mut urgent = Vec::new();
    let mut urgent_seen = HashSet::new();
    push_piece_range(&mut urgent, &mut urgent_seen, playhead, urgent_end, piece_length)?;
    push_piece_range(
        &mut urgent,
        &mut urgent_seen,
        file_start,
        (file_start + 1).min(file_end),
        piece_length,
    )?;
    push_piece_range(
        &mut urgent,
        &mut urgent_seen,
        file_end - 1,
        file_end,
        piece_length,
    )?;

    let mut lookahead = Vec::new();
    let mut lookahead_seen = urgent_seen;
    push_piece_range(
        &mut lookahead,
        &mut lookahead_seen,
        urgent_end,
        lookahead_end,
        piece_length,
    )?;
    Ok(StreamPiecePriorityPlan { urgent, lookahead })
}

fn push_piece_range(
    out: &mut Vec<u32>,
    seen: &mut HashSet<u32>,
    start: u64,
    end: u64,
    piece_length: u64,
) -> Result<(), String> {
    if start >= end {
        return Ok(());
    }
    let first = start / piece_length;
    let last = (end - 1) / piece_length;
    for piece_index in first..=last {
        let piece_index =
            u32::try_from(piece_index).map_err(|_| "torrent has too many pieces".to_string())?;
        if seen.insert(piece_index) {
            out.push(piece_index);
        }
    }
    Ok(())
}

fn normalize_stream_window_bytes(value: Option<u64>, default: u64) -> u64 {
    value
        .unwrap_or(default)
        .clamp(1, MAX_STREAM_LOOKAHEAD_BYTES)
}

fn limit_piece_assignments(assignments: &mut [Vec<u32>], max_per_peer: usize) {
    if max_per_peer == 0 {
        return;
    }
    for pieces in assignments {
        pieces.truncate(max_per_peer);
    }
}

fn delete_torrent_payload_files(
    output_root: &Path,
    torrent_name: &str,
    files: &[TorrentFile],
    info_hash: [u8; 20],
) -> Result<(usize, u64), String> {
    let multi_file = files.len() > 1 || files.first().is_some_and(|file| file.components.len() > 1);
    let mut deleted_files = 0usize;
    let mut deleted_bytes = 0u64;
    let mut candidate_dirs = HashSet::<PathBuf>::new();

    for file in files {
        let path = storage::output_path_for_file(output_root, torrent_name, file, multi_file)?;
        if let Some(parent) = path.parent() {
            candidate_dirs.insert(parent.to_path_buf());
        }
        match fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => {
                deleted_bytes = deleted_bytes.saturating_add(metadata.len());
                fs::remove_file(&path).map_err(|err| {
                    format!("could not delete torrent file {}: {err}", path.to_string_lossy())
                })?;
                deleted_files += 1;
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(format!(
                    "could not inspect torrent file {}: {err}",
                    path.to_string_lossy()
                ));
            }
        }
    }

    let store_dir = output_root.join(".novatorrent");
    let store_key = sha1::hex(&info_hash);
    for path in [
        store_dir.join(format!("{store_key}.part")),
        store_dir.join(format!("{store_key}.json")),
    ] {
        match fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => {
                deleted_bytes = deleted_bytes.saturating_add(metadata.len());
                fs::remove_file(&path).map_err(|err| {
                    format!("could not delete partial store file {}: {err}", path.to_string_lossy())
                })?;
                deleted_files += 1;
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(format!(
                    "could not inspect partial store file {}: {err}",
                    path.to_string_lossy()
                ));
            }
        }
    }
    candidate_dirs.insert(store_dir);
    if multi_file {
        candidate_dirs.insert(output_root.join(torrent_name));
    }

    let mut dirs = candidate_dirs.into_iter().collect::<Vec<_>>();
    dirs.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for dir in dirs {
        if dir == output_root {
            continue;
        }
        let _ = fs::remove_dir(dir);
    }

    Ok((deleted_files, deleted_bytes))
}

fn timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn default_runtime_options() -> UpdateTorrentOptionsRequest {
    UpdateTorrentOptionsRequest {
        max_connections: Some(DEFAULT_CONNECTION_LIMIT as u32),
        max_download_speed: None,
        max_upload_speed: None,
        sequential_download: false,
        seed_ratio_limit: Some(1.0),
    }
}

fn validate_runtime_options(
    mut options: UpdateTorrentOptionsRequest,
) -> Result<UpdateTorrentOptionsRequest, String> {
    options.max_connections = match options.max_connections {
        Some(0) | None => None,
        Some(limit) if limit as usize > MAX_CONNECTION_LIMIT => {
            return Err(format!(
                "max connections cannot exceed {MAX_CONNECTION_LIMIT}"
            ));
        }
        value => value,
    };
    options.max_download_speed = validate_rate_limit(options.max_download_speed, "download")?;
    options.max_upload_speed = validate_rate_limit(options.max_upload_speed, "upload")?;
    if options.seed_ratio_limit.is_some_and(|limit| {
        !limit.is_finite() || !(0.0..=1_000.0).contains(&limit)
    }) {
        return Err("seed ratio limit must be between 0 and 1000".to_string());
    }
    Ok(options)
}

fn validate_rate_limit(limit: Option<u64>, direction: &str) -> Result<Option<u64>, String> {
    match limit {
        Some(0) | None => Ok(None),
        Some(limit) if limit < MIN_RATE_LIMIT => Err(format!(
            "{direction} rate limit must be at least 1 KiB/s, or 0 for unlimited"
        )),
        Some(limit) if limit > MAX_RATE_LIMIT => Err(format!(
            "{direction} rate limit cannot exceed 10 GiB/s"
        )),
        value => Ok(value),
    }
}

fn apply_runtime_options(
    torrent: &mut TorrentTask,
    options: &UpdateTorrentOptionsRequest,
) {
    torrent.options.max_connections = options.max_connections;
    torrent.options.max_download_speed = options.max_download_speed;
    torrent.options.max_upload_speed = options.max_upload_speed;
    torrent.options.sequential_download = options.sequential_download;
    torrent.options.seed_ratio_limit = options.seed_ratio_limit;
    torrent
        .upload_gate
        .set_limit(upload_slot_limit(options.max_connections));
    torrent
        .download_limiter
        .set_limit(options.max_download_speed);
    torrent.upload_limiter.set_limit(options.max_upload_speed);
}

fn seed_ratio_reached(torrent: &TorrentTask) -> bool {
    torrent
        .options
        .seed_ratio_limit
        .is_some_and(|limit| torrent.general.ratio >= limit)
}

fn completed_torrent_state(torrent: &TorrentTask) -> TorrentState {
    if torrent.options.paused {
        TorrentState::Paused
    } else if seed_ratio_reached(torrent) {
        TorrentState::SeedRatioReached
    } else {
        TorrentState::Seeding
    }
}

fn update_completed_torrent_state(torrent: &mut TorrentTask) {
    if torrent.stats.finished {
        torrent.stats.state = completed_torrent_state(torrent);
    }
}

fn read_verified_seed_block_from_files(
    output_folder: &Path,
    torrent_name: &str,
    files: &[TorrentFile],
    piece_length: u64,
    total_length: u64,
    piece_hashes: &[[u8; 20]],
    offset: u64,
    length: u64,
) -> Result<Vec<u8>, String> {
    let (piece_index, piece_start, piece_end, relative_start) =
        seed_piece_bounds(piece_length, total_length, piece_hashes, offset, length)?;
    let piece = storage::read_torrent_range(
        output_folder,
        torrent_name,
        files,
        piece_start,
        piece_end - piece_start,
    )?;
    verified_seed_block(piece_index, piece_hashes, piece, relative_start, length)
}

fn read_verified_seed_block_from_partial_store(
    output_folder: &Path,
    key: &str,
    piece_length: u64,
    total_length: u64,
    piece_hashes: &[[u8; 20]],
    offset: u64,
    length: u64,
) -> Result<Vec<u8>, String> {
    let (piece_index, piece_start, piece_end, relative_start) =
        seed_piece_bounds(piece_length, total_length, piece_hashes, offset, length)?;
    let mut store = storage::PartialPieceStore::open_existing(
        output_folder,
        key,
        total_length,
        piece_length,
        piece_hashes,
    )?
    .ok_or_else(|| "complete partial store is unavailable for unchecked files".to_string())?;
    let piece = store.read_verified_range(piece_start, piece_end - piece_start)?;
    verified_seed_block(piece_index, piece_hashes, piece, relative_start, length)
}

fn seed_piece_bounds(
    piece_length: u64,
    total_length: u64,
    piece_hashes: &[[u8; 20]],
    offset: u64,
    length: u64,
) -> Result<(usize, u64, u64, u64), String> {
    if piece_length == 0 {
        return Err("seed piece length cannot be zero".to_string());
    }
    if length == 0 {
        return Err("seed block length cannot be zero".to_string());
    }
    let end = offset
        .checked_add(length)
        .ok_or_else(|| "seed block range overflow".to_string())?;
    if end > total_length {
        return Err("seed block range exceeds torrent length".to_string());
    }
    let piece_index = offset / piece_length;
    let piece_start = piece_index
        .checked_mul(piece_length)
        .ok_or_else(|| "seed piece offset overflow".to_string())?;
    let piece_end = piece_start
        .checked_add(piece_length)
        .unwrap_or(total_length)
        .min(total_length);
    if end > piece_end {
        return Err("seed block crosses a piece boundary".to_string());
    }
    let piece_index = usize::try_from(piece_index)
        .map_err(|_| "seed piece index is too large for this platform".to_string())?;
    if piece_index >= piece_hashes.len() {
        return Err(format!("seed piece index is out of range: {piece_index}"));
    }
    Ok((piece_index, piece_start, piece_end, offset - piece_start))
}

fn verified_seed_block(
    piece_index: usize,
    piece_hashes: &[[u8; 20]],
    piece: Vec<u8>,
    relative_start: u64,
    length: u64,
) -> Result<Vec<u8>, String> {
    let expected_hash = piece_hashes
        .get(piece_index)
        .ok_or_else(|| format!("seed piece index is out of range: {piece_index}"))?;
    if sha1::digest(&piece) != *expected_hash {
        return Err(format!("stored seed piece {piece_index} failed SHA-1 verification"));
    }
    let start = usize::try_from(relative_start)
        .map_err(|_| "seed block offset is too large for this platform".to_string())?;
    let length = usize::try_from(length)
        .map_err(|_| "seed block length is too large for this platform".to_string())?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| "seed block range overflow".to_string())?;
    piece
        .get(start..end)
        .map(|block| block.to_vec())
        .ok_or_else(|| "seed block range exceeds verified piece".to_string())
}

fn format_optional_rate(limit: Option<u64>) -> String {
    let Some(limit) = limit else {
        return "unlimited".to_string();
    };
    if limit >= 1024 * 1024 {
        format!("{:.1} MiB/s", limit as f64 / (1024.0 * 1024.0))
    } else if limit >= 1024 {
        format!("{:.1} KiB/s", limit as f64 / 1024.0)
    } else {
        format!("{limit} B/s")
    }
}

fn normalized_connection_limit(configured: Option<u32>) -> usize {
    match configured {
        Some(limit @ 1..) => (limit as usize).min(MAX_CONNECTION_LIMIT),
        Some(0) | None => DEFAULT_CONNECTION_LIMIT,
    }
}

fn upload_slot_limit(configured: Option<u32>) -> usize {
    normalized_connection_limit(configured).min(MAX_UPLOAD_SLOTS)
}

fn dht_token_bucket() -> u64 {
    (timestamp_ms() / (5 * 60 * 1_000)) as u64
}

fn dht_contact_endpoint(contact: &DhtContact) -> Result<String, String> {
    let ip = contact
        .address
        .parse()
        .map_err(|err| format!("DHT contact is not an IP address: {err}"))?;
    Ok(SocketAddr::new(ip, contact.port).to_string())
}

fn dht_maintenance_transaction(tag: u8, index: usize) -> [u8; 2] {
    [tag, (timestamp_ms() as u8).wrapping_add(index as u8)]
}

fn load_dht_state(path: &Path) -> Result<Option<PersistedDhtState>, String> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = fs::read(path).map_err(|err| format!("could not read DHT state: {err}"))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|err| format!("DHT state is not valid JSON: {err}"))
}

fn session_node_id(default_output_dir: &Path) -> [u8; 20] {
    let seed = format!(
        "NovaTorrent DHT node:{}:{}",
        default_output_dir.to_string_lossy(),
        timestamp_ms()
    );
    sha1::digest(seed.as_bytes())
}

fn session_token_secret(default_output_dir: &Path) -> [u8; 20] {
    let mut source = default_output_dir.to_string_lossy().as_bytes().to_vec();
    source.extend_from_slice(&timestamp_ms().to_be_bytes());
    source.extend_from_slice(&std::process::id().to_be_bytes());
    sha1::digest(&source)
}

fn log_level_label(level: &LogLevel) -> &'static str {
    match level {
        LogLevel::Debug => "Debug",
        LogLevel::Info => "Info",
        LogLevel::Warn => "Warn",
        LogLevel::Error => "Error",
    }
}

fn single_line(value: &str) -> String {
    value.replace(['\r', '\n'], " ")
}

fn tracker_http_event(event: UdpAnnounceEvent) -> Option<&'static str> {
    match event {
        UdpAnnounceEvent::None => None,
        UdpAnnounceEvent::Completed => Some("completed"),
        UdpAnnounceEvent::Started => Some("started"),
        UdpAnnounceEvent::Stopped => Some("stopped"),
    }
}

fn tracker_event_label(event: UdpAnnounceEvent) -> &'static str {
    tracker_http_event(event).unwrap_or("regular")
}

fn update_tracker_status(status: &mut TrackerStatus, response: &TrackerAnnounceResponse) {
    status.state = "Announced".to_string();
    status.seeders = response.seeders;
    status.leechers = response.leechers;
    status.next_announce_seconds = Some(response.interval_seconds);
    status.message = response.warning.clone();
}

struct PeerSchedule {
    peers: Vec<PeerInfo>,
    candidate_peers: usize,
    deferred_for_backoff: usize,
    deferred_for_duplicate_ip: usize,
}

#[derive(Debug, Default)]
struct DiscoveryOutcome {
    tracker_error: Option<String>,
    dht_error: Option<String>,
    dht_ran: bool,
}

impl DiscoveryOutcome {
    fn errors(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if let Some(err) = self.tracker_error.as_ref() {
            errors.push(format!("tracker discovery: {err}"));
        }
        if let Some(err) = self.dht_error.as_ref() {
            errors.push(format!("DHT discovery: {err}"));
        }
        errors
    }
}

fn schedule_peers_for_download(torrent: &TorrentTask, max_connections: usize) -> PeerSchedule {
    let now = timestamp_ms();
    let mut ready = Vec::new();
    let mut backed_off = Vec::new();
    for peer in torrent.peers.iter().cloned() {
        let health = torrent.peer_health.get(&peer_key(&peer.address, peer.port));
        if peer_is_in_backoff(health, now) {
            backed_off.push(peer);
        } else {
            ready.push(peer);
        }
    }
    ready.sort_by(|left, right| {
        let left_health = torrent.peer_health.get(&peer_key(&left.address, left.port));
        let right_health = torrent.peer_health.get(&peer_key(&right.address, right.port));
        peer_success_score(right_health)
            .cmp(&peer_success_score(left_health))
            .then_with(|| peer_piece_score(right_health).cmp(&peer_piece_score(left_health)))
            .then_with(|| peer_byte_score(right_health).cmp(&peer_byte_score(left_health)))
            .then_with(|| peer_failure_count(left_health).cmp(&peer_failure_count(right_health)))
            .then_with(|| peer_unavailable_count(left_health).cmp(&peer_unavailable_count(right_health)))
            .then_with(|| peer_attempt_count(left_health).cmp(&peer_attempt_count(right_health)))
            .then_with(|| left.address.cmp(&right.address))
            .then_with(|| left.port.cmp(&right.port))
    });
    backed_off.sort_by(|left, right| {
        let left_until = peer_backoff_until_ms(
            torrent.peer_health.get(&peer_key(&left.address, left.port)),
        )
        .unwrap_or_default();
        let right_until = peer_backoff_until_ms(
            torrent.peer_health.get(&peer_key(&right.address, right.port)),
        )
        .unwrap_or_default();
        left_until
            .cmp(&right_until)
            .then_with(|| left.address.cmp(&right.address))
            .then_with(|| left.port.cmp(&right.port))
    });
    let (ready_unique, ready_duplicates) = split_duplicate_ip_peers(ready);
    let (backed_off_unique, backed_off_duplicates) = split_duplicate_ip_peers(backed_off);
    let backed_off_count = backed_off_unique.len() + backed_off_duplicates.len();
    let duplicate_count = ready_duplicates.len() + backed_off_duplicates.len();
    let mut selected = Vec::new();
    push_peer_candidates(&mut selected, ready_unique, max_connections);
    push_peer_candidates(&mut selected, ready_duplicates, max_connections);
    push_peer_candidates(&mut selected, backed_off_unique, max_connections);
    push_peer_candidates(&mut selected, backed_off_duplicates, max_connections);
    let selected_backed_off = selected
        .iter()
        .filter(|peer| {
            peer_is_in_backoff(
                torrent.peer_health.get(&peer_key(&peer.address, peer.port)),
                now,
            )
        })
        .count();
    let mut selected_addresses = HashSet::new();
    let selected_duplicates = selected
        .iter()
        .filter(|peer| !selected_addresses.insert(peer.address.clone()))
        .count();
    PeerSchedule {
        peers: selected,
        candidate_peers: torrent.peers.len(),
        deferred_for_backoff: backed_off_count.saturating_sub(selected_backed_off),
        deferred_for_duplicate_ip: duplicate_count.saturating_sub(selected_duplicates),
    }
}

fn split_duplicate_ip_peers(peers: Vec<PeerInfo>) -> (Vec<PeerInfo>, Vec<PeerInfo>) {
    let mut seen = HashSet::new();
    let mut unique = Vec::new();
    let mut duplicates = Vec::new();
    for peer in peers {
        if seen.insert(peer.address.clone()) {
            unique.push(peer);
        } else {
            duplicates.push(peer);
        }
    }
    (unique, duplicates)
}

fn push_peer_candidates(target: &mut Vec<PeerInfo>, peers: Vec<PeerInfo>, max_connections: usize) {
    let remaining = max_connections.saturating_sub(target.len());
    if remaining > 0 {
        target.extend(peers.into_iter().take(remaining));
    }
}

fn recoverable_runtime_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    [
        "no peers",
        "no discovered peers",
        "all known peer endpoints",
        "swarm is missing",
        "all discovered peers failed",
        "all tracker announces failed",
        "tracker discovery:",
        "dht discovery:",
        "could not resolve magnet metadata",
        "no peers or supported webseeds are available",
    ]
    .iter()
    .any(|needle| error.contains(needle))
}

fn peer_key(address: &str, port: u16) -> String {
    format!("{address}|{port}")
}

fn peer_health_entry<'a>(
    torrent: &'a mut TorrentTask,
    address: &str,
    port: u16,
) -> &'a mut PeerHealth {
    torrent
        .peer_health
        .entry(peer_key(address, port))
        .or_default()
}

fn peer_backoff_duration_ms(consecutive_failures: u32) -> u128 {
    match consecutive_failures {
        0 => 0,
        1 => 10_000,
        2 => 30_000,
        3 => 120_000,
        _ => 300_000,
    }
}

fn peer_backoff_until_ms(health: Option<&PeerHealth>) -> Option<u128> {
    let health = health?;
    let last_failure = health.last_failure_ms?;
    Some(last_failure.saturating_add(peer_backoff_duration_ms(
        health.consecutive_failures,
    )))
}

fn peer_is_in_backoff(health: Option<&PeerHealth>, now: u128) -> bool {
    peer_backoff_until_ms(health).is_some_and(|until| until > now)
}

fn peer_success_score(health: Option<&PeerHealth>) -> u128 {
    health.and_then(|health| health.last_success_ms).unwrap_or_default()
}

fn peer_piece_score(health: Option<&PeerHealth>) -> u64 {
    health.map(|health| health.pieces_downloaded).unwrap_or_default()
}

fn peer_byte_score(health: Option<&PeerHealth>) -> u64 {
    health.map(|health| health.bytes_downloaded).unwrap_or_default()
}

fn peer_rate_score(health: Option<&PeerHealth>) -> u64 {
    health
        .map(|health| health.recent_bytes_per_second)
        .unwrap_or_default()
}

fn peer_failure_count(health: Option<&PeerHealth>) -> u32 {
    health
        .map(|health| health.consecutive_failures)
        .unwrap_or_default()
}

fn peer_unavailable_count(health: Option<&PeerHealth>) -> u64 {
    health.map(|health| health.unavailable_pieces).unwrap_or_default()
}

fn peer_attempt_count(health: Option<&PeerHealth>) -> u32 {
    health
        .map(|health| health.connection_attempts)
        .unwrap_or_default()
}

fn transfer_rate_bytes_per_second(bytes: u64, elapsed: Duration) -> u64 {
    if bytes == 0 {
        return 0;
    }
    let millis = elapsed.as_millis().max(1);
    let rate = u128::from(bytes).saturating_mul(1000) / millis;
    rate.min(u128::from(u64::MAX)) as u64
}

fn smooth_transfer_rate(previous: u64, current: u64) -> u64 {
    if previous == 0 {
        return current;
    }
    ((u128::from(previous) * 3) + u128::from(current))
        .saturating_div(4)
        .min(u128::from(u64::MAX)) as u64
}

fn merge_peers(existing: &mut Vec<PeerInfo>, next: Vec<PeerInfo>) {
    for peer in next {
        let seen = existing
            .iter()
            .any(|current| current.address == peer.address && current.port == peer.port);
        if !seen {
            existing.push(peer);
        }
    }
}

fn pex_seed_candidates(peers: &[PeerInfo], remote: Option<SocketAddr>) -> Vec<PeerInfo> {
    let mut seen_addresses = HashSet::new();
    let remote_ip = remote.map(|remote| remote.ip());
    let mut out = Vec::new();
    for peer in peers {
        if out.len() >= 50 || peer.port == 0 {
            break;
        }
        let Ok(address) = peer.address.parse::<std::net::IpAddr>() else {
            continue;
        };
        if !address.is_ipv4()
            || remote_ip == Some(address)
            || address.is_unspecified()
            || address.is_loopback()
            || address.is_multicast()
            || matches!(address, std::net::IpAddr::V4(address) if address.is_broadcast() || address.is_private())
        {
            continue;
        }
        if !seen_addresses.insert(peer.address.clone()) {
            continue;
        }
        out.push(peer.clone());
    }
    out
}

fn merge_dht_announce_targets(
    existing: &mut Vec<DhtAnnounceTarget>,
    next: Vec<DhtAnnounceTarget>,
) {
    for target in next {
        if let Some(current) = existing
            .iter_mut()
            .find(|current| current.contact == target.contact)
        {
            current.token = target.token;
        } else {
            existing.push(target);
        }
    }
}

fn verified_piece_bytes(
    total_length: u64,
    piece_length: u64,
    verified: &[bool],
) -> Result<u64, String> {
    if piece_length == 0 {
        return Err("piece length cannot be zero".to_string());
    }
    verified.iter().enumerate().try_fold(0u64, |total, (index, complete)| {
        if !complete {
            return Ok(total);
        }
        let start = (index as u64)
            .checked_mul(piece_length)
            .ok_or_else(|| "piece offset overflow".to_string())?;
        let length = total_length.saturating_sub(start).min(piece_length);
        total
            .checked_add(length)
            .ok_or_else(|| "verified byte count overflow".to_string())
    })
}

fn peer_id_for(id: u64) -> [u8; 20] {
    let mut peer_id = *b"-NV0001-000000000000";
    let suffix = format!("{:012x}", id & 0x0000_ffff_ffff_ffff);
    peer_id[8..20].copy_from_slice(suffix.as_bytes());
    peer_id
}

fn tracker_transaction_id(id: u64, index: usize) -> u32 {
    let mixed = id
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(index as u64);
    (mixed ^ (mixed >> 32)) as u32
}

fn dht_announce_transaction_id(id: u64, index: usize) -> [u8; 4] {
    let mixed = id
        .wrapping_mul(0x9E37_79B9)
        .wrapping_add(index as u64)
        .to_be_bytes();
    [mixed[4], mixed[5], mixed[6], mixed[7]]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        net::{TcpListener, UdpSocket},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use std::{
        io::{Read, Write},
        net::TcpStream,
    };

    use crate::torrent::{
        bencode::{self, BencodeNode},
        dht,
        metadata::{self, MetadataMessageType},
        peer::{self, PeerMessage, HANDSHAKE_LEN},
        peerwire::{PeerDownloadPlan, PeerSeedPlan},
        pex::PexPeer,
        sha1,
    };

    #[test]
    fn rarest_first_assignment_is_unique_and_balanced() {
        let assignments = assign_rarest_pieces(
            &[false, false, false, true],
            &[
                vec![true, true, false, false],
                vec![false, true, true, true],
                vec![false, true, false, true],
            ],
        )
        .expect("assignments build");

        assert_eq!(assignments, vec![vec![0], vec![2], vec![1]]);
    }

    #[test]
    fn piece_assignment_rounds_are_bounded_per_peer() {
        let mut assignments = vec![vec![0, 3, 6, 9, 12], vec![1, 4], vec![2, 5, 8, 11]];
        limit_piece_assignments(&mut assignments, 3);
        assert_eq!(assignments, vec![vec![0, 3, 6], vec![1, 4], vec![2, 5, 8]]);
    }

    #[test]
    fn swarm_coverage_summary_counts_missing_piece_availability() {
        let summary = swarm_coverage_summary(
            &[true, false, false, false],
            &[
                vec![false, true, false, false],
                vec![false, true, true, false],
            ],
        );

        assert_eq!(
            summary,
            SwarmCoverageSummary {
                missing_pieces: 3,
                coverable_pieces: 2,
                unavailable_pieces: 1,
                single_peer_pieces: 1,
                connected_peers: 2,
            }
        );
        assert_eq!(
            format_swarm_coverage(summary),
            "3 missing piece(s), 2 coverable by 2 connected peer(s), 1 unavailable, 1 only on one peer"
        );
    }

    #[test]
    fn peer_transfer_rate_is_measured_and_smoothed() {
        assert_eq!(
            transfer_rate_bytes_per_second(32 * 1024, Duration::from_millis(500)),
            64 * 1024
        );
        assert_eq!(smooth_transfer_rate(0, 80), 80);
        assert_eq!(smooth_transfer_rate(40, 80), 50);
    }

    #[test]
    fn stream_priority_assignment_targets_selected_file_and_seek_window() {
        let files = vec![
            TorrentFile {
                name: "extras/trailer.mp4".to_string(),
                components: vec!["extras".to_string(), "trailer.mp4".to_string()],
                length: 4,
                included: true,
            },
            TorrentFile {
                name: "Season 1/Episode 03.mp4".to_string(),
                components: vec!["Season 1".to_string(), "Episode 03.mp4".to_string()],
                length: 16,
                included: true,
            },
        ];
        let priority = StreamPriorityState {
            file_index: 1,
            playhead_offset: 8,
            urgent_bytes: 4,
            lookahead_bytes: 4,
            updated_at_ms: 1,
        };

        let assignments = assign_streaming_pieces(
            &[false, false, false, false, false],
            &[vec![true, true, true, true, true]],
            &files,
            4,
            &priority,
            &[],
            false,
        )
        .expect("stream assignments build");

        assert_eq!(&assignments[0][..3], &[3, 1, 4]);

        let assignments = assign_streaming_pieces(
            &[false, false, false, true, false],
            &[vec![true, true, true, true, true]],
            &files,
            4,
            &priority,
            &[],
            false,
        )
        .expect("stream assignments skip verified pieces");

        assert_eq!(assignments, vec![vec![1, 4]]);

        let assignments = assign_streaming_pieces(
            &[false, true, false, true, true],
            &[vec![true, true, true, true, true]],
            &files,
            4,
            &priority,
            &[],
            false,
        )
        .expect("stream assignments resume normal pieces after priority is ready");

        assert_eq!(assignments, vec![vec![0, 2]]);

        let assignments = assign_streaming_pieces(
            &[false, false, false, false, false],
            &[
                vec![true, true, true, true, true],
                vec![true, true, true, true, true],
            ],
            &files,
            4,
            &priority,
            &[
                PeerSchedulingHint::default(),
                PeerSchedulingHint {
                    success_score: 100,
                    piece_score: 8,
                    byte_score: 32,
                    rate_score: 64,
                    unavailable_score: 0,
                    failure_count: 0,
                    attempt_count: 1,
                },
            ],
            false,
        )
        .expect("stream assignments prefer proven peers");

        assert_eq!(assignments, vec![vec![], vec![3, 1, 4]]);

        let assignments = assign_streaming_pieces(
            &[false, false, false, false, false],
            &[
                vec![true, true, true, true, true],
                vec![true, true, true, true, true],
            ],
            &files,
            4,
            &priority,
            &[
                PeerSchedulingHint {
                    success_score: 100,
                    piece_score: 8,
                    byte_score: 32,
                    rate_score: 32,
                    unavailable_score: 0,
                    failure_count: 0,
                    attempt_count: 1,
                },
                PeerSchedulingHint {
                    success_score: 100,
                    piece_score: 8,
                    byte_score: 32,
                    rate_score: 256,
                    unavailable_score: 0,
                    failure_count: 0,
                    attempt_count: 1,
                },
            ],
            false,
        )
        .expect("stream assignments prefer faster proven peers");

        assert_eq!(assignments[1][0], 3);

        let assignments = assign_streaming_pieces(
            &[false, false, false, false, false],
            &[
                vec![true, true, true, true, true],
                vec![true, true, true, true, true],
            ],
            &files,
            4,
            &priority,
            &[
                PeerSchedulingHint {
                    success_score: 100,
                    piece_score: 8,
                    byte_score: 32,
                    rate_score: 128,
                    unavailable_score: 5,
                    failure_count: 0,
                    attempt_count: 1,
                },
                PeerSchedulingHint {
                    success_score: 100,
                    piece_score: 8,
                    byte_score: 32,
                    rate_score: 128,
                    unavailable_score: 0,
                    failure_count: 0,
                    attempt_count: 1,
                },
            ],
            false,
        )
        .expect("stream assignments avoid stale availability peers");

        assert_eq!(assignments[1][0], 3);
    }

    #[test]
    fn peer_scheduler_prioritizes_success_and_defers_risky_candidates() {
        let root = temp_dir("session-peer-scheduler");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        fs::write(&torrent_path, build_multi_file_torrent(b"abcdefghi")).expect("fixture writes");
        let session = TorrentSession::new(root.join("default"));
        let id = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: true,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds")
            .id
            .expect("id assigned");
        let now = timestamp_ms();
        let peer = |address: &str, port| PeerInfo {
            address: address.to_string(),
            port,
            client: None,
            progress: 0.0,
            download_speed: 0,
            upload_speed: 0,
            connection: "Discovered".to_string(),
        };
        let torrents = &mut session.torrents.lock().expect("torrent lock");
        let torrent = torrents
            .iter_mut()
            .find(|torrent| torrent.id == id)
            .expect("torrent exists");
        torrent.peers = vec![
            peer("127.0.0.1", 6001),
            peer("127.0.0.2", 6002),
            peer("127.0.0.3", 6003),
            peer("127.0.0.3", 6004),
        ];
        torrent.peer_health.insert(
            peer_key("127.0.0.1", 6001),
            PeerHealth {
                connection_attempts: 3,
                consecutive_failures: 2,
                last_failure_ms: Some(now),
                last_success_ms: None,
                bytes_downloaded: 0,
                pieces_downloaded: 0,
                recent_bytes_per_second: 0,
                unavailable_pieces: 0,
            },
        );
        torrent.peer_health.insert(
            peer_key("127.0.0.3", 6003),
            PeerHealth {
                connection_attempts: 1,
                consecutive_failures: 0,
                last_failure_ms: None,
                last_success_ms: Some(now.saturating_sub(1_000)),
                bytes_downloaded: 32_768,
                pieces_downloaded: 2,
                recent_bytes_per_second: 16_384,
                unavailable_pieces: 0,
            },
        );

        let schedule = schedule_peers_for_download(torrent, 2);

        assert_eq!(schedule.candidate_peers, 4);
        assert_eq!(schedule.deferred_for_backoff, 1);
        assert_eq!(schedule.deferred_for_duplicate_ip, 1);
        assert_eq!(
            schedule
                .peers
                .iter()
                .map(|peer| peer.port)
                .collect::<Vec<_>>(),
            vec![6003, 6002]
        );
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn endgame_race_keeps_first_verified_piece_and_cancels_slow_pipeline() {
        let block_size = crate::torrent::piece::DEFAULT_BLOCK_SIZE as usize;
        let data = vec![17u8; block_size * 3];
        let info_hash = [31u8; 20];
        let fast_listener = TcpListener::bind("127.0.0.1:0").expect("fast peer binds");
        let fast_port = fast_listener.local_addr().expect("fast peer address").port();
        let slow_listener = TcpListener::bind("127.0.0.1:0").expect("slow peer binds");
        let slow_port = slow_listener.local_addr().expect("slow peer address").port();

        let fast_data = data.clone();
        let fast_seed = thread::spawn(move || {
            peerwire::seed_single_peer(
                fast_listener,
                PeerSeedPlan {
                    info_hash,
                    peer_id: [21u8; 20],
                    dht_port: None,
                    enable_pex: false,
                    pex_peers: Vec::new(),
                    piece_length: fast_data.len() as u64,
                    bytes: fast_data,
                    available_pieces: None,
                    disconnect_after_blocks: None,
                    block_response_delay: None,
                },
            )
            .expect("fast peer serves final piece")
        });
        let slow_seed = thread::spawn(move || {
            let (mut socket, _) = slow_listener.accept().expect("slow peer accepts");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("slow peer read timeout sets");
            let mut handshake = [0u8; HANDSHAKE_LEN];
            socket.read_exact(&mut handshake).expect("slow peer reads handshake");
            socket
                .write_all(&peer::build_handshake(info_hash, [22u8; 20]))
                .expect("slow peer writes handshake");
            assert!(matches!(
                peerwire::read_peer_message(&mut socket).expect("slow peer reads interest"),
                PeerMessage::Interested
            ));
            socket
                .write_all(&peer::build_bitfield(&[0b1000_0000]))
                .expect("slow peer writes bitfield");
            socket
                .write_all(&peer::build_unchoke())
                .expect("slow peer unchokes");
            for _ in 0..3 {
                assert!(matches!(
                    peerwire::read_peer_message(&mut socket).expect("slow peer reads request"),
                    PeerMessage::Request { .. }
                ));
            }
            let mut cancelled_offsets = Vec::new();
            for _ in 0..3 {
                let PeerMessage::Cancel { begin, .. } =
                    peerwire::read_peer_message(&mut socket).expect("slow peer reads cancel")
                else {
                    panic!("expected endgame cancel");
                };
                cancelled_offsets.push(begin);
            }
            cancelled_offsets
        });

        let plan = PeerDownloadPlan {
            info_hash,
            peer_id: [23u8; 20],
            dht_port: None,
            enable_pex: false,
            total_length: data.len() as u64,
            piece_length: data.len() as u64,
            piece_hashes: vec![sha1::digest(&data)],
            cancelled: None,
        };
        let fast_connection = peerwire::connect_peer_for_download(
            "127.0.0.1",
            fast_port,
            plan.clone(),
        )
        .expect("fast endgame peer connects");
        let slow_connection = peerwire::connect_peer_for_download(
            "127.0.0.1",
            slow_port,
            plan,
        )
        .expect("slow endgame peer connects");
        let result = race_endgame_piece(
            vec![
                ConnectedPeer {
                    peer: PeerInfo {
                        address: "127.0.0.1".to_string(),
                        port: fast_port,
                        client: Some("fast endgame peer".to_string()),
                        progress: 1.0,
                        download_speed: 0,
                        upload_speed: 0,
                        connection: "Ready".to_string(),
                    },
                    connection: fast_connection,
                },
                ConnectedPeer {
                    peer: PeerInfo {
                        address: "127.0.0.1".to_string(),
                        port: slow_port,
                        client: Some("slow endgame peer".to_string()),
                        progress: 1.0,
                        download_speed: 0,
                        upload_speed: 0,
                        connection: "Ready".to_string(),
                    },
                    connection: slow_connection,
                },
            ],
            0,
            &AtomicBool::new(false),
        );

        let (winner, piece) = result.winner.as_ref().expect("endgame race has a winner");
        assert_eq!(winner.port, fast_port);
        assert_eq!(piece.bytes, data);
        assert!(result.errors.is_empty());
        assert!(result.worker_panics.is_empty());
        assert!(result.cancelled.iter().any(|peer| peer.port == slow_port));
        assert_eq!(result.reusable.len(), 2);
        drop(result.reusable);
        assert_eq!(fast_seed.join().expect("fast peer exits").bytes_uploaded, data.len() as u64);
        assert_eq!(
            slow_seed.join().expect("slow peer exits"),
            vec![
                0,
                crate::torrent::piece::DEFAULT_BLOCK_SIZE,
                crate::torrent::piece::DEFAULT_BLOCK_SIZE * 2,
            ]
        );
    }

    #[test]
    fn peer_download_is_parallel_and_reassigns_failed_work() {
        let root = temp_dir("session-peer-parallel");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        let data = b"abcdefghi".to_vec();
        fs::write(&torrent_path, build_multi_file_torrent(&data)).expect("fixture writes");

        let session = TorrentSession::new(output_dir.clone());
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID");
        let info_hash = session
            .torrents
            .lock()
            .expect("torrent lock")
            .iter()
            .find(|torrent| torrent.id == id)
            .expect("torrent exists")
            .info_hash;

        let first_listener = TcpListener::bind("127.0.0.1:0").expect("first peer binds");
        let first_port = first_listener.local_addr().expect("first peer address").port();
        let second_listener = TcpListener::bind("127.0.0.1:0").expect("second peer binds");
        let second_port = second_listener.local_addr().expect("second peer address").port();
        {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            for (port, client) in [
                (first_port, "NovaTorrent parallel seed A"),
                (second_port, "NovaTorrent parallel seed B"),
            ] {
                torrent.peers.push(PeerInfo {
                    address: "127.0.0.1".to_string(),
                    port,
                    client: Some(client.to_string()),
                    progress: 0.0,
                    download_speed: 0,
                    upload_speed: 0,
                    connection: "Discovered".to_string(),
                });
            }
        }

        let first_data = data.clone();
        let first_seed = thread::spawn(move || {
            peerwire::seed_single_peer(
                first_listener,
                PeerSeedPlan {
                    info_hash,
                    peer_id: *b"-NV0001-PARALLEL0001",
                    dht_port: None,
                    enable_pex: false,
                    pex_peers: Vec::new(),
                    piece_length: 4,
                    bytes: first_data,
                    available_pieces: Some(vec![true, false, true]),
                    disconnect_after_blocks: Some(1),
                    block_response_delay: Some(Duration::from_millis(800)),
                },
            )
            .expect("first peer serves its pieces")
        });
        let second_data = data.clone();
        let second_seed = thread::spawn(move || {
            peerwire::seed_single_peer(
                second_listener,
                PeerSeedPlan {
                    info_hash,
                    peer_id: *b"-NV0001-PARALLEL0002",
                    dht_port: None,
                    enable_pex: false,
                    pex_peers: Vec::new(),
                    piece_length: 4,
                    bytes: second_data,
                    available_pieces: Some(vec![true, true, true]),
                    disconnect_after_blocks: None,
                    block_response_delay: Some(Duration::from_millis(800)),
                },
            )
            .expect("second peer serves its piece")
        });

        let started = std::time::Instant::now();
        session
            .download_from_peers(&id.to_string())
            .expect("parallel swarm completes");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(2_900),
            "parallel swarm took {elapsed:?}; fully sequential responses require at least 3.2 seconds"
        );
        assert_eq!(first_seed.join().expect("first peer exits").bytes_uploaded, 4);
        assert_eq!(second_seed.join().expect("second peer exits").bytes_uploaded, 5);
        assert_eq!(
            storage::read_torrent_bytes(
                &output_dir,
                "Example",
                &session
                    .torrents
                    .lock()
                    .expect("torrent lock")
                    .iter()
                    .find(|torrent| torrent.id == id)
                    .expect("torrent exists")
                    .files,
            )
            .expect("parallel output reads"),
            data
        );

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn peer_download_admits_late_connection_when_current_peers_stall() {
        let root = temp_dir("session-peer-late-connection");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        let data = b"abcdefghi".to_vec();
        fs::write(&torrent_path, build_multi_file_torrent(&data)).expect("fixture writes");

        let session = TorrentSession::new(output_dir.clone());
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID");
        let info_hash = session
            .torrents
            .lock()
            .expect("torrent lock")
            .iter()
            .find(|torrent| torrent.id == id)
            .expect("torrent exists")
            .info_hash;

        let early_listener = TcpListener::bind("127.0.0.1:0").expect("early peer binds");
        let early_port = early_listener.local_addr().expect("early peer address").port();
        let late_listener = TcpListener::bind("127.0.0.1:0").expect("late peer binds");
        let late_port = late_listener.local_addr().expect("late peer address").port();
        {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            for (port, client) in [
                (early_port, "NovaTorrent partial early seed"),
                (late_port, "NovaTorrent late complete seed"),
            ] {
                torrent.peers.push(PeerInfo {
                    address: "127.0.0.1".to_string(),
                    port,
                    client: Some(client.to_string()),
                    progress: 0.0,
                    download_speed: 0,
                    upload_speed: 0,
                    connection: "Discovered".to_string(),
                });
            }
        }

        let early_data = data.clone();
        let early_seed = thread::spawn(move || {
            peerwire::seed_single_peer(
                early_listener,
                PeerSeedPlan {
                    info_hash,
                    peer_id: *b"-NV0001-LATEPEER0001",
                    dht_port: None,
                    enable_pex: false,
                    pex_peers: Vec::new(),
                    piece_length: 4,
                    bytes: early_data,
                    available_pieces: Some(vec![true, false, false]),
                    disconnect_after_blocks: None,
                    block_response_delay: None,
                },
            )
            .expect("early peer serves its one piece")
        });
        let late_data = data.clone();
        let late_seed = thread::spawn(move || {
            let (mut stream, _) = late_listener.accept().expect("late peer accepts");
            thread::sleep(Duration::from_millis(1_200));
            let handshake = peerwire::read_incoming_handshake(&mut stream)
                .expect("late peer reads handshake");
            peerwire::seed_connected_peer(
                stream,
                handshake,
                PeerSeedPlan {
                    info_hash,
                    peer_id: *b"-NV0001-LATEPEER0002",
                    dht_port: None,
                    enable_pex: false,
                    pex_peers: Vec::new(),
                    piece_length: 4,
                    bytes: late_data,
                    available_pieces: Some(vec![true, true, true]),
                    disconnect_after_blocks: None,
                    block_response_delay: None,
                },
            )
            .expect("late peer serves remaining pieces")
        });

        session
            .download_from_peers(&id.to_string())
            .expect("swarm completes after admitting the late peer");
        assert_eq!(early_seed.join().expect("early peer exits").bytes_uploaded, 4);
        assert_eq!(late_seed.join().expect("late peer exits").bytes_uploaded, 5);
        assert_eq!(
            storage::read_torrent_bytes(
                &output_dir,
                "Example",
                &session
                    .torrents
                    .lock()
                    .expect("torrent lock")
                    .iter()
                    .find(|torrent| torrent.id == id)
                    .expect("torrent exists")
                    .files,
            )
            .expect("late peer output reads"),
            data
        );

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn peer_download_refreshes_newly_discovered_peers_after_snapshot_stalls() {
        let root = temp_dir("session-peer-refresh");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        let data = b"abcdefghi".to_vec();
        fs::write(&torrent_path, build_multi_file_torrent(&data)).expect("fixture writes");

        let session = TorrentSession::new(output_dir.clone());
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID");
        let info_hash = session
            .torrents
            .lock()
            .expect("torrent lock")
            .iter()
            .find(|torrent| torrent.id == id)
            .expect("torrent exists")
            .info_hash;

        let partial_listener = TcpListener::bind("127.0.0.1:0").expect("partial peer binds");
        let partial_port = partial_listener
            .local_addr()
            .expect("partial peer address")
            .port();
        let fresh_listener = TcpListener::bind("127.0.0.1:0").expect("fresh peer binds");
        let fresh_port = fresh_listener.local_addr().expect("fresh peer address").port();
        {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            torrent.peers.push(PeerInfo {
                address: "127.0.0.1".to_string(),
                port: partial_port,
                client: Some("NovaTorrent initial partial seed".to_string()),
                progress: 0.0,
                download_speed: 0,
                upload_speed: 0,
                connection: "Discovered".to_string(),
            });
        }

        let partial_data = data.clone();
        let partial_seed = thread::spawn(move || {
            peerwire::seed_single_peer(
                partial_listener,
                PeerSeedPlan {
                    info_hash,
                    peer_id: *b"-NV0001-REFRESH00001",
                    dht_port: None,
                    enable_pex: false,
                    pex_peers: Vec::new(),
                    piece_length: 4,
                    bytes: partial_data,
                    available_pieces: Some(vec![true, false, false]),
                    disconnect_after_blocks: None,
                    block_response_delay: Some(Duration::from_millis(300)),
                },
            )
            .expect("partial peer serves one piece")
        });
        let fresh_data = data.clone();
        let fresh_seed = thread::spawn(move || {
            peerwire::seed_single_peer(
                fresh_listener,
                PeerSeedPlan {
                    info_hash,
                    peer_id: *b"-NV0001-REFRESH00002",
                    dht_port: None,
                    enable_pex: false,
                    pex_peers: Vec::new(),
                    piece_length: 4,
                    bytes: fresh_data,
                    available_pieces: Some(vec![true, true, true]),
                    disconnect_after_blocks: None,
                    block_response_delay: None,
                },
            )
            .expect("fresh peer serves remaining pieces")
        });

        std::thread::scope(|scope| {
            let add_fresh_peer = scope.spawn(|| {
                thread::sleep(Duration::from_millis(75));
                let mut torrents = session.torrents.lock().expect("torrent lock");
                let torrent = torrents
                    .iter_mut()
                    .find(|torrent| torrent.id == id)
                    .expect("torrent exists");
                torrent.peers.push(PeerInfo {
                    address: "127.0.0.1".to_string(),
                    port: fresh_port,
                    client: Some("NovaTorrent fresh complete seed".to_string()),
                    progress: 0.0,
                    download_speed: 0,
                    upload_speed: 0,
                    connection: "DHT discovered".to_string(),
                });
            });
            session
                .download_from_peers(&id.to_string())
                .expect("swarm refresh discovers the fresh peer and completes");
            add_fresh_peer.join().expect("fresh peer is inserted");
        });

        assert_eq!(partial_seed.join().expect("partial peer exits").bytes_uploaded, 4);
        assert_eq!(fresh_seed.join().expect("fresh peer exits").bytes_uploaded, 5);
        assert_eq!(
            storage::read_torrent_bytes(
                &output_dir,
                "Example",
                &session
                    .torrents
                    .lock()
                    .expect("torrent lock")
                    .iter()
                    .find(|torrent| torrent.id == id)
                    .expect("torrent exists")
                    .files,
            )
            .expect("refreshed peer output reads"),
            data
        );

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn runtime_retry_policy_keeps_recoverable_public_torrents_alive() {
        let root = temp_dir("session-runtime-retry");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        fs::write(&torrent_path, build_multi_file_torrent(b"abcdefghi")).expect("fixture writes");

        let session = TorrentSession::new(output_dir.clone());
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID").to_string();

        assert_eq!(
            session.retry_delay_after_runtime_failure(
                &id,
                "peer download: swarm is missing 2 pieces",
                0
            ),
            Some(Duration::from_secs(5))
        );
        session
            .mark_runtime_retry_scheduled(
                &id,
                "peer download: swarm is missing 2 pieces",
                Duration::from_secs(5),
            )
            .expect("retry state marks");
        let stats = session
            .details(&id)
            .expect("details load")
            .stats
            .expect("stats exist");
        assert_eq!(stats.state, "Queued");
        assert!(stats
            .error
            .as_deref()
            .is_some_and(|error| error.contains("retrying in 5 seconds")));
        assert_eq!(
            session.retry_delay_after_runtime_failure(
                &id,
                "peer download: swarm is missing 2 pieces",
                8
            ),
            Some(MAX_RUNTIME_RETRY_DELAY)
        );

        session.pause(&id).expect("torrent pauses");
        assert_eq!(
            session.retry_delay_after_runtime_failure(
                &id,
                "peer download: swarm is missing 2 pieces",
                0
            ),
            None
        );
        assert_eq!(
            session.retry_delay_after_runtime_failure(&id, "existing files are corrupt", 0),
            None
        );

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn peer_download_records_pex_candidates_from_connected_peer() {
        let root = temp_dir("session-peer-pex");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        let data = b"abcdefghi".to_vec();
        fs::write(&torrent_path, build_multi_file_torrent(&data)).expect("fixture writes");

        let session = TorrentSession::new(output_dir.clone());
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID");
        let info_hash = session
            .torrents
            .lock()
            .expect("torrent lock")
            .iter()
            .find(|torrent| torrent.id == id)
            .expect("torrent exists")
            .info_hash;

        let listener = TcpListener::bind("127.0.0.1:0").expect("PEX seed binds");
        let port = listener.local_addr().expect("PEX seed address").port();
        {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            torrent.peers.push(PeerInfo {
                address: "127.0.0.1".to_string(),
                port,
                client: Some("NovaTorrent PEX seed".to_string()),
                progress: 0.0,
                download_speed: 0,
                upload_speed: 0,
                connection: "Discovered".to_string(),
            });
        }

        let server_data = data.clone();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("PEX client connects");
            let mut handshake = [0u8; HANDSHAKE_LEN];
            socket.read_exact(&mut handshake).expect("client handshake reads");
            assert!(peer::supports_extension_protocol(
                &peer::parse_handshake_full(&handshake).expect("client handshake parses")
            ));
            socket
                .write_all(&peer::build_extended_handshake(
                    info_hash,
                    *b"-NV0001-SESSIONPEX01",
                ))
                .expect("server extension handshake writes");
            let PeerMessage::Extended {
                extension_id: 0,
                payload,
            } = read_peer_message_for_test(&mut socket).expect("client extension handshake reads")
            else {
                panic!("expected extension handshake");
            };
            assert_eq!(
                metadata::parse_extension_handshake(&payload)
                    .expect("extension handshake parses")
                    .ut_pex,
                Some(4)
            );
            assert!(matches!(
                read_peer_message_for_test(&mut socket).expect("interested reads"),
                PeerMessage::Interested
            ));
            let payload = crate::torrent::pex::build_pex_message(
                &[PexPeer {
                    address: "203.0.113.7".to_string(),
                    port: 6881,
                    flags: 0x10,
                }],
                &[],
            )
            .expect("PEX payload builds");
            socket
                .write_all(&peer::build_extended_message(4, &payload))
                .expect("PEX message writes");
            socket
                .write_all(&peer::build_bitfield(&[0b1110_0000]))
                .expect("bitfield writes");
            socket.write_all(&peer::build_unchoke()).expect("unchoke writes");
            for _ in 0..3 {
                let PeerMessage::Request {
                    index,
                    begin,
                    length,
                } = read_peer_message_for_test(&mut socket).expect("request reads")
                else {
                    panic!("expected request");
                };
                let start = index as usize * 4 + begin as usize;
                let end = (start + length as usize).min(server_data.len());
                socket
                    .write_all(&peer::build_piece(index, begin, &server_data[start..end]))
                    .expect("piece writes");
            }
        });

        session
            .download_from_peers(&id.to_string())
            .expect("PEX seed download completes");
        server.join().expect("PEX seed exits");
        let details = session.details(&id.to_string()).expect("details load");
        assert!(details.peers.iter().any(|peer| {
            peer.address == "203.0.113.7"
                && peer.port == 6881
                && peer.connection == "PEX discovered"
        }));
        assert_eq!(fs::read(output_dir.join("Example").join("dir").join("one.bin")).expect("file reads"), b"abc");

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn peer_download_writes_selected_multi_file_outputs() {
        let root = temp_dir("session-peer-multi");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        let data = b"abcdefghi".to_vec();
        fs::write(&torrent_path, build_multi_file_torrent(&data)).expect("fixture writes");

        let session = TorrentSession::new(output_dir.clone());
        let add = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: Some(vec![0, 2]),
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = add.id.expect("added torrent has id");
        let info_hash = {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents.iter_mut().find(|torrent| torrent.id == id).expect("torrent exists");
            torrent.info_hash
        };

        let listener = TcpListener::bind("127.0.0.1:0").expect("local listener binds");
        let port = listener.local_addr().expect("listener address").port();
        {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents.iter_mut().find(|torrent| torrent.id == id).expect("torrent exists");
            torrent.peers.push(PeerInfo {
                address: "127.0.0.1".to_string(),
                port,
                client: Some("NovaTorrent local seed".to_string()),
                progress: 1.0,
                download_speed: 0,
                upload_speed: 0,
                connection: "Discovered".to_string(),
            });
        }

        let seed_data = data.clone();
        let seed = thread::spawn(move || {
            peerwire::seed_single_peer(
                listener,
                PeerSeedPlan {
                    info_hash,
                    peer_id: *b"-NV0001-SEEDER000001",
                    dht_port: None,
                    enable_pex: false,
                    pex_peers: Vec::new(),
                    piece_length: 4,
                    bytes: seed_data,
                    available_pieces: None,
                    disconnect_after_blocks: None,
                    block_response_delay: None,
                },
            )
            .expect("seed serves data")
        });

        session
            .download_from_peers(&id.to_string())
            .expect("peer download completes");
        let seed_result = seed.join().expect("seed exits");

        assert_eq!(seed_result.bytes_uploaded, data.len() as u64);
        assert_eq!(
            fs::read(output_dir.join("Example").join("dir").join("one.bin")).expect("first file reads"),
            b"abc"
        );
        assert!(!output_dir.join("Example").join("two.bin").exists());
        assert_eq!(
            fs::read(output_dir.join("Example").join("three.bin")).expect("third file reads"),
            b"hi"
        );
        let details = session.details(&id.to_string()).expect("details load");
        assert_eq!(details.stats.expect("stats").state, "Seeding");
        assert_eq!(
            details.peers[0].client.as_deref(),
            Some("NovaTorrent 0.0.0.1")
        );

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn peer_download_resumes_verified_pieces_after_session_restart() {
        let root = temp_dir("session-peer-swarm");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        let data = b"abcdefghi".to_vec();
        fs::write(&torrent_path, build_multi_file_torrent(&data)).expect("fixture writes");

        let session = TorrentSession::new(output_dir.clone());
        let add = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = add.id.expect("added torrent has id");
        let info_hash = {
            let torrents = session.torrents.lock().expect("torrent lock");
            torrents
                .iter()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists")
                .info_hash
        };

        let first_listener = TcpListener::bind("127.0.0.1:0").expect("first listener binds");
        let first_port = first_listener.local_addr().expect("first listener address").port();
        {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            torrent.peers.push(PeerInfo {
                address: "127.0.0.1".to_string(),
                port: first_port,
                client: Some("NovaTorrent interrupted seed".to_string()),
                progress: 1.0,
                download_speed: 0,
                upload_speed: 0,
                connection: "Discovered".to_string(),
            });
        }
        let first_data = data.clone();
        let first_seed = thread::spawn(move || {
            peerwire::seed_single_peer(
                first_listener,
                PeerSeedPlan {
                    info_hash,
                    peer_id: *b"-NV0001-PARTIAL00001",
                    dht_port: None,
                    enable_pex: false,
                    pex_peers: Vec::new(),
                    piece_length: 4,
                    bytes: first_data,
                    available_pieces: Some(vec![true, true, true]),
                    disconnect_after_blocks: Some(1),
                    block_response_delay: None,
                },
            )
            .expect("interrupted seed contributes one piece")
        });

        let first_error = session
            .download_from_peers(&id.to_string())
            .expect_err("first swarm attempt remains incomplete");
        assert!(first_error.contains("missing 2 pieces"));
        assert_eq!(first_seed.join().expect("first seed exits").bytes_uploaded, 4);
        let partial_path = output_dir
            .join(".novatorrent")
            .join(format!("{}.part", sha1::hex(&info_hash)));
        assert!(partial_path.exists());
        assert_eq!(
            session
                .details(&id.to_string())
                .expect("partial details load")
                .stats
                .expect("partial stats exist")
                .progress_bytes,
            4
        );

        drop(session);
        let session = TorrentSession::new(output_dir.clone());
        let resumed = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent re-adds after session restart");
        let id = resumed.id.expect("re-added torrent has id");

        let second_listener = TcpListener::bind("127.0.0.1:0").expect("second listener binds");
        let second_port = second_listener.local_addr().expect("second listener address").port();
        {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            torrent.peers.push(PeerInfo {
                address: "127.0.0.1".to_string(),
                port: second_port,
                client: Some("NovaTorrent resume seed".to_string()),
                progress: 0.67,
                download_speed: 0,
                upload_speed: 0,
                connection: "Discovered".to_string(),
            });
        }
        let second_data = data.clone();
        let second_seed = thread::spawn(move || {
            peerwire::seed_single_peer(
                second_listener,
                PeerSeedPlan {
                    info_hash,
                    peer_id: *b"-NV0001-PARTIAL00001",
                    dht_port: None,
                    enable_pex: false,
                    pex_peers: Vec::new(),
                    piece_length: 4,
                    bytes: second_data,
                    available_pieces: Some(vec![false, true, true]),
                    disconnect_after_blocks: None,
                    block_response_delay: None,
                },
            )
            .expect("resume seed contributes missing pieces")
        });

        session
            .download_from_peers(&id.to_string())
            .expect("resumed swarm download completes");
        assert_eq!(second_seed.join().expect("second seed exits").bytes_uploaded, 5);
        assert!(!partial_path.exists());
        assert_eq!(
            storage::read_torrent_bytes(
                &output_dir,
                "Example",
                &session
                    .torrents
                    .lock()
                    .expect("torrent lock")
                    .iter()
                    .find(|torrent| torrent.id == id)
                    .expect("torrent exists")
                    .files,
            )
            .expect("assembled swarm output reads"),
            data
        );
        let details = session.details(&id.to_string()).expect("details load");
        let stats = details.stats.expect("stats exist");
        assert_eq!(stats.state, "Seeding");
        assert_eq!(stats.progress_bytes, 9);
        assert!(details
            .peers
            .iter()
            .any(|peer| peer.port == second_port && peer.connection.starts_with("Swarm complete")));

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn background_runtime_downloads_multiple_torrents_concurrently() {
        let root = temp_dir("session-runtime-multi");
        let data = b"abcdefghi".to_vec();
        let session = Arc::new(TorrentSession::new(root.join("default")));
        let mut torrent_ids = Vec::new();
        let mut seed_handles = Vec::new();

        for index in 0..2 {
            let torrent_path = root.join(format!("multi-{index}.torrent"));
            let output_dir = root.join(format!("out-{index}"));
            fs::write(&torrent_path, build_multi_file_torrent(&data)).expect("fixture writes");
            let add = session
                .add(AddTorrentRequest {
                    source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                    destination: Some(output_dir.to_string_lossy().into_owned()),
                    paused: false,
                    overwrite: true,
                    disable_trackers: true,
                    only_files: None,
                    sub_folder: None,
                })
                .expect("torrent adds");
            let id = add.id.expect("added torrent has id");
            let info_hash = {
                let torrents = session.torrents.lock().expect("torrent lock");
                torrents
                    .iter()
                    .find(|torrent| torrent.id == id)
                    .expect("torrent exists")
                    .info_hash
            };
            let listener = TcpListener::bind("127.0.0.1:0").expect("local listener binds");
            let port = listener.local_addr().expect("listener address").port();
            {
                let mut torrents = session.torrents.lock().expect("torrent lock");
                let torrent = torrents
                    .iter_mut()
                    .find(|torrent| torrent.id == id)
                    .expect("torrent exists");
                torrent.peers.push(PeerInfo {
                    address: "127.0.0.1".to_string(),
                    port,
                    client: Some("NovaTorrent local seed".to_string()),
                    progress: 1.0,
                    download_speed: 0,
                    upload_speed: 0,
                    connection: "Discovered".to_string(),
                });
            }

            let seed_data = data.clone();
            seed_handles.push(thread::spawn(move || {
                peerwire::seed_single_peer(
                    listener,
                    PeerSeedPlan {
                        info_hash,
                        peer_id: *b"-NV0001-SEEDER000001",
                        dht_port: None,
                        enable_pex: false,
                        pex_peers: Vec::new(),
                        piece_length: 4,
                        bytes: seed_data,
                        available_pieces: None,
                        disconnect_after_blocks: None,
                        block_response_delay: None,
                    },
                )
                .expect("seed serves data")
            }));
            torrent_ids.push((id, output_dir));
        }

        let workers = torrent_ids
            .iter()
            .map(|(id, _)| {
                let session = Arc::clone(&session);
                let id = id.to_string();
                thread::spawn(move || session.run_torrent(&id).expect("runtime completes"))
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("runtime worker exits");
        }
        for seed in seed_handles {
            assert_eq!(seed.join().expect("seed exits").bytes_uploaded, data.len() as u64);
        }

        for (id, output_dir) in torrent_ids {
            assert_eq!(
                fs::read(output_dir.join("Example").join("dir").join("one.bin"))
                    .expect("first file reads"),
                b"abc"
            );
            let details = session.details(&id.to_string()).expect("details load");
            let stats = details.stats.expect("stats exist");
            assert_eq!(stats.state, "Seeding");
            assert!(stats.finished);
        }

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn restores_persisted_torrents_options_and_stable_ids() {
        let root = temp_dir("session-persistence");
        let default_dir = root.join("default");
        let output_dir = root.join("out");
        let torrent_path = root.join("multi.torrent");
        fs::write(&torrent_path, build_multi_file_torrent(b"abcdefghi")).expect("fixture writes");

        let first_id = {
            let session = TorrentSession::new(default_dir.clone());
            let added = session
                .add(AddTorrentRequest {
                    source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                    destination: Some(output_dir.to_string_lossy().into_owned()),
                    paused: true,
                    overwrite: true,
                    disable_trackers: true,
                    only_files: Some(vec![0, 2]),
                    sub_folder: Some("chosen".to_string()),
                })
                .expect("torrent adds");
            assert!(session.session_file_path().is_file());
            let id = added.id.expect("torrent has ID");
            session
                .update_options(
                    &id.to_string(),
                    UpdateTorrentOptionsRequest {
                        max_connections: Some(12),
                        max_download_speed: Some(256 * 1024),
                        max_upload_speed: Some(64 * 1024),
                        sequential_download: true,
                        seed_ratio_limit: Some(2.5),
                    },
                )
                .expect("runtime options update");
            id
        };

        let session = TorrentSession::new(default_dir.clone());
        let restored = session.list().torrents;
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].id, Some(first_id));
        assert_eq!(restored[0].output_folder, output_dir.to_string_lossy());
        assert!(restored[0].options.paused);
        assert!(restored[0].options.overwrite);
        assert!(restored[0].options.disable_trackers);
        assert_eq!(restored[0].options.sub_folder.as_deref(), Some("chosen"));
        assert_eq!(restored[0].options.max_connections, Some(12));
        assert_eq!(restored[0].options.max_download_speed, Some(256 * 1024));
        assert_eq!(restored[0].options.max_upload_speed, Some(64 * 1024));
        assert!(restored[0].options.sequential_download);
        assert_eq!(restored[0].options.seed_ratio_limit, Some(2.5));
        assert_eq!(
            restored[0]
                .files
                .as_ref()
                .expect("files restore")
                .iter()
                .map(|file| file.included)
                .collect::<Vec<_>>(),
            vec![true, false, true]
        );
        assert!(session.runnable_ids().is_empty());

        session.resume(&first_id.to_string()).expect("resume persists");
        assert_eq!(session.runnable_ids(), vec![first_id]);
        let second = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(root.join("out-2").to_string_lossy().into_owned()),
                paused: true,
                overwrite: false,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("second torrent adds");
        assert_eq!(second.id, Some(first_id + 1));
        session
            .delete(&first_id.to_string(), false)
            .expect("first torrent deletes");
        drop(session);

        let restored_again = TorrentSession::new(default_dir).list().torrents;
        assert_eq!(restored_again.len(), 1);
        assert_eq!(restored_again[0].id, second.id);
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn corrupt_session_manifest_is_logged_and_skipped() {
        let root = temp_dir("session-corrupt-manifest");
        fs::create_dir_all(&root).expect("session directory creates");
        fs::write(root.join("novatorrent-session.json"), b"{not valid json")
            .expect("corrupt manifest writes");

        let session = TorrentSession::new(root.clone());
        assert!(session.list().torrents.is_empty());
        assert!(session
            .logs(None)
            .iter()
            .any(|entry| entry.message.contains("not valid JSON")));
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn lsd_announce_adds_public_lan_peer_and_skips_private_torrents() {
        let root = temp_dir("session-lsd");
        let torrent_path = root.join("public.torrent");
        let output_dir = root.join("out");
        fs::write(&torrent_path, build_multi_file_torrent(b"abcdefghi")).expect("fixture writes");

        let session = TorrentSession::new(output_dir.clone());
        session.set_listen_port(6881);
        let add = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("public torrent adds");
        let id = add.id.expect("added torrent has id");
        let info_hash = {
            let torrents = session.torrents.lock().expect("torrent lock");
            torrents
                .iter()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists")
                .info_hash
        };

        let announce = lsd::LsdAnnounce {
            port: 51413,
            info_hashes: vec![info_hash],
            cookie: None,
        };
        let added = session
            .handle_lsd_announce(&announce, "192.168.1.44:6771".parse().expect("source parses"))
            .expect("LSD announce applies");
        assert_eq!(added, 1);
        let details = session.details(&id.to_string()).expect("details load");
        assert!(details.peers.iter().any(|peer| {
            peer.address == "192.168.1.44"
                && peer.port == 51413
                && peer.connection == "LSD discovered"
        }));

        {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            torrent.general.private = true;
        }
        let skipped = session
            .handle_lsd_announce(
                &lsd::LsdAnnounce {
                    port: 51414,
                    info_hashes: vec![info_hash],
                    cookie: None,
                },
                "192.168.1.45:6771".parse().expect("source parses"),
            )
            .expect("private LSD announce is ignored");
        assert_eq!(skipped, 0);
        let details = session.details(&id.to_string()).expect("details load");
        assert!(!details
            .peers
            .iter()
            .any(|peer| peer.address == "192.168.1.45"));
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn stream_file_availability_reports_verified_partial_ranges() {
        let root = temp_dir("session-stream-ranges");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        let data = b"abcdefghi";
        fs::write(&torrent_path, build_multi_file_torrent(data)).expect("fixture writes");
        let session = TorrentSession::new(root.join("default"));
        let id = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: true,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds")
            .id
            .expect("id assigned");
        let (info_hash, piece_hashes) = {
            let torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            (torrent.info_hash, torrent.piece_hashes.clone())
        };
        let mut store = storage::PartialPieceStore::open(
            &output_dir,
            &sha1::hex(&info_hash),
            data.len() as u64,
            4,
            &piece_hashes,
        )
        .expect("partial store opens");
        store.write_piece(0, &data[..4]).expect("first piece writes");
        drop(store);

        let partial = session
            .stream_file_availability(&id.to_string(), 1)
            .expect("partial availability loads");
        assert_eq!(partial.name, "two.bin");
        assert_eq!(partial.length, 4);
        assert_eq!(partial.verified_bytes, 1);
        assert!(!partial.complete);
        assert!(partial.partial_store_present);
        assert_eq!(
            partial.ranges,
            vec![storage::VerifiedByteRange {
                offset: 0,
                length: 1,
            }]
        );

        let mut store = storage::PartialPieceStore::open(
            &output_dir,
            &sha1::hex(&info_hash),
            data.len() as u64,
            4,
            &piece_hashes,
        )
        .expect("partial store reopens");
        store.write_piece(1, &data[4..8]).expect("second piece writes");
        drop(store);

        let complete = session
            .stream_file_availability(&id.to_string(), 1)
            .expect("complete file availability loads");
        assert_eq!(complete.verified_bytes, 4);
        assert!(complete.complete);
        assert_eq!(
            complete.ranges,
            vec![storage::VerifiedByteRange {
                offset: 0,
                length: 4,
            }]
        );
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    fn build_multi_file_torrent(data: &[u8]) -> Vec<u8> {
        let piece_hashes = data.chunks(4).map(sha1::digest).collect::<Vec<_>>();
        let mut out = Vec::new();
        out.extend_from_slice(b"d4:infod5:filesl");
        out.extend_from_slice(b"d6:lengthi3e4:pathl3:dir7:one.binee");
        out.extend_from_slice(b"d6:lengthi4e4:pathl7:two.binee");
        out.extend_from_slice(b"d6:lengthi2e4:pathl9:three.binee");
        out.extend_from_slice(b"e4:name7:Example12:piece lengthi4e6:pieces");
        let mut pieces = Vec::new();
        for hash in piece_hashes {
            pieces.extend_from_slice(&hash);
        }
        out.extend_from_slice(pieces.len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(&pieces);
        out.extend_from_slice(b"ee");
        out
    }

    fn build_webseed_tracker_torrent(
        data: &[u8],
        tracker_url: &str,
        webseed_url: &str,
    ) -> Vec<u8> {
        let mut pieces = Vec::new();
        for hash in data.chunks(4).map(sha1::digest) {
            pieces.extend_from_slice(&hash);
        }
        let mut out = Vec::new();
        out.extend_from_slice(b"d8:announce");
        out.extend_from_slice(tracker_url.len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(tracker_url.as_bytes());
        out.extend_from_slice(b"4:infod6:lengthi");
        out.extend_from_slice(data.len().to_string().as_bytes());
        out.extend_from_slice(b"e4:name8:file.bin12:piece lengthi4e6:pieces");
        out.extend_from_slice(pieces.len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(&pieces);
        out.extend_from_slice(b"e8:url-list");
        out.extend_from_slice(webseed_url.len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(webseed_url.as_bytes());
        out.push(b'e');
        out
    }

    #[test]
    fn recheck_updates_progress_from_stored_files() {
        let root = temp_dir("session-recheck");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        let data = b"abcdefghi".to_vec();
        fs::write(&torrent_path, build_multi_file_torrent(&data)).expect("fixture writes");

        let session = TorrentSession::new(output_dir.clone());
        let add = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = add.id.expect("added torrent has id");
        let files = {
            let torrents = session.torrents.lock().expect("torrent lock");
            torrents
                .iter()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists")
                .files
                .clone()
        };
        storage::write_torrent_bytes(&output_dir, "Example", &files, &data, true)
            .expect("stored files write");

        assert!(session
            .hash_torrent_file(&id.to_string(), 0)
            .expect_err("unfinished torrent cannot be reputation checked")
            .contains("completed, verified torrent"));
        session.recheck(&id.to_string()).expect("recheck completes");
        let details = session.details(&id.to_string()).expect("details load");
        let stats = details.stats.expect("stats");
        let file_hash = session
            .hash_torrent_file(&id.to_string(), 0)
            .expect("verified torrent file hashes");

        assert_eq!(stats.state, "Seeding");
        assert_eq!(stats.progress_bytes, data.len() as u64);
        assert_eq!(stats.file_progress, vec![3, 4, 2]);
        assert_eq!(
            file_hash.sha256,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            file_hash.virustotal_url,
            format!("https://www.virustotal.com/gui/file/{}", file_hash.sha256)
        );
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn incoming_listener_routes_by_info_hash_and_records_upload() {
        let root = temp_dir("session-incoming-seed");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        let data = b"abcdefghi".to_vec();
        fs::write(&torrent_path, build_multi_file_torrent(&data)).expect("fixture writes");

        let session = Arc::new(TorrentSession::new(output_dir.clone()));
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID");
        let (info_hash, files, piece_hashes) = {
            let torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            (torrent.info_hash, torrent.files.clone(), torrent.piece_hashes.clone())
        };
        storage::write_torrent_bytes(&output_dir, "Example", &files, &data, true)
            .expect("complete payload writes");
        session.recheck(&id.to_string()).expect("stored payload verifies");

        let listener = TcpListener::bind("127.0.0.1:0").expect("incoming listener binds");
        let port = listener.local_addr().expect("listener address").port();
        let server_session = Arc::clone(&session);
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("incoming peer connects");
            server_session
                .serve_incoming_peer(stream)
                .expect("incoming peer is served")
        });

        let result = peerwire::download_from_peer(
            "127.0.0.1",
            port,
            PeerDownloadPlan {
                info_hash,
                peer_id: *b"-NV0001-LEECHER00001",
                dht_port: None,
                enable_pex: false,
                total_length: data.len() as u64,
                piece_length: 4,
                piece_hashes,
                cancelled: None,
            },
        )
        .expect("leecher downloads from session listener");
        let uploaded = server.join().expect("incoming server exits");

        assert_eq!(result.bytes, data);
        assert_eq!(uploaded.bytes_uploaded, data.len() as u64);
        let details = session.details(&id.to_string()).expect("details load");
        let stats = details.stats.expect("stats exist");
        assert_eq!(stats.uploaded_bytes, data.len() as u64);
        assert_eq!(details.general.uploaded, data.len() as u64);
        assert_eq!(details.general.ratio, 1.0);
        assert!(details
            .peers
            .iter()
            .any(|peer| peer.connection == "Uploaded requested blocks"));

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn completed_torrents_remain_runnable_until_seed_ratio_is_reached() {
        let root = temp_dir("session-complete-runnable");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        let data = b"abcdefghi".to_vec();
        fs::write(&torrent_path, build_multi_file_torrent(&data)).expect("fixture writes");

        let session = TorrentSession::new(output_dir.clone());
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID");
        let files = {
            let torrents = session.torrents.lock().expect("torrent lock");
            torrents
                .iter()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists")
                .files
                .clone()
        };
        storage::write_torrent_bytes(&output_dir, "Example", &files, &data, true)
            .expect("complete payload writes");
        session.recheck(&id.to_string()).expect("stored payload verifies");

        assert_eq!(session.runnable_ids(), vec![id]);
        session
            .record_upload(id, None, data.len() as u64)
            .expect("upload is recorded");
        assert!(session.runnable_ids().is_empty());
        assert_eq!(
            session
                .details(&id.to_string())
                .expect("details load")
                .stats
                .expect("stats exist")
                .state,
            "Seed ratio reached"
        );

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn incoming_seed_sends_pex_candidates_to_extension_leecher() {
        let root = temp_dir("session-incoming-pex");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        let data = b"abcdefghi".to_vec();
        fs::write(&torrent_path, build_multi_file_torrent(&data)).expect("fixture writes");

        let session = Arc::new(TorrentSession::new(output_dir.clone()));
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID");
        let (info_hash, files) = {
            let torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            (torrent.info_hash, torrent.files.clone())
        };
        storage::write_torrent_bytes(&output_dir, "Example", &files, &data, true)
            .expect("complete payload writes");
        session.recheck(&id.to_string()).expect("stored payload verifies");
        {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            torrent.peers.push(PeerInfo {
                address: "203.0.113.7".to_string(),
                port: 6881,
                client: Some("PEX candidate".to_string()),
                progress: 1.0,
                download_speed: 0,
                upload_speed: 0,
                connection: "Known".to_string(),
            });
        }

        let listener = TcpListener::bind("127.0.0.1:0").expect("incoming listener binds");
        let port = listener.local_addr().expect("listener address").port();
        let server_session = Arc::clone(&session);
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("incoming peer connects");
            server_session
                .serve_incoming_peer(stream)
                .expect("incoming PEX leecher is served")
        });

        let mut socket = TcpStream::connect(("127.0.0.1", port)).expect("leecher connects");
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("leecher timeout sets");
        socket
            .write_all(&peer::build_feature_handshake(
                info_hash,
                *b"-NV0001-PEXLEECHR001",
                true,
                false,
            ))
            .expect("leecher handshake writes");
        let mut handshake = [0u8; HANDSHAKE_LEN];
        socket.read_exact(&mut handshake).expect("seed handshake reads");
        assert!(peer::supports_extension_protocol(
            &peer::parse_handshake_full(&handshake).expect("seed handshake parses")
        ));
        assert!(matches!(
            read_peer_message_for_test(&mut socket).expect("seed extension handshake reads"),
            PeerMessage::Extended { extension_id: 0, .. }
        ));
        socket
            .write_all(&peer::build_extended_message(
                0,
                &metadata::build_extension_handshake_with_pex(None, None, Some(9)),
            ))
            .expect("leecher extension handshake writes");
        socket
            .write_all(&peer::build_interested())
            .expect("leecher interested writes");
        let PeerMessage::Extended {
            extension_id,
            payload,
        } = read_peer_message_for_test(&mut socket).expect("seed PEX reads")
        else {
            panic!("expected PEX message");
        };
        assert_eq!(extension_id, 9);
        let message = crate::torrent::pex::parse_pex_message(&payload).expect("PEX parses");
        assert_eq!(message.added[0].address, "203.0.113.7");
        assert_eq!(message.added[0].port, 6881);
        assert!(matches!(
            read_peer_message_for_test(&mut socket).expect("seed bitfield reads"),
            PeerMessage::Bitfield(_)
        ));
        assert!(matches!(
            read_peer_message_for_test(&mut socket).expect("seed unchoke reads"),
            PeerMessage::Unchoke
        ));
        socket
            .write_all(&peer::build_not_interested())
            .expect("leecher not interested writes");
        assert_eq!(server.join().expect("incoming server exits").bytes_uploaded, 0);

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn seed_ratio_limit_sends_stopped_and_suppresses_tracker_maintenance() {
        let root = temp_dir("session-seed-ratio-stop");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        let data = b"abcdefghi".to_vec();
        fs::write(&torrent_path, build_multi_file_torrent(&data)).expect("fixture writes");

        let tracker_listener = TcpListener::bind("127.0.0.1:0").expect("tracker binds");
        let tracker_port = tracker_listener.local_addr().expect("tracker address").port();
        let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");
        let tracker = thread::spawn(move || {
            let mut events = Vec::new();
            for _ in 0..2 {
                let (mut stream, _) = tracker_listener.accept().expect("announce connects");
                let mut buffer = [0u8; 4096];
                let length = stream.read(&mut buffer).expect("announce request reads");
                let request = String::from_utf8_lossy(&buffer[..length]);
                let event = if request.contains("event=started") {
                    "started"
                } else if request.contains("event=stopped") {
                    "stopped"
                } else {
                    "other"
                };
                events.push(event.to_string());
                let body = b"d8:intervali60e5:peers0:e";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).expect("tracker headers write");
                stream.write_all(body).expect("tracker body writes");
            }
            events
        });

        let session = Arc::new(TorrentSession::new(output_dir.clone()));
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: false,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID");
        {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            torrent.trackers.push(TrackerStatus {
                url: tracker_url,
                state: "Not contacted".to_string(),
                seeders: None,
                leechers: None,
                next_announce_seconds: None,
                message: None,
            });
        }
        session.set_listen_port(6999);
        session.announce(&id.to_string()).expect("started announce succeeds");

        let (info_hash, files, piece_hashes) = {
            let torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            (torrent.info_hash, torrent.files.clone(), torrent.piece_hashes.clone())
        };
        storage::write_torrent_bytes(&output_dir, "Example", &files, &data, true)
            .expect("complete payload writes");
        session.recheck(&id.to_string()).expect("stored payload verifies");

        let listener = TcpListener::bind("127.0.0.1:0").expect("incoming listener binds");
        let port = listener.local_addr().expect("listener address").port();
        let server_session = Arc::clone(&session);
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("incoming peer connects");
            server_session
                .serve_incoming_peer(stream)
                .expect("incoming peer is served")
        });

        let result = peerwire::download_from_peer(
            "127.0.0.1",
            port,
            PeerDownloadPlan {
                info_hash,
                peer_id: *b"-NV0001-SEEDRATIO001",
                dht_port: None,
                enable_pex: false,
                total_length: data.len() as u64,
                piece_length: 4,
                piece_hashes,
                cancelled: None,
            },
        )
        .expect("leecher downloads from session listener");
        assert_eq!(result.bytes, data);
        assert_eq!(server.join().expect("incoming server exits").bytes_uploaded, data.len() as u64);

        let details = session.details(&id.to_string()).expect("details load");
        assert_eq!(details.stats.expect("stats exist").state, "Seed ratio reached");
        assert!(session.due_tracker_ids().is_empty());
        assert_eq!(tracker.join().expect("tracker exits"), vec!["started", "stopped"]);

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn tracker_announce_uses_the_active_listener_port() {
        let root = temp_dir("session-listen-port");
        let torrent_path = root.join("multi.torrent");
        fs::write(&torrent_path, build_multi_file_torrent(b"abcdefghi")).expect("fixture writes");
        let tracker_listener = TcpListener::bind("127.0.0.1:0").expect("tracker binds");
        let tracker_port = tracker_listener.local_addr().expect("tracker address").port();
        let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");

        let tracker = thread::spawn(move || {
            let mut requests = Vec::new();
            for _ in 0..4 {
                let (mut stream, _) = tracker_listener.accept().expect("announce connects");
                let mut buffer = [0u8; 4096];
                let length = stream.read(&mut buffer).expect("announce request reads");
                requests.push(String::from_utf8_lossy(&buffer[..length]).into_owned());
                let body = b"d8:intervali60e5:peers0:e";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).expect("tracker headers write");
                stream.write_all(body).expect("tracker body writes");
            }
            requests
        });

        let session = TorrentSession::new(root.join("out"));
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(root.join("out").to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: false,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID");
        {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            torrent.trackers.push(TrackerStatus {
                url: tracker_url,
                state: "Not contacted".to_string(),
                seeders: None,
                leechers: None,
                next_announce_seconds: None,
                message: None,
            });
        }
        session.set_listen_port(6999);
        session.announce(&id.to_string()).expect("announce succeeds");
        assert!(session.due_tracker_ids().is_empty());
        let files = session
            .torrents
            .lock()
            .expect("torrent lock")
            .iter()
            .find(|torrent| torrent.id == id)
            .expect("torrent exists")
            .files
            .clone();
        storage::write_torrent_bytes(&root.join("out"), "Example", &files, b"abcdefghi", true)
            .expect("complete data writes");
        session.recheck(&id.to_string()).expect("complete data rechecks");
        session.announce(&id.to_string()).expect("completed announce succeeds");
        session.announce(&id.to_string()).expect("regular announce succeeds");
        session
            .delete(&id.to_string(), false)
            .expect("delete sends stopped announce");
        let requests = tracker.join().expect("tracker exits");
        assert!(requests[0].contains("port=6999"));
        assert!(requests[0].contains("event=started"));
        assert!(requests[1].contains("event=completed"));
        assert!(!requests[2].contains("event="));
        assert!(requests[3].contains("event=stopped"));

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn tracker_announce_uses_fast_peers_before_slow_trackers_finish() {
        let root = temp_dir("session-early-tracker-peers");
        let torrent_path = root.join("multi.torrent");
        fs::write(&torrent_path, build_multi_file_torrent(b"abcdefghi")).expect("fixture writes");

        let fast_listener = TcpListener::bind("127.0.0.1:0").expect("fast tracker binds");
        let fast_port = fast_listener.local_addr().expect("fast tracker address").port();
        let fast_url = format!("http://127.0.0.1:{fast_port}/announce");
        let fast_tracker = thread::spawn(move || {
            let (mut stream, _) = fast_listener.accept().expect("fast announce connects");
            let mut buffer = [0u8; 4096];
            let _ = stream.read(&mut buffer).expect("fast announce request reads");
            let mut body = b"d8:intervali60e5:peers6:".to_vec();
            body.extend_from_slice(&[127, 0, 0, 1, 0x1a, 0xe1]);
            body.push(b'e');
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).expect("fast tracker headers write");
            stream.write_all(&body).expect("fast tracker body writes");
        });

        let slow_listener = TcpListener::bind("127.0.0.1:0").expect("slow tracker binds");
        let slow_port = slow_listener.local_addr().expect("slow tracker address").port();
        let slow_url = format!("http://127.0.0.1:{slow_port}/announce");
        let slow_tracker = thread::spawn(move || {
            let (mut stream, _) = slow_listener.accept().expect("slow announce connects");
            let mut buffer = [0u8; 4096];
            let _ = stream.read(&mut buffer).expect("slow announce request reads");
            thread::sleep(Duration::from_millis(1_400));
            let body = b"d8:intervali60e5:peers0:e";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).expect("slow tracker headers write");
            stream.write_all(body).expect("slow tracker body writes");
        });

        let session = TorrentSession::new(root.join("out"));
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(root.join("out").to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: false,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID");
        {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            torrent.trackers.push(TrackerStatus {
                url: fast_url,
                state: "Not contacted".to_string(),
                seeders: None,
                leechers: None,
                next_announce_seconds: None,
                message: None,
            });
            torrent.trackers.push(TrackerStatus {
                url: slow_url,
                state: "Not contacted".to_string(),
                seeders: None,
                leechers: None,
                next_announce_seconds: None,
                message: None,
            });
        }
        session.set_listen_port(6999);

        let started = Instant::now();
        session.announce(&id.to_string()).expect("announce succeeds");
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_millis(1_000),
            "announce waited for the slow tracker: {elapsed:?}"
        );
        let peers = {
            let torrents = session.torrents.lock().expect("torrent lock");
            torrents
                .iter()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists")
                .peers
                .clone()
        };
        assert!(peers
            .iter()
            .any(|peer| peer.address == "127.0.0.1" && peer.port == 6881));
        fast_tracker.join().expect("fast tracker exits");
        slow_tracker.join().expect("slow tracker exits");

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn completion_announce_sends_started_then_completed_when_tracker_was_never_started() {
        let root = temp_dir("session-completion-announces");
        let torrent_path = root.join("multi.torrent");
        fs::write(&torrent_path, build_multi_file_torrent(b"abcdefghi")).expect("fixture writes");
        let tracker_listener = TcpListener::bind("127.0.0.1:0").expect("tracker binds");
        let tracker_port = tracker_listener.local_addr().expect("tracker address").port();
        let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");

        let tracker = thread::spawn(move || {
            let mut events = Vec::new();
            for _ in 0..2 {
                let (mut stream, _) = tracker_listener.accept().expect("announce connects");
                let mut buffer = [0u8; 4096];
                let length = stream.read(&mut buffer).expect("announce request reads");
                let request = String::from_utf8_lossy(&buffer[..length]);
                let event = if request.contains("event=started") {
                    "started"
                } else if request.contains("event=completed") {
                    "completed"
                } else {
                    "none"
                };
                events.push(event.to_string());
                let body = b"d8:intervali60e5:peers0:e";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).expect("tracker headers write");
                stream.write_all(body).expect("tracker body writes");
            }
            events
        });

        let session = TorrentSession::new(root.join("out"));
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(root.join("out").to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: false,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID");
        {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            torrent.trackers.push(TrackerStatus {
                url: tracker_url,
                state: "Not contacted".to_string(),
                seeders: None,
                leechers: None,
                next_announce_seconds: None,
                message: None,
            });
            torrent.stats.progress_bytes = torrent.stats.total_bytes;
            torrent.general.downloaded = torrent.stats.total_bytes;
            torrent.stats.finished = true;
            update_completed_torrent_state(torrent);
            assert!(!torrent.tracker_started);
            assert!(!torrent.tracker_completed);
        }
        session.set_listen_port(6999);
        session
            .announce_completion_after_verification(&id.to_string())
            .expect("completion announce succeeds");
        assert_eq!(
            tracker.join().expect("tracker exits"),
            vec!["started", "completed"]
        );

        let details = session.details(&id.to_string()).expect("details load");
        let stats = details.stats.expect("stats exist");
        assert_eq!(stats.state, "Seeding");
        {
            let torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            assert!(torrent.tracker_started);
            assert!(torrent.tracker_completed);
        }

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn runtime_announces_started_before_webseed_and_completed_afterward() {
        let root = temp_dir("session-tracker-runtime-order");
        let output_dir = root.join("out");
        let torrent_path = root.join("webseed.torrent");
        let data = b"abcdefghi".to_vec();
        let events = std::sync::Arc::new(Mutex::new(Vec::new()));

        let tracker_listener = TcpListener::bind("127.0.0.1:0").expect("tracker binds");
        let tracker_port = tracker_listener.local_addr().expect("tracker address").port();
        let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");
        let tracker_events = std::sync::Arc::clone(&events);
        let tracker = thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = tracker_listener.accept().expect("announce connects");
                let mut buffer = [0u8; 4096];
                let length = stream.read(&mut buffer).expect("announce request reads");
                let request = String::from_utf8_lossy(&buffer[..length]);
                let event = if request.contains("event=started") {
                    "started"
                } else if request.contains("event=completed") {
                    "completed"
                } else {
                    "none"
                };
                tracker_events.lock().expect("events lock").push(event.to_string());
                let body = b"d8:intervali60e5:peers0:e";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).expect("tracker headers write");
                stream.write_all(body).expect("tracker body writes");
            }
        });

        let webseed_listener = TcpListener::bind("127.0.0.1:0").expect("webseed binds");
        let webseed_port = webseed_listener.local_addr().expect("webseed address").port();
        let webseed_url = format!("http://127.0.0.1:{webseed_port}/file.bin");
        let webseed_events = std::sync::Arc::clone(&events);
        let webseed_data = data.clone();
        let webseed = thread::spawn(move || {
            let (mut stream, _) = webseed_listener.accept().expect("webseed connects");
            let mut buffer = [0u8; 4096];
            let length = stream.read(&mut buffer).expect("webseed request reads");
            assert!(String::from_utf8_lossy(&buffer[..length]).starts_with("GET /file.bin "));
            webseed_events.lock().expect("events lock").push("webseed".to_string());
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                webseed_data.len()
            );
            stream.write_all(response.as_bytes()).expect("webseed headers write");
            stream.write_all(&webseed_data).expect("webseed body writes");
        });

        fs::write(
            &torrent_path,
            build_webseed_tracker_torrent(&data, &tracker_url, &webseed_url),
        )
        .expect("fixture writes");
        let session = TorrentSession::new(output_dir.clone());
        session.set_listen_port(6999);
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: false,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID");

        session.run_torrent(&id.to_string()).expect("runtime completes");
        tracker.join().expect("tracker exits");
        webseed.join().expect("webseed exits");
        assert_eq!(
            *events.lock().expect("events lock"),
            vec!["started", "webseed", "completed"]
        );
        assert_eq!(fs::read(output_dir.join("file.bin")).expect("output reads"), data);

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn fetch_metadata_populates_magnet_torrent() {
        let root = temp_dir("session-metadata");
        let info = raw_single_file_info();
        let info_hash = sha1::digest(&info);
        let magnet = format!(
            "magnet:?xt=urn:btih:{}&dn=Example&tr=udp%3A%2F%2Ftracker.example%3A6969%2Fannounce",
            sha1::hex(&info_hash)
        );
        let output_dir = root.join("out");
        let session = TorrentSession::new(output_dir.clone());
        let add = session
            .add(AddTorrentRequest {
                source: TorrentSource::Magnet(magnet),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: false,
                only_files: None,
                sub_folder: None,
            })
            .expect("magnet adds");
        let id = add.id.expect("added torrent has id");

        let listener = TcpListener::bind("127.0.0.1:0").expect("local listener binds");
        let port = listener.local_addr().expect("listener address").port();
        {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents.iter_mut().find(|torrent| torrent.id == id).expect("torrent exists");
            assert!(torrent.files.is_empty());
            torrent.peers.push(PeerInfo {
                address: "127.0.0.1".to_string(),
                port,
                client: Some("NovaTorrent metadata peer".to_string()),
                progress: 0.0,
                download_speed: 0,
                upload_speed: 0,
                connection: "Discovered".to_string(),
            });
        }

        let server_info = info.clone();
        let server = thread::spawn(move || {
            serve_metadata_once(listener, info_hash, server_info);
        });

        session.fetch_metadata(&id.to_string()).expect("metadata fetch completes");
        server.join().expect("metadata server exits");

        let details = session.details(&id.to_string()).expect("details load");
        let stats = details.stats.expect("stats");
        assert_eq!(details.name.as_deref(), Some("example.txt"));
        assert_eq!(details.general.total_size, 11);
        assert_eq!(details.general.piece_size, 4);
        assert_eq!(details.general.piece_count, 3);
        assert_eq!(stats.state, "Queued");
        assert_eq!(stats.total_bytes, 11);
        assert_eq!(details.files.expect("files")[0].name, "example.txt");
        assert_eq!(
            details.peers[0].client.as_deref(),
            Some("NovaTorrent 0.0.0.1")
        );
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn query_dht_adds_discovered_peers_to_magnet() {
        let root = temp_dir("session-dht");
        let info_hash = sha1::digest(&raw_single_file_info());
        let magnet = format!("magnet:?xt=urn:btih:{}&dn=Example", sha1::hex(&info_hash));
        let output_dir = root.join("out");
        let session = TorrentSession::new(output_dir.clone());
        let add = session
            .add(AddTorrentRequest {
                source: TorrentSource::Magnet(magnet),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("magnet adds");
        let id = add.id.expect("added torrent has id");
        let server = UdpSocket::bind("127.0.0.1:0").expect("local DHT socket binds");
        let contact = dht::DhtContact {
            address: "127.0.0.1".to_string(),
            port: server.local_addr().expect("DHT address").port(),
        };

        let handle = thread::spawn(move || {
            serve_dht_peer_values_once(server, info_hash, 51413);
        });

        session
            .query_dht_with_seeds(&id.to_string(), &[contact], Duration::from_secs(2), 2)
            .expect("DHT lookup completes");
        handle.join().expect("DHT server exits");

        let details = session.details(&id.to_string()).expect("details load");
        let stats = details.stats.expect("stats");
        assert_eq!(stats.state, "Metadata");
        assert_eq!(details.peers.len(), 1);
        assert_eq!(details.peers[0].address, "127.0.0.1");
        assert_eq!(details.peers[0].port, 51413);
        assert_eq!(details.peers[0].connection, "Discovered");
        assert!(session
            .logs(Some(id))
            .iter()
            .any(|entry| entry.scope == "dht" && entry.message.contains("DHT lookup finished")));
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn inbound_dht_node_issues_tokens_stores_announces_and_rejects_bad_tokens() {
        let root = temp_dir("session-dht-node");
        let session = Arc::new(TorrentSession::new(root.clone()));
        let server = UdpSocket::bind("127.0.0.1:0").expect("DHT server binds");
        let server_address = server.local_addr().expect("DHT server address");
        session.set_dht_port(server_address.port());
        let server_session = Arc::clone(&session);
        let server_handle = thread::spawn(move || {
            let mut buffer = [0u8; 4096];
            for _ in 0..4 {
                let (length, source) = server.recv_from(&mut buffer).expect("DHT packet arrives");
                let response = server_session.handle_dht_packet(&buffer[..length], source);
                server.send_to(&response, source).expect("DHT response sends");
            }
        });
        let client = UdpSocket::bind("127.0.0.1:0").expect("DHT client binds");
        let client_address = client.local_addr().expect("DHT client address");
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("DHT client timeout sets");
        let node_id = *b"abcdefghij0123456789";
        let info_hash = *b"mnopqrstuvwxyz123456";
        let exchange = |packet: &[u8]| {
            client.send_to(packet, server_address).expect("DHT query sends");
            let mut buffer = [0u8; 4096];
            let (length, _) = client.recv_from(&mut buffer).expect("DHT response arrives");
            buffer[..length].to_vec()
        };

        let initial = exchange(&dht::build_get_peers_query(b"g1", node_id, info_hash));
        let initial = dht::parse_dht_response(&initial).expect("initial get_peers parses");
        assert!(initial.nodes.is_empty());
        let questionable = session
            .dht_routing
            .lock()
            .expect("routing lock")
            .questionable_contacts(1);
        assert_eq!(questionable.len(), 1);
        assert_eq!(questionable[0].address, client_address.ip().to_string());
        assert_eq!(questionable[0].port, client_address.port());
        let token = initial.token.expect("get_peers returns token");
        assert!(initial.peers.is_empty());

        let announce = exchange(&dht::build_announce_peer_query(
            b"a1",
            node_id,
            info_hash,
            51413,
            &token,
            false,
        ));
        assert_eq!(
            dht::parse_dht_response(&announce)
                .expect("announce response parses")
                .transaction_id,
            b"a1"
        );

        let populated = exchange(&dht::build_get_peers_query(b"g2", node_id, info_hash));
        let populated = dht::parse_dht_response(&populated).expect("populated get_peers parses");
        assert_eq!(populated.peers.len(), 1);
        assert_eq!(populated.peers[0].address, "127.0.0.1");
        assert_eq!(populated.peers[0].port, 51413);

        let rejected = exchange(&dht::build_announce_peer_query(
            b"a2",
            node_id,
            info_hash,
            51414,
            b"wrong-token",
            false,
        ));
        let rejected = dht::parse_dht_error(&rejected).expect("bad token error parses");
        assert_eq!(rejected.code, 203);
        assert_eq!(rejected.message, "invalid token");

        server_handle.join().expect("DHT server exits");
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn dht_tokens_are_bound_to_the_requesting_ip() {
        let root = temp_dir("session-dht-token-ip");
        let session = TorrentSession::new(root.clone());
        let bucket = dht_token_bucket();
        let first_ip = "127.0.0.1".parse().expect("first IP parses");
        let second_ip = "127.0.0.2".parse().expect("second IP parses");
        let token = session.dht_token(first_ip, bucket);

        assert!(session.valid_dht_token(first_ip, &token));
        assert!(!session.valid_dht_token(second_ip, &token));

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn peer_advertised_dht_port_is_verified_before_routing_admission() {
        let root = temp_dir("session-peer-dht-port");
        let session = TorrentSession::new(root.clone());
        let remote_node_id = *b"peer-dht-node-id-001";
        let server = UdpSocket::bind("127.0.0.1:0").expect("peer DHT node binds");
        let port = server.local_addr().expect("peer DHT address").port();
        let responder = thread::spawn(move || {
            let mut buffer = [0u8; 4096];
            let (length, source) = server.recv_from(&mut buffer).expect("ping arrives");
            let query = dht::parse_dht_query(&buffer[..length]).expect("ping query parses");
            assert_eq!(query.kind, DhtQueryKind::Ping);
            server
                .send_to(
                    &dht::build_id_response(&query.transaction_id, remote_node_id),
                    source,
                )
                .expect("ping response sends");
        });

        session
            .learn_peer_dht_node("127.0.0.1", port, 41)
            .expect("peer DHT node verifies");
        responder.join().expect("peer DHT responder exits");
        let closest = session
            .dht_routing
            .lock()
            .expect("routing lock")
            .closest_nodes(remote_node_id, 1);
        assert_eq!(closest.len(), 1);
        assert_eq!(closest[0].id, remote_node_id);
        assert_eq!(closest[0].port, port);

        let spoofed = UdpSocket::bind("127.0.0.1:0").expect("spoofed DHT node binds");
        let spoofed_port = spoofed.local_addr().expect("spoofed DHT address").port();
        let spoofed_responder = thread::spawn(move || {
            let mut buffer = [0u8; 4096];
            let (_, source) = spoofed.recv_from(&mut buffer).expect("spoofed ping arrives");
            spoofed
                .send_to(&dht::build_id_response(b"xx", [99u8; 20]), source)
                .expect("spoofed response sends");
        });
        assert!(session
            .learn_peer_dht_node("127.0.0.1", spoofed_port, 42)
            .expect_err("mismatched transaction is rejected")
            .contains("transaction ID mismatch"));
        spoofed_responder.join().expect("spoofed responder exits");
        assert_eq!(session.dht_routing.lock().expect("routing lock").len(), 1);

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn dht_maintenance_verifies_questionable_nodes_and_evicts_two_time_failures() {
        let root = temp_dir("session-dht-maintenance");
        let session = TorrentSession::new(root.clone());
        let good_node_id = *b"good-dht-node-id-001";
        let bad_node_id = *b"bad--dht-node-id-001";
        let good = UdpSocket::bind("127.0.0.1:0").expect("good DHT node binds");
        let good_port = good.local_addr().expect("good DHT address").port();
        let good_responder = thread::spawn(move || {
            let mut buffer = [0u8; 4096];
            let (length, source) = good.recv_from(&mut buffer).expect("good ping arrives");
            let query = dht::parse_dht_query(&buffer[..length]).expect("good ping parses");
            good.send_to(
                &dht::build_id_response(&query.transaction_id, good_node_id),
                source,
            )
            .expect("good ping response sends");
        });
        let bad = UdpSocket::bind("127.0.0.1:0").expect("bad DHT node binds");
        let bad_port = bad.local_addr().expect("bad DHT address").port();
        let bad_responder = thread::spawn(move || {
            let mut buffer = [0u8; 4096];
            for _ in 0..2 {
                let (_, source) = bad.recv_from(&mut buffer).expect("bad ping arrives");
                bad.send_to(&dht::build_id_response(b"zz", bad_node_id), source)
                    .expect("bad ping response sends");
            }
        });
        {
            let mut routing = session.dht_routing.lock().expect("routing lock");
            assert!(routing.observe_query(DhtNode {
                id: good_node_id,
                address: "127.0.0.1".to_string(),
                port: good_port,
            }));
            assert!(routing.observe_query(DhtNode {
                id: bad_node_id,
                address: "127.0.0.1".to_string(),
                port: bad_port,
            }));
        }

        let first = session.maintain_dht_routing().expect("first maintenance runs");
        assert_eq!(first.pinged, 2);
        assert_eq!(first.verified, 1);
        assert_eq!(first.evicted, 0);
        let second = session.maintain_dht_routing().expect("second maintenance runs");
        assert_eq!(second.pinged, 1);
        assert_eq!(second.verified, 0);
        assert_eq!(second.evicted, 1);

        good_responder.join().expect("good responder exits");
        bad_responder.join().expect("bad responder exits");
        let routing = session.dht_routing.lock().expect("routing lock");
        assert_eq!(routing.len(), 1);
        assert_eq!(routing.closest_nodes(good_node_id, 1)[0].id, good_node_id);
        drop(routing);
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn dht_startup_self_lookup_revalidates_contact_and_learns_candidates() {
        let root = temp_dir("session-dht-startup-self-lookup");
        let session = TorrentSession::new(root.clone());
        let validated_node_id = [61u8; 20];
        let discovered_node_id = [62u8; 20];
        let server = UdpSocket::bind("127.0.0.1:0").expect("startup DHT node binds");
        let server_port = server.local_addr().expect("startup DHT address").port();
        let own_id = session.dht_node_id;
        let responder = thread::spawn(move || {
            let mut buffer = [0u8; 4096];

            let (length, source) = server.recv_from(&mut buffer).expect("startup ping arrives");
            let ping = dht::parse_dht_query(&buffer[..length]).expect("startup ping parses");
            assert!(matches!(ping.kind, dht::DhtQueryKind::Ping));
            server
                .send_to(
                    &dht::build_id_response(&ping.transaction_id, validated_node_id),
                    source,
                )
                .expect("startup ping response sends");

            let (length, source) = server.recv_from(&mut buffer).expect("self lookup arrives");
            let lookup = dht::parse_dht_query(&buffer[..length]).expect("self lookup parses");
            assert!(matches!(
                lookup.kind,
                dht::DhtQueryKind::FindNode { target } if target == own_id
            ));
            let candidate = DhtNode {
                id: discovered_node_id,
                address: "127.0.0.2".to_string(),
                port: 49090,
            };
            server
                .send_to(
                    &dht::build_find_node_response(
                        &lookup.transaction_id,
                        validated_node_id,
                        &[candidate],
                    )
                    .expect("self lookup response builds"),
                    source,
                )
                .expect("self lookup response sends");
        });
        assert!(session
            .dht_routing
            .lock()
            .expect("routing lock")
            .insert_persisted_candidate(DhtNode {
                id: validated_node_id,
                address: "127.0.0.1".to_string(),
                port: server_port,
            }));

        let summary = session
            .bootstrap_dht_routing()
            .expect("startup bootstrap succeeds");
        responder.join().expect("startup DHT responder exits");
        assert_eq!(summary.pinged, 1);
        assert_eq!(summary.refreshes, 1);
        assert_eq!(summary.verified, 2);
        assert_eq!(summary.candidates, 1);
        let routing = session.dht_routing.lock().expect("routing lock");
        assert_eq!(routing.len(), 2);
        assert_eq!(routing.closest_nodes(own_id, 8).len(), 1);
        assert!(routing
            .questionable_contacts(8)
            .iter()
            .any(|contact| contact.address == "127.0.0.2" && contact.port == 49090));
        drop(routing);
        assert!(session.logs(None).iter().any(|entry| {
            entry.scope == "dht" && entry.message.contains("startup self-lookup queried 1")
        }));

        drop(session);
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn dht_identity_and_verified_contacts_persist_as_questionable() {
        let root = temp_dir("session-dht-persistence");
        let session = TorrentSession::new(root.clone());
        let original_node_id = session.dht_node_id;
        let verified_node_id = *b"save-dht-node-id-001";
        let verified = UdpSocket::bind("127.0.0.1:0").expect("verified DHT node binds");
        let verified_port = verified.local_addr().expect("verified DHT address").port();
        let responder = thread::spawn(move || {
            let mut buffer = [0u8; 4096];
            let (length, source) = verified.recv_from(&mut buffer).expect("ping arrives");
            let query = dht::parse_dht_query(&buffer[..length]).expect("ping parses");
            verified
                .send_to(
                    &dht::build_id_response(&query.transaction_id, verified_node_id),
                    source,
                )
                .expect("ping response sends");
        });
        {
            let mut routing = session.dht_routing.lock().expect("routing lock");
            assert!(routing.observe_query(DhtNode {
                id: *b"unsaved-node-id-0001",
                address: "127.0.0.1".to_string(),
                port: 49999,
            }));
        }
        session
            .learn_peer_dht_node("127.0.0.1", verified_port, 51)
            .expect("verified contact is learned");
        responder.join().expect("verified responder exits");

        let state = load_dht_state(session.dht_state_file_path())
            .expect("DHT state reads")
            .expect("DHT state exists");
        assert_eq!(state.node_id, original_node_id);
        assert_eq!(state.nodes.len(), 1);
        assert_eq!(state.nodes[0].id, verified_node_id);
        drop(session);

        let restored = TorrentSession::new(root.clone());
        assert_eq!(restored.dht_node_id, original_node_id);
        let routing = restored.dht_routing.lock().expect("routing lock");
        assert_eq!(routing.len(), 1);
        assert!(routing.closest_nodes(verified_node_id, 1).is_empty());
        assert_eq!(routing.questionable_contacts(8).len(), 1);
        drop(routing);
        assert!(restored.logs(None).iter().any(|entry| {
            entry.scope == "dht"
                && entry
                    .message
                    .contains("restored 1 DHT contacts as questionable")
        }));
        drop(restored);
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn corrupt_dht_state_is_logged_and_replaced() {
        let root = temp_dir("session-dht-corrupt-state");
        let state_path = root.join("novatorrent-dht.json");
        fs::write(&state_path, b"{not valid JSON").expect("corrupt state writes");

        let session = TorrentSession::new(root.clone());
        assert_ne!(session.dht_node_id, [0u8; 20]);
        assert!(session.logs(None).iter().any(|entry| {
            entry.scope == "dht" && entry.message.contains("could not restore DHT state")
        }));
        let state = load_dht_state(&state_path)
            .expect("replacement state parses")
            .expect("replacement state exists");
        assert_eq!(state.version, 1);
        assert_eq!(state.node_id, session.dht_node_id);

        drop(session);
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn inbound_dht_rejects_private_torrent_info_hashes() {
        let root = temp_dir("session-dht-private");
        let torrent_path = root.join("private.torrent");
        let output_dir = root.join("out");
        fs::write(&torrent_path, build_multi_file_torrent(b"private payload"))
            .expect("fixture writes");
        let session = TorrentSession::new(output_dir.clone());
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: true,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID");
        let info_hash = {
            let mut torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter_mut()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            torrent.general.private = true;
            torrent.info_hash
        };

        let response = session.handle_dht_packet(
            &dht::build_get_peers_query(b"pr", *b"abcdefghij0123456789", info_hash),
            "127.0.0.1:53000".parse().expect("source address parses"),
        );
        let error = dht::parse_dht_error(&response).expect("private torrent error parses");
        assert_eq!(error.code, 203);
        assert_eq!(error.message, "private torrent is not available through DHT");

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn completed_torrent_uses_dht_token_to_announce_listener_port() {
        let root = temp_dir("session-dht-announce");
        let torrent_path = root.join("multi.torrent");
        let output_dir = root.join("out");
        let data = b"abcdefghi".to_vec();
        fs::write(&torrent_path, build_multi_file_torrent(&data)).expect("fixture writes");
        let session = TorrentSession::new(output_dir.clone());
        let added = session
            .add(AddTorrentRequest {
                source: TorrentSource::File(torrent_path.to_string_lossy().into_owned()),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("torrent adds");
        let id = added.id.expect("torrent has ID");
        let (info_hash, files) = {
            let torrents = session.torrents.lock().expect("torrent lock");
            let torrent = torrents
                .iter()
                .find(|torrent| torrent.id == id)
                .expect("torrent exists");
            (torrent.info_hash, torrent.files.clone())
        };
        storage::write_torrent_bytes(&output_dir, "Example", &files, &data, true)
            .expect("complete data writes");
        session.recheck(&id.to_string()).expect("complete data rechecks");
        session.set_listen_port(6999);

        let server = UdpSocket::bind("127.0.0.1:0").expect("local DHT socket binds");
        let contact = dht::DhtContact {
            address: "127.0.0.1".to_string(),
            port: server.local_addr().expect("DHT address").port(),
        };
        let handle = thread::spawn(move || {
            let mut buffer = [0u8; 2048];
            let (length, client) = server.recv_from(&mut buffer).expect("get_peers arrives");
            let transaction_id = assert_dht_get_peers_query(&buffer[..length], info_hash);
            let mut lookup_response = b"d1:rd2:id".to_vec();
            write_bencoded_bytes(&mut lookup_response, b"dhtnode-abcdefghijkl");
            lookup_response.extend_from_slice(b"5:token10:seed-tokene1:t");
            write_bencoded_bytes(&mut lookup_response, &transaction_id);
            lookup_response.extend_from_slice(b"1:y1:re");
            server
                .send_to(&lookup_response, client)
                .expect("get_peers response sends");

            let (length, client) = server.recv_from(&mut buffer).expect("announce_peer arrives");
            let root = bencode::parse(&buffer[..length]).expect("announce_peer parses");
            assert_eq!(
                root.dict_get(b"q").and_then(BencodeNode::as_bytes),
                Some(&b"announce_peer"[..])
            );
            let args = root.dict_get(b"a").expect("announce_peer has args");
            assert_eq!(
                args.dict_get(b"info_hash").and_then(BencodeNode::as_bytes),
                Some(&info_hash[..])
            );
            assert_eq!(args.dict_get(b"port").and_then(BencodeNode::as_i64), Some(6999));
            assert_eq!(
                args.dict_get(b"token").and_then(BencodeNode::as_bytes),
                Some(&b"seed-token"[..])
            );
            let transaction_id = root
                .dict_get(b"t")
                .and_then(BencodeNode::as_bytes)
                .expect("announce_peer has transaction ID");
            let mut announce_response = b"d1:rd2:id".to_vec();
            write_bencoded_bytes(&mut announce_response, b"dhtnode-abcdefghijkl");
            announce_response.extend_from_slice(b"e1:t");
            write_bencoded_bytes(&mut announce_response, transaction_id);
            announce_response.extend_from_slice(b"1:y1:re");
            server
                .send_to(&announce_response, client)
                .expect("announce_peer response sends");
        });

        session
            .query_dht_with_seeds(&id.to_string(), &[contact], Duration::from_secs(2), 2)
            .expect("DHT lookup and announce complete");
        handle.join().expect("DHT server exits");
        assert_eq!(
            session
                .details(&id.to_string())
                .expect("details load")
                .stats
                .expect("stats exist")
                .state,
            "Seeding"
        );
        assert!(session.logs(Some(id)).iter().any(|entry| {
            entry.scope == "dht" && entry.message.contains("advertised to 1/1")
        }));

        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn resolve_magnet_queries_dht_then_fetches_metadata() {
        let root = temp_dir("session-resolve-magnet");
        let info = raw_single_file_info();
        let info_hash = sha1::digest(&info);
        let magnet = format!("magnet:?xt=urn:btih:{}&dn=Example", sha1::hex(&info_hash));
        let output_dir = root.join("out");
        let session = TorrentSession::new(output_dir.clone());
        let add = session
            .add(AddTorrentRequest {
                source: TorrentSource::Magnet(magnet),
                destination: Some(output_dir.to_string_lossy().into_owned()),
                paused: false,
                overwrite: true,
                disable_trackers: true,
                only_files: None,
                sub_folder: None,
            })
            .expect("magnet adds");
        let id = add.id.expect("added torrent has id");

        let metadata_listener = TcpListener::bind("127.0.0.1:0").expect("metadata listener binds");
        let metadata_port = metadata_listener
            .local_addr()
            .expect("metadata listener address")
            .port();
        let dht_server = UdpSocket::bind("127.0.0.1:0").expect("local DHT socket binds");
        let dht_contact = dht::DhtContact {
            address: "127.0.0.1".to_string(),
            port: dht_server.local_addr().expect("DHT address").port(),
        };

        let dht_handle = thread::spawn(move || {
            serve_dht_peer_values_once(dht_server, info_hash, metadata_port);
        });
        let metadata_info = info.clone();
        let metadata_handle = thread::spawn(move || {
            serve_metadata_once(metadata_listener, info_hash, metadata_info);
        });

        session
            .resolve_magnet_with_seeds(&id.to_string(), &[dht_contact], Duration::from_secs(2), 2)
            .expect("magnet resolves");
        dht_handle.join().expect("DHT server exits");
        metadata_handle.join().expect("metadata server exits");

        let details = session.details(&id.to_string()).expect("details load");
        let stats = details.stats.expect("stats");
        assert_eq!(stats.state, "Queued");
        assert_eq!(details.name.as_deref(), Some("example.txt"));
        assert_eq!(details.general.total_size, 11);
        assert_eq!(details.files.expect("files")[0].name, "example.txt");
        assert_eq!(details.peers.len(), 1);
        assert_eq!(details.peers[0].connection, "Metadata received");
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    #[test]
    fn writes_readable_logs_to_disk() {
        let root = temp_dir("session-logs");
        let session = TorrentSession::new(root.clone());

        session.log(
            LogLevel::Info,
            "test",
            "first line\nsecond line",
            Some(42),
        );

        let contents = fs::read_to_string(root.join("novatorrent.log")).expect("log file reads");
        assert!(contents.contains("[Info] scope=test torrent=42 message=first line second line"));
        fs::remove_dir_all(root).expect("temp dir removes");
    }

    fn raw_single_file_info() -> Vec<u8> {
        b"d6:lengthi11e4:name11:example.txt12:piece lengthi4e6:pieces60:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaae".to_vec()
    }

    fn serve_metadata_once(listener: TcpListener, info_hash: [u8; 20], info: Vec<u8>) {
        let (mut socket, _) = listener.accept().expect("client connects");
        socket
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .expect("read timeout sets");
        socket
            .set_write_timeout(Some(std::time::Duration::from_secs(10)))
            .expect("write timeout sets");

        let mut handshake = [0u8; HANDSHAKE_LEN];
        socket.read_exact(&mut handshake).expect("client handshake arrives");
        let handshake = peer::parse_handshake_full(&handshake).expect("client handshake parses");
        assert_eq!(handshake.info_hash, info_hash);
        assert!(peer::supports_extension_protocol(&handshake));
        socket
            .write_all(&peer::build_extended_handshake(
                info_hash,
                *b"-NV0001-METAPEER0001",
            ))
            .expect("server handshake writes");

        let message = read_peer_message_for_test(&mut socket).expect("client extension handshake arrives");
        let PeerMessage::Extended {
            extension_id: 0,
            payload,
        } = message
        else {
            panic!("expected client extension handshake");
        };
        assert!(metadata::parse_extension_handshake(&payload)
            .expect("client extension handshake parses")
            .ut_metadata
            .is_some());

        socket
            .write_all(&peer::build_extended_message(
                0,
                &metadata::build_extension_handshake(7, Some(info.len() as u64)),
            ))
            .expect("server extension handshake writes");

        let message = read_peer_message_for_test(&mut socket).expect("metadata request arrives");
        let PeerMessage::Extended {
            extension_id: 7,
            payload,
        } = message
        else {
            panic!("expected metadata request");
        };
        let request = metadata::parse_metadata_message(&payload).expect("request parses");
        assert_eq!(request.message_type, MetadataMessageType::Request);
        assert_eq!(request.piece, 0);

        socket
            .write_all(&peer::build_extended_message(
                7,
                &metadata::build_metadata_data(0, info.len() as u64, &info),
            ))
            .expect("metadata data writes");
    }

    fn read_peer_message_for_test(stream: &mut TcpStream) -> Result<PeerMessage, String> {
        let mut length_buf = [0u8; 4];
        stream
            .read_exact(&mut length_buf)
            .map_err(|err| format!("could not read frame length: {err}"))?;
        let length = u32::from_be_bytes(length_buf) as usize;
        let mut frame = Vec::with_capacity(4 + length);
        frame.extend_from_slice(&length_buf);
        let mut payload = vec![0u8; length];
        stream
            .read_exact(&mut payload)
            .map_err(|err| format!("could not read frame payload: {err}"))?;
        frame.extend_from_slice(&payload);
        peer::parse_message_frame(&frame)?
            .map(|(message, _)| message)
            .ok_or_else(|| "frame did not parse".to_string())
    }

    fn serve_dht_peer_values_once(socket: UdpSocket, expected_info_hash: [u8; 20], peer_port: u16) {
        let mut buffer = [0u8; 2048];
        let (length, client) = socket.recv_from(&mut buffer).expect("DHT query arrives");
        let transaction_id = assert_dht_get_peers_query(&buffer[..length], expected_info_hash);
        let compact_peer = {
            let mut out = vec![127, 0, 0, 1];
            out.extend_from_slice(&peer_port.to_be_bytes());
            out
        };
        let mut response = b"d1:rd2:id".to_vec();
        write_bencoded_bytes(&mut response, b"dhtnode-abcdefghijkl");
        response.extend_from_slice(b"5:token2:tk6:valuesl");
        write_bencoded_bytes(&mut response, &compact_peer);
        response.extend_from_slice(b"ee1:t");
        write_bencoded_bytes(&mut response, &transaction_id);
        response.extend_from_slice(b"1:y1:re");
        socket.send_to(&response, client).expect("DHT response sends");
    }

    fn assert_dht_get_peers_query(input: &[u8], expected_info_hash: [u8; 20]) -> Vec<u8> {
        let root = bencode::parse(input).expect("DHT query parses");
        assert_eq!(root.dict_get(b"y").and_then(BencodeNode::as_bytes), Some(&b"q"[..]));
        assert_eq!(
            root.dict_get(b"q").and_then(BencodeNode::as_bytes),
            Some(&b"get_peers"[..])
        );
        let args = root.dict_get(b"a").expect("DHT query has args");
        assert_eq!(
            args.dict_get(b"info_hash").and_then(BencodeNode::as_bytes),
            Some(&expected_info_hash[..])
        );
        root.dict_get(b"t")
            .and_then(BencodeNode::as_bytes)
            .expect("DHT query has transaction id")
            .to_vec()
    }

    fn write_bencoded_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
        out.extend_from_slice(bytes.len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(bytes);
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "novatorrent-{label}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("temp dir creates");
        path
    }
}
