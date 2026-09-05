use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};

use crate::torrent::{
    metadata::{self, MetadataMessageType},
    peer::{self, PeerMessage, HANDSHAKE_LEN},
    pex, piece,
};

const MAX_PEER_FRAME_LENGTH: usize = 4 * 1024 * 1024;
const MAX_METADATA_SIZE: u64 = 8 * 1024 * 1024;
const LOCAL_UT_METADATA_ID: u8 = 3;
const LOCAL_UT_PEX_ID: u8 = 4;
const DEFAULT_REQUEST_PIPELINE_DEPTH: usize = 16;
const MIN_REQUEST_PIPELINE_DEPTH: usize = 2;
const MAX_REQUEST_PIPELINE_DEPTH: usize = 500;
const REQUEST_PIPELINE_TIME_TARGET: Duration = Duration::from_secs(3);
const PEER_READ_POLL_INTERVAL: Duration = Duration::from_secs(1);
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const PEER_READ_IDLE_TIMEOUT: Duration = Duration::from_secs(8);
const INBOUND_FRAME_DEADLINE: Duration = Duration::from_secs(20);
const UPLOAD_SLOT_WAIT_TIMEOUT: Duration = Duration::from_secs(25);
const UPLOAD_SLOT_LEASE_DURATION: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct UploadGate {
    inner: Arc<UploadGateInner>,
}

#[derive(Debug)]
struct UploadGateInner {
    limit: AtomicUsize,
    next_ticket: AtomicU64,
    state: Mutex<UploadGateState>,
    available: Condvar,
}

#[derive(Debug, Default)]
struct UploadGateState {
    active: usize,
    waiting: VecDeque<u64>,
}

#[derive(Debug)]
struct UploadSlotPermit {
    inner: Arc<UploadGateInner>,
}

impl UploadGate {
    pub fn new(limit: usize) -> Self {
        Self {
            inner: Arc::new(UploadGateInner {
                limit: AtomicUsize::new(limit.max(1)),
                next_ticket: AtomicU64::new(0),
                state: Mutex::new(UploadGateState::default()),
                available: Condvar::new(),
            }),
        }
    }

    pub fn set_limit(&self, limit: usize) {
        self.inner.limit.store(limit.max(1), Ordering::Release);
        self.inner.available.notify_all();
    }

    fn try_acquire(&self) -> Result<Option<UploadSlotPermit>, String> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| "upload slot lock poisoned".to_string())?;
        if state.active >= self.inner.limit.load(Ordering::Acquire) || !state.waiting.is_empty() {
            return Ok(None);
        }
        state.active += 1;
        Ok(Some(UploadSlotPermit {
            inner: Arc::clone(&self.inner),
        }))
    }

    fn acquire_timeout(&self, timeout: Duration) -> Result<Option<UploadSlotPermit>, String> {
        let started = Instant::now();
        let ticket = self.inner.next_ticket.fetch_add(1, Ordering::Relaxed);
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| "upload slot lock poisoned".to_string())?;
        state.waiting.push_back(ticket);
        loop {
            let is_next = state.waiting.front().copied() == Some(ticket);
            if is_next && state.active < self.inner.limit.load(Ordering::Acquire) {
                state.waiting.pop_front();
                state.active += 1;
                self.inner.available.notify_all();
                return Ok(Some(UploadSlotPermit {
                    inner: Arc::clone(&self.inner),
                }));
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                state.waiting.retain(|waiting| *waiting != ticket);
                self.inner.available.notify_all();
                return Ok(None);
            }
            let (next_state, wait) = self
                .inner
                .available
                .wait_timeout(state, remaining)
                .map_err(|_| "upload slot lock poisoned while waiting".to_string())?;
            state = next_state;
            if wait.timed_out() {
                state.waiting.retain(|waiting| *waiting != ticket);
                self.inner.available.notify_all();
                return Ok(None);
            }
        }
    }

    fn has_waiters(&self) -> bool {
        self.inner
            .state
            .lock()
            .map(|state| !state.waiting.is_empty())
            .unwrap_or(false)
    }

    #[cfg(test)]
    fn waiting_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .map(|state| state.waiting.len())
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub struct BandwidthLimiter {
    inner: Arc<BandwidthLimiterInner>,
}

#[derive(Debug)]
struct BandwidthLimiterInner {
    bytes_per_second: AtomicU64,
    next_available: Mutex<Instant>,
}

impl BandwidthLimiter {
    pub fn new(bytes_per_second: Option<u64>) -> Self {
        Self {
            inner: Arc::new(BandwidthLimiterInner {
                bytes_per_second: AtomicU64::new(bytes_per_second.unwrap_or(0)),
                next_available: Mutex::new(Instant::now()),
            }),
        }
    }

    pub fn set_limit(&self, bytes_per_second: Option<u64>) {
        self.inner
            .bytes_per_second
            .store(bytes_per_second.unwrap_or(0), Ordering::Release);
        if let Ok(mut next_available) = self.inner.next_available.lock() {
            *next_available = Instant::now();
        }
    }

    pub(crate) fn throttle(
        &self,
        bytes: usize,
        cancelled: Option<&AtomicBool>,
    ) -> Result<(), String> {
        let bytes_per_second = self.inner.bytes_per_second.load(Ordering::Acquire);
        if bytes == 0 || bytes_per_second == 0 {
            return Ok(());
        }
        let spacing = Duration::from_secs_f64(bytes as f64 / bytes_per_second as f64);
        let wait = {
            let mut next_available = self
                .inner
                .next_available
                .lock()
                .map_err(|_| "bandwidth limiter lock poisoned".to_string())?;
            let now = Instant::now();
            let reservation = (*next_available).max(now);
            *next_available = reservation.checked_add(spacing).unwrap_or(reservation);
            reservation.saturating_duration_since(now)
        };
        let started = Instant::now();
        while started.elapsed() < wait {
            ensure_download_active(cancelled)?;
            std::thread::sleep(
                wait.saturating_sub(started.elapsed())
                    .min(Duration::from_millis(100)),
            );
        }
        ensure_download_active(cancelled)
    }
}

impl Drop for UploadSlotPermit {
    fn drop(&mut self) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.active = state.active.saturating_sub(1);
            self.inner.available.notify_all();
        }
    }
}

#[derive(Debug, Clone)]
pub struct PeerDownloadPlan {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub dht_port: Option<u16>,
    pub enable_pex: bool,
    pub total_length: u64,
    pub piece_length: u64,
    pub piece_hashes: Vec<[u8; 20]>,
    pub cancelled: Option<Arc<AtomicBool>>,
}

#[derive(Debug, Clone)]
pub struct PeerDownloadResult {
    pub peer_id: [u8; 20],
    pub bytes: Vec<u8>,
    pub pieces_verified: usize,
}

#[derive(Debug, Clone)]
pub struct DownloadedPiece {
    pub index: u32,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct PeerPieceDownloadResult {
    pub peer_id: [u8; 20],
    pub pieces: Vec<DownloadedPiece>,
    pub unavailable: Vec<u32>,
    pub error: Option<String>,
}

pub struct PeerDownloadConnection {
    stream: TcpStream,
    peer_id: [u8; 20],
    pieces: Vec<piece::PiecePlan>,
    availability: Vec<bool>,
    choked: bool,
    accept_dht_port: bool,
    remote_dht_port: Option<u16>,
    pex_peers: Vec<peer::PeerInfo>,
    cancelled: Option<Arc<AtomicBool>>,
    download_limiter: Option<BandwidthLimiter>,
    request_pipeline_depth: usize,
    remote_request_queue_limit: usize,
}

impl PeerDownloadConnection {
    pub fn peer_id(&self) -> [u8; 20] {
        self.peer_id
    }

    pub fn availability(&self) -> &[bool] {
        &self.availability
    }

    pub fn request_pipeline_depth(&self) -> usize {
        self.request_pipeline_depth
            .min(self.remote_request_queue_limit)
    }

    pub fn remote_request_queue_limit(&self) -> usize {
        self.remote_request_queue_limit
    }

    pub fn take_remote_dht_port(&mut self) -> Option<u16> {
        self.remote_dht_port.take()
    }

    pub fn take_pex_peers(&mut self) -> Vec<peer::PeerInfo> {
        std::mem::take(&mut self.pex_peers)
    }

    pub fn download_pieces(&mut self, wanted_pieces: &[u32]) -> PeerPieceDownloadResult {
        let mut wanted = vec![false; self.pieces.len()];
        for index in wanted_pieces {
            let Some(slot) = wanted.get_mut(*index as usize) else {
                return peer_piece_error(
                    self.peer_id,
                    Vec::new(),
                    Vec::new(),
                    format!("requested piece index is out of range: {index}"),
                );
            };
            *slot = true;
        }

        let mut unavailable = Vec::new();
        if let Err(err) = ensure_download_active(self.cancelled.as_deref()) {
            return peer_piece_error(self.peer_id, Vec::new(), unavailable, err);
        }
        if self.choked {
            if let Err(err) = wait_until_unchoked(
                &mut self.stream,
                &mut self.availability,
                &mut self.choked,
                self.accept_dht_port,
                &mut self.remote_dht_port,
                &mut self.pex_peers,
                &mut self.remote_request_queue_limit,
                self.cancelled.as_deref(),
            ) {
                return peer_piece_error(self.peer_id, Vec::new(), unavailable, err);
            }
        }

        let mut selected = Vec::new();
        for piece_plan in &self.pieces {
            if !wanted[piece_plan.index as usize] {
                continue;
            }
            if peer_has_piece(&self.availability, piece_plan.index) {
                selected.push(piece_plan.clone());
            } else {
                unavailable.push(piece_plan.index);
            }
        }

        let batch_started = Instant::now();
        let outcome = download_piece_batch_pipelined(
            &mut self.stream,
            &selected,
            self.request_pipeline_depth,
            &mut self.availability,
            &mut self.choked,
            self.accept_dht_port,
            &mut self.remote_dht_port,
            &mut self.pex_peers,
            self.cancelled.as_deref(),
            self.download_limiter.as_ref(),
            &mut self.remote_request_queue_limit,
        );
        let (mut downloaded, error) = match outcome {
            Ok(downloaded) => (downloaded, None),
            Err((downloaded, error)) => (downloaded, Some(error)),
        };
        downloaded.sort_by_key(|piece| piece.index);
        let downloaded_bytes = downloaded.iter().map(|piece| piece.bytes.len()).sum();
        self.request_pipeline_depth = adapt_request_pipeline_depth(
            self.request_pipeline_depth,
            downloaded_bytes,
            batch_started.elapsed(),
            error.is_some(),
        )
        .min(self.remote_request_queue_limit);

        if let Some(error) = error {
            return peer_piece_error(self.peer_id, downloaded, unavailable, error);
        }

        PeerPieceDownloadResult {
            peer_id: self.peer_id,
            pieces: downloaded,
            unavailable,
            error: None,
        }
    }

    pub fn download_pieces_with_round_cancel(
        &mut self,
        wanted_pieces: &[u32],
        round_cancelled: Arc<AtomicBool>,
    ) -> PeerPieceDownloadResult {
        let torrent_cancelled = self.cancelled.replace(round_cancelled);
        let result = self.download_pieces(wanted_pieces);
        self.cancelled = torrent_cancelled;
        result
    }
}

impl Drop for PeerDownloadConnection {
    fn drop(&mut self) {
        let _ = self.stream.write_all(&peer::build_not_interested());
    }
}

fn peer_piece_error(
    peer_id: [u8; 20],
    pieces: Vec<DownloadedPiece>,
    unavailable: Vec<u32>,
    error: String,
) -> PeerPieceDownloadResult {
    PeerPieceDownloadResult {
        peer_id,
        pieces,
        unavailable,
        error: Some(error),
    }
}

#[derive(Debug, Clone)]
pub struct PeerSeedPlan {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub dht_port: Option<u16>,
    pub enable_pex: bool,
    pub pex_peers: Vec<peer::PeerInfo>,
    pub piece_length: u64,
    pub bytes: Vec<u8>,
    pub available_pieces: Option<Vec<bool>>,
    pub disconnect_after_blocks: Option<usize>,
    pub block_response_delay: Option<Duration>,
}

pub type SeedBlockReader = Arc<dyn Fn(u64, u64) -> Result<Vec<u8>, String> + Send + Sync>;

#[derive(Clone)]
pub struct PeerSeedReadPlan {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub dht_port: Option<u16>,
    pub enable_pex: bool,
    pub pex_peers: Vec<peer::PeerInfo>,
    pub piece_length: u64,
    pub total_length: u64,
    pub read_block: SeedBlockReader,
    pub available_pieces: Option<Vec<bool>>,
    pub disconnect_after_blocks: Option<usize>,
    pub block_response_delay: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct PeerSeedResult {
    pub peer_id: [u8; 20],
    pub bytes_uploaded: u64,
    pub blocks_served: usize,
    pub upload_rotations: usize,
    pub remote_dht_port: Option<u16>,
}

#[derive(Debug, Clone)]
pub struct MetadataFetchPlan {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub dht_port: Option<u16>,
}

#[derive(Debug, Clone)]
pub struct MetadataFetchResult {
    pub peer_id: [u8; 20],
    pub info_bytes: Vec<u8>,
    pub pieces_received: usize,
    pub remote_dht_port: Option<u16>,
}

#[derive(Debug, Default)]
struct RemotePeerExtensions {
    dht_port: Option<u16>,
    ut_pex: Option<u8>,
}

pub fn download_from_peer(
    address: &str,
    port: u16,
    plan: PeerDownloadPlan,
) -> Result<PeerDownloadResult, String> {
    let total_length = plan.total_length;
    let piece_length = plan.piece_length;
    let piece_count = plan.piece_hashes.len();
    let wanted = (0..piece_count)
        .map(|index| u32::try_from(index).map_err(|_| "torrent has too many pieces".to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    let result = download_pieces_from_peer(address, port, plan, &wanted)?;
    if let Some(err) = result.error {
        return Err(err);
    }
    if !result.unavailable.is_empty() || result.pieces.len() != piece_count {
        return Err(format!(
            "peer does not have all requested pieces; {} unavailable",
            result.unavailable.len()
        ));
    }

    let mut bytes = vec![0u8; checked_total_length(total_length)?];
    for downloaded in &result.pieces {
        let start = downloaded.index as u64 * piece_length;
        let end = start
            .checked_add(downloaded.bytes.len() as u64)
            .ok_or_else(|| "downloaded piece offset overflow".to_string())?;
        let target = bytes
            .get_mut(start as usize..end as usize)
            .ok_or_else(|| "downloaded piece exceeds torrent length".to_string())?;
        target.copy_from_slice(&downloaded.bytes);
    }

    Ok(PeerDownloadResult {
        peer_id: result.peer_id,
        bytes,
        pieces_verified: result.pieces.len(),
    })
}

pub fn download_pieces_from_peer(
    address: &str,
    port: u16,
    plan: PeerDownloadPlan,
    wanted_pieces: &[u32],
) -> Result<PeerPieceDownloadResult, String> {
    let mut connection = connect_peer_for_download(address, port, plan)?;
    Ok(connection.download_pieces(wanted_pieces))
}

pub fn connect_peer_for_download(
    address: &str,
    port: u16,
    plan: PeerDownloadPlan,
) -> Result<PeerDownloadConnection, String> {
    connect_peer_for_download_with_limiter(address, port, plan, None)
}

pub fn connect_peer_for_download_with_limiter(
    address: &str,
    port: u16,
    plan: PeerDownloadPlan,
    download_limiter: Option<BandwidthLimiter>,
) -> Result<PeerDownloadConnection, String> {
    ensure_download_active(plan.cancelled.as_deref())?;
    let local_dht_port = plan.dht_port.filter(|port| *port != 0);
    let socket_addrs = (address, port)
        .to_socket_addrs()
        .map_err(|err| format!("could not resolve peer address: {err}"))?
        .collect::<Vec<_>>();
    let mut stream = connect_to_first_available_peer(
        &socket_addrs,
        PEER_CONNECT_TIMEOUT,
        plan.cancelled.as_deref(),
        "peer",
    )?;
    stream
        .set_read_timeout(Some(PEER_READ_POLL_INTERVAL))
        .map_err(|err| format!("could not set peer read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .map_err(|err| format!("could not set peer write timeout: {err}"))?;

    let local_handshake = peer::build_feature_handshake(
        plan.info_hash,
        plan.peer_id,
        plan.enable_pex,
        local_dht_port.is_some(),
    );
    stream
        .write_all(&local_handshake)
        .map_err(|err| format!("could not send peer handshake: {err}"))?;
    let mut handshake = [0u8; HANDSHAKE_LEN];
    read_exact_cancellable(&mut stream, &mut handshake, plan.cancelled.as_deref())
        .map_err(|err| format!("could not read peer handshake: {err}"))?;
    let handshake = peer::parse_handshake_full(&handshake)?;
    if handshake.info_hash != plan.info_hash {
        return Err("peer replied with a different info hash".to_string());
    }
    let accept_dht_port = local_dht_port.is_some() && peer::supports_dht(&handshake);
    if accept_dht_port {
        stream
            .write_all(&peer::build_port(
                local_dht_port.expect("DHT port is present"),
            ))
            .map_err(|err| format!("could not send DHT port message: {err}"))?;
    }
    let supports_extensions = peer::supports_extension_protocol(&handshake);
    let accept_pex = plan.enable_pex && supports_extensions;
    if supports_extensions {
        let local_extension_handshake = metadata::build_extension_handshake_with_pex(
            None,
            None,
            accept_pex.then_some(LOCAL_UT_PEX_ID),
        );
        stream
            .write_all(&peer::build_extended_message(0, &local_extension_handshake))
            .map_err(|err| format!("could not send PEX extension handshake: {err}"))?;
    }

    stream
        .write_all(&peer::build_interested())
        .map_err(|err| format!("could not send interested message: {err}"))?;

    let pieces = piece::build_piece_plan(plan.total_length, plan.piece_length, &plan.piece_hashes)?;
    let mut availability = vec![false; pieces.len()];
    let mut choked = true;
    let mut remote_dht_port = None;
    let mut pex_peers = Vec::new();
    let mut remote_request_queue_limit = MAX_REQUEST_PIPELINE_DEPTH;
    wait_until_unchoked(
        &mut stream,
        &mut availability,
        &mut choked,
        accept_dht_port,
        &mut remote_dht_port,
        &mut pex_peers,
        &mut remote_request_queue_limit,
        plan.cancelled.as_deref(),
    )?;

    Ok(PeerDownloadConnection {
        stream,
        peer_id: handshake.peer_id,
        pieces,
        availability,
        choked,
        accept_dht_port,
        remote_dht_port,
        pex_peers,
        cancelled: plan.cancelled,
        download_limiter,
        request_pipeline_depth: DEFAULT_REQUEST_PIPELINE_DEPTH.min(remote_request_queue_limit),
        remote_request_queue_limit,
    })
}

pub fn fetch_metadata_from_peer(
    address: &str,
    port: u16,
    plan: MetadataFetchPlan,
) -> Result<MetadataFetchResult, String> {
    let local_dht_port = plan.dht_port.filter(|port| *port != 0);
    let socket_addrs = (address, port)
        .to_socket_addrs()
        .map_err(|err| format!("could not resolve metadata peer address: {err}"))?
        .collect::<Vec<_>>();
    let mut stream = connect_to_first_available_peer(
        &socket_addrs,
        PEER_CONNECT_TIMEOUT,
        None,
        "metadata peer",
    )?;
    stream
        .set_read_timeout(Some(Duration::from_secs(8)))
        .map_err(|err| format!("could not set metadata peer read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .map_err(|err| format!("could not set metadata peer write timeout: {err}"))?;

    stream
        .write_all(&peer::build_feature_handshake(
            plan.info_hash,
            plan.peer_id,
            true,
            local_dht_port.is_some(),
        ))
        .map_err(|err| format!("could not send metadata peer handshake: {err}"))?;
    let mut handshake = [0u8; HANDSHAKE_LEN];
    stream
        .read_exact(&mut handshake)
        .map_err(|err| format!("could not read metadata peer handshake: {err}"))?;
    let handshake = peer::parse_handshake_full(&handshake)?;
    if handshake.info_hash != plan.info_hash {
        return Err("metadata peer replied with a different info hash".to_string());
    }
    if !peer::supports_extension_protocol(&handshake) {
        return Err("metadata peer does not advertise extension protocol support".to_string());
    }
    let accept_dht_port = local_dht_port.is_some() && peer::supports_dht(&handshake);
    if accept_dht_port {
        stream
            .write_all(&peer::build_port(
                local_dht_port.expect("DHT port is present"),
            ))
            .map_err(|err| format!("could not send metadata peer DHT port: {err}"))?;
    }

    let local_handshake = metadata::build_extension_handshake(LOCAL_UT_METADATA_ID, None);
    stream
        .write_all(&peer::build_extended_message(0, &local_handshake))
        .map_err(|err| format!("could not send extension handshake: {err}"))?;

    let mut remote_dht_port = None;
    let extension =
        read_metadata_extension_handshake(&mut stream, accept_dht_port, &mut remote_dht_port)?;
    let remote_metadata_id = extension
        .ut_metadata
        .ok_or_else(|| "metadata peer did not advertise ut_metadata".to_string())?;
    let metadata_size = extension
        .metadata_size
        .ok_or_else(|| "metadata peer did not advertise metadata_size".to_string())?;
    if metadata_size == 0 || metadata_size > MAX_METADATA_SIZE {
        return Err(format!("metadata size is not supported: {metadata_size}"));
    }

    let piece_count = metadata::metadata_piece_count(metadata_size);
    let mut info_bytes = vec![0u8; metadata_size as usize];
    for piece_index in 0..piece_count {
        let request = metadata::build_metadata_request(piece_index as u32);
        stream
            .write_all(&peer::build_extended_message(remote_metadata_id, &request))
            .map_err(|err| format!("could not request metadata piece {piece_index}: {err}"))?;
        let message = read_metadata_piece(
            &mut stream,
            remote_metadata_id,
            piece_index as u32,
            metadata_size,
            accept_dht_port,
            &mut remote_dht_port,
        )?;
        let start = piece_index * metadata::UT_METADATA_BLOCK_SIZE;
        let end = start + message.data.len();
        let target = info_bytes
            .get_mut(start..end)
            .ok_or_else(|| "metadata piece exceeds advertised metadata size".to_string())?;
        target.copy_from_slice(&message.data);
    }

    if !metadata::verify_metadata_info_hash(&info_bytes, plan.info_hash) {
        return Err("metadata info dictionary failed info-hash verification".to_string());
    }

    Ok(MetadataFetchResult {
        peer_id: handshake.peer_id,
        info_bytes,
        pieces_received: piece_count,
        remote_dht_port,
    })
}

pub fn seed_single_peer(
    listener: TcpListener,
    plan: PeerSeedPlan,
) -> Result<PeerSeedResult, String> {
    let (mut stream, _) = listener
        .accept()
        .map_err(|err| format!("could not accept peer connection: {err}"))?;
    let handshake = read_incoming_handshake(&mut stream)?;
    seed_connected_peer(stream, handshake, plan)
}

pub fn read_incoming_handshake(stream: &mut TcpStream) -> Result<peer::PeerHandshake, String> {
    stream
        .set_nodelay(true)
        .map_err(|err| format!("could not enable TCP_NODELAY for incoming peer: {err}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .map_err(|err| format!("could not set incoming peer read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .map_err(|err| format!("could not set incoming peer write timeout: {err}"))?;
    let mut handshake = [0u8; HANDSHAKE_LEN];
    read_exact_until(
        stream,
        &mut handshake,
        Instant::now() + INBOUND_FRAME_DEADLINE,
    )
    .map_err(|err| format!("could not read incoming peer handshake: {err}"))?;
    peer::parse_handshake_full(&handshake)
}

pub fn seed_connected_peer(
    stream: TcpStream,
    handshake: peer::PeerHandshake,
    plan: PeerSeedPlan,
) -> Result<PeerSeedResult, String> {
    seed_connected_peer_with_gate(stream, handshake, plan, None, None)
}

pub fn seed_connected_peer_with_gate(
    stream: TcpStream,
    handshake: peer::PeerHandshake,
    plan: PeerSeedPlan,
    upload_gate: Option<&UploadGate>,
    upload_limiter: Option<&BandwidthLimiter>,
) -> Result<PeerSeedResult, String> {
    let bytes = Arc::new(plan.bytes);
    let total_length = bytes.len() as u64;
    seed_connected_peer_with_reader_and_gate(
        stream,
        handshake,
        PeerSeedReadPlan {
            info_hash: plan.info_hash,
            peer_id: plan.peer_id,
            dht_port: plan.dht_port,
            enable_pex: plan.enable_pex,
            pex_peers: plan.pex_peers,
            piece_length: plan.piece_length,
            total_length,
            read_block: {
                let bytes = Arc::clone(&bytes);
                Arc::new(move |offset, length| {
                    let start = usize::try_from(offset).map_err(|_| {
                        "seed block offset is too large for this platform".to_string()
                    })?;
                    let length = usize::try_from(length).map_err(|_| {
                        "seed block length is too large for this platform".to_string()
                    })?;
                    let end = start
                        .checked_add(length)
                        .ok_or_else(|| "seed block range overflowed".to_string())?;
                    bytes
                        .get(start..end)
                        .map(|block| block.to_vec())
                        .ok_or_else(|| "leecher requested bytes outside the torrent".to_string())
                })
            },
            available_pieces: plan.available_pieces,
            disconnect_after_blocks: plan.disconnect_after_blocks,
            block_response_delay: plan.block_response_delay,
        },
        upload_gate,
        upload_limiter,
    )
}

pub fn seed_connected_peer_with_reader_and_gate(
    mut stream: TcpStream,
    handshake: peer::PeerHandshake,
    plan: PeerSeedReadPlan,
    upload_gate: Option<&UploadGate>,
    upload_limiter: Option<&BandwidthLimiter>,
) -> Result<PeerSeedResult, String> {
    let piece_length = checked_piece_length(plan.piece_length)?;
    let total_length = usize::try_from(plan.total_length)
        .map_err(|_| "seed torrent is too large for this platform".to_string())?;
    let piece_count = total_length.div_ceil(piece_length);
    let available_pieces = plan
        .available_pieces
        .clone()
        .unwrap_or_else(|| vec![true; piece_count]);
    if available_pieces.len() != piece_count {
        return Err(format!(
            "seed availability length mismatch: expected {piece_count}, got {}",
            available_pieces.len()
        ));
    }
    let expected_upload_bytes = available_pieces
        .iter()
        .enumerate()
        .filter(|(_, available)| **available)
        .map(|(index, _)| {
            let start = index * piece_length;
            total_length.saturating_sub(start).min(piece_length) as u64
        })
        .sum::<u64>();
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .map_err(|err| format!("could not set seed peer read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .map_err(|err| format!("could not set seed peer write timeout: {err}"))?;

    if handshake.info_hash != plan.info_hash {
        return Err("leecher requested a different info hash".to_string());
    }

    let local_dht_port = plan.dht_port.filter(|port| *port != 0);
    let accept_dht_port = local_dht_port.is_some() && peer::supports_dht(&handshake);
    let accept_pex = plan.enable_pex && peer::supports_extension_protocol(&handshake);
    let local_handshake = peer::build_feature_handshake(
        plan.info_hash,
        plan.peer_id,
        accept_pex,
        local_dht_port.is_some(),
    );
    stream
        .write_all(&local_handshake)
        .map_err(|err| format!("could not send seed handshake: {err}"))?;
    if accept_dht_port {
        stream
            .write_all(&peer::build_port(
                local_dht_port.expect("DHT port is present"),
            ))
            .map_err(|err| format!("could not send seed DHT port message: {err}"))?;
    }
    if accept_pex {
        let local_extension_handshake =
            metadata::build_extension_handshake_with_pex(None, None, Some(LOCAL_UT_PEX_ID));
        stream
            .write_all(&peer::build_extended_message(0, &local_extension_handshake))
            .map_err(|err| format!("could not send seed PEX extension handshake: {err}"))?;
    }

    let remote_extensions = wait_for_interested(&mut stream, accept_dht_port, accept_pex)?;
    let mut remote_dht_port = remote_extensions.dht_port;
    if accept_pex {
        if let Some(remote_pex_id) = remote_extensions.ut_pex {
            if !plan.pex_peers.is_empty() {
                let pex_peers = plan
                    .pex_peers
                    .iter()
                    .map(|peer| pex::PexPeer {
                        address: peer.address.clone(),
                        port: peer.port,
                        flags: if peer.progress >= 1.0 { 0x02 } else { 0 },
                    })
                    .collect::<Vec<_>>();
                let payload = pex::build_pex_message(&pex_peers, &[])?;
                stream
                    .write_all(&peer::build_extended_message(remote_pex_id, &payload))
                    .map_err(|err| format!("could not send seed PEX message: {err}"))?;
            }
        }
    }
    stream
        .write_all(&peer::build_bitfield(&build_availability_bitfield(
            &available_pieces,
        )))
        .map_err(|err| format!("could not send seed bitfield: {err}"))?;
    let mut upload_slot = if let Some(gate) = upload_gate {
        match gate.try_acquire()? {
            Some(permit) => Some(permit),
            None => {
                stream
                    .write_all(&peer::build_choke())
                    .map_err(|err| format!("could not keep leecher choked: {err}"))?;
                Some(
                    gate.acquire_timeout(UPLOAD_SLOT_WAIT_TIMEOUT)?
                        .ok_or_else(|| "all upload slots remained busy".to_string())?,
                )
            }
        }
    } else {
        None
    };
    stream
        .write_all(&peer::build_unchoke())
        .map_err(|err| format!("could not unchoke leecher: {err}"))?;

    let mut bytes_uploaded = 0u64;
    let mut blocks_served = 0usize;
    let mut upload_rotations = 0usize;
    let mut upload_lease_started = Instant::now();
    while bytes_uploaded < expected_upload_bytes {
        match read_peer_message(&mut stream)? {
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                if !available_pieces
                    .get(index as usize)
                    .copied()
                    .unwrap_or(false)
                {
                    return Err(format!("leecher requested unavailable piece {index}"));
                }
                if length == 0 || length > piece::DEFAULT_BLOCK_SIZE {
                    return Err(format!("leecher requested invalid block length {length}"));
                }
                let piece_start = (index as usize)
                    .checked_mul(piece_length)
                    .ok_or_else(|| "leecher piece offset overflowed".to_string())?;
                let piece_end = piece_start
                    .checked_add(piece_length)
                    .map(|end| end.min(total_length))
                    .ok_or_else(|| "leecher piece end overflowed".to_string())?;
                let absolute = piece_start
                    .checked_add(begin as usize)
                    .ok_or_else(|| "leecher request offset overflowed".to_string())?;
                let end = absolute
                    .checked_add(length as usize)
                    .ok_or_else(|| "leecher request overflowed".to_string())?;
                if absolute < piece_start || end > piece_end {
                    return Err("leecher request crosses a piece boundary".to_string());
                }
                let block = (plan.read_block)(absolute as u64, length as u64)?;
                if block.len() != length as usize {
                    return Err(format!(
                        "seed block reader returned {} byte(s), expected {length}",
                        block.len()
                    ));
                }
                if let Some(limiter) = upload_limiter {
                    limiter.throttle(block.len(), None)?;
                }
                if let Some(delay) = plan.block_response_delay {
                    std::thread::sleep(delay);
                }
                stream
                    .write_all(&peer::build_piece(index, begin, &block))
                    .map_err(|err| format!("could not send piece block: {err}"))?;
                bytes_uploaded += block.len() as u64;
                blocks_served += 1;
                if let Some(gate) = upload_gate {
                    if upload_slot.is_some()
                        && upload_lease_started.elapsed() >= UPLOAD_SLOT_LEASE_DURATION
                        && gate.has_waiters()
                    {
                        stream
                            .write_all(&peer::build_choke())
                            .map_err(|err| format!("could not rotate upload slot: {err}"))?;
                        drop(upload_slot.take());
                        upload_slot =
                            Some(gate.acquire_timeout(UPLOAD_SLOT_WAIT_TIMEOUT)?.ok_or_else(
                                || "timed out while rotating upload slot".to_string(),
                            )?);
                        stream.write_all(&peer::build_unchoke()).map_err(|err| {
                            format!("could not resume rotated upload slot: {err}")
                        })?;
                        upload_rotations += 1;
                        upload_lease_started = Instant::now();
                    }
                }
                if plan
                    .disconnect_after_blocks
                    .is_some_and(|limit| blocks_served >= limit)
                {
                    return Ok(PeerSeedResult {
                        peer_id: handshake.peer_id,
                        bytes_uploaded,
                        blocks_served,
                        upload_rotations,
                        remote_dht_port,
                    });
                }
            }
            PeerMessage::Cancel { .. } | PeerMessage::KeepAlive => {}
            PeerMessage::NotInterested => break,
            PeerMessage::Choke => {}
            PeerMessage::Port { port } if accept_dht_port && port != 0 => {
                remote_dht_port = Some(port);
            }
            PeerMessage::Interested
            | PeerMessage::Unchoke
            | PeerMessage::Have { .. }
            | PeerMessage::Bitfield(_)
            | PeerMessage::Piece { .. }
            | PeerMessage::Port { .. }
            | PeerMessage::Extended { .. }
            | PeerMessage::Unknown { .. } => {}
        }
    }

    Ok(PeerSeedResult {
        peer_id: handshake.peer_id,
        bytes_uploaded,
        blocks_served,
        upload_rotations,
        remote_dht_port,
    })
}

fn read_metadata_extension_handshake(
    stream: &mut TcpStream,
    accept_dht_port: bool,
    remote_dht_port: &mut Option<u16>,
) -> Result<metadata::ExtensionHandshake, String> {
    for _ in 0..64 {
        match read_peer_message(stream)? {
            PeerMessage::Extended {
                extension_id: 0,
                payload,
            } => return metadata::parse_extension_handshake(&payload),
            PeerMessage::Port { port } if accept_dht_port && port != 0 => {
                *remote_dht_port = Some(port);
            }
            PeerMessage::KeepAlive
            | PeerMessage::Choke
            | PeerMessage::Unchoke
            | PeerMessage::Have { .. }
            | PeerMessage::Bitfield(_)
            | PeerMessage::Interested
            | PeerMessage::NotInterested
            | PeerMessage::Request { .. }
            | PeerMessage::Piece { .. }
            | PeerMessage::Cancel { .. }
            | PeerMessage::Port { .. }
            | PeerMessage::Extended { .. }
            | PeerMessage::Unknown { .. } => {}
        }
    }
    Err("metadata peer did not send an extension handshake".to_string())
}

fn read_metadata_piece(
    stream: &mut TcpStream,
    remote_metadata_id: u8,
    expected_piece: u32,
    metadata_size: u64,
    accept_dht_port: bool,
    remote_dht_port: &mut Option<u16>,
) -> Result<metadata::MetadataMessage, String> {
    for _ in 0..128 {
        match read_peer_message(stream)? {
            PeerMessage::Extended {
                extension_id,
                payload,
            } if extension_id == remote_metadata_id => {
                let message = metadata::parse_metadata_message(&payload)?;
                if message.piece != expected_piece {
                    continue;
                }
                match message.message_type {
                    MetadataMessageType::Data => {
                        if message.total_size != Some(metadata_size) {
                            return Err(
                                "metadata piece total_size does not match extension handshake"
                                    .to_string(),
                            );
                        }
                        return Ok(message);
                    }
                    MetadataMessageType::Reject => {
                        return Err(format!("metadata peer rejected piece {expected_piece}"));
                    }
                    MetadataMessageType::Request => {}
                }
            }
            PeerMessage::Port { port } if accept_dht_port && port != 0 => {
                *remote_dht_port = Some(port);
            }
            PeerMessage::KeepAlive
            | PeerMessage::Choke
            | PeerMessage::Unchoke
            | PeerMessage::Have { .. }
            | PeerMessage::Bitfield(_)
            | PeerMessage::Interested
            | PeerMessage::NotInterested
            | PeerMessage::Request { .. }
            | PeerMessage::Piece { .. }
            | PeerMessage::Cancel { .. }
            | PeerMessage::Port { .. }
            | PeerMessage::Extended { .. }
            | PeerMessage::Unknown { .. } => {}
        }
    }
    Err(format!("metadata peer did not send piece {expected_piece}"))
}

fn wait_until_unchoked(
    stream: &mut TcpStream,
    availability: &mut [bool],
    choked: &mut bool,
    accept_dht_port: bool,
    remote_dht_port: &mut Option<u16>,
    pex_peers: &mut Vec<peer::PeerInfo>,
    remote_request_queue_limit: &mut usize,
    cancelled: Option<&AtomicBool>,
) -> Result<(), String> {
    while *choked {
        ensure_download_active(cancelled)?;
        let message = read_peer_message_cancellable(stream, cancelled)?;
        update_peer_state(
            message,
            availability,
            choked,
            accept_dht_port,
            remote_dht_port,
            pex_peers,
            remote_request_queue_limit,
        )?;
    }
    Ok(())
}

struct BatchPieceAssembly {
    plan: piece::PiecePlan,
    bytes: Vec<u8>,
    remaining_blocks: usize,
}

fn download_piece_batch_pipelined(
    stream: &mut TcpStream,
    piece_plans: &[piece::PiecePlan],
    pipeline_depth: usize,
    availability: &mut [bool],
    choked: &mut bool,
    accept_dht_port: bool,
    remote_dht_port: &mut Option<u16>,
    pex_peers: &mut Vec<peer::PeerInfo>,
    cancelled: Option<&AtomicBool>,
    download_limiter: Option<&BandwidthLimiter>,
    remote_request_queue_limit: &mut usize,
) -> Result<Vec<DownloadedPiece>, (Vec<DownloadedPiece>, String)> {
    let mut assemblies = piece_plans
        .iter()
        .cloned()
        .map(|plan| BatchPieceAssembly {
            bytes: vec![0u8; plan.length as usize],
            remaining_blocks: plan.blocks.len(),
            plan,
        })
        .collect::<Vec<_>>();
    let requests = piece_plans
        .iter()
        .flat_map(|plan| plan.blocks.iter().cloned())
        .collect::<Vec<_>>();
    let mut next_request = 0usize;
    let mut pending = Vec::<piece::BlockRequest>::new();
    let mut downloaded = Vec::new();

    while next_request < requests.len() || !pending.is_empty() {
        if let Err(error) = ensure_download_active(cancelled) {
            cancel_pending_batch_requests(stream, &pending);
            return Err((downloaded, error));
        }
        let queue_limit = pipeline_depth.min(*remote_request_queue_limit).max(1);
        while pending.len() < queue_limit && next_request < requests.len() {
            let request = requests[next_request].clone();
            if let Err(error) = stream.write_all(&peer::build_request(
                request.piece_index,
                request.begin,
                request.length,
            )) {
                cancel_pending_batch_requests(stream, &pending);
                return Err((downloaded, format!("could not send piece request: {error}")));
            }
            pending.push(request);
            next_request += 1;
        }

        let message = match read_peer_message_cancellable(stream, cancelled) {
            Ok(message) => message,
            Err(read_error) => {
                cancel_pending_batch_requests(stream, &pending);
                let error = ensure_download_active(cancelled)
                    .err()
                    .unwrap_or(read_error);
                return Err((downloaded, error));
            }
        };
        match message {
            PeerMessage::Piece {
                index,
                begin,
                block,
            } => {
                let Some(pending_index) = pending
                    .iter()
                    .position(|request| request.piece_index == index && request.begin == begin)
                else {
                    continue;
                };
                let request = pending.swap_remove(pending_index);
                if block.len() != request.length as usize {
                    cancel_pending_batch_requests(stream, &pending);
                    return Err((
                        downloaded,
                        format!(
                            "peer returned block length {} for piece {index} offset {begin}; expected {}",
                            block.len(),
                            request.length
                        ),
                    ));
                }
                let Some(assembly) = assemblies.iter_mut().find(|item| item.plan.index == index)
                else {
                    continue;
                };
                let start = begin as usize;
                let Some(end) = start.checked_add(block.len()) else {
                    cancel_pending_batch_requests(stream, &pending);
                    return Err((downloaded, "peer block offset overflowed".to_string()));
                };
                let Some(target) = assembly.bytes.get_mut(start..end) else {
                    cancel_pending_batch_requests(stream, &pending);
                    return Err((
                        downloaded,
                        format!("peer returned bytes outside piece {index} at offset {begin}"),
                    ));
                };
                target.copy_from_slice(&block);
                assembly.remaining_blocks = assembly.remaining_blocks.saturating_sub(1);
                if let Some(limiter) = download_limiter {
                    if let Err(error) = limiter.throttle(block.len(), cancelled) {
                        cancel_pending_batch_requests(stream, &pending);
                        return Err((downloaded, error));
                    }
                }
                if assembly.remaining_blocks == 0 {
                    if !piece::verify_piece(&assembly.bytes, assembly.plan.hash) {
                        cancel_pending_batch_requests(stream, &pending);
                        return Err((
                            downloaded,
                            format!("piece {index} failed SHA-1 verification"),
                        ));
                    }
                    downloaded.push(DownloadedPiece {
                        index,
                        bytes: std::mem::take(&mut assembly.bytes),
                    });
                }
            }
            other => {
                if let Err(error) = update_peer_state(
                    other,
                    availability,
                    choked,
                    accept_dht_port,
                    remote_dht_port,
                    pex_peers,
                    remote_request_queue_limit,
                ) {
                    cancel_pending_batch_requests(stream, &pending);
                    return Err((downloaded, error));
                }
            }
        }
        if *choked {
            cancel_pending_batch_requests(stream, &pending);
            return Err((
                downloaded,
                "peer choked with pipelined block requests outstanding".to_string(),
            ));
        }
    }

    Ok(downloaded)
}

fn cancel_pending_batch_requests(stream: &mut TcpStream, pending: &[piece::BlockRequest]) {
    for request in pending {
        let _ = stream.write_all(&peer::build_cancel(
            request.piece_index,
            request.begin,
            request.length,
        ));
    }
}

fn download_piece_pipelined(
    stream: &mut TcpStream,
    piece_plan: &piece::PiecePlan,
    pipeline_depth: usize,
    availability: &mut [bool],
    choked: &mut bool,
    accept_dht_port: bool,
    remote_dht_port: &mut Option<u16>,
    pex_peers: &mut Vec<peer::PeerInfo>,
    cancelled: Option<&AtomicBool>,
    download_limiter: Option<&BandwidthLimiter>,
    remote_request_queue_limit: &mut usize,
) -> Result<Vec<u8>, String> {
    let mut piece_bytes = vec![0u8; piece_plan.length as usize];
    let mut next_request = 0usize;
    let mut pending = Vec::<(u32, u32)>::new();
    let pipeline_depth = pipeline_depth
        .clamp(MIN_REQUEST_PIPELINE_DEPTH, MAX_REQUEST_PIPELINE_DEPTH)
        .min((*remote_request_queue_limit).max(1));

    while next_request < piece_plan.blocks.len() || !pending.is_empty() {
        while next_request < piece_plan.blocks.len() && pending.len() < pipeline_depth {
            if let Err(err) = ensure_download_active(cancelled) {
                cancel_pending_requests(stream, piece_plan.index, &pending);
                return Err(err);
            }
            let block = &piece_plan.blocks[next_request];
            if let Err(err) = stream.write_all(&peer::build_request(
                block.piece_index,
                block.begin,
                block.length,
            )) {
                cancel_pending_requests(stream, piece_plan.index, &pending);
                return Err(format!("could not send piece request: {err}"));
            }
            pending.push((block.begin, block.length));
            next_request += 1;
        }

        if let Err(err) = ensure_download_active(cancelled) {
            cancel_pending_requests(stream, piece_plan.index, &pending);
            return Err(err);
        }
        let message = match read_peer_message_cancellable(stream, cancelled) {
            Ok(message) => message,
            Err(read_error) => {
                if let Err(cancel_error) = ensure_download_active(cancelled) {
                    cancel_pending_requests(stream, piece_plan.index, &pending);
                    return Err(cancel_error);
                }
                return Err(read_error);
            }
        };
        match message {
            PeerMessage::Piece {
                index,
                begin: block_begin,
                block,
            } => {
                if index != piece_plan.index {
                    continue;
                }
                let Some(pending_index) =
                    pending.iter().position(|(begin, _)| *begin == block_begin)
                else {
                    continue;
                };
                let expected_length = pending[pending_index].1 as usize;
                if block.len() != expected_length {
                    return Err(format!(
                        "peer returned block length {} for piece {} offset {}; expected {expected_length}",
                        block.len(),
                        piece_plan.index,
                        block_begin
                    ));
                }
                let start = block_begin as usize;
                let end = start
                    .checked_add(block.len())
                    .ok_or_else(|| "peer block offset overflowed".to_string())?;
                let target = piece_bytes.get_mut(start..end).ok_or_else(|| {
                    format!(
                        "peer returned bytes outside piece {} at offset {block_begin}",
                        piece_plan.index
                    )
                })?;
                target.copy_from_slice(&block);
                pending.swap_remove(pending_index);
                if let Some(limiter) = download_limiter {
                    if let Err(err) = limiter.throttle(block.len(), cancelled) {
                        cancel_pending_requests(stream, piece_plan.index, &pending);
                        return Err(err);
                    }
                }
            }
            other => update_peer_state(
                other,
                availability,
                choked,
                accept_dht_port,
                remote_dht_port,
                pex_peers,
                remote_request_queue_limit,
            )?,
        }
        if *choked {
            return Err("peer choked with pipelined block requests outstanding".to_string());
        }
    }

    Ok(piece_bytes)
}

fn adapt_request_pipeline_depth(
    current: usize,
    bytes: usize,
    elapsed: Duration,
    failed: bool,
) -> usize {
    let current = current.clamp(MIN_REQUEST_PIPELINE_DEPTH, MAX_REQUEST_PIPELINE_DEPTH);
    if failed {
        return (current / 2).max(MIN_REQUEST_PIPELINE_DEPTH);
    }

    if bytes == 0 {
        return current.saturating_sub(1).max(MIN_REQUEST_PIPELINE_DEPTH);
    }

    let elapsed_micros = elapsed.as_micros().max(1);
    let bytes_per_second = (bytes as u128).saturating_mul(1_000_000) / elapsed_micros;
    let target_bytes =
        bytes_per_second.saturating_mul(REQUEST_PIPELINE_TIME_TARGET.as_micros()) / 1_000_000;
    let block_size = piece::DEFAULT_BLOCK_SIZE as u128;
    let target = target_bytes
        .saturating_add(block_size - 1)
        .checked_div(block_size)
        .unwrap_or(MAX_REQUEST_PIPELINE_DEPTH as u128)
        .min(MAX_REQUEST_PIPELINE_DEPTH as u128) as usize;
    let target = target.clamp(MIN_REQUEST_PIPELINE_DEPTH, MAX_REQUEST_PIPELINE_DEPTH);

    // Smooth toward the measured bandwidth/time product. Increasing by one quarter
    // of the gap fills fast paths promptly without letting one burst allocate the
    // maximum queue. Decreases use the same damping; failures above remain immediate.
    if target > current {
        current + (target - current).div_ceil(4)
    } else if target < current {
        current - (current - target).div_ceil(4)
    } else {
        current
    }
}

fn connect_to_first_available_peer(
    addresses: &[SocketAddr],
    timeout: Duration,
    cancelled: Option<&AtomicBool>,
    label: &str,
) -> Result<TcpStream, String> {
    if addresses.is_empty() {
        return Err(format!("{label} address did not resolve"));
    }

    let mut failures = Vec::new();
    let mut attempted = Vec::new();
    for address in addresses {
        if attempted.contains(address) {
            continue;
        }
        attempted.push(*address);
        ensure_download_active(cancelled)?;
        match TcpStream::connect_timeout(address, timeout) {
            Ok(stream) => match stream.set_nodelay(true) {
                Ok(()) => return Ok(stream),
                Err(err) => failures.push(format!(
                    "{address}: connected but could not enable TCP_NODELAY: {err}"
                )),
            },
            Err(err) => failures.push(format!("{address}: {err}")),
        }
    }

    Err(format!(
        "could not connect to {label} using {} resolved address(es): {}",
        attempted.len(),
        failures.join("; ")
    ))
}

fn cancel_pending_requests(stream: &mut TcpStream, piece_index: u32, pending: &[(u32, u32)]) {
    for (begin, length) in pending {
        let _ = stream.write_all(&peer::build_cancel(piece_index, *begin, *length));
    }
}

fn merge_pex_peers(existing: &mut Vec<peer::PeerInfo>, next: Vec<peer::PeerInfo>) {
    for peer in next {
        let duplicate = existing
            .iter()
            .any(|current| current.address == peer.address);
        if !duplicate {
            existing.push(peer);
        }
    }
}

fn update_peer_state(
    message: PeerMessage,
    availability: &mut [bool],
    choked: &mut bool,
    accept_dht_port: bool,
    remote_dht_port: &mut Option<u16>,
    pex_peers: &mut Vec<peer::PeerInfo>,
    remote_request_queue_limit: &mut usize,
) -> Result<(), String> {
    match message {
        PeerMessage::KeepAlive => {}
        PeerMessage::Choke => *choked = true,
        PeerMessage::Unchoke => *choked = false,
        PeerMessage::Have { index } => {
            let piece = availability
                .get_mut(index as usize)
                .ok_or_else(|| format!("peer advertised out-of-range piece {index}"))?;
            *piece = true;
        }
        PeerMessage::Bitfield(bitfield) => apply_bitfield(&bitfield, availability)?,
        PeerMessage::Piece { .. } => {}
        PeerMessage::Port { port } if accept_dht_port && port != 0 => {
            *remote_dht_port = Some(port);
        }
        PeerMessage::Extended {
            extension_id: 0,
            payload,
        } => {
            if let Some(limit) = metadata::parse_extension_handshake(&payload)?.request_queue_limit
            {
                *remote_request_queue_limit = limit.clamp(1, MAX_REQUEST_PIPELINE_DEPTH);
            }
        }
        PeerMessage::Extended {
            extension_id,
            payload,
        } if extension_id == LOCAL_UT_PEX_ID => {
            if let Ok(message) = pex::parse_pex_message(&payload) {
                merge_pex_peers(pex_peers, pex::pex_peers_to_peer_info(&message.added));
            }
        }
        PeerMessage::Interested
        | PeerMessage::NotInterested
        | PeerMessage::Request { .. }
        | PeerMessage::Cancel { .. }
        | PeerMessage::Port { .. }
        | PeerMessage::Extended { .. }
        | PeerMessage::Unknown { .. } => {}
    }
    Ok(())
}

fn read_peer_message_cancellable(
    stream: &mut TcpStream,
    cancelled: Option<&AtomicBool>,
) -> Result<PeerMessage, String> {
    let mut length_buf = [0u8; 4];
    read_exact_cancellable(stream, &mut length_buf, cancelled)
        .map_err(|err| format!("could not read peer frame length: {err}"))?;
    let length = u32::from_be_bytes(length_buf) as usize;
    if length > MAX_PEER_FRAME_LENGTH {
        return Err(format!("peer frame is too large: {length} bytes"));
    }
    let mut frame = Vec::with_capacity(4 + length);
    frame.extend_from_slice(&length_buf);
    let mut payload = vec![0u8; length];
    read_exact_cancellable(stream, &mut payload, cancelled)
        .map_err(|err| format!("could not read peer frame payload: {err}"))?;
    frame.extend_from_slice(&payload);
    let Some((message, _)) = peer::parse_message_frame(&frame)? else {
        return Err("complete peer frame did not parse".to_string());
    };
    Ok(message)
}

fn read_exact_cancellable(
    stream: &mut TcpStream,
    buffer: &mut [u8],
    cancelled: Option<&AtomicBool>,
) -> Result<(), String> {
    let mut filled = 0usize;
    let mut last_progress = std::time::Instant::now();
    while filled < buffer.len() {
        ensure_download_active(cancelled)?;
        match stream.read(&mut buffer[filled..]) {
            Ok(0) => return Err("peer closed the connection".to_string()),
            Ok(read) => {
                filled += read;
                last_progress = std::time::Instant::now();
            }
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if last_progress.elapsed() >= PEER_READ_IDLE_TIMEOUT {
                    return Err(format!(
                        "peer made no read progress for {} seconds",
                        PEER_READ_IDLE_TIMEOUT.as_secs()
                    ));
                }
            }
            Err(err) => return Err(err.to_string()),
        }
    }
    Ok(())
}

pub(crate) fn read_peer_message(stream: &mut TcpStream) -> Result<PeerMessage, String> {
    let deadline = Instant::now() + INBOUND_FRAME_DEADLINE;
    let mut length_buf = [0u8; 4];
    read_exact_until(stream, &mut length_buf, deadline)
        .map_err(|err| format!("could not read peer frame length: {err}"))?;
    let length = u32::from_be_bytes(length_buf) as usize;
    if length > MAX_PEER_FRAME_LENGTH {
        return Err(format!("peer frame is too large: {length} bytes"));
    }
    let mut frame = Vec::with_capacity(4 + length);
    frame.extend_from_slice(&length_buf);
    let mut payload = vec![0u8; length];
    read_exact_until(stream, &mut payload, deadline)
        .map_err(|err| format!("could not read peer frame payload: {err}"))?;
    frame.extend_from_slice(&payload);
    let Some((message, _)) = peer::parse_message_frame(&frame)? else {
        return Err("complete peer frame did not parse".to_string());
    };
    Ok(message)
}

fn read_exact_until(
    stream: &mut TcpStream,
    buffer: &mut [u8],
    deadline: Instant,
) -> Result<(), String> {
    let mut filled = 0usize;
    while filled < buffer.len() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| "peer frame deadline expired".to_string())?;
        stream
            .set_read_timeout(Some(remaining))
            .map_err(|err| format!("could not set peer frame deadline: {err}"))?;
        match stream.read(&mut buffer[filled..]) {
            Ok(0) => return Err("peer closed the connection".to_string()),
            Ok(read) => filled += read,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return Err("peer frame deadline expired".to_string());
            }
            Err(err) => return Err(err.to_string()),
        }
    }
    Ok(())
}

fn wait_for_interested(
    stream: &mut TcpStream,
    accept_dht_port: bool,
    accept_pex: bool,
) -> Result<RemotePeerExtensions, String> {
    let mut remote = RemotePeerExtensions::default();
    loop {
        match read_peer_message(stream)? {
            PeerMessage::Interested => return Ok(remote),
            PeerMessage::Port { port } if accept_dht_port && port != 0 => {
                remote.dht_port = Some(port);
            }
            PeerMessage::Extended {
                extension_id: 0,
                payload,
            } if accept_pex => {
                remote.ut_pex = metadata::parse_extension_handshake(&payload)?.ut_pex;
            }
            PeerMessage::KeepAlive => {}
            PeerMessage::NotInterested => {
                return Err("leecher is not interested".to_string());
            }
            _ => {}
        }
    }
}

fn apply_bitfield(bitfield: &[u8], availability: &mut [bool]) -> Result<(), String> {
    let expected_length = availability.len().div_ceil(8);
    if bitfield.len() != expected_length {
        return Err(format!(
            "peer bitfield length mismatch: expected {expected_length}, got {}",
            bitfield.len()
        ));
    }
    let used_bits = availability.len() % 8;
    if used_bits != 0 {
        let unused_mask = (1u8 << (8 - used_bits)) - 1;
        if bitfield.last().is_some_and(|byte| byte & unused_mask != 0) {
            return Err("peer bitfield has non-zero spare bits".to_string());
        }
    }
    for (index, piece) in availability.iter_mut().enumerate() {
        let byte = bitfield[index / 8];
        let mask = 0x80 >> (index % 8);
        *piece = byte & mask != 0;
    }
    Ok(())
}

fn peer_has_piece(availability: &[bool], piece_index: u32) -> bool {
    availability
        .get(piece_index as usize)
        .copied()
        .unwrap_or(false)
}

fn checked_total_length(total_length: u64) -> Result<usize, String> {
    usize::try_from(total_length)
        .map_err(|_| "torrent is too large for this peer download buffer".to_string())
}

fn checked_piece_length(piece_length: u64) -> Result<usize, String> {
    if piece_length == 0 {
        return Err("piece length cannot be zero".to_string());
    }
    usize::try_from(piece_length).map_err(|_| "piece length is too large".to_string())
}

fn ensure_download_active(cancelled: Option<&AtomicBool>) -> Result<(), String> {
    if cancelled.is_some_and(|cancelled| cancelled.load(Ordering::Relaxed)) {
        Err("peer download cancelled".to_string())
    } else {
        Ok(())
    }
}

fn build_full_bitfield(piece_count: usize) -> Vec<u8> {
    build_availability_bitfield(&vec![true; piece_count])
}

fn build_availability_bitfield(available_pieces: &[bool]) -> Vec<u8> {
    let mut out = vec![0u8; available_pieces.len().div_ceil(8)];
    for (index, available) in available_pieces.iter().enumerate() {
        if *available {
            out[index / 8] |= 0x80 >> (index % 8);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    use crate::torrent::{metadata::MetadataMessageType, pex::PexPeer, sha1};

    #[test]
    fn applies_bitfield_high_bit_first() {
        let mut availability = vec![false; 10];
        apply_bitfield(&[0b1010_0000, 0b0100_0000], &mut availability).expect("bitfield applies");

        assert_eq!(
            availability,
            vec![true, false, true, false, false, false, false, false, false, true]
        );
    }

    #[test]
    fn rejects_wrong_length_and_nonzero_bitfield_spare_bits() {
        let mut availability = vec![false; 10];
        assert!(apply_bitfield(&[0b1000_0000], &mut availability)
            .expect_err("short bitfield is rejected")
            .contains("length mismatch"));
        assert!(
            apply_bitfield(&[0b1000_0000, 0b0100_0001], &mut availability)
                .expect_err("non-zero spare bit is rejected")
                .contains("spare bits")
        );
    }

    #[test]
    fn shared_bandwidth_limiter_paces_reservations_and_honors_cancellation() {
        let limiter = BandwidthLimiter::new(Some(2_000));
        limiter
            .throttle(100, None)
            .expect("first reservation starts immediately");
        let paced_at = Instant::now();
        limiter
            .throttle(100, None)
            .expect("second reservation is paced");
        assert!(
            paced_at.elapsed() >= Duration::from_millis(30),
            "shared limiter did not pace the second reservation"
        );

        let cancelled = AtomicBool::new(true);
        let cancelled_at = Instant::now();
        assert!(limiter
            .throttle(100, Some(&cancelled))
            .expect_err("cancelled reservation stops")
            .contains("cancelled"));
        assert!(cancelled_at.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn request_pipeline_depth_adapts_to_peer_progress() {
        assert_eq!(
            adapt_request_pipeline_depth(8, 512 * 1024, Duration::from_millis(500), false),
            54
        );
        assert_eq!(
            adapt_request_pipeline_depth(8, 16 * 1024, Duration::from_secs(5), false),
            6
        );
        assert_eq!(
            adapt_request_pipeline_depth(3, 16 * 1024, Duration::from_millis(20), true),
            MIN_REQUEST_PIPELINE_DEPTH
        );
        assert_eq!(
            adapt_request_pipeline_depth(500, 512 * 1024, Duration::from_millis(1), false),
            MAX_REQUEST_PIPELINE_DEPTH
        );
    }

    #[test]
    fn remote_extension_handshake_caps_request_pipeline() {
        let mut availability = vec![false; 1];
        let mut choked = true;
        let mut dht_port = None;
        let mut pex_peers = Vec::new();
        let mut remote_request_queue_limit = MAX_REQUEST_PIPELINE_DEPTH;

        update_peer_state(
            PeerMessage::Extended {
                extension_id: 0,
                payload: b"d1:mde4:reqqi7ee".to_vec(),
            },
            &mut availability,
            &mut choked,
            false,
            &mut dht_port,
            &mut pex_peers,
            &mut remote_request_queue_limit,
        )
        .expect("extension handshake updates peer state");

        assert_eq!(remote_request_queue_limit, 7);
    }

    #[test]
    fn peer_connection_falls_back_to_later_resolved_address() {
        let unavailable = TcpListener::bind("127.0.0.1:0").expect("bind unavailable address");
        let unavailable_address = unavailable.local_addr().expect("unavailable local address");
        drop(unavailable);

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fallback listener");
        let fallback_address = listener.local_addr().expect("fallback local address");
        let accept = thread::spawn(move || listener.accept().expect("accept fallback connection"));

        let stream = connect_to_first_available_peer(
            &[unavailable_address, fallback_address],
            Duration::from_millis(250),
            None,
            "test peer",
        )
        .expect("later address connects");
        assert_eq!(stream.peer_addr().expect("peer address"), fallback_address);
        assert!(stream.nodelay().expect("TCP_NODELAY state"));
        drop(stream);
        accept.join().expect("join fallback acceptor");
    }

    #[test]
    fn upload_gate_admits_waiters_in_fifo_order() {
        let gate = UploadGate::new(1);
        let active = gate
            .try_acquire()
            .expect("first slot acquisition succeeds")
            .expect("first slot is available");
        let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
        let (release_first_tx, release_first_rx) = std::sync::mpsc::channel();

        let first_gate = gate.clone();
        let first_tx = acquired_tx.clone();
        let first_waiter = thread::spawn(move || {
            let permit = first_gate
                .acquire_timeout(Duration::from_secs(2))
                .expect("first waiter does not fail")
                .expect("first waiter acquires a slot");
            first_tx.send(1).expect("first acquisition is reported");
            release_first_rx.recv().expect("first waiter is released");
            drop(permit);
        });
        wait_for_upload_waiters(&gate, 1);

        let second_gate = gate.clone();
        let second_waiter = thread::spawn(move || {
            let _permit = second_gate
                .acquire_timeout(Duration::from_secs(2))
                .expect("second waiter does not fail")
                .expect("second waiter acquires a slot");
            acquired_tx.send(2).expect("second acquisition is reported");
        });
        wait_for_upload_waiters(&gate, 2);

        drop(active);
        assert_eq!(
            acquired_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("first waiter runs"),
            1
        );
        release_first_tx
            .send(())
            .expect("first waiter release sends");
        assert_eq!(
            acquired_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("second waiter runs"),
            2
        );
        first_waiter.join().expect("first waiter exits");
        second_waiter.join().expect("second waiter exits");
    }

    fn wait_for_upload_waiters(gate: &UploadGate, expected: usize) {
        let started = Instant::now();
        while gate.waiting_count() < expected {
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "upload waiter did not queue"
            );
            thread::yield_now();
        }
    }

    #[test]
    fn downloads_and_verifies_from_local_peer() {
        let data = (0..96).map(|value| value as u8).collect::<Vec<_>>();
        let piece_length = 16u64;
        let hashes = data
            .chunks(piece_length as usize)
            .map(sha1::digest)
            .collect::<Vec<_>>();
        let info_hash = [9u8; 20];
        let client_peer_id = *b"-NV0001-CLIENT000001";
        let server_peer_id = *b"-NV0001-SERVER000001";
        let listener = TcpListener::bind("127.0.0.1:0").expect("local listener binds");
        let port = listener.local_addr().expect("listener has addr").port();
        let seed_data = data.clone();

        let handle = thread::spawn(move || {
            seed_single_peer(
                listener,
                PeerSeedPlan {
                    info_hash,
                    peer_id: server_peer_id,
                    dht_port: None,
                    enable_pex: false,
                    pex_peers: Vec::new(),
                    piece_length,
                    bytes: seed_data,
                    available_pieces: None,
                    disconnect_after_blocks: None,
                    block_response_delay: None,
                },
            )
            .expect("seed peer serves torrent")
        });

        let result = download_from_peer(
            "127.0.0.1",
            port,
            PeerDownloadPlan {
                info_hash,
                peer_id: client_peer_id,
                dht_port: None,
                enable_pex: false,
                total_length: data.len() as u64,
                piece_length,
                piece_hashes: hashes,
                cancelled: None,
            },
        )
        .expect("peer download completes");

        let seed_result = handle.join().expect("server thread exits");
        assert_eq!(seed_result.peer_id, client_peer_id);
        assert_eq!(seed_result.bytes_uploaded, result.bytes.len() as u64);
        assert_eq!(seed_result.blocks_served, 6);
        assert_eq!(result.peer_id, server_peer_id);
        assert_eq!(result.bytes, data);
        assert_eq!(result.pieces_verified, 6);
    }

    #[test]
    fn download_and_seed_exchange_negotiated_dht_ports() {
        let data = b"dht port exchange".to_vec();
        let info_hash = [6u8; 20];
        let listener = TcpListener::bind("127.0.0.1:0").expect("local listener binds");
        let port = listener.local_addr().expect("listener has address").port();
        let seed_data = data.clone();
        let seed = thread::spawn(move || {
            seed_single_peer(
                listener,
                PeerSeedPlan {
                    info_hash,
                    peer_id: *b"-NV0001-DHTSEED00001",
                    dht_port: Some(49002),
                    enable_pex: false,
                    pex_peers: Vec::new(),
                    piece_length: seed_data.len() as u64,
                    bytes: seed_data,
                    available_pieces: None,
                    disconnect_after_blocks: None,
                    block_response_delay: None,
                },
            )
            .expect("DHT-capable seed serves data")
        });

        let mut connection = connect_peer_for_download(
            "127.0.0.1",
            port,
            PeerDownloadPlan {
                info_hash,
                peer_id: *b"-NV0001-DHTCLIENT001",
                dht_port: Some(49001),
                enable_pex: false,
                total_length: data.len() as u64,
                piece_length: data.len() as u64,
                piece_hashes: vec![sha1::digest(&data)],
                cancelled: None,
            },
        )
        .expect("DHT-capable peer connects");
        assert_eq!(connection.take_remote_dht_port(), Some(49002));
        let result = connection.download_pieces(&[0]);
        assert!(result.error.is_none());
        assert_eq!(result.pieces[0].bytes, data);
        drop(connection);

        let seed_result = seed.join().expect("seed exits");
        assert_eq!(seed_result.remote_dht_port, Some(49001));
    }

    #[test]
    fn peer_download_collects_pex_peers_from_extension_messages() {
        let data = b"pex extension data".to_vec();
        let info_hash = [4u8; 20];
        let listener = TcpListener::bind("127.0.0.1:0").expect("PEX peer binds");
        let port = listener.local_addr().expect("PEX peer address").port();
        let server_data = data.clone();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("PEX client connects");
            let mut handshake = [0u8; HANDSHAKE_LEN];
            socket
                .read_exact(&mut handshake)
                .expect("client handshake reads");
            assert!(peer::supports_extension_protocol(
                &peer::parse_handshake_full(&handshake).expect("client handshake parses")
            ));
            socket
                .write_all(&peer::build_extended_handshake(
                    info_hash,
                    *b"-NV0001-PEXSEED00001",
                ))
                .expect("server extension handshake writes");
            let PeerMessage::Extended {
                extension_id: 0,
                payload,
            } = read_peer_message(&mut socket).expect("client extension handshake reads")
            else {
                panic!("expected extension handshake");
            };
            assert_eq!(
                metadata::parse_extension_handshake(&payload)
                    .expect("client extension handshake parses")
                    .ut_pex,
                Some(LOCAL_UT_PEX_ID)
            );
            assert!(matches!(
                read_peer_message(&mut socket).expect("interested reads"),
                PeerMessage::Interested
            ));
            let pex_payload = pex::build_pex_message(
                &[PexPeer {
                    address: "203.0.113.7".to_string(),
                    port: 6881,
                    flags: 0x10,
                }],
                &[],
            )
            .expect("PEX payload builds");
            socket
                .write_all(&peer::build_extended_message(LOCAL_UT_PEX_ID, &pex_payload))
                .expect("PEX message writes");
            socket
                .write_all(&peer::build_bitfield(&[0b1000_0000]))
                .expect("bitfield writes");
            socket
                .write_all(&peer::build_unchoke())
                .expect("unchoke writes");
            let PeerMessage::Request {
                index,
                begin,
                length,
            } = read_peer_message(&mut socket).expect("request reads")
            else {
                panic!("expected request");
            };
            assert_eq!((index, begin, length), (0, 0, server_data.len() as u32));
            socket
                .write_all(&peer::build_piece(0, 0, &server_data))
                .expect("piece writes");
        });

        let mut connection = connect_peer_for_download(
            "127.0.0.1",
            port,
            PeerDownloadPlan {
                info_hash,
                peer_id: *b"-NV0001-PEXCLIENT001",
                dht_port: None,
                enable_pex: true,
                total_length: data.len() as u64,
                piece_length: data.len() as u64,
                piece_hashes: vec![sha1::digest(&data)],
                cancelled: None,
            },
        )
        .expect("PEX-capable peer connects");
        assert_eq!(connection.take_pex_peers()[0].address, "203.0.113.7");
        let result = connection.download_pieces(&[0]);
        assert!(result.error.is_none());
        assert_eq!(result.pieces[0].bytes, data);
        drop(connection);
        server.join().expect("PEX peer exits");
    }

    #[test]
    fn malformed_pex_does_not_end_peer_connection() {
        let mut availability = vec![false; 1];
        let mut choked = false;
        let mut remote_dht_port = None;
        let mut pex_peers = Vec::new();
        let mut remote_request_queue_limit = MAX_REQUEST_PIPELINE_DEPTH;

        update_peer_state(
            PeerMessage::Extended {
                extension_id: LOCAL_UT_PEX_ID,
                payload: b"not-bencode".to_vec(),
            },
            &mut availability,
            &mut choked,
            false,
            &mut remote_dht_port,
            &mut pex_peers,
            &mut remote_request_queue_limit,
        )
        .expect("optional malformed PEX is ignored");
        assert!(pex_peers.is_empty());
    }

    #[test]
    fn seed_peer_sends_pex_candidates_to_extension_capable_leecher() {
        let data = b"seed pex data".to_vec();
        let info_hash = [9u8; 20];
        let listener = TcpListener::bind("127.0.0.1:0").expect("PEX seed binds");
        let port = listener.local_addr().expect("PEX seed address").port();
        let seed_data = data.clone();
        let seed = thread::spawn(move || {
            seed_single_peer(
                listener,
                PeerSeedPlan {
                    info_hash,
                    peer_id: *b"-NV0001-PEXSEED00002",
                    dht_port: None,
                    enable_pex: true,
                    pex_peers: vec![peer::PeerInfo {
                        address: "203.0.113.7".to_string(),
                        port: 6881,
                        client: None,
                        progress: 1.0,
                        download_speed: 0,
                        upload_speed: 0,
                        connection: "Known".to_string(),
                    }],
                    piece_length: seed_data.len() as u64,
                    bytes: seed_data,
                    available_pieces: None,
                    disconnect_after_blocks: None,
                    block_response_delay: None,
                },
            )
            .expect("PEX seed exits cleanly")
        });

        let mut socket = TcpStream::connect(("127.0.0.1", port)).expect("leecher connects");
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("leecher read timeout sets");
        socket
            .write_all(&peer::build_feature_handshake(
                info_hash,
                *b"-NV0001-PEXLEECH0001",
                true,
                false,
            ))
            .expect("leecher handshake writes");
        let mut handshake = [0u8; HANDSHAKE_LEN];
        socket
            .read_exact(&mut handshake)
            .expect("seed handshake reads");
        assert!(peer::supports_extension_protocol(
            &peer::parse_handshake_full(&handshake).expect("seed handshake parses")
        ));
        let PeerMessage::Extended {
            extension_id: 0,
            payload,
        } = read_peer_message(&mut socket).expect("seed extension handshake reads")
        else {
            panic!("expected seed extension handshake");
        };
        assert_eq!(
            metadata::parse_extension_handshake(&payload)
                .expect("seed extension handshake parses")
                .ut_pex,
            Some(LOCAL_UT_PEX_ID)
        );
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
        } = read_peer_message(&mut socket).expect("seed PEX message reads")
        else {
            panic!("expected seed PEX message");
        };
        assert_eq!(extension_id, 9);
        let pex = pex::parse_pex_message(&payload).expect("seed PEX parses");
        assert_eq!(pex.added[0].address, "203.0.113.7");
        assert_eq!(pex.added[0].flags, 0x02);
        assert!(matches!(
            read_peer_message(&mut socket).expect("seed bitfield reads"),
            PeerMessage::Bitfield(_)
        ));
        assert!(matches!(
            read_peer_message(&mut socket).expect("seed unchoke reads"),
            PeerMessage::Unchoke
        ));
        socket
            .write_all(&peer::build_not_interested())
            .expect("leecher not interested writes");
        assert_eq!(seed.join().expect("seed exits").bytes_uploaded, 0);
    }

    #[test]
    fn dht_port_is_not_exchanged_without_mutual_handshake_support() {
        let data = b"no DHT exchange".to_vec();
        let info_hash = [5u8; 20];
        let listener = TcpListener::bind("127.0.0.1:0").expect("local listener binds");
        let port = listener.local_addr().expect("listener has address").port();
        let seed_data = data.clone();
        let seed = thread::spawn(move || {
            seed_single_peer(
                listener,
                PeerSeedPlan {
                    info_hash,
                    peer_id: *b"-NV0001-DHTSEED00002",
                    dht_port: Some(49002),
                    enable_pex: false,
                    pex_peers: Vec::new(),
                    piece_length: seed_data.len() as u64,
                    bytes: seed_data,
                    available_pieces: None,
                    disconnect_after_blocks: None,
                    block_response_delay: None,
                },
            )
            .expect("seed serves non-DHT peer")
        });

        let mut connection = connect_peer_for_download(
            "127.0.0.1",
            port,
            PeerDownloadPlan {
                info_hash,
                peer_id: *b"-NV0001-NODHTCLIENT1",
                dht_port: None,
                enable_pex: false,
                total_length: data.len() as u64,
                piece_length: data.len() as u64,
                piece_hashes: vec![sha1::digest(&data)],
                cancelled: None,
            },
        )
        .expect("non-DHT peer connects");
        assert_eq!(connection.take_remote_dht_port(), None);
        let result = connection.download_pieces(&[0]);
        assert!(result.error.is_none());
        drop(connection);

        let seed_result = seed.join().expect("seed exits");
        assert_eq!(seed_result.remote_dht_port, None);
    }

    #[test]
    fn pipelines_requests_and_accepts_reverse_order_blocks() {
        let total_length = piece::DEFAULT_BLOCK_SIZE as usize * 3;
        let data = (0..total_length)
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        let info_hash = [13u8; 20];
        let listener = TcpListener::bind("127.0.0.1:0").expect("pipeline peer binds");
        let port = listener.local_addr().expect("pipeline peer address").port();
        let server_data = data.clone();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("pipeline client connects");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("pipeline read timeout sets");
            let mut handshake = [0u8; HANDSHAKE_LEN];
            socket
                .read_exact(&mut handshake)
                .expect("client handshake reads");
            assert_eq!(
                peer::parse_handshake_full(&handshake)
                    .expect("client handshake parses")
                    .info_hash,
                info_hash
            );
            socket
                .write_all(&peer::build_handshake(info_hash, *b"-NV0001-PIPELINE0001"))
                .expect("server handshake writes");
            assert!(matches!(
                read_peer_message(&mut socket).expect("interested reads"),
                PeerMessage::Interested
            ));
            socket
                .write_all(&peer::build_bitfield(&[0b1000_0000]))
                .expect("bitfield writes");
            socket
                .write_all(&peer::build_unchoke())
                .expect("unchoke writes");

            let mut requests = Vec::new();
            for _ in 0..3 {
                let PeerMessage::Request {
                    index,
                    begin,
                    length,
                } = read_peer_message(&mut socket).expect("pipelined request reads")
                else {
                    panic!("expected pipelined request");
                };
                requests.push((index, begin, length));
            }
            assert_eq!(
                requests
                    .iter()
                    .map(|(_, begin, _)| *begin)
                    .collect::<Vec<_>>(),
                vec![0, piece::DEFAULT_BLOCK_SIZE, piece::DEFAULT_BLOCK_SIZE * 2]
            );
            for (index, begin, length) in requests.into_iter().rev() {
                let start = begin as usize;
                let end = start + length as usize;
                socket
                    .write_all(&peer::build_piece(index, begin, &server_data[start..end]))
                    .expect("reverse-order block writes");
            }
        });

        let result = download_from_peer(
            "127.0.0.1",
            port,
            PeerDownloadPlan {
                info_hash,
                peer_id: *b"-NV0001-PIPECLIENT01",
                dht_port: None,
                enable_pex: false,
                total_length: data.len() as u64,
                piece_length: data.len() as u64,
                piece_hashes: vec![sha1::digest(&data)],
                cancelled: None,
            },
        )
        .expect("pipelined download succeeds");

        assert_eq!(result.bytes, data);
        server.join().expect("pipeline peer exits");
    }

    #[test]
    fn pipelines_requests_across_piece_boundaries() {
        let block_size = piece::DEFAULT_BLOCK_SIZE as usize;
        let data = (0..block_size * 4)
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        let piece_hashes = data
            .chunks(block_size)
            .map(sha1::digest)
            .collect::<Vec<_>>();
        let info_hash = [41u8; 20];
        let listener = TcpListener::bind("127.0.0.1:0").expect("cross-piece peer binds");
        let port = listener
            .local_addr()
            .expect("cross-piece peer address")
            .port();
        let server_data = data.clone();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("cross-piece client connects");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("cross-piece read timeout sets");
            let mut handshake = [0u8; HANDSHAKE_LEN];
            socket
                .read_exact(&mut handshake)
                .expect("client handshake reads");
            socket
                .write_all(&peer::build_handshake(info_hash, [42u8; 20]))
                .expect("server handshake writes");
            assert!(matches!(
                read_peer_message(&mut socket).expect("interested reads"),
                PeerMessage::Interested
            ));
            socket
                .write_all(&peer::build_bitfield(&[0b1111_0000]))
                .expect("bitfield writes");
            socket
                .write_all(&peer::build_unchoke())
                .expect("unchoke writes");

            let mut requests = Vec::new();
            for _ in 0..4 {
                let PeerMessage::Request {
                    index,
                    begin,
                    length,
                } = read_peer_message(&mut socket).expect("cross-piece request reads")
                else {
                    panic!("expected cross-piece request");
                };
                requests.push((index, begin, length));
            }
            assert_eq!(
                requests
                    .iter()
                    .map(|(index, _, _)| *index)
                    .collect::<Vec<_>>(),
                vec![0, 1, 2, 3]
            );
            for (index, begin, length) in requests.into_iter().rev() {
                let start = index as usize * block_size + begin as usize;
                let end = start + length as usize;
                socket
                    .write_all(&peer::build_piece(index, begin, &server_data[start..end]))
                    .expect("cross-piece block writes");
            }
        });

        let mut connection = connect_peer_for_download(
            "127.0.0.1",
            port,
            PeerDownloadPlan {
                info_hash,
                peer_id: [43u8; 20],
                dht_port: None,
                enable_pex: false,
                total_length: data.len() as u64,
                piece_length: block_size as u64,
                piece_hashes,
                cancelled: None,
            },
        )
        .expect("cross-piece peer connects");
        let result = connection.download_pieces(&[0, 1, 2, 3]);

        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(result.pieces.len(), 4);
        assert_eq!(
            result
                .pieces
                .iter()
                .flat_map(|piece| piece.bytes.iter().copied())
                .collect::<Vec<_>>(),
            data
        );
        drop(connection);
        server.join().expect("cross-piece peer exits");
    }

    #[test]
    fn cancellation_sends_cancels_for_pipelined_requests() {
        let total_length = piece::DEFAULT_BLOCK_SIZE as usize * 3;
        let data = vec![7u8; total_length];
        let info_hash = [14u8; 20];
        let cancelled = Arc::new(AtomicBool::new(false));
        let server_cancelled = Arc::clone(&cancelled);
        let listener = TcpListener::bind("127.0.0.1:0").expect("cancel peer binds");
        let port = listener.local_addr().expect("cancel peer address").port();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("cancel client connects");
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("cancel read timeout sets");
            let mut handshake = [0u8; HANDSHAKE_LEN];
            socket
                .read_exact(&mut handshake)
                .expect("client handshake reads");
            socket
                .write_all(&peer::build_handshake(info_hash, *b"-NV0001-PIPECANCEL01"))
                .expect("server handshake writes");
            assert!(matches!(
                read_peer_message(&mut socket).expect("interested reads"),
                PeerMessage::Interested
            ));
            socket
                .write_all(&peer::build_bitfield(&[0b1000_0000]))
                .expect("bitfield writes");
            socket
                .write_all(&peer::build_unchoke())
                .expect("unchoke writes");

            for _ in 0..3 {
                assert!(matches!(
                    read_peer_message(&mut socket).expect("request reads"),
                    PeerMessage::Request { .. }
                ));
            }
            server_cancelled.store(true, Ordering::Relaxed);
            let mut cancelled_offsets = Vec::new();
            for _ in 0..3 {
                let PeerMessage::Cancel { begin, .. } =
                    read_peer_message(&mut socket).expect("cancel reads")
                else {
                    panic!("expected cancel message");
                };
                cancelled_offsets.push(begin);
            }
            cancelled_offsets
        });

        let error = download_from_peer(
            "127.0.0.1",
            port,
            PeerDownloadPlan {
                info_hash,
                peer_id: *b"-NV0001-PIPECLIENT02",
                dht_port: None,
                enable_pex: false,
                total_length: data.len() as u64,
                piece_length: data.len() as u64,
                piece_hashes: vec![sha1::digest(&data)],
                cancelled: Some(cancelled),
            },
        )
        .expect_err("cancelled pipeline stops");

        assert!(error.contains("cancelled"));
        assert_eq!(
            server.join().expect("cancel peer exits"),
            vec![0, piece::DEFAULT_BLOCK_SIZE, piece::DEFAULT_BLOCK_SIZE * 2]
        );
    }

    #[test]
    fn downloads_only_pieces_advertised_by_a_peer() {
        let data = b"abcdefghijkl".to_vec();
        let piece_length = 4u64;
        let hashes = data
            .chunks(piece_length as usize)
            .map(sha1::digest)
            .collect::<Vec<_>>();
        let info_hash = [7u8; 20];
        let listener = TcpListener::bind("127.0.0.1:0").expect("local listener binds");
        let port = listener.local_addr().expect("listener has addr").port();
        let seed_data = data.clone();

        let handle = thread::spawn(move || {
            seed_single_peer(
                listener,
                PeerSeedPlan {
                    info_hash,
                    peer_id: *b"-NV0001-PARTIAL00001",
                    dht_port: None,
                    enable_pex: false,
                    pex_peers: Vec::new(),
                    piece_length,
                    bytes: seed_data,
                    available_pieces: Some(vec![true, false, true]),
                    disconnect_after_blocks: None,
                    block_response_delay: None,
                },
            )
            .expect("partial seed serves advertised pieces")
        });

        let result = download_pieces_from_peer(
            "127.0.0.1",
            port,
            PeerDownloadPlan {
                info_hash,
                peer_id: *b"-NV0001-CLIENT000001",
                dht_port: None,
                enable_pex: false,
                total_length: data.len() as u64,
                piece_length,
                piece_hashes: hashes,
                cancelled: None,
            },
            &[0, 1, 2],
        )
        .expect("targeted piece download completes");

        let seed = handle.join().expect("partial seed exits");
        assert_eq!(seed.bytes_uploaded, 8);
        assert_eq!(result.unavailable, vec![1]);
        assert!(result.error.is_none());
        assert_eq!(
            result
                .pieces
                .iter()
                .map(|piece| piece.index)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(result.pieces[0].bytes, b"abcd");
        assert_eq!(result.pieces[1].bytes, b"ijkl");
    }

    #[test]
    fn cancelled_peer_download_stops_before_connecting() {
        let cancelled = Arc::new(AtomicBool::new(true));
        let err = download_from_peer(
            "127.0.0.1",
            1,
            PeerDownloadPlan {
                info_hash: [1; 20],
                peer_id: *b"-NV0001-CANCEL000001",
                dht_port: None,
                enable_pex: false,
                total_length: 1,
                piece_length: 1,
                piece_hashes: vec![sha1::digest(b"x")],
                cancelled: Some(cancelled),
            },
        )
        .expect_err("cancelled peer download stops");
        assert!(err.contains("cancelled"));
    }

    #[test]
    fn fetches_metadata_from_local_extension_peer() {
        let info = b"d6:lengthi11e4:name11:example.txt12:piece lengthi4e6:pieces60:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaae".to_vec();
        let info_hash = sha1::digest(&info);
        let client_peer_id = *b"-NV0001-CLIENT000001";
        let server_peer_id = *b"-NV0001-METAPEER0001";
        let listener = TcpListener::bind("127.0.0.1:0").expect("local listener binds");
        let port = listener.local_addr().expect("listener has addr").port();
        let server_info = info.clone();

        let handle = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("client connects");
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .expect("read timeout sets");
            socket
                .set_write_timeout(Some(Duration::from_secs(10)))
                .expect("write timeout sets");

            let mut handshake = [0u8; HANDSHAKE_LEN];
            socket
                .read_exact(&mut handshake)
                .expect("client handshake arrives");
            let handshake =
                peer::parse_handshake_full(&handshake).expect("client handshake parses");
            assert_eq!(handshake.info_hash, info_hash);
            assert!(peer::supports_extension_protocol(&handshake));
            assert!(peer::supports_dht(&handshake));
            socket
                .write_all(&peer::build_feature_handshake(
                    info_hash,
                    server_peer_id,
                    true,
                    true,
                ))
                .expect("server handshake writes");

            assert_eq!(
                read_peer_message(&mut socket).expect("client DHT port arrives"),
                PeerMessage::Port { port: 49001 }
            );
            socket
                .write_all(&peer::build_port(49002))
                .expect("server DHT port writes");

            let message =
                read_peer_message(&mut socket).expect("client extension handshake arrives");
            let PeerMessage::Extended {
                extension_id: 0,
                payload,
            } = message
            else {
                panic!("expected client extension handshake");
            };
            assert_eq!(
                metadata::parse_extension_handshake(&payload)
                    .expect("client extension handshake parses")
                    .ut_metadata,
                Some(LOCAL_UT_METADATA_ID)
            );

            socket
                .write_all(&peer::build_extended_message(
                    0,
                    &metadata::build_extension_handshake(7, Some(server_info.len() as u64)),
                ))
                .expect("server extension handshake writes");

            let message = read_peer_message(&mut socket).expect("metadata request arrives");
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
                    &metadata::build_metadata_data(0, server_info.len() as u64, &server_info),
                ))
                .expect("metadata data writes");
            handshake.peer_id
        });

        let result = fetch_metadata_from_peer(
            "127.0.0.1",
            port,
            MetadataFetchPlan {
                info_hash,
                peer_id: client_peer_id,
                dht_port: Some(49001),
            },
        )
        .expect("metadata fetch completes");

        let remote_seen_peer = handle.join().expect("server exits");
        assert_eq!(remote_seen_peer, client_peer_id);
        assert_eq!(result.peer_id, server_peer_id);
        assert_eq!(result.info_bytes, info);
        assert_eq!(result.pieces_received, 1);
        assert_eq!(result.remote_dht_port, Some(49002));
    }
}
