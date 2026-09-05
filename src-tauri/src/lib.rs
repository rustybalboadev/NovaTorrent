use std::{
    collections::{HashMap, HashSet},
    env,
    io::{ErrorKind, Read, Write},
    net::{IpAddr, SocketAddrV4, TcpListener, TcpStream, UdpSocket},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tauri::{
    webview::PageLoadEvent, window::Color, AppHandle, Emitter, Manager, WebviewUrl,
    WebviewWindowBuilder,
};

use crate::torrent::session::{
    AddTorrentRequest, AddTorrentResponse, EmptyJsonResponse, LogEntry, LogLevel,
    StreamPriorityRequest, StreamPriorityStatus, TorrentDetails, TorrentFileAvailability,
    TorrentListResponse, TorrentSession, TorrentSummaryListResponse, UpdateTorrentOptionsRequest,
};

mod torrent;

struct AppState {
    session: Arc<TorrentSession>,
    pending_sources: Mutex<Vec<String>>,
    active_workers: Arc<Mutex<HashSet<String>>>,
    media_stream_port: AtomicU16,
    media_stream_tokens: Arc<Mutex<HashMap<String, MediaStreamRoute>>>,
    next_media_stream_token: AtomicU64,
    shutdown_started: AtomicBool,
}

#[derive(Debug, Clone)]
struct MediaStreamRoute {
    id: String,
    file_index: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MediaPlayerLogRequest {
    id: String,
    file_index: usize,
    event: String,
    current_time: Option<f64>,
    duration: Option<f64>,
    ready_state: Option<u16>,
    network_state: Option<u16>,
    paused: Option<bool>,
    seeking: Option<bool>,
    target_time: Option<f64>,
    target_offset: Option<u64>,
    target_ready: Option<bool>,
    buffered_ahead_seconds: Option<f64>,
    retry_key: Option<u32>,
    network_retries: Option<u32>,
    message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SubtitleFileText {
    file_index: usize,
    name: String,
    text: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct MediaPlayerClosedPayload {
    torrent_id: String,
    file_index: usize,
}

const MEDIA_STREAM_CHUNK_LIMIT: u64 = 4 * 1024 * 1024;
const MEDIA_STREAM_WAIT_TIMEOUT: Duration = Duration::from_secs(45);
const MEDIA_STREAM_WAIT_INTERVAL: Duration = Duration::from_millis(150);
const MEDIA_STREAM_URGENT_PRIORITY_BYTES: u64 = 16 * 1024 * 1024;
const MEDIA_STREAM_LOOKAHEAD_PRIORITY_BYTES: u64 = 192 * 1024 * 1024;

impl AppState {
    fn new(
        default_output_dir: PathBuf,
        state_dir: PathBuf,
        log_file_path: PathBuf,
        pending_sources: Vec<String>,
    ) -> Self {
        let session = Arc::new(TorrentSession::new_with_state_and_log_file(
            default_output_dir,
            state_dir,
            log_file_path,
        ));
        session.log(
            LogLevel::Info,
            "app",
            "NovaTorrent backend started with manual BitTorrent core",
            None,
        );
        session.log(
            LogLevel::Info,
            "logs",
            format!(
                "writing readable backend logs to {}",
                session.log_file_path().to_string_lossy()
            ),
            None,
        );
        let state = Self {
            session,
            pending_sources: Mutex::new(pending_sources),
            active_workers: Arc::new(Mutex::new(HashSet::new())),
            media_stream_port: AtomicU16::new(0),
            media_stream_tokens: Arc::new(Mutex::new(HashMap::new())),
            next_media_stream_token: AtomicU64::new(1),
            shutdown_started: AtomicBool::new(false),
        };
        if let Err(err) = state.start_media_stream_server() {
            state.session.log(LogLevel::Error, "stream", err, None);
        }
        match state.start_peer_listener() {
            Ok(port) => {
                if let Err(err) = state.start_dht_listener(port) {
                    state.session.log(LogLevel::Error, "dht", err, None);
                }
            }
            Err(err) => {
                state.session.log(LogLevel::Error, "seed", err, None);
                if let Err(err) = state.start_dht_listener(0) {
                    state.session.log(LogLevel::Error, "dht", err, None);
                }
            }
        }
        if let Err(err) = state.start_dht_scheduler() {
            state.session.log(LogLevel::Error, "dht", err, None);
        }
        if let Err(err) = state.start_tracker_scheduler() {
            state.session.log(LogLevel::Error, "tracker", err, None);
        }
        if let Err(err) = state.start_lsd_service() {
            state.session.log(LogLevel::Warn, "lsd", err, None);
        }
        for id in state.session.runnable_ids() {
            if let Err(err) = state.start_torrent_worker(id.to_string()) {
                state.session.log(LogLevel::Error, "runtime", err, Some(id));
            }
        }
        state
    }

    fn take_pending_sources(&self) -> Vec<String> {
        self.pending_sources
            .lock()
            .map(|mut pending| std::mem::take(&mut *pending))
            .unwrap_or_default()
    }

    fn start_media_stream_server(&self) -> Result<u16, String> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .map_err(|err| format!("could not bind local media stream server: {err}"))?;
        let port = listener
            .local_addr()
            .map_err(|err| format!("could not inspect local media stream server: {err}"))?
            .port();
        self.media_stream_port.store(port, Ordering::Release);

        let session = Arc::clone(&self.session);
        let routes = Arc::clone(&self.media_stream_tokens);
        thread::Builder::new()
            .name("novatorrent-media-stream".to_string())
            .spawn(move || {
                for connection in listener.incoming() {
                    match connection {
                        Ok(stream) => {
                            let session = Arc::clone(&session);
                            let routes = Arc::clone(&routes);
                            let _ = thread::Builder::new()
                                .name("novatorrent-media-range".to_string())
                                .spawn(move || {
                                    handle_media_stream_connection(stream, session, routes);
                                });
                        }
                        Err(err) => {
                            session.log(
                                LogLevel::Warn,
                                "stream",
                                format!("local media stream accept failed: {err}"),
                                None,
                            );
                        }
                    }
                }
            })
            .map_err(|err| format!("could not start local media stream server: {err}"))?;

        self.session.log(
            LogLevel::Info,
            "stream",
            format!("local media stream server listening on 127.0.0.1:{port}"),
            None,
        );
        Ok(port)
    }

    fn start_torrent_worker(&self, id: String) -> Result<(), String> {
        let session = Arc::clone(&self.session);
        {
            let mut active = self
                .active_workers
                .lock()
                .map_err(|_| "torrent worker lock poisoned".to_string())?;
            if !active.insert(id.clone()) {
                session.log(
                    LogLevel::Debug,
                    "runtime",
                    "torrent already has an active or scheduled background worker",
                    id.parse().ok(),
                );
                return Ok(());
            }
        }
        let active_workers = Arc::clone(&self.active_workers);
        let worker_id = id.clone();
        let worker_id_for_spawn_error = worker_id.clone();
        let thread_label = id.chars().take(12).collect::<String>();
        match thread::Builder::new()
            .name(format!("novatorrent-{thread_label}"))
            .spawn(move || {
                let mut failures = 0u32;
                loop {
                    match session.run_torrent(&id) {
                        Ok(_) => break,
                        Err(err) => {
                            let Some(delay) =
                                session.retry_delay_after_runtime_failure(&id, &err, failures)
                            else {
                                break;
                            };
                            failures = failures.saturating_add(1);
                            if let Err(mark_err) =
                                session.mark_runtime_retry_scheduled(&id, &err, delay)
                            {
                                session.log(
                                    LogLevel::Warn,
                                    "runtime",
                                    format!("could not mark torrent retry state: {mark_err}"),
                                    id.parse().ok(),
                                );
                            }
                            session.log(
                                LogLevel::Info,
                                "runtime",
                                format!(
                                    "background torrent worker will retry after recoverable failure in {} seconds: {err}",
                                    delay.as_secs()
                                ),
                                id.parse().ok(),
                            );
                            thread::sleep(delay);
                        }
                    }
                }
                if let Ok(mut active) = active_workers.lock() {
                    active.remove(&worker_id);
                }
            }) {
            Ok(_) => Ok(()),
            Err(err) => {
                if let Ok(mut active) = self.active_workers.lock() {
                    active.remove(&worker_id_for_spawn_error);
                }
                Err(format!("could not start torrent worker: {err}"))
            }
        }
    }

    fn start_peer_listener(&self) -> Result<u16, String> {
        let listener = (6881u16..=6889)
            .find_map(|port| TcpListener::bind(("0.0.0.0", port)).ok())
            .or_else(|| TcpListener::bind(("0.0.0.0", 0)).ok())
            .ok_or_else(|| "could not bind an inbound BitTorrent TCP listener".to_string())?;
        let port = listener
            .local_addr()
            .map_err(|err| format!("could not inspect inbound listener address: {err}"))?
            .port();
        self.session.set_listen_port(port);
        self.session.log(
            LogLevel::Info,
            "seed",
            format!("inbound BitTorrent listener active on TCP port {port}"),
            None,
        );
        let session = Arc::clone(&self.session);
        thread::Builder::new()
            .name("novatorrent-peer-listener".to_string())
            .spawn(move || {
                for incoming in listener.incoming() {
                    match incoming {
                        Ok(stream) => {
                            let remote = stream.peer_addr().ok();
                            match session.try_acquire_incoming_peer() {
                                Ok(permit) => {
                                    let worker_session = Arc::clone(&session);
                                    if let Err(err) = thread::Builder::new()
                                        .name("novatorrent-incoming-peer".to_string())
                                        .spawn(move || {
                                            if let Err(err) = worker_session
                                                .serve_incoming_peer_with_permit(stream, permit)
                                            {
                                                worker_session.log(
                                                    LogLevel::Warn,
                                                    "seed",
                                                    format!("incoming peer session ended: {err}"),
                                                    None,
                                                );
                                            }
                                        })
                                    {
                                        session.log(
                                            LogLevel::Error,
                                            "seed",
                                            format!("could not start incoming peer worker: {err}"),
                                            None,
                                        );
                                    }
                                }
                                Err(err) => session.log(
                                    LogLevel::Debug,
                                    "seed",
                                    format!(
                                        "rejected inbound peer {}: {err}",
                                        remote
                                            .map(|address| address.to_string())
                                            .unwrap_or_else(|| "unknown address".to_string())
                                    ),
                                    None,
                                ),
                            }
                        }
                        Err(err) => {
                            session.log(
                                LogLevel::Error,
                                "seed",
                                format!("inbound listener stopped: {err}"),
                                None,
                            );
                            session.set_listen_port(0);
                            break;
                        }
                    }
                }
            })
            .map_err(|err| {
                self.session.set_listen_port(0);
                format!("could not start inbound peer listener: {err}")
            })?;
        Ok(port)
    }

    fn start_dht_listener(&self, preferred_port: u16) -> Result<u16, String> {
        let socket = UdpSocket::bind(("0.0.0.0", preferred_port))
            .or_else(|_| UdpSocket::bind(("0.0.0.0", 0)))
            .map_err(|err| format!("could not bind the inbound DHT UDP listener: {err}"))?;
        let port = socket
            .local_addr()
            .map_err(|err| format!("could not inspect DHT listener address: {err}"))?
            .port();
        self.session.set_dht_port(port);
        self.session.log(
            LogLevel::Info,
            "dht",
            format!("inbound DHT node active on UDP port {port}"),
            None,
        );
        let session = Arc::clone(&self.session);
        thread::Builder::new()
            .name("novatorrent-dht-listener".to_string())
            .spawn(move || {
                let mut buffer = [0u8; 4096];
                loop {
                    match socket.recv_from(&mut buffer) {
                        Ok((length, source)) => {
                            let response = session.handle_dht_packet(&buffer[..length], source);
                            if let Err(err) = socket.send_to(&response, source) {
                                session.log(
                                    LogLevel::Debug,
                                    "dht",
                                    format!("could not send DHT response to {source}: {err}"),
                                    None,
                                );
                            }
                        }
                        Err(err) => {
                            session.log(
                                LogLevel::Error,
                                "dht",
                                format!("inbound DHT listener stopped: {err}"),
                                None,
                            );
                            session.set_dht_port(0);
                            break;
                        }
                    }
                }
            })
            .map_err(|err| {
                self.session.set_dht_port(0);
                format!("could not start inbound DHT listener: {err}")
            })?;
        Ok(port)
    }

    fn start_tracker_scheduler(&self) -> Result<(), String> {
        let session = Arc::clone(&self.session);
        thread::Builder::new()
            .name("novatorrent-tracker-scheduler".to_string())
            .spawn(move || loop {
                thread::sleep(Duration::from_secs(5));
                for id in session.due_tracker_ids() {
                    let session = Arc::clone(&session);
                    let _ = thread::Builder::new()
                        .name(format!("novatorrent-tracker-{id}"))
                        .spawn(move || {
                            let _ = session.announce(&id.to_string());
                        });
                }
            })
            .map(|_| ())
            .map_err(|err| format!("could not start tracker scheduler: {err}"))
    }

    fn start_dht_scheduler(&self) -> Result<(), String> {
        let session = Arc::clone(&self.session);
        thread::Builder::new()
            .name("novatorrent-dht-maintenance".to_string())
            .spawn(move || {
                if let Err(err) = session.bootstrap_dht_routing() {
                    session.log(
                        LogLevel::Error,
                        "dht",
                        format!("DHT startup self-lookup failed: {err}"),
                        None,
                    );
                }
                loop {
                    thread::sleep(Duration::from_secs(60));
                    if let Err(err) = session.maintain_dht_routing() {
                        session.log(
                            LogLevel::Error,
                            "dht",
                            format!("DHT routing maintenance failed: {err}"),
                            None,
                        );
                    }
                }
            })
            .map(|_| ())
            .map_err(|err| format!("could not start DHT maintenance scheduler: {err}"))
    }

    fn start_lsd_service(&self) -> Result<(), String> {
        let listen_port = self.session.listen_port();
        if listen_port == 0 {
            return Err(
                "LSD disabled because no inbound BitTorrent TCP listener is active".to_string(),
            );
        }

        let mut receive_enabled = true;
        let socket = match UdpSocket::bind(("0.0.0.0", crate::torrent::lsd::LSD_PORT)) {
            Ok(socket) => socket,
            Err(err) => {
                receive_enabled = false;
                self.session.log(
                    LogLevel::Warn,
                    "lsd",
                    format!(
                        "could not bind UDP {} for LAN peer discovery receives: {err}; LSD announces will still be sent",
                        crate::torrent::lsd::LSD_PORT
                    ),
                    None,
                );
                UdpSocket::bind(("0.0.0.0", 0)).map_err(|err| {
                    format!("could not bind a UDP socket for LSD announces: {err}")
                })?
            }
        };
        socket
            .set_multicast_ttl_v4(1)
            .map_err(|err| format!("could not set LSD multicast TTL: {err}"))?;
        socket
            .set_read_timeout(Some(Duration::from_secs(1)))
            .map_err(|err| format!("could not set LSD read timeout: {err}"))?;
        if receive_enabled {
            if let Err(err) = socket.join_multicast_v4(
                &crate::torrent::lsd::LSD_IPV4_GROUP,
                &std::net::Ipv4Addr::UNSPECIFIED,
            ) {
                receive_enabled = false;
                self.session.log(
                    LogLevel::Warn,
                    "lsd",
                    format!(
                        "could not join BEP 14 multicast group {}: {err}; LSD announces will still be sent",
                        crate::torrent::lsd::LSD_IPV4_GROUP
                    ),
                    None,
                );
            }
        }

        self.session.log(
            LogLevel::Info,
            "lsd",
            format!(
                "Local Service Discovery active on UDP multicast {}:{} with TCP port {listen_port}",
                crate::torrent::lsd::LSD_IPV4_GROUP,
                crate::torrent::lsd::LSD_PORT
            ),
            None,
        );
        let session = Arc::clone(&self.session);
        let cookie = format!("novatorrent-{}-{listen_port}", std::process::id());
        thread::Builder::new()
            .name("novatorrent-lsd".to_string())
            .spawn(move || {
                let target = SocketAddrV4::new(
                    crate::torrent::lsd::LSD_IPV4_GROUP,
                    crate::torrent::lsd::LSD_PORT,
                );
                let now = Instant::now();
                let mut last_announce = now.checked_sub(Duration::from_secs(300)).unwrap_or(now);
                let mut announce_cursor = 0usize;
                let mut buffer = [0u8; 2048];
                loop {
                    if last_announce.elapsed() >= Duration::from_secs(300) {
                        let hashes = session.lsd_public_info_hashes();
                        let port = session.listen_port();
                        if port != 0 && !hashes.is_empty() {
                            let (batch, next_cursor) =
                                crate::torrent::lsd::round_robin_info_hash_batch(
                                    &hashes,
                                    announce_cursor,
                                );
                            announce_cursor = next_cursor;
                            match crate::torrent::lsd::build_lsd_announce(
                                &batch,
                                port,
                                Some(&cookie),
                            ) {
                                Ok(packet) => match socket.send_to(&packet, target) {
                                    Ok(_) => session.log(
                                        LogLevel::Debug,
                                        "lsd",
                                        format!(
                                            "sent LAN peer announce for {} of {} active public torrent(s)",
                                            batch.len(),
                                            hashes.len()
                                        ),
                                        None,
                                    ),
                                    Err(err) => session.log(
                                        LogLevel::Warn,
                                        "lsd",
                                        format!("could not send LAN peer announce: {err}"),
                                        None,
                                    ),
                                },
                                Err(err) => session.log(
                                    LogLevel::Warn,
                                    "lsd",
                                    format!("could not build LAN peer announce: {err}"),
                                    None,
                                ),
                            }
                        }
                        last_announce = Instant::now();
                    }

                    if !receive_enabled {
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                    match socket.recv_from(&mut buffer) {
                        Ok((length, source)) => {
                            if matches!(source.ip(), IpAddr::V4(_)) {
                                match crate::torrent::lsd::parse_lsd_announce(&buffer[..length]) {
                                    Ok(announce) => {
                                        if announce.cookie.as_deref() == Some(cookie.as_str()) {
                                            continue;
                                        }
                                        if let Err(err) =
                                            session.handle_lsd_announce(&announce, source)
                                        {
                                            session.log(
                                                LogLevel::Warn,
                                                "lsd",
                                                format!("could not apply LAN peer announce: {err}"),
                                                None,
                                            );
                                        }
                                    }
                                    Err(err) => session.log(
                                        LogLevel::Debug,
                                        "lsd",
                                        format!("ignored malformed LAN peer announce from {source}: {err}"),
                                        None,
                                    ),
                                }
                            }
                        }
                        Err(err)
                            if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
                        Err(err) => {
                            session.log(
                                LogLevel::Warn,
                                "lsd",
                                format!("LAN peer discovery receive failed: {err}"),
                                None,
                            );
                            thread::sleep(Duration::from_secs(5));
                        }
                    }
                }
            })
            .map(|_| ())
            .map_err(|err| format!("could not start LSD worker: {err}"))
    }

    fn stop_tracker_background(&self, id: String) {
        let session = Arc::clone(&self.session);
        let _ = thread::Builder::new()
            .name(format!("novatorrent-tracker-stop-{id}"))
            .spawn(move || {
                let _ = session.announce_stopped(&id);
            });
    }

    fn shutdown_gracefully(&self, timeout: Duration) {
        self.session.prepare_shutdown();
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self
                .active_workers
                .lock()
                .map(|workers| workers.is_empty())
                .unwrap_or(true)
            {
                break;
            }
            thread::sleep(Duration::from_millis(25));
        }
    }
}

struct MediaHttpRequest {
    method: String,
    path: String,
    range: Option<String>,
}

fn handle_media_stream_connection(
    mut stream: TcpStream,
    session: Arc<TorrentSession>,
    routes: Arc<Mutex<HashMap<String, MediaStreamRoute>>>,
) {
    let request = match read_media_http_request(&mut stream) {
        Ok(request) => request,
        Err(err) => {
            let _ = write_media_response(
                &mut stream,
                "400 Bad Request",
                &[("Content-Type", "text/plain; charset=utf-8")],
                err.as_bytes(),
            );
            return;
        }
    };
    if !matches!(request.method.as_str(), "GET" | "HEAD") {
        let _ = write_media_response(
            &mut stream,
            "405 Method Not Allowed",
            &[
                ("Allow", "GET, HEAD"),
                ("Content-Type", "text/plain; charset=utf-8"),
            ],
            b"method not allowed",
        );
        return;
    }
    let token = match request.path.strip_prefix("/stream/") {
        Some(token)
            if !token.is_empty()
                && token.len() <= 128
                && token.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
        {
            token
        }
        _ => {
            let _ = write_media_response(
                &mut stream,
                "404 Not Found",
                &[("Content-Type", "text/plain; charset=utf-8")],
                b"stream route not found",
            );
            return;
        }
    };
    let route = match routes
        .lock()
        .ok()
        .and_then(|routes| routes.get(token).cloned())
    {
        Some(route) => route,
        None => {
            let _ = write_media_response(
                &mut stream,
                "404 Not Found",
                &[("Content-Type", "text/plain; charset=utf-8")],
                b"stream route not found",
            );
            return;
        }
    };

    let availability = match session.stream_file_availability(&route.id, route.file_index) {
        Ok(availability) => availability,
        Err(err) => {
            session.log(
                LogLevel::Warn,
                "stream",
                format!("could not inspect media stream availability: {err}"),
                None,
            );
            let _ = write_media_response(
                &mut stream,
                "404 Not Found",
                &[("Content-Type", "text/plain; charset=utf-8")],
                err.as_bytes(),
            );
            return;
        }
    };
    if request.method == "HEAD" {
        let content_length = availability.length.to_string();
        let content_type = media_content_type(&availability.name);
        let _ = write_media_response(
            &mut stream,
            "200 OK",
            &[
                ("Accept-Ranges", "bytes"),
                ("Cache-Control", "no-store"),
                ("Content-Type", content_type),
                ("Content-Length", &content_length),
            ],
            b"",
        );
        return;
    }
    if availability.length == 0 {
        let _ = write_media_response(
            &mut stream,
            "200 OK",
            &[
                ("Accept-Ranges", "bytes"),
                ("Cache-Control", "no-store"),
                ("Content-Type", media_content_type(&availability.name)),
                ("Content-Length", "0"),
            ],
            b"",
        );
        return;
    }

    let Some((start, end)) = parse_media_range(request.range.as_deref(), availability.length)
    else {
        let content_range = format!("bytes */{}", availability.length);
        let _ = write_media_response(
            &mut stream,
            "416 Range Not Satisfiable",
            &[
                ("Accept-Ranges", "bytes"),
                ("Content-Range", &content_range),
                ("Content-Type", "text/plain; charset=utf-8"),
            ],
            b"range not satisfiable",
        );
        return;
    };
    session.log(
        LogLevel::Debug,
        "stream",
        format!(
            "media request {} '{}' range={} parsed={start}-{end} verified={} across {} range(s)",
            request.method,
            availability.name,
            request.range.as_deref().unwrap_or("<none>"),
            availability.verified_bytes,
            availability.ranges.len()
        ),
        parse_torrent_log_id(&route.id),
    );
    let (urgent_bytes, lookahead_bytes) = session
        .stream_priority_window(&route.id, route.file_index)
        .ok()
        .flatten()
        .unwrap_or((
            MEDIA_STREAM_URGENT_PRIORITY_BYTES,
            MEDIA_STREAM_LOOKAHEAD_PRIORITY_BYTES,
        ));
    let _ = session.update_stream_priority_quietly(
        &route.id,
        StreamPriorityRequest {
            file_index: route.file_index,
            playhead_offset: start,
            urgent_bytes: Some(urgent_bytes),
            lookahead_bytes: Some(lookahead_bytes),
            supplemental_file_indices: None,
        },
    );
    match read_media_stream_range_with_wait(&session, &route, start, end) {
        Ok(read) => {
            let content_range = format!("bytes {start}-{}/{}", read.end, read.total_length);
            let content_length = read.bytes.len().to_string();
            let _ = write_media_response(
                &mut stream,
                "206 Partial Content",
                &[
                    ("Accept-Ranges", "bytes"),
                    ("Access-Control-Allow-Origin", "*"),
                    ("Cache-Control", "no-store"),
                    ("Content-Type", media_content_type(&availability.name)),
                    ("Content-Range", &content_range),
                    ("Content-Length", &content_length),
                ],
                &read.bytes,
            );
        }
        Err(err) => {
            let (status, retry) = if is_waitable_media_stream_error(&err) {
                ("503 Service Unavailable", true)
            } else {
                ("500 Internal Server Error", false)
            };
            let content_range = format!("bytes */{}", availability.length);
            let mut headers = vec![
                ("Accept-Ranges", "bytes"),
                ("Access-Control-Allow-Origin", "*"),
                ("Cache-Control", "no-store"),
                ("Content-Range", content_range.as_str()),
                ("Content-Type", "text/plain; charset=utf-8"),
            ];
            if retry {
                headers.push(("Retry-After", "1"));
            }
            session.log(
                LogLevel::Debug,
                "stream",
                format!("media stream range {start}-{end} unavailable: {err}"),
                parse_torrent_log_id(&route.id),
            );
            let _ = write_media_response(&mut stream, status, &headers, err.as_bytes());
        }
    }
}

struct MediaStreamRangeRead {
    bytes: Vec<u8>,
    end: u64,
    total_length: u64,
}

fn read_media_stream_range_with_wait(
    session: &TorrentSession,
    route: &MediaStreamRoute,
    start: u64,
    requested_end: u64,
) -> Result<MediaStreamRangeRead, String> {
    let started = Instant::now();
    let mut attempts = 0u32;
    loop {
        attempts = attempts.saturating_add(1);
        let availability = session.stream_file_availability(&route.id, route.file_index)?;
        let read_end = verified_media_range_end(&availability.ranges, start, requested_end);
        let Some(read_end) = read_end else {
            if started.elapsed() < MEDIA_STREAM_WAIT_TIMEOUT {
                thread::sleep(MEDIA_STREAM_WAIT_INTERVAL);
                continue;
            }
            return Err(format!(
                "stream byte range is not verified yet after {} ms; verified ranges: {}",
                started.elapsed().as_millis(),
                verified_media_ranges_summary(&availability.ranges)
            ));
        };
        let length = read_end - start + 1;
        match session.read_stream_file_range(&route.id, route.file_index, start, length) {
            Ok(read) => {
                if attempts > 1 {
                    session.log(
                        LogLevel::Info,
                        "stream",
                        format!(
                            "media stream range starting at {start} became available after {} ms and {attempts} read attempt(s)",
                            started.elapsed().as_millis()
                        ),
                        None,
                    );
                }
                return Ok(MediaStreamRangeRead {
                    bytes: read.bytes,
                    end: read_end,
                    total_length: read.total_length,
                });
            }
            Err(err)
                if is_waitable_media_stream_error(&err)
                    && started.elapsed() < MEDIA_STREAM_WAIT_TIMEOUT =>
            {
                thread::sleep(MEDIA_STREAM_WAIT_INTERVAL);
            }
            Err(err) => return Err(err),
        }
    }
}

fn verified_media_ranges_summary(ranges: &[crate::torrent::storage::VerifiedByteRange]) -> String {
    if ranges.is_empty() {
        return "none".to_string();
    }
    let mut summary = ranges
        .iter()
        .take(6)
        .map(|range| {
            let end = range.offset.saturating_add(range.length.saturating_sub(1));
            format!("{}-{}", range.offset, end)
        })
        .collect::<Vec<_>>()
        .join(", ");
    if ranges.len() > 6 {
        summary.push_str(&format!(", +{} more", ranges.len() - 6));
    }
    summary
}

fn verified_media_range_end(
    ranges: &[crate::torrent::storage::VerifiedByteRange],
    start: u64,
    requested_end: u64,
) -> Option<u64> {
    ranges.iter().find_map(|range| {
        let range_end = range.offset.checked_add(range.length)?.checked_sub(1)?;
        (range.offset <= start && start <= range_end).then(|| range_end.min(requested_end))
    })
}

fn is_waitable_media_stream_error(err: &str) -> bool {
    err.contains("not verified yet") || err.contains("not buffered yet")
}

fn read_media_http_request(stream: &mut TcpStream) -> Result<MediaHttpRequest, String> {
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .map_err(|err| format!("could not configure media stream timeout: {err}"))?;
    let mut bytes = Vec::with_capacity(1024);
    let mut buffer = [0u8; 1024];
    while bytes.len() < 16 * 1024 {
        let read = stream
            .read(&mut buffer)
            .map_err(|err| format!("could not read media stream request: {err}"))?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let request = std::str::from_utf8(&bytes)
        .map_err(|_| "media stream request was not valid UTF-8".to_string())?;
    let mut lines = request.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| "media stream request was empty".to_string())?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| "media stream request omitted method".to_string())?
        .to_string();
    let path = request_parts
        .next()
        .ok_or_else(|| "media stream request omitted path".to_string())?
        .split('?')
        .next()
        .unwrap_or_default()
        .to_string();
    let mut range = None;
    for line in lines {
        if line.is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("range") {
            range = Some(value.trim().to_string());
        }
    }
    Ok(MediaHttpRequest {
        method,
        path,
        range,
    })
}

fn parse_media_range(header: Option<&str>, total_length: u64) -> Option<(u64, u64)> {
    if total_length == 0 {
        return Some((0, 0));
    }
    let max_end_from_start = |start: u64| {
        total_length
            .saturating_sub(1)
            .min(start.saturating_add(MEDIA_STREAM_CHUNK_LIMIT - 1))
    };
    let Some(header) = header else {
        return Some((0, max_end_from_start(0)));
    };
    let spec = header.trim().strip_prefix("bytes=")?;
    let (start_text, end_text) = spec.split_once('-')?;
    if start_text.is_empty() {
        let suffix = end_text.parse::<u64>().ok()?;
        if suffix == 0 {
            return None;
        }
        let start = total_length.saturating_sub(suffix);
        return Some((start, total_length - 1));
    }

    let start = start_text.parse::<u64>().ok()?;
    if start >= total_length {
        return None;
    }
    let requested_end = if end_text.is_empty() {
        total_length - 1
    } else {
        end_text.parse::<u64>().ok()?.min(total_length - 1)
    };
    if requested_end < start {
        return None;
    }
    Some((start, requested_end.min(max_end_from_start(start))))
}

fn media_content_type(name: &str) -> &'static str {
    let extension = Path::new(name)
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);
    match extension.as_deref() {
        Some("mp4") | Some("m4v") => "video/mp4",
        Some("webm") => "video/webm",
        Some("ogv") => "video/ogg",
        Some("mov") => "video/quicktime",
        Some("mkv") => "video/x-matroska",
        Some("avi") => "video/x-msvideo",
        Some("srt") => "application/x-subrip; charset=utf-8",
        Some("vtt") => "text/vtt; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn write_media_response(
    stream: &mut TcpStream,
    status: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> std::io::Result<()> {
    let has_content_length = headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("content-length"));
    let mut response = format!("HTTP/1.1 {status}\r\nConnection: close\r\n");
    if !has_content_length {
        response.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    for (name, value) in headers {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");
    stream.write_all(response.as_bytes())?;
    stream.write_all(body)
}

#[tauri::command]
fn default_download_dir(state: tauri::State<'_, AppState>) -> String {
    state
        .session
        .default_output_dir()
        .to_string_lossy()
        .into_owned()
}

#[tauri::command]
fn backend_log_file_path(state: tauri::State<'_, AppState>) -> String {
    state.session.log_file_path().to_string_lossy().into_owned()
}

#[tauri::command]
fn take_pending_open_sources(state: tauri::State<'_, AppState>) -> Vec<String> {
    state.take_pending_sources()
}

#[tauri::command]
fn list_torrents(state: tauri::State<'_, AppState>) -> TorrentListResponse {
    state.session.list()
}

#[tauri::command]
fn list_torrent_summaries(state: tauri::State<'_, AppState>) -> TorrentSummaryListResponse {
    state.session.list_summaries()
}

#[tauri::command]
fn torrent_details(
    state: tauri::State<'_, AppState>,
    id: String,
) -> Result<TorrentDetails, String> {
    state.session.details(&id)
}

#[tauri::command]
fn preview_torrent(
    state: tauri::State<'_, AppState>,
    request: AddTorrentRequest,
) -> Result<AddTorrentResponse, String> {
    state.session.preview(request)
}

#[tauri::command]
fn add_torrent(
    state: tauri::State<'_, AppState>,
    request: AddTorrentRequest,
) -> Result<AddTorrentResponse, String> {
    let start_now = !request.paused;
    let response = state.session.add(request)?;
    if start_now {
        if let Some(id) = response.id {
            if let Err(err) = state.start_torrent_worker(id.to_string()) {
                state.session.log(LogLevel::Error, "runtime", err, Some(id));
            }
        }
    }
    Ok(response)
}

#[tauri::command]
fn pause_torrent(
    state: tauri::State<'_, AppState>,
    id: String,
) -> Result<EmptyJsonResponse, String> {
    let response = state.session.pause(&id)?;
    state.stop_tracker_background(id);
    Ok(response)
}

#[tauri::command]
fn resume_torrent(
    state: tauri::State<'_, AppState>,
    id: String,
) -> Result<EmptyJsonResponse, String> {
    let response = state.session.resume(&id)?;
    state.start_torrent_worker(id)?;
    Ok(response)
}

#[tauri::command]
fn announce_torrent(
    state: tauri::State<'_, AppState>,
    id: String,
) -> Result<EmptyJsonResponse, String> {
    state.session.announce(&id)
}

#[tauri::command]
fn download_webseed_torrent(
    state: tauri::State<'_, AppState>,
    id: String,
) -> Result<EmptyJsonResponse, String> {
    state.session.download_webseed(&id)
}

#[tauri::command]
fn download_peer_torrent(
    state: tauri::State<'_, AppState>,
    id: String,
) -> Result<EmptyJsonResponse, String> {
    state.session.download_from_peers(&id)
}

#[tauri::command]
fn recheck_torrent(
    state: tauri::State<'_, AppState>,
    id: String,
) -> Result<EmptyJsonResponse, String> {
    state.session.recheck(&id)
}

#[tauri::command]
fn query_dht_torrent(
    state: tauri::State<'_, AppState>,
    id: String,
) -> Result<EmptyJsonResponse, String> {
    state.session.query_dht(&id)
}

#[tauri::command]
fn resolve_magnet_torrent(
    state: tauri::State<'_, AppState>,
    id: String,
) -> Result<EmptyJsonResponse, String> {
    state.session.resolve_magnet(&id)
}

#[tauri::command]
fn fetch_metadata_torrent(
    state: tauri::State<'_, AppState>,
    id: String,
) -> Result<EmptyJsonResponse, String> {
    state.session.fetch_metadata(&id)
}

#[tauri::command]
fn delete_torrent(
    state: tauri::State<'_, AppState>,
    id: String,
    delete_files: bool,
) -> Result<EmptyJsonResponse, String> {
    state.stop_tracker_background(id.clone());
    let cleanup = state.session.remove_for_delete(&id, delete_files)?;
    if cleanup.delete_files {
        let session = Arc::clone(&state.session);
        let _ = thread::Builder::new()
            .name(format!("novatorrent-delete-files-{}", cleanup.id))
            .spawn(move || {
                session.cleanup_deleted_torrent(cleanup);
            });
    }
    Ok(EmptyJsonResponse {})
}

#[tauri::command]
fn update_torrent_files(
    state: tauri::State<'_, AppState>,
    id: String,
    only_files: Vec<usize>,
) -> Result<EmptyJsonResponse, String> {
    state.session.update_files(&id, only_files)
}

#[tauri::command]
fn update_torrent_file_priority(
    state: tauri::State<'_, AppState>,
    id: String,
    file_index: usize,
    priority: u8,
) -> Result<EmptyJsonResponse, String> {
    state
        .session
        .update_file_priority(&id, file_index, priority)
}

#[tauri::command]
fn update_torrent_options(
    state: tauri::State<'_, AppState>,
    id: String,
    request: UpdateTorrentOptionsRequest,
) -> Result<EmptyJsonResponse, String> {
    state.session.update_options(&id, request)
}

#[tauri::command]
async fn stream_file_availability(
    state: tauri::State<'_, AppState>,
    id: String,
    file_index: usize,
) -> Result<TorrentFileAvailability, String> {
    let session = Arc::clone(&state.session);
    tauri::async_runtime::spawn_blocking(move || session.stream_file_availability(&id, file_index))
        .await
        .map_err(|err| format!("file availability worker failed: {err}"))?
}

#[tauri::command]
async fn stream_file_url(
    state: tauri::State<'_, AppState>,
    id: String,
    file_index: usize,
) -> Result<String, String> {
    let session = Arc::clone(&state.session);
    let validation_id = id.clone();
    let availability = tauri::async_runtime::spawn_blocking(move || {
        session.stream_file_availability(&validation_id, file_index)
    })
    .await
    .map_err(|err| format!("file stream URL worker failed: {err}"))??;

    let port = state.media_stream_port.load(Ordering::Acquire);
    if port == 0 {
        return Err("local media stream server is not running".to_string());
    }
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or_default();
    let sequence = state
        .next_media_stream_token
        .fetch_add(1, Ordering::Relaxed);
    let token = format!("{timestamp:016x}{sequence:016x}");
    state
        .media_stream_tokens
        .lock()
        .map_err(|_| "media stream route lock poisoned".to_string())?
        .insert(
            token.clone(),
            MediaStreamRoute {
                id: id.clone(),
                file_index,
            },
        );
    state.session.log(
        LogLevel::Info,
        "stream",
        format!("prepared local playback URL for '{}'", availability.name),
        None,
    );
    Ok(format!("http://127.0.0.1:{port}/stream/{token}"))
}

#[tauri::command]
async fn subtitle_file_text(
    state: tauri::State<'_, AppState>,
    id: String,
    file_index: usize,
) -> Result<SubtitleFileText, String> {
    let session = Arc::clone(&state.session);
    tauri::async_runtime::spawn_blocking(move || {
        let availability = session.stream_file_availability(&id, file_index)?;
        let extension = Path::new(&availability.name)
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_ascii_lowercase);
        if !matches!(extension.as_deref(), Some("srt") | Some("vtt")) {
            return Err("only SRT and WebVTT subtitle files can be loaded".to_string());
        }
        if availability.length > MEDIA_STREAM_CHUNK_LIMIT {
            return Err(format!(
                "subtitle file exceeds the {MEDIA_STREAM_CHUNK_LIMIT} byte limit"
            ));
        }
        if !availability.complete {
            return Err("subtitle file is still downloading".to_string());
        }
        let read = session.read_stream_file_range(&id, file_index, 0, availability.length)?;
        Ok(SubtitleFileText {
            file_index,
            name: availability.name,
            text: decode_subtitle_text(&read.bytes),
        })
    })
    .await
    .map_err(|err| format!("subtitle worker failed: {err}"))?
}

fn decode_subtitle_text(bytes: &[u8]) -> String {
    if bytes.starts_with(&[0xff, 0xfe]) {
        let utf16 = bytes[2..]
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect::<Vec<_>>();
        return String::from_utf16_lossy(&utf16);
    }
    if bytes.starts_with(&[0xfe, 0xff]) {
        let utf16 = bytes[2..]
            .chunks_exact(2)
            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
            .collect::<Vec<_>>();
        return String::from_utf16_lossy(&utf16);
    }
    String::from_utf8_lossy(bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes)).into_owned()
}

#[tauri::command]
async fn open_media_window(
    app: AppHandle,
    state: tauri::State<'_, AppState>,
    id: String,
    file_index: usize,
) -> Result<(), String> {
    let session = Arc::clone(&state.session);
    let validation_id = id.clone();
    let availability = tauri::async_runtime::spawn_blocking(move || {
        session.stream_file_availability(&validation_id, file_index)
    })
    .await
    .map_err(|err| format!("media window validation failed: {err}"))??;

    let label = media_window_label(&id, file_index);
    if let Some(window) = app.get_webview_window(&label) {
        window.show().map_err(error_to_string)?;
        window.set_focus().map_err(error_to_string)?;
        return Ok(());
    }

    let mut route = String::from(media_player_route());
    route.push_str("?id=");
    route.push_str(&percent_encode_uri_component(&id));
    route.push_str("&fileIndex=");
    route.push_str(&file_index.to_string());

    let window = WebviewWindowBuilder::new(&app, label, WebviewUrl::App(route.into()))
        .title(format!("NovaTorrent - {}", availability.name))
        .inner_size(1040.0, 720.0)
        .min_inner_size(700.0, 460.0)
        .resizable(true)
        .build()
        .map_err(error_to_string)?;
    let closed_app = app.clone();
    let closed_torrent_id = id.clone();
    window.on_window_event(move |event| {
        if matches!(event, tauri::WindowEvent::Destroyed) {
            if let Some(state) = closed_app.try_state::<AppState>() {
                let _ = state.session.clear_stream_priority(&closed_torrent_id);
            }
            let _ = closed_app.emit(
                "media-player-closed",
                MediaPlayerClosedPayload {
                    torrent_id: closed_torrent_id.clone(),
                    file_index,
                },
            );
        }
    });
    window.set_focus().map_err(error_to_string)
}

#[tauri::command]
fn close_media_window(
    app: AppHandle,
    state: tauri::State<'_, AppState>,
    id: String,
    file_index: usize,
) -> Result<(), String> {
    let _ = state.session.clear_stream_priority(&id);
    if let Some(window) = app.get_webview_window(&media_window_label(&id, file_index)) {
        window.close().map_err(error_to_string)?;
    }
    Ok(())
}

#[tauri::command]
fn media_player_log(
    state: tauri::State<'_, AppState>,
    request: MediaPlayerLogRequest,
) -> Result<EmptyJsonResponse, String> {
    let mut parts = vec![
        format!("event={}", request.event),
        format!("file_index={}", request.file_index),
    ];
    if let Some(value) = request.current_time {
        parts.push(format!("current_time={value:.3}"));
    }
    if let Some(value) = request.duration {
        parts.push(format!("duration={value:.3}"));
    }
    if let Some(value) = request.ready_state {
        parts.push(format!("ready_state={value}"));
    }
    if let Some(value) = request.network_state {
        parts.push(format!("network_state={value}"));
    }
    if let Some(value) = request.paused {
        parts.push(format!("paused={value}"));
    }
    if let Some(value) = request.seeking {
        parts.push(format!("seeking={value}"));
    }
    if let Some(value) = request.target_time {
        parts.push(format!("target_time={value:.3}"));
    }
    if let Some(value) = request.target_offset {
        parts.push(format!("target_offset={value}"));
    }
    if let Some(value) = request.target_ready {
        parts.push(format!("target_ready={value}"));
    }
    if let Some(value) = request.buffered_ahead_seconds {
        parts.push(format!("buffered_ahead_seconds={value:.3}"));
    }
    if let Some(value) = request.retry_key {
        parts.push(format!("retry_key={value}"));
    }
    if let Some(value) = request.network_retries {
        parts.push(format!("network_retries={value}"));
    }
    if let Some(message) = request.message {
        if !message.is_empty() {
            parts.push(format!("note={message}"));
        }
    }
    state.session.log(
        LogLevel::Debug,
        "media-ui",
        parts.join(" "),
        parse_torrent_log_id(&request.id),
    );
    Ok(EmptyJsonResponse {})
}

#[tauri::command]
fn set_stream_priority(
    state: tauri::State<'_, AppState>,
    id: String,
    request: StreamPriorityRequest,
) -> Result<StreamPriorityStatus, String> {
    state.session.set_stream_priority(&id, request)
}

#[tauri::command]
fn clear_stream_priority(
    state: tauri::State<'_, AppState>,
    id: String,
) -> Result<EmptyJsonResponse, String> {
    state.session.clear_stream_priority(&id)
}

#[tauri::command]
fn backend_logs(state: tauri::State<'_, AppState>, torrent_id: Option<u64>) -> Vec<LogEntry> {
    state.session.logs(torrent_id)
}

#[tauri::command]
fn backend_logs_after(
    state: tauri::State<'_, AppState>,
    torrent_id: Option<u64>,
    after_id: Option<u64>,
) -> Vec<LogEntry> {
    state.session.logs_after(torrent_id, after_id)
}

#[tauri::command]
async fn open_add_torrent_window(app: AppHandle, source: Option<String>) -> Result<(), String> {
    open_add_window(&app, source)
}

#[tauri::command]
fn close_add_torrent_window(app: AppHandle) -> Result<(), String> {
    if let Some(window) = app.get_webview_window("add-torrent") {
        window.close().map_err(error_to_string)?;
    }
    Ok(())
}

fn open_add_window(app: &AppHandle, source: Option<String>) -> Result<(), String> {
    let label = "add-torrent";
    if let Some(window) = app.get_webview_window(label) {
        window.show().map_err(error_to_string)?;
        window.set_focus().map_err(error_to_string)?;
        if let Some(source) = source {
            window
                .emit("add-torrent-source", source)
                .map_err(error_to_string)?;
        }
        return Ok(());
    }

    let mut route = String::from(add_torrent_route());
    if let Some(source) = source.as_ref() {
        route.push_str("?source=");
        route.push_str(&percent_encode_uri_component(source));
    }

    WebviewWindowBuilder::new(app, label, WebviewUrl::App(route.into()))
        .title("Add Torrent")
        .inner_size(780.0, 720.0)
        .min_inner_size(620.0, 560.0)
        .resizable(true)
        .visible(false)
        .background_color(Color(16, 20, 25, 255))
        .on_page_load(|window, payload| {
            if payload.event() == PageLoadEvent::Finished {
                let _ = window.show();
                let _ = window.set_focus();
            }
        })
        .build()
        .map_err(error_to_string)?;

    Ok(())
}

fn add_torrent_route() -> &'static str {
    if cfg!(debug_assertions) {
        "add/"
    } else {
        "add/index.html"
    }
}

fn media_player_route() -> &'static str {
    if cfg!(debug_assertions) {
        "media/"
    } else {
        "media/index.html"
    }
}

fn media_window_label(id: &str, file_index: usize) -> String {
    let mut safe_id = id
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(32)
        .collect::<String>();
    if safe_id.is_empty() {
        safe_id.push_str("torrent");
    }
    format!("media-{safe_id}-{file_index}")
}

fn parse_torrent_log_id(id: &str) -> Option<u64> {
    id.parse().ok()
}

fn supported_open_sources(
    args: impl IntoIterator<Item = String>,
    working_directory: Option<&Path>,
) -> Vec<String> {
    let mut seen = HashSet::new();
    args.into_iter()
        .filter_map(|arg| normalize_open_source(&arg, working_directory, 0))
        .filter(|source| seen.insert(source.clone()))
        .collect()
}

fn normalize_open_source(
    value: &str,
    working_directory: Option<&Path>,
    depth: usize,
) -> Option<String> {
    if depth > 2 {
        return None;
    }
    let value = value.trim();
    if value.is_empty() || value.len() > 32_768 || value.chars().any(char::is_control) {
        return None;
    }

    if let Some(rest) = strip_ascii_prefix(value, "magnet:?") {
        let normalized = format!("magnet:?{rest}");
        torrent::magnet::MagnetLink::parse(&normalized).ok()?;
        return Some(normalized);
    }

    if let Some(rest) = strip_ascii_prefix(value, "novatorrent://") {
        let (route, query) = rest.split_once('?')?;
        let route = route.trim_end_matches('/');
        if !route.eq_ignore_ascii_case("open") && !route.eq_ignore_ascii_case("add") {
            return None;
        }
        for pair in query.split('&') {
            let (key, encoded_value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = percent_decode_query_component(key)?;
            if !matches!(key.as_str(), "source" | "magnet" | "url" | "file" | "path") {
                continue;
            }
            let nested = percent_decode_query_component(encoded_value)?;
            if let Some(source) = normalize_open_source(&nested, working_directory, depth + 1) {
                return Some(source);
            }
        }
        return None;
    }

    let path = PathBuf::from(value);
    let path = if path.is_absolute() {
        path
    } else {
        working_directory?.join(path)
    };
    if !path.is_file()
        || !path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("torrent"))
    {
        return None;
    }
    std::fs::canonicalize(path)
        .ok()?
        .to_str()
        .map(str::to_owned)
}

fn strip_ascii_prefix<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    let candidate = value.get(..prefix.len())?;
    candidate
        .eq_ignore_ascii_case(prefix)
        .then(|| &value[prefix.len()..])
}

fn percent_decode_query_component(value: &str) -> Option<String> {
    let source = value.as_bytes();
    let mut decoded = Vec::with_capacity(source.len());
    let mut index = 0;
    while index < source.len() {
        match source[index] {
            b'%' => {
                let high = *source.get(index + 1)?;
                let low = *source.get(index + 2)?;
                decoded.push((hex_nibble(high)? << 4) | hex_nibble(low)?);
                index += 3;
            }
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded).ok()
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn percent_encode_uri_component(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        let is_unreserved =
            matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~');
        if is_unreserved {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }
    out
}

fn error_to_string(err: impl std::fmt::Display) -> String {
    err.to_string()
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let mut builder = tauri::Builder::default();

    #[cfg(desktop)]
    {
        builder = builder.plugin(tauri_plugin_single_instance::init(|app, argv, cwd| {
            let sources = supported_open_sources(argv, Some(Path::new(&cwd)));
            if sources.is_empty() {
                return;
            }

            if let Some(state) = app.try_state::<AppState>() {
                for source in sources {
                    state.session.log(
                        LogLevel::Info,
                        "open-with",
                        "received and validated torrent source from a secondary launch",
                        None,
                    );
                    if let Err(err) = open_add_window(app, Some(source)) {
                        state.session.log(
                            LogLevel::Warn,
                            "open-with",
                            format!("could not open the Add Torrent window: {err}"),
                            None,
                        );
                    }
                }
            }
        }));
    }

    let app = builder
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            let default_output_dir = app.path().download_dir()?;
            let state_dir = default_output_dir.join("NovaTorrent");
            std::fs::create_dir_all(&state_dir)?;
            let log_dir = app.path().app_log_dir()?;
            std::fs::create_dir_all(&log_dir)?;
            let log_file_path = log_dir.join("novatorrent.log");
            let working_directory = env::current_dir().ok();
            let pending_sources = supported_open_sources(
                env::args().skip(1).collect::<Vec<_>>(),
                working_directory.as_deref(),
            );
            app.manage(AppState::new(
                default_output_dir,
                state_dir,
                log_file_path,
                pending_sources,
            ));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            default_download_dir,
            backend_log_file_path,
            take_pending_open_sources,
            list_torrents,
            list_torrent_summaries,
            torrent_details,
            preview_torrent,
            add_torrent,
            close_add_torrent_window,
            pause_torrent,
            resume_torrent,
            announce_torrent,
            download_webseed_torrent,
            download_peer_torrent,
            recheck_torrent,
            query_dht_torrent,
            resolve_magnet_torrent,
            fetch_metadata_torrent,
            delete_torrent,
            update_torrent_files,
            update_torrent_file_priority,
            update_torrent_options,
            stream_file_availability,
            stream_file_url,
            subtitle_file_text,
            open_media_window,
            close_media_window,
            media_player_log,
            set_stream_priority,
            clear_stream_priority,
            backend_logs,
            backend_logs_after,
            open_add_torrent_window
        ])
        .build(tauri::generate_context!())
        .expect("error while building NovaTorrent");

    app.run(|app_handle, event| {
        if let tauri::RunEvent::ExitRequested { api, .. } = event {
            if let Some(state) = app_handle.try_state::<AppState>() {
                if !state.shutdown_started.swap(true, Ordering::AcqRel) {
                    api.prevent_exit();
                    state.shutdown_gracefully(Duration::from_secs(5));
                    app_handle.exit(0);
                }
            }
        }
    });
}

#[cfg(test)]
mod open_source_tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    const MAGNET: &str =
        "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=NovaTorrent";

    #[test]
    fn validates_deduplicates_and_resolves_open_sources() {
        let directory = env::temp_dir().join(format!(
            "novatorrent-open-source-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock should be after the epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).expect("test directory should be created");
        let torrent = directory.join("sample.torrent");
        std::fs::write(&torrent, b"de").expect("test torrent should be written");

        let sources = supported_open_sources(
            vec![
                MAGNET.to_string(),
                MAGNET.to_string(),
                "sample.torrent".to_string(),
                "missing.torrent".to_string(),
                "--unsafe.torrent".to_string(),
            ],
            Some(&directory),
        );

        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0], MAGNET);
        assert_eq!(
            sources[1],
            std::fs::canonicalize(&torrent).unwrap().to_str().unwrap()
        );
        std::fs::remove_dir_all(directory).expect("test directory should be removed");
    }

    #[test]
    fn unwraps_only_valid_novatorrent_routes_and_payloads() {
        let encoded = percent_encode_uri_component(MAGNET);
        assert_eq!(
            normalize_open_source(&format!("novatorrent://open?magnet={encoded}"), None, 0)
                .as_deref(),
            Some(MAGNET)
        );
        assert!(normalize_open_source("novatorrent://settings?magnet=x", None, 0).is_none());
        assert!(normalize_open_source("novatorrent://open?magnet=%ZZ", None, 0).is_none());
        assert!(normalize_open_source("magnet:?xt=urn:btih:not-a-hash", None, 0).is_none());
    }

    #[test]
    fn media_stream_ranges_are_capped_and_retryable_errors_are_identified() {
        assert_eq!(
            parse_media_range(None, 10 * 1024 * 1024),
            Some((0, MEDIA_STREAM_CHUNK_LIMIT - 1))
        );
        assert_eq!(parse_media_range(Some("bytes=4-9"), 20), Some((4, 9)));
        assert_eq!(parse_media_range(Some("bytes=18-99"), 20), Some((18, 19)));
        assert_eq!(parse_media_range(Some("bytes=50-99"), 20), None);
        assert_eq!(
            verified_media_range_end(
                &[crate::torrent::storage::VerifiedByteRange {
                    offset: 0,
                    length: 1_048_436,
                }],
                0,
                2_097_151,
            ),
            Some(1_048_435)
        );
        assert_eq!(
            verified_media_range_end(
                &[crate::torrent::storage::VerifiedByteRange {
                    offset: 3_145_728,
                    length: 1_932_323,
                }],
                3_145_728,
                5_242_879,
            ),
            Some(5_078_050)
        );
        assert_eq!(
            verified_media_range_end(
                &[crate::torrent::storage::VerifiedByteRange {
                    offset: 0,
                    length: 1_048_436,
                }],
                1_048_436,
                2_097_151,
            ),
            None
        );
        assert!(is_waitable_media_stream_error(
            "stream byte range is not verified yet"
        ));
        assert!(is_waitable_media_stream_error(
            "stream data is not buffered yet"
        ));
        assert!(!is_waitable_media_stream_error(
            "torrent metadata is not available yet"
        ));
    }

    #[test]
    fn subtitle_text_decoding_handles_common_boms_and_caption_mime_types() {
        assert_eq!(decode_subtitle_text(b"\xef\xbb\xbfHello"), "Hello");
        assert_eq!(decode_subtitle_text(&[0xff, 0xfe, b'H', 0, b'i', 0]), "Hi");
        assert_eq!(decode_subtitle_text(&[0xfe, 0xff, 0, b'H', 0, b'i']), "Hi");
        assert_eq!(
            media_content_type("captions.SRT"),
            "application/x-subrip; charset=utf-8"
        );
        assert_eq!(
            media_content_type("captions.vtt"),
            "text/vtt; charset=utf-8"
        );
    }
}
