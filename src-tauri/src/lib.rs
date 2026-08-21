use std::{
    collections::HashSet,
    env,
    io::ErrorKind,
    net::{IpAddr, SocketAddrV4, TcpListener, UdpSocket},
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager, WebviewUrl, WebviewWindowBuilder};

use crate::torrent::session::{
    AddTorrentRequest, AddTorrentResponse, EmptyJsonResponse, LogEntry, LogLevel, TorrentDetails,
    TorrentFileHash, TorrentListResponse, TorrentSession, UpdateTorrentOptionsRequest,
};

mod torrent;

struct AppState {
    session: Arc<TorrentSession>,
    pending_sources: Mutex<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
struct SafeTestTorrent {
    label: String,
    path: String,
    source_url: String,
    sha256: String,
    payload_size: u64,
}

impl AppState {
    fn new(default_output_dir: PathBuf, pending_sources: Vec<String>) -> Self {
        let session = Arc::new(TorrentSession::new(default_output_dir));
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
        };
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
            state
                .session
                .log(LogLevel::Error, "tracker", err, None);
        }
        if let Err(err) = state.start_lsd_service() {
            state.session.log(LogLevel::Warn, "lsd", err, None);
        }
        for id in state.session.runnable_ids() {
            if let Err(err) = state.start_torrent_worker(id.to_string()) {
                state
                    .session
                    .log(LogLevel::Error, "runtime", err, Some(id));
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

    fn start_torrent_worker(&self, id: String) -> Result<(), String> {
        let session = Arc::clone(&self.session);
        let thread_label = id.chars().take(12).collect::<String>();
        thread::Builder::new()
            .name(format!("novatorrent-{thread_label}"))
            .spawn(move || {
                let _ = session.run_torrent(&id);
            })
            .map(|_| ())
            .map_err(|err| format!("could not start torrent worker: {err}"))
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
            return Err("LSD disabled because no inbound BitTorrent TCP listener is active"
                .to_string());
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
                UdpSocket::bind(("0.0.0.0", 0))
                    .map_err(|err| format!("could not bind a UDP socket for LSD announces: {err}"))?
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
}

#[tauri::command]
fn default_download_dir(state: tauri::State<'_, AppState>) -> String {
    state.session.default_output_dir().to_string_lossy().into_owned()
}

#[tauri::command]
fn backend_log_file_path(state: tauri::State<'_, AppState>) -> String {
    state.session.log_file_path().to_string_lossy().into_owned()
}

#[tauri::command]
fn safe_test_torrents() -> Vec<SafeTestTorrent> {
    let project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(PathBuf::from);
    let Some(project_root) = project_root else {
        return Vec::new();
    };

    let fixtures = [
        SafeTestTorrent {
            label: "Alpine minirootfs 3.6 MiB".to_string(),
            path: project_root
                .join("fixtures")
                .join("safe")
                .join("alpine-minirootfs-3.23.3-x86_64.tar.gz.torrent")
                .to_string_lossy()
                .into_owned(),
            source_url: "https://fosstorrents.com/files/download.php?file=alpine-minirootfs-3.23.3-x86_64.tar.gz.torrent".to_string(),
            sha256: "8DABC8875DE14A68C587F32F07AF4B667ABC56327B35F1A7748E08A690B855A8".to_string(),
            payload_size: 3_713_234,
        },
        SafeTestTorrent {
            label: "Debian 13.6 netinst 755 MiB".to_string(),
            path: project_root
                .join("fixtures")
                .join("safe")
                .join("debian-13.6.0-amd64-netinst.iso.torrent")
                .to_string_lossy()
                .into_owned(),
            source_url: "https://cdimage.debian.org/debian-cd/current/amd64/bt-cd/debian-13.6.0-amd64-netinst.iso.torrent".to_string(),
            sha256: "763E5F84C8AFF61DA94F20604E078900825AB8C4D44DC66D6B9DE73C5BE29976".to_string(),
            payload_size: 791_674_880,
        },
    ];

    fixtures
        .into_iter()
        .filter(|fixture| PathBuf::from(&fixture.path).exists())
        .collect()
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
                state
                    .session
                    .log(LogLevel::Error, "runtime", err, Some(id));
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
fn update_torrent_options(
    state: tauri::State<'_, AppState>,
    id: String,
    request: UpdateTorrentOptionsRequest,
) -> Result<EmptyJsonResponse, String> {
    state.session.update_options(&id, request)
}

#[tauri::command]
async fn hash_torrent_file(
    state: tauri::State<'_, AppState>,
    id: String,
    file_index: usize,
) -> Result<TorrentFileHash, String> {
    let session = Arc::clone(&state.session);
    tauri::async_runtime::spawn_blocking(move || session.hash_torrent_file(&id, file_index))
        .await
        .map_err(|err| format!("file hash worker failed: {err}"))?
}

#[tauri::command]
fn open_virustotal_report(sha256: String) -> Result<(), String> {
    if sha256.len() != 64 || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("VirusTotal report requires a 64-character SHA-256 hash".to_string());
    }
    let url = format!(
        "https://www.virustotal.com/gui/file/{}",
        sha256.to_ascii_lowercase()
    );
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = Command::new("rundll32.exe");
        command.args(["url.dll,FileProtocolHandler", &url]);
        command
    };
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = Command::new("open");
        command.arg(&url);
        command
    };
    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut command = Command::new("xdg-open");
        command.arg(&url);
        command
    };
    command
        .spawn()
        .map(|_| ())
        .map_err(|err| format!("could not open VirusTotal report: {err}"))
}

#[tauri::command]
fn backend_logs(state: tauri::State<'_, AppState>, torrent_id: Option<u64>) -> Vec<LogEntry> {
    state.session.logs(torrent_id)
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
        .build()
        .map_err(error_to_string)?
        .set_focus()
        .map_err(error_to_string)
}

fn add_torrent_route() -> &'static str {
    if cfg!(debug_assertions) {
        "add/"
    } else {
        "add/index.html"
    }
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
    std::fs::canonicalize(path).ok()?.to_str().map(str::to_owned)
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

    builder
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            let default_output_dir = app.path().download_dir()?.join("NovaTorrent");
            std::fs::create_dir_all(&default_output_dir)?;
            let working_directory = env::current_dir().ok();
            let pending_sources = supported_open_sources(
                env::args().skip(1).collect::<Vec<_>>(),
                working_directory.as_deref(),
            );
            app.manage(AppState::new(default_output_dir, pending_sources));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            default_download_dir,
            backend_log_file_path,
            safe_test_torrents,
            take_pending_open_sources,
            list_torrents,
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
            update_torrent_options,
            hash_torrent_file,
            open_virustotal_report,
            backend_logs,
            open_add_torrent_window
        ])
        .run(tauri::generate_context!())
        .expect("error while running NovaTorrent");
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
        assert_eq!(sources[1], std::fs::canonicalize(&torrent).unwrap().to_str().unwrap());
        std::fs::remove_dir_all(directory).expect("test directory should be removed");
    }

    #[test]
    fn unwraps_only_valid_novatorrent_routes_and_payloads() {
        let encoded = percent_encode_uri_component(MAGNET);
        assert_eq!(
            normalize_open_source(
                &format!("novatorrent://open?magnet={encoded}"),
                None,
                0
            )
            .as_deref(),
            Some(MAGNET)
        );
        assert!(normalize_open_source("novatorrent://settings?magnet=x", None, 0).is_none());
        assert!(normalize_open_source("novatorrent://open?magnet=%ZZ", None, 0).is_none());
        assert!(normalize_open_source("magnet:?xt=urn:btih:not-a-hash", None, 0).is_none());
    }
}
