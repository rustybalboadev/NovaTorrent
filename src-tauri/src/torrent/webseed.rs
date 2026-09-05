use std::{
    io::{ErrorKind, Read, Write},
    net::{TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use native_tls::TlsConnector;
use serde::{Deserialize, Serialize};

use crate::torrent::{metainfo::TorrentFile, peerwire::BandwidthLimiter, sha1, storage};

const MAX_WEBSEED_REDIRECTS: usize = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSeedStatus {
    pub url: String,
    pub state: String,
    pub message: Option<String>,
    pub bytes_downloaded: u64,
}

#[derive(Debug, Clone)]
pub struct WebSeedDownloadResult {
    pub url: String,
    pub output_path: PathBuf,
    pub bytes_written: u64,
    pub files_written: usize,
    pub pieces_verified: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpEndpoint {
    pub host: String,
    pub port: u16,
    pub path_and_query: String,
    pub use_tls: bool,
}

pub fn initial_statuses(urls: &[String]) -> Vec<WebSeedStatus> {
    urls.iter()
        .map(|url| WebSeedStatus {
            url: url.clone(),
            state: "Not contacted".to_string(),
            message: None,
            bytes_downloaded: 0,
        })
        .collect()
}

pub fn build_file_url(
    seed_url: &str,
    torrent_name: &str,
    file: &TorrentFile,
    multi_file: bool,
) -> String {
    if !seed_url.ends_with('/') {
        return seed_url.to_string();
    }

    let mut out = seed_url.to_string();
    if multi_file {
        out.push_str(&encode_path_component(torrent_name));
        out.push('/');
    }
    for (index, component) in file.components.iter().enumerate() {
        if index > 0 {
            out.push('/');
        }
        out.push_str(&encode_path_component(component));
    }
    out
}

pub fn download_single_file(
    seed_url: &str,
    torrent_name: &str,
    output_folder: &Path,
    file: &TorrentFile,
    piece_length: u64,
    piece_hashes: &[[u8; 20]],
    multi_file: bool,
) -> Result<WebSeedDownloadResult, String> {
    download_single_file_cancellable(
        seed_url,
        torrent_name,
        output_folder,
        file,
        piece_length,
        piece_hashes,
        multi_file,
        Arc::new(AtomicBool::new(false)),
        false,
    )
}

pub fn download_single_file_cancellable(
    seed_url: &str,
    torrent_name: &str,
    output_folder: &Path,
    file: &TorrentFile,
    piece_length: u64,
    piece_hashes: &[[u8; 20]],
    multi_file: bool,
    cancelled: Arc<AtomicBool>,
    overwrite: bool,
) -> Result<WebSeedDownloadResult, String> {
    download_torrent_cancellable(
        seed_url,
        torrent_name,
        output_folder,
        std::slice::from_ref(file),
        piece_length,
        piece_hashes,
        multi_file,
        cancelled,
        overwrite,
    )
}

pub fn download_torrent_cancellable(
    seed_url: &str,
    torrent_name: &str,
    output_folder: &Path,
    files: &[TorrentFile],
    piece_length: u64,
    piece_hashes: &[[u8; 20]],
    multi_file: bool,
    cancelled: Arc<AtomicBool>,
    overwrite: bool,
) -> Result<WebSeedDownloadResult, String> {
    download_torrent_cancellable_with_limiter(
        seed_url,
        torrent_name,
        output_folder,
        files,
        piece_length,
        piece_hashes,
        multi_file,
        cancelled,
        overwrite,
        None,
    )
}

pub fn download_torrent_cancellable_with_limiter(
    seed_url: &str,
    torrent_name: &str,
    output_folder: &Path,
    files: &[TorrentFile],
    piece_length: u64,
    piece_hashes: &[[u8; 20]],
    multi_file: bool,
    cancelled: Arc<AtomicBool>,
    overwrite: bool,
    download_limiter: Option<&BandwidthLimiter>,
) -> Result<WebSeedDownloadResult, String> {
    if files.is_empty() {
        return Err("webseed torrent has no files".to_string());
    }
    if multi_file && !seed_url.ends_with('/') {
        return Err("multi-file webseed base URL must end with '/'".to_string());
    }
    ensure_not_cancelled(&cancelled)?;
    let total_length = files.iter().try_fold(0u64, |total, file| {
        total
            .checked_add(file.length)
            .ok_or_else(|| "webseed torrent length overflowed".to_string())
    })?;
    let capacity = usize::try_from(total_length)
        .map_err(|_| "webseed torrent is too large for this platform".to_string())?;
    let mut body = Vec::with_capacity(capacity);
    let mut result_url = seed_url.to_string();
    for file in files {
        ensure_not_cancelled(&cancelled)?;
        let url = build_file_url(seed_url, torrent_name, file, multi_file);
        if !multi_file {
            result_url = url.clone();
        }
        if file.length == 0 {
            continue;
        }
        let endpoint = parse_http_url(&url)?;
        let file_body = get_http_body(&endpoint, file.length, &cancelled, download_limiter)?;
        if file_body.len() as u64 != file.length {
            return Err(format!(
                "webseed body length mismatch for {}: expected {}, got {}",
                file.name,
                file.length,
                file_body.len()
            ));
        }
        body.extend_from_slice(&file_body);
    }
    ensure_not_cancelled(&cancelled)?;
    if body.len() as u64 != total_length {
        return Err(format!(
            "webseed torrent length mismatch: expected {total_length}, got {}",
            body.len()
        ));
    }
    let pieces_verified = verify_pieces(&body, piece_length, piece_hashes)?;
    let summary =
        storage::write_torrent_bytes(output_folder, torrent_name, files, &body, overwrite)?;
    let output_path = if storage::requires_torrent_name_folder(files) {
        output_folder.join(torrent_name)
    } else if multi_file {
        files
            .first()
            .and_then(|file| file.components.first())
            .map(|folder| output_folder.join(folder))
            .unwrap_or_else(|| output_folder.to_path_buf())
    } else {
        summary
            .paths
            .first()
            .cloned()
            .unwrap_or_else(|| output_folder.join(torrent_name))
    };

    Ok(WebSeedDownloadResult {
        url: result_url,
        output_path,
        bytes_written: total_length,
        files_written: summary.files_written,
        pieces_verified,
    })
}

pub fn parse_http_url(url: &str) -> Result<HttpEndpoint, String> {
    if url
        .bytes()
        .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err("webseed URL contains unsafe whitespace or control characters".to_string());
    }
    let (rest, use_tls, default_port) = if let Some(rest) = url.strip_prefix("http://") {
        (rest, false, 80)
    } else if let Some(rest) = url.strip_prefix("https://") {
        (rest, true, 443)
    } else {
        return Err("webseed URL must start with http:// or https://".to_string());
    };
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.is_empty() {
        return Err("webseed URL is missing host".to_string());
    }
    if authority.contains('@') {
        return Err("webseed URLs with user info are not supported".to_string());
    }
    let (host, port) = parse_authority(authority, default_port)?;
    let path_and_query = if path.is_empty() {
        "/".to_string()
    } else {
        format!("/{path}")
    };
    Ok(HttpEndpoint {
        host,
        port,
        path_and_query,
        use_tls,
    })
}

pub fn parse_http_response(response: &[u8]) -> Result<Vec<u8>, String> {
    let (status, headers, body) = parse_http_response_parts(response)?;
    if !(200..300).contains(&status) {
        return Err(format!("HTTP webseed returned status {status}"));
    }
    if headers
        .lines()
        .any(|line| line.to_ascii_lowercase() == "transfer-encoding: chunked")
    {
        decode_chunked_body(body)
    } else {
        Ok(body.to_vec())
    }
}

pub fn build_http_get_request(endpoint: &HttpEndpoint, range: Option<(u64, u64)>) -> String {
    let range_header = range
        .map(|(start, end)| format!("Range: bytes={start}-{end}\r\n"))
        .unwrap_or_default();
    format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: NovaTorrent/0.1\r\nAccept: */*\r\n{}Connection: close\r\n\r\n",
        endpoint.path_and_query,
        host_header(endpoint),
        range_header
    )
}

pub fn verify_pieces(
    data: &[u8],
    piece_length: u64,
    piece_hashes: &[[u8; 20]],
) -> Result<usize, String> {
    if piece_length == 0 {
        return Err("piece length cannot be zero".to_string());
    }
    let expected_pieces = (data.len() as u64).div_ceil(piece_length) as usize;
    if expected_pieces != piece_hashes.len() {
        return Err(format!(
            "webseed piece count mismatch: expected {expected_pieces}, got {}",
            piece_hashes.len()
        ));
    }

    for (index, expected_hash) in piece_hashes.iter().enumerate() {
        let start = index as u64 * piece_length;
        let end = (start + piece_length).min(data.len() as u64);
        let actual_hash = sha1::digest(&data[start as usize..end as usize]);
        if actual_hash != *expected_hash {
            return Err(format!("webseed piece {index} failed SHA-1 verification"));
        }
    }
    Ok(piece_hashes.len())
}

fn get_http_body(
    endpoint: &HttpEndpoint,
    expected_length: u64,
    cancelled: &AtomicBool,
    download_limiter: Option<&BandwidthLimiter>,
) -> Result<Vec<u8>, String> {
    let mut endpoint = endpoint.clone();
    for redirect_count in 0..=MAX_WEBSEED_REDIRECTS {
        let response = get_http_response(&endpoint, expected_length, cancelled, download_limiter)?;
        let (status, headers, body) = parse_http_response_parts(&response)?;
        if (300..400).contains(&status) {
            if redirect_count == MAX_WEBSEED_REDIRECTS {
                return Err("HTTP webseed redirected too many times".to_string());
            }
            let location = http_header(headers, "location").ok_or_else(|| {
                format!("HTTP webseed returned redirect status {status} without Location")
            })?;
            endpoint = resolve_http_redirect(&endpoint, &location)?;
            continue;
        }
        if !(200..300).contains(&status) {
            return Err(format!("HTTP webseed returned status {status}"));
        }
        if headers
            .lines()
            .any(|line| line.to_ascii_lowercase() == "transfer-encoding: chunked")
        {
            return decode_chunked_body(body);
        }
        return Ok(body.to_vec());
    }
    Err("HTTP webseed redirected too many times".to_string())
}

fn get_http_response(
    endpoint: &HttpEndpoint,
    expected_length: u64,
    cancelled: &AtomicBool,
    download_limiter: Option<&BandwidthLimiter>,
) -> Result<Vec<u8>, String> {
    let addresses = (endpoint.host.as_str(), endpoint.port)
        .to_socket_addrs()
        .map_err(|err| format!("could not resolve webseed host: {err}"))?
        .collect::<Vec<_>>();
    let mut stream = connect_http_webseed_addresses(endpoint, &addresses, cancelled)?;
    let max_response_length = usize::try_from(expected_length)
        .map_err(|_| "webseed file is too large for this platform".to_string())?
        .checked_add(1024 * 1024)
        .ok_or_else(|| "webseed response limit overflowed".to_string())?;

    let request = build_http_get_request(endpoint, None);
    stream
        .write_all(request.as_bytes())
        .map_err(|err| format!("could not write HTTP webseed request: {err}"))?;

    let mut response = Vec::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut last_progress = Instant::now();
    loop {
        ensure_not_cancelled(cancelled)?;
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(length) => {
                if response.len().saturating_add(length) > max_response_length {
                    return Err(
                        "webseed response exceeded the expected file length allowance".to_string(),
                    );
                }
                response.extend_from_slice(&buffer[..length]);
                if let Some(limiter) = download_limiter {
                    limiter.throttle(length, Some(cancelled))?;
                }
                last_progress = Instant::now();
            }
            Err(err) if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                if last_progress.elapsed() >= Duration::from_secs(30) {
                    return Err("HTTP webseed stopped sending data for 30 seconds".to_string());
                }
            }
            Err(err) => return Err(format!("could not read HTTP webseed response: {err}")),
        }
    }
    Ok(response)
}

fn parse_http_response_parts(response: &[u8]) -> Result<(u16, &str, &[u8]), String> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| "HTTP webseed response is missing header terminator".to_string())?;
    let headers = std::str::from_utf8(&response[..header_end])
        .map_err(|_| "HTTP webseed headers are not valid UTF-8".to_string())?;
    let status_line = headers
        .lines()
        .next()
        .ok_or_else(|| "HTTP webseed response is missing status line".to_string())?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "HTTP webseed response is missing status code".to_string())?
        .parse::<u16>()
        .map_err(|err| format!("HTTP webseed status code is invalid: {err}"))?;
    Ok((status, headers, &response[header_end + 4..]))
}

fn http_header(headers: &str, name: &str) -> Option<String> {
    headers.lines().skip(1).find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name)
            .then(|| value.trim().to_string())
    })
}

fn resolve_http_redirect(current: &HttpEndpoint, location: &str) -> Result<HttpEndpoint, String> {
    if location.starts_with("http://") || location.starts_with("https://") {
        return parse_http_url(location);
    }
    if location.starts_with('/') {
        return Ok(HttpEndpoint {
            host: current.host.clone(),
            port: current.port,
            path_and_query: location.to_string(),
            use_tls: current.use_tls,
        });
    }
    Err("HTTP webseed redirect Location must be absolute or root-relative".to_string())
}

fn ensure_not_cancelled(cancelled: &AtomicBool) -> Result<(), String> {
    if cancelled.load(Ordering::Relaxed) {
        Err("webseed download cancelled".to_string())
    } else {
        Ok(())
    }
}

trait WebSeedHttpStream: Read + Write {}

impl<T: Read + Write> WebSeedHttpStream for T {}

fn connect_http_webseed_addresses(
    endpoint: &HttpEndpoint,
    addresses: &[std::net::SocketAddr],
    cancelled: &AtomicBool,
) -> Result<Box<dyn WebSeedHttpStream>, String> {
    if addresses.is_empty() {
        return Err("webseed host did not resolve to an address".to_string());
    }

    let mut failures = Vec::new();
    for (index, address) in addresses.iter().copied().enumerate() {
        if addresses[..index].contains(&address) {
            continue;
        }
        ensure_not_cancelled(cancelled)?;
        match connect_http_webseed_stream(endpoint, address) {
            Ok(stream) => return Ok(stream),
            Err(err) => failures.push(format!("{address}: {err}")),
        }
    }
    Err(format!(
        "could not connect to webseed using any resolved address: {}",
        failures.join("; ")
    ))
}

fn connect_http_webseed_stream(
    endpoint: &HttpEndpoint,
    address: std::net::SocketAddr,
) -> Result<Box<dyn WebSeedHttpStream>, String> {
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(10))
        .map_err(|err| format!("could not connect to HTTP webseed: {err}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .map_err(|err| format!("could not set webseed read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .map_err(|err| format!("could not set webseed write timeout: {err}"))?;
    if endpoint.use_tls {
        let connector = TlsConnector::new()
            .map_err(|err| format!("could not initialize webseed TLS: {err}"))?;
        let stream = connector
            .connect(&endpoint.host, stream)
            .map_err(|err| format!("could not establish webseed TLS: {err}"))?;
        Ok(Box::new(stream))
    } else {
        Ok(Box::new(stream))
    }
}

fn parse_authority(authority: &str, default_port: u16) -> Result<(String, u16), String> {
    if authority.starts_with('[') {
        let end = authority
            .find(']')
            .ok_or_else(|| "IPv6 webseed host is missing closing bracket".to_string())?;
        let host = authority[1..end].to_string();
        let port = authority[end + 1..]
            .strip_prefix(':')
            .map(parse_port)
            .transpose()?
            .unwrap_or(default_port);
        return Ok((host, port));
    }
    match authority.rsplit_once(':') {
        Some((host, port)) if port.chars().all(|ch| ch.is_ascii_digit()) => {
            if host.is_empty() {
                return Err("webseed URL is missing host".to_string());
            }
            Ok((host.to_string(), parse_port(port)?))
        }
        _ => Ok((authority.to_string(), default_port)),
    }
}

fn host_header(endpoint: &HttpEndpoint) -> String {
    let host = if endpoint.host.contains(':') {
        format!("[{}]", endpoint.host)
    } else {
        endpoint.host.clone()
    };
    let default_port = if endpoint.use_tls { 443 } else { 80 };
    if endpoint.port == default_port {
        host
    } else {
        format!("{host}:{}", endpoint.port)
    }
}

fn parse_port(port: &str) -> Result<u16, String> {
    port.parse::<u16>()
        .map_err(|err| format!("webseed port is invalid: {err}"))
}

fn decode_chunked_body(body: &[u8]) -> Result<Vec<u8>, String> {
    let mut cursor = 0usize;
    let mut decoded = Vec::new();
    loop {
        let line_end = find_crlf(body, cursor)
            .ok_or_else(|| "chunked webseed body is missing chunk size".to_string())?;
        let size_line = std::str::from_utf8(&body[cursor..line_end])
            .map_err(|_| "chunk size line is not valid UTF-8".to_string())?;
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|err| format!("chunk size is invalid: {err}"))?;
        cursor = line_end + 2;
        if size == 0 {
            return Ok(decoded);
        }
        let chunk_end = cursor
            .checked_add(size)
            .ok_or_else(|| "chunk size overflow".to_string())?;
        if body.len() < chunk_end + 2 {
            return Err("chunked webseed body ended before chunk data completed".to_string());
        }
        decoded.extend_from_slice(&body[cursor..chunk_end]);
        if &body[chunk_end..chunk_end + 2] != b"\r\n" {
            return Err("chunked webseed body chunk is missing trailing CRLF".to_string());
        }
        cursor = chunk_end + 2;
    }
}

fn find_crlf(input: &[u8], start: usize) -> Option<usize> {
    input
        .get(start..)?
        .windows(2)
        .position(|window| window == b"\r\n")
        .map(|offset| start + offset)
}

fn encode_path_component(component: &str) -> String {
    let mut out = String::new();
    for byte in component.bytes() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        net::TcpListener,
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn file(name: &str, length: u64) -> TorrentFile {
        TorrentFile {
            name: name.to_string(),
            components: vec![name.to_string()],
            length,
            included: true,
        }
    }

    #[test]
    fn builds_single_file_url_from_folder_seed() {
        let url = build_file_url(
            "http://mirror.test/pub/",
            "example.bin",
            &file("example.bin", 1),
            false,
        );
        assert_eq!(url, "http://mirror.test/pub/example.bin");
    }

    #[test]
    fn preserves_complete_file_seed_url() {
        let url = build_file_url(
            "http://mirror.test/pub/example.bin",
            "example.bin",
            &file("example.bin", 1),
            false,
        );
        assert_eq!(url, "http://mirror.test/pub/example.bin");
    }

    #[test]
    fn parses_http_url() {
        assert_eq!(
            parse_http_url("http://mirror.test:8080/path/file.bin").expect("URL parses"),
            HttpEndpoint {
                host: "mirror.test".to_string(),
                port: 8080,
                path_and_query: "/path/file.bin".to_string(),
                use_tls: false,
            }
        );
    }

    #[test]
    fn rejects_webseed_request_injection() {
        assert!(parse_http_url("http://mirror.test/file\r\nX-Test: injected").is_err());
    }

    #[test]
    fn parses_https_webseed_url_with_default_port() {
        let endpoint = parse_http_url("https://mirror.test/path/file.bin").expect("URL parses");
        assert_eq!(
            endpoint,
            HttpEndpoint {
                host: "mirror.test".to_string(),
                port: 443,
                path_and_query: "/path/file.bin".to_string(),
                use_tls: true,
            }
        );
        assert!(build_http_get_request(&endpoint, None).contains("Host: mirror.test\r\n"));
    }

    #[test]
    fn parses_http_response_body() {
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
        assert_eq!(
            parse_http_response(response).expect("response parses"),
            b"hello".to_vec()
        );
    }

    #[test]
    fn builds_http_range_request() {
        let endpoint = HttpEndpoint {
            host: "mirror.test".to_string(),
            port: 80,
            path_and_query: "/file.bin".to_string(),
            use_tls: false,
        };
        let request = build_http_get_request(&endpoint, Some((1024, 2047)));
        assert!(request.contains("Range: bytes=1024-2047\r\n"));
        assert!(request.ends_with("Connection: close\r\n\r\n"));
    }

    #[test]
    fn parses_partial_content_response_body() {
        let response = b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-4/10\r\n\r\nhello";
        assert_eq!(
            parse_http_response(response).expect("partial response parses"),
            b"hello".to_vec()
        );
    }

    #[test]
    fn decodes_chunked_response_body() {
        let response =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
        assert_eq!(
            parse_http_response(response).expect("chunked response parses"),
            b"hello".to_vec()
        );
    }

    #[test]
    fn follows_http_webseed_redirect() {
        let server = TcpListener::bind("127.0.0.1:0").expect("webseed binds");
        let port = server.local_addr().expect("server address").port();

        let server_thread = thread::spawn(move || {
            let (mut stream, _) = server.accept().expect("redirect accepts");
            let mut request = [0u8; 512];
            let _ = stream.read(&mut request).expect("redirect request reads");
            stream
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: /payload.bin\r\nContent-Length: 0\r\n\r\n",
                )
                .expect("redirect response writes");
            drop(stream);

            let (mut stream, _) = server.accept().expect("target accepts");
            let mut request = [0u8; 512];
            let length = stream.read(&mut request).expect("target request reads");
            assert!(String::from_utf8_lossy(&request[..length]).starts_with("GET /payload.bin "));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
                .expect("target response writes");
        });

        let endpoint = parse_http_url(&format!("http://127.0.0.1:{port}/old.bin"))
            .expect("redirect URL parses");
        assert_eq!(
            get_http_body(&endpoint, 5, &AtomicBool::new(false), None).expect("redirect follows"),
            b"hello".to_vec()
        );
        server_thread.join().expect("server exits");
    }

    #[test]
    fn verifies_piece_hashes() {
        let data = b"abcdef";
        let hashes = [sha1::digest(b"abc"), sha1::digest(b"def")];
        assert_eq!(verify_pieces(data, 3, &hashes).expect("pieces verify"), 2);
    }

    #[test]
    fn rejects_bad_piece_hash() {
        assert!(verify_pieces(b"abcdef", 3, &[[0; 20], [0; 20]]).is_err());
    }

    #[test]
    fn cancellable_download_stops_before_network_activity() {
        let cancelled = Arc::new(AtomicBool::new(true));
        let err = download_single_file_cancellable(
            "http://127.0.0.1:1/payload.bin",
            "payload.bin",
            &temp_dir_path("cancel"),
            &file("payload.bin", 1),
            1,
            &[sha1::digest(b"x")],
            false,
            cancelled,
            false,
        )
        .expect_err("cancelled download stops");
        assert!(err.contains("cancelled"));
    }

    #[test]
    fn downloads_and_verifies_multi_file_webseed_across_piece_boundaries() {
        let data = b"abcdefghi";
        let files = vec![
            TorrentFile {
                name: "one.bin".to_string(),
                components: vec!["dir".to_string(), "one.bin".to_string()],
                length: 3,
                included: true,
            },
            TorrentFile {
                name: "two.bin".to_string(),
                components: vec!["two.bin".to_string()],
                length: 4,
                included: false,
            },
            TorrentFile {
                name: "three.bin".to_string(),
                components: vec!["three.bin".to_string()],
                length: 2,
                included: true,
            },
        ];
        let piece_hashes = data.chunks(4).map(sha1::digest).collect::<Vec<_>>();
        let listener = TcpListener::bind("127.0.0.1:0").expect("webseed binds");
        let port = listener.local_addr().expect("webseed address").port();
        let server = thread::spawn(move || {
            let expected = [
                ("/base/Example/dir/one.bin", b"abc".as_slice()),
                ("/base/Example/two.bin", b"defg".as_slice()),
                ("/base/Example/three.bin", b"hi".as_slice()),
            ];
            for (expected_path, body) in expected {
                let (mut stream, _) = listener.accept().expect("webseed request connects");
                let mut request = [0u8; 2048];
                let length = stream.read(&mut request).expect("webseed request reads");
                let request = std::str::from_utf8(&request[..length]).expect("request is UTF-8");
                assert!(request.starts_with(&format!("GET {expected_path} HTTP/1.1\r\n")));
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .expect("webseed response headers write");
                stream
                    .write_all(body)
                    .expect("webseed response body writes");
            }
        });

        let output = temp_dir_path("multi");
        let result = download_torrent_cancellable(
            &format!("http://127.0.0.1:{port}/base/"),
            "Example",
            &output,
            &files,
            4,
            &piece_hashes,
            true,
            Arc::new(AtomicBool::new(false)),
            false,
        )
        .expect("multi-file webseed downloads");

        assert_eq!(result.bytes_written, data.len() as u64);
        assert_eq!(result.files_written, 2);
        assert_eq!(result.pieces_verified, 3);
        assert_eq!(
            fs::read(output.join("Example").join("dir").join("one.bin"))
                .expect("first selected file reads"),
            b"abc"
        );
        assert!(!output.join("Example").join("two.bin").exists());
        assert_eq!(
            fs::read(output.join("Example").join("three.bin")).expect("last selected file reads"),
            b"hi"
        );
        server.join().expect("webseed server exits");
        fs::remove_dir_all(output).expect("webseed output removes");
    }

    fn temp_dir_path(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "novatorrent-webseed-{label}-{}-{unique}",
            std::process::id()
        ))
    }
}
