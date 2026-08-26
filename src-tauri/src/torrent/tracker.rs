use std::{
    io::{Read, Write},
    net::{Ipv6Addr, TcpStream, ToSocketAddrs, UdpSocket},
    time::Duration,
};

use native_tls::TlsConnector;
use crate::torrent::{
    bencode::{self, BencodeNode, BencodeValue},
    peer::{self, PeerInfo},
    sha1,
};
use serde::{Deserialize, Serialize};

const HTTP_TRACKER_CONNECT_TIMEOUT: Duration = Duration::from_secs(4);
const HTTP_TRACKER_READ_TIMEOUT: Duration = Duration::from_secs(6);
const HTTP_TRACKER_WRITE_TIMEOUT: Duration = Duration::from_secs(4);
const UDP_TRACKER_READ_TIMEOUT: Duration = Duration::from_secs(5);
const UDP_TRACKER_WRITE_TIMEOUT: Duration = Duration::from_secs(4);
const MAX_HTTP_TRACKER_REDIRECTS: usize = 3;

pub const UDP_PROTOCOL_ID: u64 = 0x41727101980;
pub const UDP_ACTION_CONNECT: u32 = 0;
pub const UDP_ACTION_ANNOUNCE: u32 = 1;
pub const UDP_ACTION_ERROR: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerStatus {
    pub url: String,
    pub state: String,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    pub next_announce_seconds: Option<u64>,
    pub message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TrackerAnnounceResponse {
    pub interval_seconds: u64,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    pub peers: Vec<PeerInfo>,
    pub warning: Option<String>,
}

#[derive(Debug, Copy, Clone)]
pub enum UdpAnnounceEvent {
    None,
    Completed,
    Started,
    Stopped,
}

#[derive(Debug, Clone)]
pub struct UdpAnnounceRequest {
    pub connection_id: u64,
    pub transaction_id: u32,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub downloaded: u64,
    pub left: u64,
    pub uploaded: u64,
    pub event: UdpAnnounceEvent,
    pub key: u32,
    pub num_want: i32,
    pub port: u16,
}

#[derive(Debug, Copy, Clone)]
pub struct UdpConnectResponse {
    pub transaction_id: u32,
    pub connection_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpTrackerEndpoint {
    pub host: String,
    pub port: u16,
    pub path_and_query: String,
    pub use_tls: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpTrackerEndpoint {
    pub host: String,
    pub port: u16,
}

pub fn build_announce_url(
    announce: &str,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    event: Option<&str>,
) -> String {
    let separator = if announce.contains('?') { '&' } else { '?' };
    let mut url = format!(
        "{announce}{separator}info_hash={}&peer_id={}&port={port}&uploaded={uploaded}&downloaded={downloaded}&left={left}&compact=1",
        percent_encode_bytes(&info_hash),
        percent_encode_bytes(&peer_id)
    );
    if let Some(event) = event {
        url.push_str("&event=");
        url.push_str(event);
    }
    url
}

pub fn announce_http(
    announce: &str,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    event: Option<&str>,
) -> Result<TrackerAnnounceResponse, String> {
    let announce_url = build_announce_url(
        announce,
        info_hash,
        peer_id,
        port,
        uploaded,
        downloaded,
        left,
        event,
    );
    let mut endpoint = parse_http_tracker_url(&announce_url)?;
    for redirect_count in 0..=MAX_HTTP_TRACKER_REDIRECTS {
        let response = read_http_tracker_response(&endpoint)?;
        let (status, headers, body) = parse_http_tracker_response_parts(&response)?;
        if (300..400).contains(&status) {
            if redirect_count == MAX_HTTP_TRACKER_REDIRECTS {
                return Err("HTTP tracker redirected too many times".to_string());
            }
            let location = http_header(headers, "location").ok_or_else(|| {
                format!("HTTP tracker returned redirect status {status} without Location")
            })?;
            endpoint = resolve_http_tracker_redirect(&endpoint, &location)?;
            continue;
        }
        if !(200..300).contains(&status) {
            return Err(format!("HTTP tracker returned status {status}"));
        }
        let body = if headers
            .lines()
            .any(|line| line.to_ascii_lowercase() == "transfer-encoding: chunked")
        {
            decode_chunked_body(body)?
        } else {
            body.to_vec()
        };
        return parse_http_announce_response(&body);
    }
    Err("HTTP tracker redirected too many times".to_string())
}

fn read_http_tracker_response(endpoint: &HttpTrackerEndpoint) -> Result<Vec<u8>, String> {
    let address = (endpoint.host.as_str(), endpoint.port)
        .to_socket_addrs()
        .map_err(|err| format!("could not resolve tracker host: {err}"))?
        .next()
        .ok_or_else(|| "tracker host did not resolve to an address".to_string())?;
    let mut stream = connect_http_tracker_stream(&endpoint, address)?;
    let request = build_http_tracker_request(&endpoint);
    stream
        .write_all(request.as_bytes())
        .map_err(|err| format!("could not write HTTP tracker request: {err}"))?;

    const MAX_TRACKER_RESPONSE: u64 = 4 * 1024 * 1024;
    let mut response = Vec::new();
    (&mut stream)
        .take(MAX_TRACKER_RESPONSE + 1)
        .read_to_end(&mut response)
        .map_err(|err| format!("could not read HTTP tracker response: {err}"))?;
    if response.len() as u64 > MAX_TRACKER_RESPONSE {
        return Err("HTTP tracker response exceeded 4 MiB".to_string());
    }
    Ok(response)
}

pub fn parse_http_announce_response(input: &[u8]) -> Result<TrackerAnnounceResponse, String> {
    let root = bencode::parse(input)?;
    if let Some(reason) = root.dict_get(b"failure reason").and_then(BencodeNode::as_str_lossy) {
        return Err(format!("tracker failure: {reason}"));
    }

    let interval_seconds = root
        .dict_get(b"interval")
        .and_then(BencodeNode::as_i64)
        .ok_or_else(|| "tracker response is missing interval".to_string())?
        .try_into()
        .map_err(|_| "tracker interval cannot be negative".to_string())?;
    let seeders = optional_u32(root.dict_get(b"complete"), "complete")?;
    let leechers = optional_u32(root.dict_get(b"incomplete"), "incomplete")?;
    let warning = root
        .dict_get(b"warning message")
        .and_then(BencodeNode::as_str_lossy);
    let mut peers = match root.dict_get(b"peers") {
        Some(node) => parse_tracker_peers(node)?,
        None => Vec::new(),
    };
    if let Some(node) = root.dict_get(b"peers6") {
        peers.extend(parse_tracker_peers6(node)?);
    }

    Ok(TrackerAnnounceResponse {
        interval_seconds,
        seeders,
        leechers,
        peers,
        warning,
    })
}

pub fn parse_compact_peers(bytes: &[u8]) -> Result<Vec<PeerInfo>, String> {
    if bytes.len() % 6 != 0 {
        return Err("compact peer list length must be a multiple of 6".to_string());
    }
    Ok(bytes
        .chunks_exact(6)
        .filter_map(|chunk| {
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            (port != 0).then(|| PeerInfo {
                address: format!("{}.{}.{}.{}", chunk[0], chunk[1], chunk[2], chunk[3]),
                port,
                client: None,
                progress: 0.0,
                download_speed: 0,
                upload_speed: 0,
                connection: "Discovered".to_string(),
            })
        })
        .collect())
}

pub fn parse_compact_peers6(bytes: &[u8]) -> Result<Vec<PeerInfo>, String> {
    if bytes.len() % 18 != 0 {
        return Err("compact IPv6 peer list length must be a multiple of 18".to_string());
    }
    Ok(bytes
        .chunks_exact(18)
        .filter_map(|chunk| {
            let port = u16::from_be_bytes([chunk[16], chunk[17]]);
            (port != 0).then(|| PeerInfo {
                address: Ipv6Addr::from(<[u8; 16]>::try_from(&chunk[..16]).expect("IPv6 slice"))
                    .to_string(),
                port,
                client: None,
                progress: 0.0,
                download_speed: 0,
                upload_speed: 0,
                connection: "Discovered".to_string(),
            })
        })
        .collect())
}

pub fn build_udp_connect_request(transaction_id: u32) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[0..8].copy_from_slice(&UDP_PROTOCOL_ID.to_be_bytes());
    out[8..12].copy_from_slice(&UDP_ACTION_CONNECT.to_be_bytes());
    out[12..16].copy_from_slice(&transaction_id.to_be_bytes());
    out
}

pub fn parse_udp_connect_response(input: &[u8], expected_transaction_id: u32) -> Result<UdpConnectResponse, String> {
    if input.len() < 8 {
        return Err("UDP tracker response is too short".to_string());
    }
    let action = read_u32(input, 0)?;
    let transaction_id = read_u32(input, 4)?;
    if transaction_id != expected_transaction_id {
        return Err("UDP tracker transaction ID mismatch".to_string());
    }
    if action == UDP_ACTION_ERROR {
        return Err(format!("UDP tracker error: {}", String::from_utf8_lossy(&input[8..])));
    }
    if action != UDP_ACTION_CONNECT {
        return Err(format!("unexpected UDP tracker action: {action}"));
    }
    if input.len() < 16 {
        return Err("UDP tracker connect response is too short".to_string());
    }
    Ok(UdpConnectResponse {
        transaction_id,
        connection_id: read_u64(input, 8)?,
    })
}

pub fn build_udp_announce_request(request: UdpAnnounceRequest) -> [u8; 98] {
    let mut out = [0u8; 98];
    out[0..8].copy_from_slice(&request.connection_id.to_be_bytes());
    out[8..12].copy_from_slice(&UDP_ACTION_ANNOUNCE.to_be_bytes());
    out[12..16].copy_from_slice(&request.transaction_id.to_be_bytes());
    out[16..36].copy_from_slice(&request.info_hash);
    out[36..56].copy_from_slice(&request.peer_id);
    out[56..64].copy_from_slice(&request.downloaded.to_be_bytes());
    out[64..72].copy_from_slice(&request.left.to_be_bytes());
    out[72..80].copy_from_slice(&request.uploaded.to_be_bytes());
    out[80..84].copy_from_slice(&udp_event_code(request.event).to_be_bytes());
    out[84..88].copy_from_slice(&0u32.to_be_bytes());
    out[88..92].copy_from_slice(&request.key.to_be_bytes());
    out[92..96].copy_from_slice(&request.num_want.to_be_bytes());
    out[96..98].copy_from_slice(&request.port.to_be_bytes());
    out
}

pub fn parse_udp_announce_response(input: &[u8], expected_transaction_id: u32) -> Result<TrackerAnnounceResponse, String> {
    if input.len() < 8 {
        return Err("UDP tracker response is too short".to_string());
    }
    let action = read_u32(input, 0)?;
    let transaction_id = read_u32(input, 4)?;
    if transaction_id != expected_transaction_id {
        return Err("UDP tracker transaction ID mismatch".to_string());
    }
    if action == UDP_ACTION_ERROR {
        return Err(format!("UDP tracker error: {}", String::from_utf8_lossy(&input[8..])));
    }
    if action != UDP_ACTION_ANNOUNCE {
        return Err(format!("unexpected UDP tracker action: {action}"));
    }
    if input.len() < 20 {
        return Err("UDP tracker announce response is too short".to_string());
    }

    Ok(TrackerAnnounceResponse {
        interval_seconds: read_u32(input, 8)?.into(),
        leechers: Some(read_u32(input, 12)?),
        seeders: Some(read_u32(input, 16)?),
        peers: parse_compact_peers(&input[20..])?,
        warning: None,
    })
}

pub fn announce_udp(
    announce: &str,
    mut request: UdpAnnounceRequest,
) -> Result<TrackerAnnounceResponse, String> {
    let endpoint = parse_udp_tracker_url(announce)?;
    let address = (endpoint.host.as_str(), endpoint.port)
        .to_socket_addrs()
        .map_err(|err| format!("could not resolve UDP tracker host: {err}"))?
        .next()
        .ok_or_else(|| "UDP tracker host did not resolve to an address".to_string())?;
    let bind_address = if address.is_ipv6() { "[::]:0" } else { "0.0.0.0:0" };
    let socket = UdpSocket::bind(bind_address)
        .map_err(|err| format!("could not bind UDP tracker socket: {err}"))?;
    socket
        .set_read_timeout(Some(UDP_TRACKER_READ_TIMEOUT))
        .map_err(|err| format!("could not set UDP tracker read timeout: {err}"))?;
    socket
        .set_write_timeout(Some(UDP_TRACKER_WRITE_TIMEOUT))
        .map_err(|err| format!("could not set UDP tracker write timeout: {err}"))?;

    let connect_transaction_id = request.transaction_id;
    let connect_packet = build_udp_connect_request(connect_transaction_id);
    socket
        .send_to(&connect_packet, address)
        .map_err(|err| format!("could not send UDP tracker connect request: {err}"))?;
    let mut buffer = [0u8; 2048];
    let (length, _) = socket
        .recv_from(&mut buffer)
        .map_err(|err| format!("could not read UDP tracker connect response: {err}"))?;
    let connect = parse_udp_connect_response(&buffer[..length], connect_transaction_id)?;

    request.connection_id = connect.connection_id;
    request.transaction_id = connect_transaction_id.wrapping_add(1);
    let announce_packet = build_udp_announce_request(request.clone());
    socket
        .send_to(&announce_packet, address)
        .map_err(|err| format!("could not send UDP tracker announce request: {err}"))?;
    let (length, _) = socket
        .recv_from(&mut buffer)
        .map_err(|err| format!("could not read UDP tracker announce response: {err}"))?;
    parse_udp_announce_response(&buffer[..length], request.transaction_id)
}

pub fn parse_http_tracker_url(url: &str) -> Result<HttpTrackerEndpoint, String> {
    let (rest, use_tls, default_port) = if let Some(rest) = url.strip_prefix("http://") {
        (rest, false, 80)
    } else if let Some(rest) = url.strip_prefix("https://") {
        (rest, true, 443)
    } else {
        return Err("HTTP tracker URL must start with http:// or https://".to_string());
    };
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.is_empty() {
        return Err("HTTP tracker URL is missing host".to_string());
    }
    if authority.contains('@') {
        return Err("HTTP tracker URLs with user info are not supported".to_string());
    }

    let (host, port) = parse_http_authority(authority, default_port)?;
    let path_and_query = if path.is_empty() {
        "/".to_string()
    } else {
        format!("/{path}")
    };
    Ok(HttpTrackerEndpoint {
        host,
        port,
        path_and_query,
        use_tls,
    })
}

pub fn parse_udp_tracker_url(url: &str) -> Result<UdpTrackerEndpoint, String> {
    let rest = url
        .strip_prefix("udp://")
        .ok_or_else(|| "UDP tracker URL must start with udp://".to_string())?;
    let authority = rest.split_once('/').map(|(authority, _)| authority).unwrap_or(rest);
    if authority.is_empty() {
        return Err("UDP tracker URL is missing host".to_string());
    }
    let (host, port) = parse_udp_authority(authority)?;
    Ok(UdpTrackerEndpoint { host, port })
}

pub fn build_http_tracker_request(endpoint: &HttpTrackerEndpoint) -> String {
    format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: NovaTorrent/0.1\r\nAccept: */*\r\nConnection: close\r\n\r\n",
        endpoint.path_and_query,
        host_header(endpoint)
    )
}

pub fn parse_http_tracker_response(response: &[u8]) -> Result<TrackerAnnounceResponse, String> {
    let (status, headers, body) = parse_http_tracker_response_parts(response)?;
    if !(200..300).contains(&status) {
        return Err(format!("HTTP tracker returned status {status}"));
    }

    let body = if headers
        .lines()
        .any(|line| line.to_ascii_lowercase() == "transfer-encoding: chunked")
    {
        decode_chunked_body(body)?
    } else {
        body.to_vec()
    };
    parse_http_announce_response(&body)
}

fn parse_http_tracker_response_parts(response: &[u8]) -> Result<(u16, &str, &[u8]), String> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| "HTTP tracker response is missing header terminator".to_string())?;
    let headers = std::str::from_utf8(&response[..header_end])
        .map_err(|_| "HTTP tracker headers are not valid UTF-8".to_string())?;
    let status_line = headers
        .lines()
        .next()
        .ok_or_else(|| "HTTP tracker response is missing status line".to_string())?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "HTTP tracker response is missing status code".to_string())?
        .parse::<u16>()
        .map_err(|err| format!("HTTP tracker status code is invalid: {err}"))?;
    Ok((status, headers, &response[header_end + 4..]))
}

pub fn percent_encode_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 3);
    for byte in bytes {
        let is_unreserved = matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-' | b'_' | b'~');
        if is_unreserved {
            out.push(*byte as char);
        } else {
            out.push('%');
            out.push_str(&sha1::hex(&[*byte]).to_uppercase());
        }
    }
    out
}

trait TrackerHttpStream: Read + Write {}

impl<T: Read + Write> TrackerHttpStream for T {}

fn connect_http_tracker_stream(
    endpoint: &HttpTrackerEndpoint,
    address: std::net::SocketAddr,
) -> Result<Box<dyn TrackerHttpStream>, String> {
    let stream = TcpStream::connect_timeout(&address, HTTP_TRACKER_CONNECT_TIMEOUT)
        .map_err(|err| format!("could not connect to HTTP tracker: {err}"))?;
    stream
        .set_read_timeout(Some(HTTP_TRACKER_READ_TIMEOUT))
        .map_err(|err| format!("could not set tracker read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(HTTP_TRACKER_WRITE_TIMEOUT))
        .map_err(|err| format!("could not set tracker write timeout: {err}"))?;
    if endpoint.use_tls {
        let connector = TlsConnector::new()
            .map_err(|err| format!("could not initialize tracker TLS: {err}"))?;
        let stream = connector
            .connect(&endpoint.host, stream)
            .map_err(|err| format!("could not establish tracker TLS: {err}"))?;
        Ok(Box::new(stream))
    } else {
        Ok(Box::new(stream))
    }
}

fn http_header(headers: &str, name: &str) -> Option<String> {
    headers.lines().skip(1).find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name)
            .then(|| value.trim().to_string())
    })
}

fn resolve_http_tracker_redirect(
    current: &HttpTrackerEndpoint,
    location: &str,
) -> Result<HttpTrackerEndpoint, String> {
    if location.starts_with("http://") || location.starts_with("https://") {
        return parse_http_tracker_url(location);
    }
    if location.starts_with('/') {
        return Ok(HttpTrackerEndpoint {
            host: current.host.clone(),
            port: current.port,
            path_and_query: location.to_string(),
            use_tls: current.use_tls,
        });
    }
    Err("HTTP tracker redirect Location must be absolute or root-relative".to_string())
}

fn parse_http_authority(authority: &str, default_port: u16) -> Result<(String, u16), String> {
    if authority.starts_with('[') {
        let end = authority
            .find(']')
            .ok_or_else(|| "IPv6 tracker host is missing closing bracket".to_string())?;
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
                return Err("HTTP tracker URL is missing host".to_string());
            }
            Ok((host.to_string(), parse_port(port)?))
        }
        _ => Ok((authority.to_string(), default_port)),
    }
}

fn host_header(endpoint: &HttpTrackerEndpoint) -> String {
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

fn parse_udp_authority(authority: &str) -> Result<(String, u16), String> {
    if authority.starts_with('[') {
        let end = authority
            .find(']')
            .ok_or_else(|| "IPv6 UDP tracker host is missing closing bracket".to_string())?;
        let host = authority[1..end].to_string();
        let port = authority[end + 1..]
            .strip_prefix(':')
            .ok_or_else(|| "UDP tracker URL is missing port".to_string())
            .and_then(parse_port)?;
        return Ok((host, port));
    }

    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| "UDP tracker URL is missing port".to_string())?;
    if host.is_empty() {
        return Err("UDP tracker URL is missing host".to_string());
    }
    Ok((host.to_string(), parse_port(port)?))
}

fn parse_port(port: &str) -> Result<u16, String> {
    port.parse::<u16>()
        .map_err(|err| format!("HTTP tracker port is invalid: {err}"))
}

fn decode_chunked_body(body: &[u8]) -> Result<Vec<u8>, String> {
    let mut cursor = 0usize;
    let mut decoded = Vec::new();
    loop {
        let line_end = find_crlf(body, cursor).ok_or_else(|| "chunked body is missing chunk size".to_string())?;
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
            return Err("chunked body ended before chunk data completed".to_string());
        }
        decoded.extend_from_slice(&body[cursor..chunk_end]);
        if &body[chunk_end..chunk_end + 2] != b"\r\n" {
            return Err("chunked body chunk is missing trailing CRLF".to_string());
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

fn parse_tracker_peers(node: &BencodeNode) -> Result<Vec<PeerInfo>, String> {
    match &node.value {
        BencodeValue::Bytes(bytes) => parse_compact_peers(bytes),
        BencodeValue::List(items) => items.iter().map(parse_peer_dictionary).collect(),
        _ => Err("tracker peers value is neither compact bytes nor a list".to_string()),
    }
}

fn parse_tracker_peers6(node: &BencodeNode) -> Result<Vec<PeerInfo>, String> {
    match &node.value {
        BencodeValue::Bytes(bytes) => parse_compact_peers6(bytes),
        _ => Err("tracker peers6 value is not compact bytes".to_string()),
    }
}

fn parse_peer_dictionary(node: &BencodeNode) -> Result<PeerInfo, String> {
    let address = node
        .dict_get(b"ip")
        .and_then(BencodeNode::as_str_lossy)
        .ok_or_else(|| "tracker peer entry is missing ip".to_string())?;
    let port = node
        .dict_get(b"port")
        .and_then(BencodeNode::as_i64)
        .ok_or_else(|| "tracker peer entry is missing port".to_string())?;
    let port = u16::try_from(port).map_err(|_| "tracker peer port is out of range".to_string())?;
    Ok(PeerInfo {
        address,
        port,
        client: node
            .dict_get(b"peer id")
            .and_then(|peer_id| peer_id.as_bytes())
            .and_then(peer::decode_peer_client),
        progress: 0.0,
        download_speed: 0,
        upload_speed: 0,
        connection: "Discovered".to_string(),
    })
}

fn optional_u32(node: Option<&BencodeNode>, label: &str) -> Result<Option<u32>, String> {
    node.map(|node| {
        node.as_i64()
            .ok_or_else(|| format!("tracker {label} field is not an integer"))
            .and_then(|value| u32::try_from(value).map_err(|_| format!("tracker {label} field is out of range")))
    })
    .transpose()
}

fn udp_event_code(event: UdpAnnounceEvent) -> u32 {
    match event {
        UdpAnnounceEvent::None => 0,
        UdpAnnounceEvent::Completed => 1,
        UdpAnnounceEvent::Started => 2,
        UdpAnnounceEvent::Stopped => 3,
    }
}

fn read_u32(input: &[u8], offset: usize) -> Result<u32, String> {
    let end = offset + 4;
    let bytes = input
        .get(offset..end)
        .ok_or_else(|| "UDP tracker packet ended early".to_string())?;
    Ok(u32::from_be_bytes(bytes.try_into().expect("four-byte slice")))
}

fn read_u64(input: &[u8], offset: usize) -> Result<u64, String> {
    let end = offset + 8;
    let bytes = input
        .get(offset..end)
        .ok_or_else(|| "UDP tracker packet ended early".to_string())?;
    Ok(u64::from_be_bytes(bytes.try_into().expect("eight-byte slice")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_compact_peer_list() {
        let peers = parse_compact_peers(&[127, 0, 0, 1, 0x1a, 0xe1]).expect("peers parse");
        assert_eq!(peers[0].address, "127.0.0.1");
        assert_eq!(peers[0].port, 6881);
    }

    #[test]
    fn ignores_compact_peers_with_zero_port() {
        let peers = parse_compact_peers(&[127, 0, 0, 1, 0, 0, 127, 0, 0, 2, 0x1a, 0xe1])
            .expect("peers parse");
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].address, "127.0.0.2");
    }

    #[test]
    fn parses_http_tracker_compact_response() {
        let response = b"d8:completei12e10:incompletei3e8:intervali1800e5:peers6:\x7f\x00\x00\x01\x1a\xe1e";
        let parsed = parse_http_announce_response(response).expect("tracker response parses");
        assert_eq!(parsed.interval_seconds, 1800);
        assert_eq!(parsed.seeders, Some(12));
        assert_eq!(parsed.leechers, Some(3));
        assert_eq!(parsed.peers[0].address, "127.0.0.1");
        assert_eq!(parsed.peers[0].port, 6881);
    }

    #[test]
    fn parses_http_tracker_peers6_response() {
        let mut response = b"d8:intervali1800e5:peers0:6:peers618:".to_vec();
        response.extend_from_slice(&[
            0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0x1a, 0xe1,
        ]);
        response.push(b'e');
        let parsed = parse_http_announce_response(&response).expect("tracker response parses");
        assert_eq!(parsed.peers[0].address, "2001:db8::5");
        assert_eq!(parsed.peers[0].port, 6881);
    }

    #[test]
    fn parses_dictionary_peer_ids_as_client_names() {
        let response = b"d8:intervali1800e5:peersld2:ip9:127.0.0.17:peer id20:-qB4520-abcdefghijkl4:porti6881eeee";
        let parsed = parse_http_announce_response(response).expect("tracker response parses");

        assert_eq!(parsed.peers[0].client.as_deref(), Some("qBittorrent 4.5.2"));
        assert_eq!(parsed.peers[0].connection, "Discovered");
    }

    #[test]
    fn parses_http_tracker_failure_response() {
        let response = b"d14:failure reason11:bad trackere";
        assert!(parse_http_announce_response(response).is_err());
    }

    #[test]
    fn parses_http_tracker_url() {
        let endpoint = parse_http_tracker_url("http://tracker.example:8080/announce?x=1").expect("URL parses");
        assert_eq!(
            endpoint,
            HttpTrackerEndpoint {
                host: "tracker.example".to_string(),
                port: 8080,
                path_and_query: "/announce?x=1".to_string(),
                use_tls: false,
            }
        );
    }

    #[test]
    fn parses_https_tracker_url_with_default_port() {
        let endpoint = parse_http_tracker_url("https://tracker.example/announce").expect("URL parses");
        assert_eq!(
            endpoint,
            HttpTrackerEndpoint {
                host: "tracker.example".to_string(),
                port: 443,
                path_and_query: "/announce".to_string(),
                use_tls: true,
            }
        );
        assert!(build_http_tracker_request(&endpoint).contains("Host: tracker.example\r\n"));
    }

    #[test]
    fn parses_udp_tracker_url() {
        let endpoint = parse_udp_tracker_url("udp://tracker.example:6969/announce").expect("UDP URL parses");
        assert_eq!(
            endpoint,
            UdpTrackerEndpoint {
                host: "tracker.example".to_string(),
                port: 6969,
            }
        );
    }

    #[test]
    fn parses_full_http_tracker_response() {
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 54\r\n\r\nd8:completei1e10:incompletei2e8:intervali30e5:peers0:e";
        let parsed = parse_http_tracker_response(response).expect("HTTP tracker response parses");
        assert_eq!(parsed.interval_seconds, 30);
        assert_eq!(parsed.seeders, Some(1));
        assert_eq!(parsed.leechers, Some(2));
        assert!(parsed.peers.is_empty());
    }

    #[test]
    fn decodes_chunked_http_tracker_response() {
        let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n36\r\nd8:completei1e10:incompletei2e8:intervali30e5:peers0:e\r\n0\r\n\r\n";
        let parsed = parse_http_tracker_response(response).expect("chunked tracker response parses");
        assert_eq!(parsed.interval_seconds, 30);
    }

    #[test]
    fn follows_http_tracker_redirect() {
        let target = std::net::TcpListener::bind("127.0.0.1:0").expect("target tracker binds");
        let target_port = target.local_addr().expect("target address").port();
        let redirect = std::net::TcpListener::bind("127.0.0.1:0").expect("redirect tracker binds");
        let redirect_port = redirect.local_addr().expect("redirect address").port();

        let target_server = std::thread::spawn(move || {
            let (mut stream, _) = target.accept().expect("target accepts");
            let mut request = [0u8; 1024];
            let length = stream.read(&mut request).expect("target request reads");
            assert!(
                String::from_utf8_lossy(&request[..length]).starts_with("GET /new-announce ")
            );
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 54\r\n\r\nd8:completei1e10:incompletei2e8:intervali30e5:peers0:e",
                )
                .expect("target response writes");
        });
        let redirect_server = std::thread::spawn(move || {
            let (mut stream, _) = redirect.accept().expect("redirect accepts");
            let mut request = [0u8; 1024];
            let length = stream.read(&mut request).expect("redirect request reads");
            assert!(String::from_utf8_lossy(&request[..length]).starts_with("GET /announce?"));
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{target_port}/new-announce\r\nContent-Length: 0\r\n\r\n"
            );
            stream.write_all(response.as_bytes()).expect("redirect response writes");
        });

        let parsed = announce_http(
            &format!("http://127.0.0.1:{redirect_port}/announce"),
            [1; 20],
            [2; 20],
            6881,
            0,
            0,
            0,
            Some("started"),
        )
        .expect("redirected tracker announces");
        assert_eq!(parsed.interval_seconds, 30);
        assert_eq!(parsed.seeders, Some(1));
        redirect_server.join().expect("redirect exits");
        target_server.join().expect("target exits");
    }

    #[test]
    fn builds_udp_connect_request() {
        let request = build_udp_connect_request(0x1020_3040);
        assert_eq!(&request[0..8], &UDP_PROTOCOL_ID.to_be_bytes());
        assert_eq!(&request[8..12], &UDP_ACTION_CONNECT.to_be_bytes());
        assert_eq!(&request[12..16], &0x1020_3040u32.to_be_bytes());
    }

    #[test]
    fn parses_udp_connect_response() {
        let mut response = Vec::new();
        response.extend_from_slice(&UDP_ACTION_CONNECT.to_be_bytes());
        response.extend_from_slice(&7u32.to_be_bytes());
        response.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());

        let parsed = parse_udp_connect_response(&response, 7).expect("UDP connect response parses");
        assert_eq!(parsed.transaction_id, 7);
        assert_eq!(parsed.connection_id, 0x0102_0304_0506_0708);
    }

    #[test]
    fn builds_udp_announce_request() {
        let request = build_udp_announce_request(UdpAnnounceRequest {
            connection_id: 1,
            transaction_id: 2,
            info_hash: [3; 20],
            peer_id: [4; 20],
            downloaded: 5,
            left: 6,
            uploaded: 7,
            event: UdpAnnounceEvent::Started,
            key: 8,
            num_want: -1,
            port: 6881,
        });

        assert_eq!(request.len(), 98);
        assert_eq!(&request[8..12], &UDP_ACTION_ANNOUNCE.to_be_bytes());
        assert_eq!(&request[80..84], &2u32.to_be_bytes());
        assert_eq!(&request[96..98], &6881u16.to_be_bytes());
    }

    #[test]
    fn parses_udp_announce_response() {
        let mut response = Vec::new();
        response.extend_from_slice(&UDP_ACTION_ANNOUNCE.to_be_bytes());
        response.extend_from_slice(&9u32.to_be_bytes());
        response.extend_from_slice(&1800u32.to_be_bytes());
        response.extend_from_slice(&3u32.to_be_bytes());
        response.extend_from_slice(&12u32.to_be_bytes());
        response.extend_from_slice(&[127, 0, 0, 1, 0x1a, 0xe1]);

        let parsed = parse_udp_announce_response(&response, 9).expect("UDP announce response parses");
        assert_eq!(parsed.interval_seconds, 1800);
        assert_eq!(parsed.seeders, Some(12));
        assert_eq!(parsed.leechers, Some(3));
        assert_eq!(parsed.peers[0].address, "127.0.0.1");
    }
}
