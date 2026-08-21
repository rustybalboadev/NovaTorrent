use std::net::{IpAddr, Ipv4Addr};

use crate::torrent::sha1;

pub const LSD_IPV4_GROUP: Ipv4Addr = Ipv4Addr::new(239, 192, 152, 143);
pub const LSD_PORT: u16 = 6771;
pub const MAX_INFOHASHES_PER_ANNOUNCE: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsdAnnounce {
    pub port: u16,
    pub info_hashes: Vec<[u8; 20]>,
    pub cookie: Option<String>,
}

pub fn build_lsd_announce(
    info_hashes: &[[u8; 20]],
    port: u16,
    cookie: Option<&str>,
) -> Result<Vec<u8>, String> {
    if port == 0 {
        return Err("LSD announce port cannot be zero".to_string());
    }
    if info_hashes.is_empty() {
        return Err("LSD announce requires at least one info hash".to_string());
    }

    let mut out = Vec::new();
    out.extend_from_slice(b"BT-SEARCH * HTTP/1.1\r\n");
    out.extend_from_slice(format!("Host: {LSD_IPV4_GROUP}:{LSD_PORT}\r\n").as_bytes());
    out.extend_from_slice(format!("Port: {port}\r\n").as_bytes());
    for info_hash in info_hashes.iter().take(MAX_INFOHASHES_PER_ANNOUNCE) {
        out.extend_from_slice(format!("Infohash: {}\r\n", sha1::hex(info_hash)).as_bytes());
    }
    if let Some(cookie) = cookie.filter(|cookie| !cookie.is_empty()) {
        out.extend_from_slice(format!("cookie: {cookie}\r\n").as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    if out.len() > 1400 {
        return Err("LSD announce would exceed the 1400 byte MTU target".to_string());
    }
    Ok(out)
}

pub fn round_robin_info_hash_batch(
    info_hashes: &[[u8; 20]],
    cursor: usize,
) -> (Vec<[u8; 20]>, usize) {
    if info_hashes.is_empty() {
        return (Vec::new(), 0);
    }
    if info_hashes.len() <= MAX_INFOHASHES_PER_ANNOUNCE {
        return (info_hashes.to_vec(), 0);
    }

    let start = cursor % info_hashes.len();
    let batch = (0..MAX_INFOHASHES_PER_ANNOUNCE)
        .map(|offset| info_hashes[(start + offset) % info_hashes.len()])
        .collect::<Vec<_>>();
    let next_cursor = (start + MAX_INFOHASHES_PER_ANNOUNCE) % info_hashes.len();
    (batch, next_cursor)
}

pub fn parse_lsd_announce(packet: &[u8]) -> Result<LsdAnnounce, String> {
    let text = std::str::from_utf8(packet)
        .map_err(|_| "LSD announce must be valid UTF-8 headers".to_string())?;
    let headers = text
        .split_once("\r\n\r\n")
        .map(|(headers, _)| headers)
        .unwrap_or(text);
    let mut lines = headers.split("\r\n");
    let request = lines
        .next()
        .ok_or_else(|| "LSD announce is empty".to_string())?;
    if request.trim() != "BT-SEARCH * HTTP/1.1" {
        return Err("LSD announce did not start with BT-SEARCH".to_string());
    }

    let mut host_seen = false;
    let mut port = None;
    let mut info_hashes = Vec::new();
    let mut cookie = None;
    for line in lines {
        if line.trim().is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        match name.as_str() {
            "host" => {
                host_seen = true;
                if !valid_host_header(value) {
                    return Err("LSD host header must name a BEP 14 multicast group".to_string());
                }
            }
            "port" => {
                let parsed = value
                    .parse::<u16>()
                    .map_err(|_| "LSD port header must be a base-10 TCP port".to_string())?;
                if parsed == 0 {
                    return Err("LSD port header cannot be zero".to_string());
                }
                port = Some(parsed);
            }
            "infohash" => info_hashes.push(parse_info_hash(value)?),
            "cookie" => cookie = Some(value.to_string()),
            _ => {}
        }
    }

    if !host_seen {
        return Err("LSD announce is missing Host".to_string());
    }
    let port = port.ok_or_else(|| "LSD announce is missing Port".to_string())?;
    if info_hashes.is_empty() {
        return Err("LSD announce is missing Infohash".to_string());
    }
    Ok(LsdAnnounce {
        port,
        info_hashes,
        cookie,
    })
}

pub fn contactable_lsd_source(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            !address.is_unspecified()
                && !address.is_multicast()
                && !address.is_broadcast()
        }
        IpAddr::V6(_) => false,
    }
}

fn valid_host_header(value: &str) -> bool {
    value == format!("{LSD_IPV4_GROUP}:{LSD_PORT}") || value == "[ff15::efc0:988f]:6771"
}

fn parse_info_hash(value: &str) -> Result<[u8; 20], String> {
    if value.len() != 40 {
        return Err("LSD infohash must be 40 hex characters".to_string());
    }
    let mut out = [0u8; 20];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_value(chunk[0])?;
        let low = hex_value(chunk[1])?;
        out[index] = (high << 4) | low;
    }
    Ok(out)
}

fn hex_value(value: u8) -> Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err("LSD infohash contains non-hex characters".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_and_parses_lsd_announce() {
        let first = [0x11; 20];
        let second = [0x22; 20];
        let packet =
            build_lsd_announce(&[first, second], 6881, Some("nova-cookie")).expect("builds");
        let parsed = parse_lsd_announce(&packet).expect("parses");

        assert_eq!(parsed.port, 6881);
        assert_eq!(parsed.info_hashes, vec![first, second]);
        assert_eq!(parsed.cookie.as_deref(), Some("nova-cookie"));
    }

    #[test]
    fn rejects_invalid_lsd_announce() {
        assert!(parse_lsd_announce(
            b"BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\nPort: 0\r\nInfohash: abc\r\n\r\n"
        )
        .expect_err("invalid announce fails")
        .contains("zero"));
    }

    #[test]
    fn round_robins_active_info_hashes() {
        let hashes = (0u8..7)
            .map(|value| [value; 20])
            .collect::<Vec<[u8; 20]>>();
        let (first, cursor) = round_robin_info_hash_batch(&hashes, 0);
        let (second, next_cursor) = round_robin_info_hash_batch(&hashes, cursor);

        assert_eq!(first, hashes[..5]);
        assert_eq!(cursor, 5);
        assert_eq!(second, vec![hashes[5], hashes[6], hashes[0], hashes[1], hashes[2]]);
        assert_eq!(next_cursor, 3);
    }
}
