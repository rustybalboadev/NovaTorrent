use crate::torrent::{peer::PeerInfo, sha1};

const MAX_MAGNET_TRACKERS: usize = 64;
const MAX_MAGNET_WEB_SEEDS: usize = 32;
const MAX_MAGNET_PEERS: usize = 200;
const MAX_MAGNET_URI_LENGTH: usize = 32 * 1024;
const MAX_MAGNET_SOURCE_LENGTH: usize = 4096;
const MAX_MAGNET_DISPLAY_NAME_LENGTH: usize = 1024;

#[derive(Debug, Clone)]
pub struct MagnetLink {
    pub display_name: Option<String>,
    pub info_hash: [u8; 20],
    pub trackers: Vec<String>,
    pub web_seeds: Vec<String>,
    pub peers: Vec<PeerInfo>,
}

impl MagnetLink {
    pub fn parse(input: &str) -> Result<Self, String> {
        if input.len() > MAX_MAGNET_URI_LENGTH {
            return Err(format!(
                "magnet link exceeds the {MAX_MAGNET_URI_LENGTH}-byte limit"
            ));
        }
        if !input
            .get(.."magnet:?".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("magnet:?"))
        {
            return Err("magnet link must start with magnet:?".to_string());
        }

        let mut display_name = None;
        let mut info_hash = None;
        let mut trackers = Vec::new();
        let mut web_seeds = Vec::new();
        let mut peers = Vec::new();

        for pair in input["magnet:?".len()..].split('&') {
            let Some((key, value)) = pair.split_once('=') else {
                continue;
            };
            let decoded = percent_decode(value)?;
            match key.to_ascii_lowercase().as_str() {
                "dn" if decoded.len() <= MAX_MAGNET_DISPLAY_NAME_LENGTH => {
                    display_name = Some(decoded)
                }
                "tr" if trackers.len() < MAX_MAGNET_TRACKERS => {
                    if decoded.len() <= MAX_MAGNET_SOURCE_LENGTH {
                        push_unique_string(&mut trackers, decoded)
                    }
                }
                "ws" if web_seeds.len() < MAX_MAGNET_WEB_SEEDS => {
                    if decoded.len() <= MAX_MAGNET_SOURCE_LENGTH {
                        push_unique_string(&mut web_seeds, decoded)
                    }
                }
                "x.pe" if peers.len() < MAX_MAGNET_PEERS => {
                    push_unique_peer(&mut peers, parse_exact_peer(&decoded)?)
                }
                "xt" => {
                    if let Some(hash) = strip_ascii_prefix(&decoded, "urn:btih:") {
                        info_hash = Some(parse_btih(hash)?);
                    }
                }
                _ => {}
            }
        }

        Ok(Self {
            display_name,
            info_hash: info_hash.ok_or_else(|| "magnet link is missing xt=urn:btih".to_string())?,
            trackers,
            web_seeds,
            peers,
        })
    }
}

fn strip_ascii_prefix<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    value
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
        .then(|| &value[prefix.len()..])
}

fn push_unique_string(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn push_unique_peer(peers: &mut Vec<PeerInfo>, peer: PeerInfo) {
    if !peers
        .iter()
        .any(|existing| existing.address == peer.address && existing.port == peer.port)
    {
        peers.push(peer);
    }
}

pub fn percent_decode(input: &str) -> Result<String, String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut cursor = 0;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'%' => {
                if cursor + 2 >= bytes.len() {
                    return Err("truncated percent escape".to_string());
                }
                let high = hex_value(bytes[cursor + 1])?;
                let low = hex_value(bytes[cursor + 2])?;
                out.push((high << 4) | low);
                cursor += 3;
            }
            b'+' => {
                out.push(b' ');
                cursor += 1;
            }
            byte => {
                out.push(byte);
                cursor += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|err| format!("decoded value is not UTF-8: {err}"))
}

fn parse_btih(value: &str) -> Result<[u8; 20], String> {
    if value.len() == 40 {
        return sha1::from_hex_20(value);
    }
    if value.len() == 32 {
        return base32_decode_20(value);
    }
    Err("btih hash must be 40 hex characters or 32 base32 characters".to_string())
}

fn base32_decode_20(value: &str) -> Result<[u8; 20], String> {
    let mut bits = 0u32;
    let mut bit_count = 0u8;
    let mut bytes = Vec::with_capacity(20);
    for byte in value.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a',
            b'2'..=b'7' => byte - b'2' + 26,
            _ => return Err("invalid base32 btih character".to_string()),
        };
        bits = (bits << 5) | value as u32;
        bit_count += 5;
        while bit_count >= 8 {
            bytes.push((bits >> (bit_count - 8)) as u8);
            bit_count -= 8;
            bits &= (1 << bit_count) - 1;
        }
    }
    bytes
        .try_into()
        .map_err(|_| "base32 btih did not decode to 20 bytes".to_string())
}

fn parse_exact_peer(value: &str) -> Result<PeerInfo, String> {
    let (address, port) = if let Some(rest) = value.strip_prefix('[') {
        let end = rest
            .find(']')
            .ok_or_else(|| "magnet x.pe IPv6 address is missing closing bracket".to_string())?;
        let address = &rest[..end];
        let port = rest[end + 1..]
            .strip_prefix(':')
            .ok_or_else(|| "magnet x.pe IPv6 address is missing port".to_string())?;
        (address, port)
    } else {
        value
            .rsplit_once(':')
            .ok_or_else(|| "magnet x.pe peer must include host:port".to_string())?
    };
    if address.is_empty() {
        return Err("magnet x.pe peer host is empty".to_string());
    }
    let port = port
        .parse::<u16>()
        .map_err(|err| format!("magnet x.pe peer port is invalid: {err}"))?;
    if port == 0 {
        return Err("magnet x.pe peer port cannot be zero".to_string());
    }
    Ok(PeerInfo {
        address: address.to_string(),
        port,
        client: None,
        progress: 0.0,
        download_speed: 0,
        upload_speed: 0,
        connection: "Magnet peer".to_string(),
    })
}

fn hex_value(value: u8) -> Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err("invalid percent escape".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hex_magnet() {
        let magnet = MagnetLink::parse(
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=Example&tr=http%3A%2F%2Ftracker.test%2Fannounce&ws=https%3A%2F%2Fmirror.test%2Ffile.bin&x.pe=127.0.0.1%3A6881",
        )
        .expect("magnet parses");
        assert_eq!(magnet.display_name.as_deref(), Some("Example"));
        assert_eq!(magnet.trackers, vec!["http://tracker.test/announce"]);
        assert_eq!(magnet.web_seeds, vec!["https://mirror.test/file.bin"]);
        assert_eq!(magnet.peers[0].address, "127.0.0.1");
        assert_eq!(magnet.peers[0].port, 6881);
        assert_eq!(
            sha1::hex(&magnet.info_hash),
            "0123456789abcdef0123456789abcdef01234567"
        );
    }

    #[test]
    fn deduplicates_magnet_sources() {
        let magnet = MagnetLink::parse(concat!(
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
            "&tr=udp%3A%2F%2Ft.test%3A80&tr=udp%3A%2F%2Ft.test%3A80",
            "&ws=https%3A%2F%2Fm.test%2Ff&ws=https%3A%2F%2Fm.test%2Ff",
            "&x.pe=127.0.0.1%3A6881&x.pe=127.0.0.1%3A6881",
        ))
        .expect("magnet parses");
        assert_eq!(magnet.trackers.len(), 1);
        assert_eq!(magnet.web_seeds.len(), 1);
        assert_eq!(magnet.peers.len(), 1);
    }

    #[test]
    fn parses_case_insensitive_magnet_parts() {
        let magnet = MagnetLink::parse(
            "MAGNET:?XT=URN:BTIH:0123456789abcdef0123456789abcdef01234567&TR=http%3A%2F%2Ft.test%2Fannounce",
        )
        .expect("magnet parses");
        assert_eq!(magnet.trackers, vec!["http://t.test/announce"]);
    }

    #[test]
    fn rejects_oversized_magnet_links() {
        let input = format!(
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn={}",
            "a".repeat(MAX_MAGNET_URI_LENGTH)
        );
        assert!(MagnetLink::parse(&input).is_err());
    }

    #[test]
    fn caps_unique_magnet_sources() {
        let trackers = (0..MAX_MAGNET_TRACKERS + 20)
            .map(|index| format!("&tr=http://tracker{index}.test/announce"))
            .collect::<String>();
        let input =
            format!("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567{trackers}");
        let magnet = MagnetLink::parse(&input).expect("bounded magnet parses");
        assert_eq!(magnet.trackers.len(), MAX_MAGNET_TRACKERS);
    }
}
