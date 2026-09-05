use std::collections::HashSet;
use std::net::{IpAddr, Ipv6Addr};

use crate::torrent::{
    bencode::{self, BencodeNode},
    peer::PeerInfo,
    tracker,
};

pub const MAX_PEX_PEERS_PER_MESSAGE: usize = 50;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PexPeer {
    pub address: String,
    pub port: u16,
    pub flags: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PexMessage {
    pub added: Vec<PexPeer>,
    pub dropped: Vec<PexPeer>,
}

pub fn build_pex_message(added: &[PexPeer], dropped: &[PexPeer]) -> Result<Vec<u8>, String> {
    if added.is_empty() && dropped.is_empty() {
        return Err("PEX message must include added or dropped peers".to_string());
    }
    let mut out = Vec::new();
    out.push(b'd');
    if !added.is_empty() {
        let (compact, flags) = compact_ipv4_peers(added)?;
        write_bytes_entry(&mut out, b"5:added", &compact);
        if flags.iter().any(|flag| *flag != 0) {
            write_bytes_entry(&mut out, b"7:added.f", &flags);
        }
    }
    if !dropped.is_empty() {
        let (compact, _) = compact_ipv4_peers(dropped)?;
        write_bytes_entry(&mut out, b"7:dropped", &compact);
    }
    out.push(b'e');
    Ok(out)
}

pub fn parse_pex_message(payload: &[u8]) -> Result<PexMessage, String> {
    let root = bencode::parse(payload)?;
    let added = parse_peer_group(
        root.dict_get(b"added"),
        root.dict_get(b"added.f"),
        root.dict_get(b"added6"),
        root.dict_get(b"added6.f"),
        "added",
    )?;
    let dropped = parse_peer_group(
        root.dict_get(b"dropped"),
        None,
        root.dict_get(b"dropped6"),
        None,
        "dropped",
    )?;
    // BEP 11 updates are advisory. Some widely used clients emit an empty
    // dictionary as a heartbeat/no-change update; it must not tear down the
    // underlying peer-wire connection.
    Ok(PexMessage { added, dropped })
}

pub fn pex_peers_to_peer_info(peers: &[PexPeer]) -> Vec<PeerInfo> {
    peers
        .iter()
        .map(|peer| PeerInfo {
            address: peer.address.clone(),
            port: peer.port,
            client: None,
            progress: if peer.flags & 0x02 != 0 { 1.0 } else { 0.0 },
            download_speed: 0,
            upload_speed: 0,
            connection: "PEX discovered".to_string(),
        })
        .collect()
}

fn parse_peer_group(
    ipv4: Option<&BencodeNode>,
    ipv4_flags: Option<&BencodeNode>,
    ipv6: Option<&BencodeNode>,
    ipv6_flags: Option<&BencodeNode>,
    label: &str,
) -> Result<Vec<PexPeer>, String> {
    let mut peers = Vec::new();
    if let Some(compact) = ipv4.and_then(BencodeNode::as_bytes) {
        let flags = flags_for(ipv4_flags, compact.len() / 6, label)?;
        for (index, peer) in tracker::parse_compact_peers(compact)?
            .into_iter()
            .enumerate()
        {
            peers.push(PexPeer {
                address: peer.address,
                port: peer.port,
                flags: flags.get(index).copied().unwrap_or_default(),
            });
        }
    }
    if let Some(compact) = ipv6.and_then(BencodeNode::as_bytes) {
        if compact.len() % 18 != 0 {
            return Err(format!("PEX {label}6 length must be a multiple of 18"));
        }
        let flags = flags_for(ipv6_flags, compact.len() / 18, label)?;
        for (index, chunk) in compact.chunks_exact(18).enumerate() {
            let address = Ipv6Addr::from(
                <[u8; 16]>::try_from(&chunk[..16]).expect("sixteen-byte IPv6 PEX slice"),
            );
            peers.push(PexPeer {
                address: address.to_string(),
                port: u16::from_be_bytes([chunk[16], chunk[17]]),
                flags: flags.get(index).copied().unwrap_or_default(),
            });
        }
    }
    Ok(sanitized_peers(peers))
}

fn flags_for(
    flags: Option<&BencodeNode>,
    expected_count: usize,
    label: &str,
) -> Result<Vec<u8>, String> {
    let Some(flags) = flags.and_then(BencodeNode::as_bytes) else {
        return Ok(Vec::new());
    };
    if flags.len() != expected_count {
        return Err(format!(
            "PEX {label} flags length {} does not match peer count {expected_count}",
            flags.len()
        ));
    }
    Ok(flags.to_vec())
}

fn sanitized_peers(peers: Vec<PexPeer>) -> Vec<PexPeer> {
    let mut seen_endpoints = HashSet::new();
    let mut seen_addresses = HashSet::new();
    let mut out = Vec::new();
    for peer in peers {
        if peer.port == 0 {
            continue;
        }
        let Ok(address) = peer.address.parse::<IpAddr>() else {
            continue;
        };
        if address.is_unspecified()
            || address.is_loopback()
            || address.is_multicast()
            || matches!(address, IpAddr::V4(address) if address.is_broadcast())
        {
            continue;
        }
        if !seen_endpoints.insert((peer.address.clone(), peer.port)) {
            continue;
        }
        if !seen_addresses.insert(peer.address.clone()) {
            continue;
        }
        out.push(peer);
        if out.len() == MAX_PEX_PEERS_PER_MESSAGE {
            break;
        }
    }
    out
}

fn compact_ipv4_peers(peers: &[PexPeer]) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut compact = Vec::with_capacity(peers.len() * 6);
    let mut flags = Vec::with_capacity(peers.len());
    for peer in peers {
        if peer.port == 0 {
            return Err("PEX peer port cannot be zero".to_string());
        }
        let address = peer
            .address
            .parse::<std::net::Ipv4Addr>()
            .map_err(|_| "PEX builder only supports IPv4 peers for now".to_string())?;
        compact.extend_from_slice(&address.octets());
        compact.extend_from_slice(&peer.port.to_be_bytes());
        flags.push(peer.flags);
    }
    Ok((compact, flags))
}

fn write_bytes_entry(out: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    out.extend_from_slice(key);
    out.extend_from_slice(value.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_and_parses_pex_added_peers() {
        let payload = build_pex_message(
            &[PexPeer {
                address: "203.0.113.7".to_string(),
                port: 6881,
                flags: 0x12,
            }],
            &[],
        )
        .expect("PEX message builds");
        let parsed = parse_pex_message(&payload).expect("PEX message parses");

        assert_eq!(
            parsed.added,
            vec![PexPeer {
                address: "203.0.113.7".to_string(),
                port: 6881,
                flags: 0x12,
            }]
        );
        assert!(parsed.dropped.is_empty());
    }

    #[test]
    fn rejects_mismatched_pex_flags() {
        let payload = b"d5:added6:\xcb\0q\x07\x1a\xe17:added.f0:e";

        assert!(parse_pex_message(payload)
            .expect_err("mismatched flags fail")
            .contains("flags length"));
    }

    #[test]
    fn accepts_empty_pex_heartbeat() {
        let parsed = parse_pex_message(b"de").expect("empty PEX heartbeat parses");
        assert!(parsed.added.is_empty());
        assert!(parsed.dropped.is_empty());
    }

    #[test]
    fn rejects_zero_port_pex_build_peer() {
        let err = build_pex_message(
            &[PexPeer {
                address: "203.0.113.7".to_string(),
                port: 0,
                flags: 0,
            }],
            &[],
        )
        .expect_err("zero-port PEX peer fails");
        assert!(err.contains("port cannot be zero"));
    }

    #[test]
    fn filters_unsafe_and_duplicate_pex_peers() {
        let payload = b"d5:added18:\x7f\0\0\x01\x1a\xe1\xcb\0q\x07\x1a\xe1\xcb\0q\x07\x1a\xe2e";
        let parsed = parse_pex_message(payload).expect("PEX message parses");

        assert_eq!(
            parsed.added,
            vec![PexPeer {
                address: "203.0.113.7".to_string(),
                port: 6881,
                flags: 0,
            }]
        );
    }

    #[test]
    fn bounds_pex_candidates_from_one_peer() {
        let peers = (1..=75)
            .map(|suffix| PexPeer {
                address: format!("203.0.113.{suffix}"),
                port: 6881,
                flags: 0,
            })
            .collect::<Vec<_>>();
        let payload = build_pex_message(&peers, &[]).expect("large PEX message builds");
        let parsed = parse_pex_message(&payload).expect("large PEX message parses");

        assert_eq!(parsed.added.len(), MAX_PEX_PEERS_PER_MESSAGE);
        assert_eq!(
            parsed.added.first().expect("first peer").address,
            "203.0.113.1"
        );
        assert_eq!(
            parsed.added.last().expect("last peer").address,
            "203.0.113.50"
        );
    }
}
