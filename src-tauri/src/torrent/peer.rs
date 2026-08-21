use serde::{Deserialize, Serialize};

pub const PROTOCOL_NAME: &[u8; 19] = b"BitTorrent protocol";
pub const HANDSHAKE_LEN: usize = 68;
pub const EXTENSION_PROTOCOL_RESERVED_BYTE: usize = 5;
pub const EXTENSION_PROTOCOL_RESERVED_MASK: u8 = 0x10;
pub const DHT_RESERVED_BYTE: usize = 7;
pub const DHT_RESERVED_MASK: u8 = 0x01;

pub const MSG_CHOKE: u8 = 0;
pub const MSG_UNCHOKE: u8 = 1;
pub const MSG_INTERESTED: u8 = 2;
pub const MSG_NOT_INTERESTED: u8 = 3;
pub const MSG_HAVE: u8 = 4;
pub const MSG_BITFIELD: u8 = 5;
pub const MSG_REQUEST: u8 = 6;
pub const MSG_PIECE: u8 = 7;
pub const MSG_CANCEL: u8 = 8;
pub const MSG_PORT: u8 = 9;
pub const MSG_EXTENDED: u8 = 20;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub address: String,
    pub port: u16,
    pub client: Option<String>,
    pub progress: f32,
    pub download_speed: u64,
    pub upload_speed: u64,
    pub connection: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerHandshake {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub reserved: [u8; 8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have { index: u32 },
    Bitfield(Vec<u8>),
    Request { index: u32, begin: u32, length: u32 },
    Piece { index: u32, begin: u32, block: Vec<u8> },
    Cancel { index: u32, begin: u32, length: u32 },
    Port { port: u16 },
    Extended { extension_id: u8, payload: Vec<u8> },
    Unknown { id: u8, payload: Vec<u8> },
}

pub fn build_handshake(info_hash: [u8; 20], peer_id: [u8; 20]) -> [u8; HANDSHAKE_LEN] {
    build_handshake_with_reserved(info_hash, peer_id, [0; 8])
}

pub fn build_extended_handshake(info_hash: [u8; 20], peer_id: [u8; 20]) -> [u8; HANDSHAKE_LEN] {
    build_feature_handshake(info_hash, peer_id, true, false)
}

pub fn build_dht_handshake(info_hash: [u8; 20], peer_id: [u8; 20]) -> [u8; HANDSHAKE_LEN] {
    build_feature_handshake(info_hash, peer_id, false, true)
}

pub fn build_feature_handshake(
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    extension_protocol: bool,
    dht: bool,
) -> [u8; HANDSHAKE_LEN] {
    let mut reserved = [0u8; 8];
    if extension_protocol {
        reserved[EXTENSION_PROTOCOL_RESERVED_BYTE] |= EXTENSION_PROTOCOL_RESERVED_MASK;
    }
    if dht {
        reserved[DHT_RESERVED_BYTE] |= DHT_RESERVED_MASK;
    }
    build_handshake_with_reserved(info_hash, peer_id, reserved)
}

pub fn build_handshake_with_reserved(
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    reserved: [u8; 8],
) -> [u8; HANDSHAKE_LEN] {
    let mut out = [0u8; HANDSHAKE_LEN];
    out[0] = PROTOCOL_NAME.len() as u8;
    out[1..20].copy_from_slice(PROTOCOL_NAME);
    out[20..28].copy_from_slice(&reserved);
    out[28..48].copy_from_slice(&info_hash);
    out[48..68].copy_from_slice(&peer_id);
    out
}

pub fn parse_handshake(input: &[u8]) -> Result<([u8; 20], [u8; 20]), String> {
    if input.len() < HANDSHAKE_LEN {
        return Err("handshake too short".to_string());
    }
    if input[0] != PROTOCOL_NAME.len() as u8 || &input[1..20] != PROTOCOL_NAME {
        return Err("not a BitTorrent protocol handshake".to_string());
    }
    let mut info_hash = [0u8; 20];
    let mut peer_id = [0u8; 20];
    info_hash.copy_from_slice(&input[28..48]);
    peer_id.copy_from_slice(&input[48..68]);
    Ok((info_hash, peer_id))
}

pub fn parse_handshake_full(input: &[u8]) -> Result<PeerHandshake, String> {
    if input.len() < HANDSHAKE_LEN {
        return Err("handshake too short".to_string());
    }
    if input[0] != PROTOCOL_NAME.len() as u8 || &input[1..20] != PROTOCOL_NAME {
        return Err("not a BitTorrent protocol handshake".to_string());
    }
    let mut reserved = [0u8; 8];
    let mut info_hash = [0u8; 20];
    let mut peer_id = [0u8; 20];
    reserved.copy_from_slice(&input[20..28]);
    info_hash.copy_from_slice(&input[28..48]);
    peer_id.copy_from_slice(&input[48..68]);
    Ok(PeerHandshake {
        info_hash,
        peer_id,
        reserved,
    })
}

pub fn supports_extension_protocol(handshake: &PeerHandshake) -> bool {
    handshake.reserved[EXTENSION_PROTOCOL_RESERVED_BYTE] & EXTENSION_PROTOCOL_RESERVED_MASK != 0
}

pub fn supports_dht(handshake: &PeerHandshake) -> bool {
    handshake.reserved[DHT_RESERVED_BYTE] & DHT_RESERVED_MASK != 0
}

pub fn decode_peer_client(peer_id: &[u8]) -> Option<String> {
    if peer_id.len() != 20 {
        return None;
    }

    if peer_id[0] == b'-' && peer_id[7] == b'-' {
        let code = std::str::from_utf8(&peer_id[1..3]).ok()?;
        let version = decode_azureus_version(&peer_id[3..7]);
        let name = azureus_client_name(code);
        return Some(match version {
            Some(version) => format!("{name} {version}"),
            None => name.to_string(),
        });
    }

    if peer_id.iter().all(|byte| byte.is_ascii_graphic() || *byte == b' ') {
        let display = String::from_utf8_lossy(peer_id).trim().to_string();
        if !display.is_empty() {
            return Some(format!("Unknown ({display})"));
        }
    }

    None
}

pub fn build_keep_alive() -> [u8; 4] {
    [0, 0, 0, 0]
}

pub fn build_choke() -> [u8; 5] {
    build_simple_message(MSG_CHOKE)
}

pub fn build_unchoke() -> [u8; 5] {
    build_simple_message(MSG_UNCHOKE)
}

pub fn build_interested() -> [u8; 5] {
    build_simple_message(MSG_INTERESTED)
}

pub fn build_not_interested() -> [u8; 5] {
    build_simple_message(MSG_NOT_INTERESTED)
}

pub fn build_have(index: u32) -> [u8; 9] {
    let mut out = [0u8; 9];
    out[0..4].copy_from_slice(&5u32.to_be_bytes());
    out[4] = MSG_HAVE;
    out[5..9].copy_from_slice(&index.to_be_bytes());
    out
}

pub fn build_bitfield(bitfield: &[u8]) -> Vec<u8> {
    let length = 1 + bitfield.len() as u32;
    let mut out = Vec::with_capacity(4 + length as usize);
    out.extend_from_slice(&length.to_be_bytes());
    out.push(MSG_BITFIELD);
    out.extend_from_slice(bitfield);
    out
}

pub fn build_request(index: u32, begin: u32, length: u32) -> [u8; 17] {
    let mut out = [0u8; 17];
    out[0..4].copy_from_slice(&13u32.to_be_bytes());
    out[4] = MSG_REQUEST;
    out[5..9].copy_from_slice(&index.to_be_bytes());
    out[9..13].copy_from_slice(&begin.to_be_bytes());
    out[13..17].copy_from_slice(&length.to_be_bytes());
    out
}

pub fn build_cancel(index: u32, begin: u32, length: u32) -> [u8; 17] {
    let mut out = build_request(index, begin, length);
    out[4] = MSG_CANCEL;
    out
}

pub fn build_port(port: u16) -> [u8; 7] {
    let mut out = [0u8; 7];
    out[0..4].copy_from_slice(&3u32.to_be_bytes());
    out[4] = MSG_PORT;
    out[5..7].copy_from_slice(&port.to_be_bytes());
    out
}

pub fn build_piece(index: u32, begin: u32, block: &[u8]) -> Vec<u8> {
    let length = 9 + block.len() as u32;
    let mut out = Vec::with_capacity(4 + length as usize);
    out.extend_from_slice(&length.to_be_bytes());
    out.push(MSG_PIECE);
    out.extend_from_slice(&index.to_be_bytes());
    out.extend_from_slice(&begin.to_be_bytes());
    out.extend_from_slice(block);
    out
}

pub fn build_extended_message(extension_id: u8, payload: &[u8]) -> Vec<u8> {
    let length = 2 + payload.len() as u32;
    let mut out = Vec::with_capacity(4 + length as usize);
    out.extend_from_slice(&length.to_be_bytes());
    out.push(MSG_EXTENDED);
    out.push(extension_id);
    out.extend_from_slice(payload);
    out
}

pub fn parse_message_frame(input: &[u8]) -> Result<Option<(PeerMessage, usize)>, String> {
    if input.len() < 4 {
        return Ok(None);
    }
    let length = read_u32(input, 0)? as usize;
    let frame_len = 4usize
        .checked_add(length)
        .ok_or_else(|| "peer frame length overflow".to_string())?;
    if input.len() < frame_len {
        return Ok(None);
    }
    if length == 0 {
        return Ok(Some((PeerMessage::KeepAlive, frame_len)));
    }

    let id = input[4];
    let payload = &input[5..frame_len];
    let message = match id {
        MSG_CHOKE => {
            require_len(payload, 0, "choke")?;
            PeerMessage::Choke
        }
        MSG_UNCHOKE => {
            require_len(payload, 0, "unchoke")?;
            PeerMessage::Unchoke
        }
        MSG_INTERESTED => {
            require_len(payload, 0, "interested")?;
            PeerMessage::Interested
        }
        MSG_NOT_INTERESTED => {
            require_len(payload, 0, "not interested")?;
            PeerMessage::NotInterested
        }
        MSG_HAVE => {
            require_len(payload, 4, "have")?;
            PeerMessage::Have {
                index: read_u32(payload, 0)?,
            }
        }
        MSG_BITFIELD => PeerMessage::Bitfield(payload.to_vec()),
        MSG_REQUEST => {
            require_len(payload, 12, "request")?;
            PeerMessage::Request {
                index: read_u32(payload, 0)?,
                begin: read_u32(payload, 4)?,
                length: read_u32(payload, 8)?,
            }
        }
        MSG_PIECE => {
            if payload.len() < 8 {
                return Err("piece payload is too short".to_string());
            }
            PeerMessage::Piece {
                index: read_u32(payload, 0)?,
                begin: read_u32(payload, 4)?,
                block: payload[8..].to_vec(),
            }
        }
        MSG_CANCEL => {
            require_len(payload, 12, "cancel")?;
            PeerMessage::Cancel {
                index: read_u32(payload, 0)?,
                begin: read_u32(payload, 4)?,
                length: read_u32(payload, 8)?,
            }
        }
        MSG_PORT => {
            require_len(payload, 2, "port")?;
            PeerMessage::Port {
                port: u16::from_be_bytes([payload[0], payload[1]]),
            }
        }
        MSG_EXTENDED => {
            if payload.is_empty() {
                return Err("extended payload is missing extension id".to_string());
            }
            PeerMessage::Extended {
                extension_id: payload[0],
                payload: payload[1..].to_vec(),
            }
        }
        _ => PeerMessage::Unknown {
            id,
            payload: payload.to_vec(),
        },
    };

    Ok(Some((message, frame_len)))
}

fn build_simple_message(id: u8) -> [u8; 5] {
    [0, 0, 0, 1, id]
}

fn require_len(payload: &[u8], expected: usize, name: &str) -> Result<(), String> {
    if payload.len() != expected {
        return Err(format!("{name} payload length must be {expected} bytes"));
    }
    Ok(())
}

fn azureus_client_name(code: &str) -> &str {
    match code {
        "AG" => "Ares",
        "AR" => "Arctic",
        "AV" => "Avicora",
        "AZ" => "Azureus",
        "BC" => "BitComet",
        "BF" => "BitFlu",
        "BG" => "BTG",
        "BR" => "BitRocket",
        "BS" => "BTSlave",
        "BX" => "Bittorrent X",
        "CD" => "Enhanced CTorrent",
        "CT" => "CTorrent",
        "DE" => "Deluge",
        "DP" => "Propagate Data Client",
        "EB" => "EBit",
        "ES" => "electric sheep",
        "FT" => "FoxTorrent",
        "FW" => "FrostWire",
        "GS" => "GSTorrent",
        "HL" => "Halite",
        "HN" => "Hydranode",
        "KG" => "KGet",
        "KT" => "KTorrent",
        "LH" => "LH-ABC",
        "LP" => "Lphant",
        "LT" => "libtorrent",
        "lt" => "libTorrent",
        "LW" => "LimeWire",
        "MO" => "MonoTorrent",
        "MP" => "MooPolice",
        "MR" => "Miro",
        "MT" => "MoonlightTorrent",
        "NX" => "Net Transport",
        "OT" => "OmegaTorrent",
        "PD" => "Pando",
        "qB" => "qBittorrent",
        "QD" => "QQDownload",
        "QT" => "Qt 4 Torrent",
        "RT" => "Retriever",
        "S~" => "Shareaza",
        "SB" => "Swiftbit",
        "SS" => "SwarmScope",
        "ST" => "SymTorrent",
        "st" => "sharktorrent",
        "SZ" => "Shareaza",
        "TN" => "TorrentDotNET",
        "TR" => "Transmission",
        "TS" => "Torrentstorm",
        "TT" => "TuoTu",
        "UL" => "uLeecher",
        "UT" => "uTorrent",
        "VG" => "Vagaa",
        "WT" => "BitLet",
        "WW" => "WebTorrent",
        "WY" => "FireTorrent",
        "XL" => "Xunlei",
        "XT" => "XanTorrent",
        "XX" => "Xtorrent",
        "ZT" => "ZipTorrent",
        "NV" => "NovaTorrent",
        _ => "Unknown",
    }
}

fn decode_azureus_version(bytes: &[u8]) -> Option<String> {
    if !bytes.iter().all(|byte| byte.is_ascii_alphanumeric()) {
        return None;
    }
    let mut parts = bytes
        .iter()
        .filter(|byte| byte.is_ascii_alphanumeric())
        .map(|byte| *byte as char)
        .collect::<Vec<_>>();
    while parts.last() == Some(&'0') && parts.len() > 1 {
        parts.pop();
    }
    Some(
        parts
            .into_iter()
            .map(|ch| ch.to_string())
            .collect::<Vec<_>>()
            .join("."),
    )
}

fn read_u32(input: &[u8], offset: usize) -> Result<u32, String> {
    let end = offset + 4;
    let bytes = input
        .get(offset..end)
        .ok_or_else(|| "peer message ended early".to_string())?;
    Ok(u32::from_be_bytes(bytes.try_into().expect("four-byte slice")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_handshake() {
        let info_hash = [7u8; 20];
        let peer_id = *b"-NV0001-123456789012";
        let handshake = build_handshake(info_hash, peer_id);
        let parsed = parse_handshake(&handshake).expect("handshake parses");
        assert_eq!(parsed, (info_hash, peer_id));
    }

    #[test]
    fn marks_extended_protocol_support() {
        let handshake = build_extended_handshake([7u8; 20], *b"-NV0001-123456789012");
        let parsed = parse_handshake_full(&handshake).expect("handshake parses");
        assert!(supports_extension_protocol(&parsed));
    }

    #[test]
    fn marks_dht_support() {
        let handshake = build_dht_handshake([7u8; 20], *b"-NV0001-123456789012");
        let parsed = parse_handshake_full(&handshake).expect("handshake parses");
        assert!(supports_dht(&parsed));
    }

    #[test]
    fn feature_handshake_can_advertise_extensions_and_dht_together() {
        let handshake =
            build_feature_handshake([7u8; 20], *b"-NV0001-123456789012", true, true);
        let parsed = parse_handshake_full(&handshake).expect("handshake parses");
        assert!(supports_extension_protocol(&parsed));
        assert!(supports_dht(&parsed));
    }

    #[test]
    fn decodes_common_azureus_style_peer_ids() {
        assert_eq!(
            decode_peer_client(b"-qB4520-abcdefghijkl").as_deref(),
            Some("qBittorrent 4.5.2")
        );
        assert_eq!(
            decode_peer_client(b"-TR3000-abcdefghijkl").as_deref(),
            Some("Transmission 3")
        );
        assert_eq!(
            decode_peer_client(b"-NV0001-abcdefghijkl").as_deref(),
            Some("NovaTorrent 0.0.0.1")
        );
    }

    #[test]
    fn exposes_printable_unknown_peer_ids_without_hashing_them() {
        assert_eq!(
            decode_peer_client(b"ABCDEFGHIJKLMNOPQRST").as_deref(),
            Some("Unknown (ABCDEFGHIJKLMNOPQRST)")
        );
        assert_eq!(decode_peer_client(&[0; 20]), None);
    }

    #[test]
    fn parses_keep_alive() {
        let parsed = parse_message_frame(&build_keep_alive()).expect("frame parses");
        assert_eq!(parsed, Some((PeerMessage::KeepAlive, 4)));
    }

    #[test]
    fn parses_request_message() {
        let parsed = parse_message_frame(&build_request(3, 16_384, 16_384)).expect("frame parses");
        assert_eq!(
            parsed,
            Some((
                PeerMessage::Request {
                    index: 3,
                    begin: 16_384,
                    length: 16_384,
                },
                17,
            ))
        );
    }

    #[test]
    fn builds_and_parses_port_message() {
        let parsed = parse_message_frame(&build_port(6881)).expect("frame parses");
        assert_eq!(parsed, Some((PeerMessage::Port { port: 6881 }, 7)));
    }

    #[test]
    fn builds_bitfield_message() {
        let parsed = parse_message_frame(&build_bitfield(&[0b1010_0000])).expect("frame parses");
        assert_eq!(parsed, Some((PeerMessage::Bitfield(vec![0b1010_0000]), 6)));
    }

    #[test]
    fn parses_piece_message() {
        let frame = build_piece(2, 4, b"data");
        let parsed = parse_message_frame(&frame).expect("frame parses");
        assert_eq!(
            parsed,
            Some((
                PeerMessage::Piece {
                    index: 2,
                    begin: 4,
                    block: b"data".to_vec(),
                },
                17,
            ))
        );
    }

    #[test]
    fn parses_extended_message() {
        let frame = build_extended_message(0, b"d1:md11:ut_metadatai1eeee");
        let parsed = parse_message_frame(&frame).expect("frame parses");
        assert_eq!(
            parsed,
            Some((
                PeerMessage::Extended {
                    extension_id: 0,
                    payload: b"d1:md11:ut_metadatai1eeee".to_vec(),
                },
                frame.len(),
            ))
        );
    }
}
