use crate::torrent::{
    bencode::{self, BencodeNode},
    sha1,
};

pub const UT_METADATA_BLOCK_SIZE: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionHandshake {
    pub ut_metadata: Option<u8>,
    pub metadata_size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataMessageType {
    Request,
    Data,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataMessage {
    pub message_type: MetadataMessageType,
    pub piece: u32,
    pub total_size: Option<u64>,
    pub data: Vec<u8>,
}

pub fn build_extension_handshake(ut_metadata_id: u8, metadata_size: Option<u64>) -> Vec<u8> {
    let mut payload = format!("d1:md11:ut_metadatai{ut_metadata_id}ee").into_bytes();
    if let Some(metadata_size) = metadata_size {
        payload.extend_from_slice(format!("13:metadata_sizei{metadata_size}e").as_bytes());
    }
    payload.push(b'e');
    payload
}

pub fn parse_extension_handshake(payload: &[u8]) -> Result<ExtensionHandshake, String> {
    let root = bencode::parse(payload)?;
    let ut_metadata = root
        .dict_get(b"m")
        .and_then(|m| m.dict_get(b"ut_metadata"))
        .and_then(BencodeNode::as_i64)
        .map(u8::try_from)
        .transpose()
        .map_err(|_| "ut_metadata extension id is out of range".to_string())?;
    let metadata_size = root
        .dict_get(b"metadata_size")
        .and_then(BencodeNode::as_i64)
        .map(u64::try_from)
        .transpose()
        .map_err(|_| "metadata_size cannot be negative".to_string())?;

    Ok(ExtensionHandshake {
        ut_metadata,
        metadata_size,
    })
}

pub fn metadata_piece_count(metadata_size: u64) -> usize {
    (metadata_size as usize).div_ceil(UT_METADATA_BLOCK_SIZE)
}

pub fn build_metadata_request(piece: u32) -> Vec<u8> {
    format!("d8:msg_typei0e5:piecei{piece}ee").into_bytes()
}

pub fn build_metadata_data(piece: u32, total_size: u64, data: &[u8]) -> Vec<u8> {
    let mut payload = format!("d8:msg_typei1e5:piecei{piece}e10:total_sizei{total_size}ee").into_bytes();
    payload.extend_from_slice(data);
    payload
}

pub fn build_metadata_reject(piece: u32) -> Vec<u8> {
    format!("d8:msg_typei2e5:piecei{piece}ee").into_bytes()
}

pub fn parse_metadata_message(payload: &[u8]) -> Result<MetadataMessage, String> {
    let header = bencode::parse_prefix(payload)?;
    let msg_type = header
        .dict_get(b"msg_type")
        .and_then(BencodeNode::as_i64)
        .ok_or_else(|| "metadata message is missing msg_type".to_string())?;
    let message_type = match msg_type {
        0 => MetadataMessageType::Request,
        1 => MetadataMessageType::Data,
        2 => MetadataMessageType::Reject,
        _ => return Err(format!("unknown metadata message type: {msg_type}")),
    };
    let piece = header
        .dict_get(b"piece")
        .and_then(BencodeNode::as_i64)
        .ok_or_else(|| "metadata message is missing piece".to_string())
        .and_then(|piece| u32::try_from(piece).map_err(|_| "metadata piece is out of range".to_string()))?;
    let total_size = header
        .dict_get(b"total_size")
        .and_then(BencodeNode::as_i64)
        .map(u64::try_from)
        .transpose()
        .map_err(|_| "metadata total_size cannot be negative".to_string())?;
    let data = if message_type == MetadataMessageType::Data {
        payload[header.span.end..].to_vec()
    } else {
        Vec::new()
    };

    Ok(MetadataMessage {
        message_type,
        piece,
        total_size,
        data,
    })
}

pub fn verify_metadata_info_hash(metadata_info: &[u8], expected_info_hash: [u8; 20]) -> bool {
    sha1::digest(metadata_info) == expected_info_hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_and_parses_extension_handshake() {
        let payload = build_extension_handshake(3, Some(65_000));
        let parsed = parse_extension_handshake(&payload).expect("handshake parses");

        assert_eq!(
            parsed,
            ExtensionHandshake {
                ut_metadata: Some(3),
                metadata_size: Some(65_000),
            }
        );
    }

    #[test]
    fn counts_metadata_pieces() {
        assert_eq!(metadata_piece_count(1), 1);
        assert_eq!(metadata_piece_count(UT_METADATA_BLOCK_SIZE as u64), 1);
        assert_eq!(metadata_piece_count(UT_METADATA_BLOCK_SIZE as u64 + 1), 2);
    }

    #[test]
    fn builds_and_parses_metadata_request() {
        let parsed = parse_metadata_message(&build_metadata_request(7)).expect("request parses");

        assert_eq!(
            parsed,
            MetadataMessage {
                message_type: MetadataMessageType::Request,
                piece: 7,
                total_size: None,
                data: Vec::new(),
            }
        );
    }

    #[test]
    fn parses_metadata_data_with_trailing_bytes() {
        let payload = build_metadata_data(2, 40_000, b"info-dict-bytes");
        let parsed = parse_metadata_message(&payload).expect("data message parses");

        assert_eq!(parsed.message_type, MetadataMessageType::Data);
        assert_eq!(parsed.piece, 2);
        assert_eq!(parsed.total_size, Some(40_000));
        assert_eq!(parsed.data, b"info-dict-bytes");
    }

    #[test]
    fn builds_and_parses_metadata_reject() {
        let parsed = parse_metadata_message(&build_metadata_reject(4)).expect("reject parses");

        assert_eq!(parsed.message_type, MetadataMessageType::Reject);
        assert_eq!(parsed.piece, 4);
        assert!(parsed.data.is_empty());
    }

    #[test]
    fn verifies_metadata_info_hash() {
        let info = b"d4:name4:teste";
        assert!(verify_metadata_info_hash(info, sha1::digest(info)));
        assert!(!verify_metadata_info_hash(info, [0; 20]));
    }
}
