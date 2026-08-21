use crate::torrent::{metainfo::TorrentFile, sha1};

pub const DEFAULT_BLOCK_SIZE: u32 = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRequest {
    pub piece_index: u32,
    pub begin: u32,
    pub length: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiecePlan {
    pub index: u32,
    pub absolute_offset: u64,
    pub length: u32,
    pub hash: [u8; 20],
    pub blocks: Vec<BlockRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileWriteSpan {
    pub file_index: usize,
    pub file_offset: u64,
    pub block_offset: usize,
    pub length: usize,
}

pub fn build_piece_plan(
    total_length: u64,
    piece_length: u64,
    hashes: &[[u8; 20]],
) -> Result<Vec<PiecePlan>, String> {
    if piece_length == 0 {
        return Err("piece length cannot be zero".to_string());
    }
    let expected_pieces = total_length.div_ceil(piece_length) as usize;
    if expected_pieces != hashes.len() {
        return Err(format!(
            "piece hash count mismatch: expected {expected_pieces}, got {}",
            hashes.len()
        ));
    }

    hashes
        .iter()
        .enumerate()
        .map(|(index, hash)| {
            let absolute_offset = index as u64 * piece_length;
            let remaining = total_length.saturating_sub(absolute_offset);
            let length = remaining.min(piece_length);
            let length = u32::try_from(length).map_err(|_| "piece length exceeds u32".to_string())?;
            Ok(PiecePlan {
                index: u32::try_from(index).map_err(|_| "piece index exceeds u32".to_string())?,
                absolute_offset,
                length,
                hash: *hash,
                blocks: build_block_requests(index as u32, length, DEFAULT_BLOCK_SIZE),
            })
        })
        .collect()
}

pub fn build_block_requests(piece_index: u32, piece_length: u32, block_size: u32) -> Vec<BlockRequest> {
    if piece_length == 0 || block_size == 0 {
        return Vec::new();
    }
    let mut blocks = Vec::new();
    let mut begin = 0;
    while begin < piece_length {
        let length = (piece_length - begin).min(block_size);
        blocks.push(BlockRequest {
            piece_index,
            begin,
            length,
        });
        begin += length;
    }
    blocks
}

pub fn verify_piece(data: &[u8], expected_hash: [u8; 20]) -> bool {
    sha1::digest(data) == expected_hash
}

pub fn map_block_to_files(
    files: &[TorrentFile],
    absolute_offset: u64,
    block_length: usize,
) -> Vec<FileWriteSpan> {
    let block_end = absolute_offset.saturating_add(block_length as u64);
    let mut file_start = 0u64;
    let mut spans = Vec::new();

    for (file_index, file) in files.iter().enumerate() {
        let file_end = file_start.saturating_add(file.length);
        let overlap_start = absolute_offset.max(file_start);
        let overlap_end = block_end.min(file_end);
        if overlap_start < overlap_end {
            spans.push(FileWriteSpan {
                file_index,
                file_offset: overlap_start - file_start,
                block_offset: (overlap_start - absolute_offset) as usize,
                length: (overlap_end - overlap_start) as usize,
            });
        }
        file_start = file_end;
        if file_start >= block_end {
            break;
        }
    }

    spans
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_last_piece_and_block_plan() {
        let hashes = vec![[1u8; 20], [2u8; 20]];
        let pieces = build_piece_plan(50_000, 40_000, &hashes).expect("piece plan builds");

        assert_eq!(pieces.len(), 2);
        assert_eq!(pieces[0].length, 40_000);
        assert_eq!(pieces[0].blocks.len(), 3);
        assert_eq!(pieces[0].blocks[0].length, DEFAULT_BLOCK_SIZE);
        assert_eq!(pieces[0].blocks[2].begin, 32_768);
        assert_eq!(pieces[0].blocks[2].length, 7_232);
        assert_eq!(pieces[1].absolute_offset, 40_000);
        assert_eq!(pieces[1].length, 10_000);
    }

    #[test]
    fn rejects_piece_hash_count_mismatch() {
        assert!(build_piece_plan(50_000, 40_000, &[[1u8; 20]]).is_err());
    }

    #[test]
    fn verifies_piece_hash() {
        let data = b"hello piece";
        assert!(verify_piece(data, sha1::digest(data)));
        assert!(!verify_piece(data, [0u8; 20]));
    }

    #[test]
    fn maps_block_across_multiple_files() {
        let files = vec![
            TorrentFile {
                name: "one.bin".to_string(),
                components: vec!["one.bin".to_string()],
                length: 5,
                included: true,
            },
            TorrentFile {
                name: "two.bin".to_string(),
                components: vec!["two.bin".to_string()],
                length: 10,
                included: true,
            },
        ];

        let spans = map_block_to_files(&files, 3, 8);

        assert_eq!(
            spans,
            vec![
                FileWriteSpan {
                    file_index: 0,
                    file_offset: 3,
                    block_offset: 0,
                    length: 2,
                },
                FileWriteSpan {
                    file_index: 1,
                    file_offset: 0,
                    block_offset: 2,
                    length: 6,
                },
            ]
        );
    }
}
