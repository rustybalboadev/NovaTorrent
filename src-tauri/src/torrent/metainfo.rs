use crate::torrent::{
    bencode::{self, BencodeValue},
    sha1,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

const MAX_TRACKER_URLS: usize = 64;
const MAX_WEB_SEED_URLS: usize = 32;
const MAX_SOURCE_URL_LENGTH: usize = 4096;
const MAX_TORRENT_FILES: usize = 10_000;
const MAX_PATH_COMPONENTS: usize = 64;
const MAX_COMMENT_LENGTH: usize = 4096;
const MAX_CREATED_BY_LENGTH: usize = 512;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentFile {
    pub name: String,
    pub components: Vec<String>,
    pub length: u64,
    pub included: bool,
}

#[derive(Debug, Clone)]
pub struct Metainfo {
    pub announce: Option<String>,
    pub announce_list: Vec<Vec<String>>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    pub creation_date: Option<i64>,
    pub web_seeds: Vec<String>,
    pub info_hash: [u8; 20],
    pub name: String,
    pub piece_length: u64,
    pub pieces: Vec<[u8; 20]>,
    pub private: bool,
    pub files: Vec<TorrentFile>,
    pub total_length: u64,
}

impl Metainfo {
    pub fn from_bytes(input: &[u8]) -> Result<Self, String> {
        let root = bencode::parse(input)?;
        let info = root
            .dict_get(b"info")
            .ok_or_else(|| "metainfo is missing info dictionary".to_string())?;
        let info_hash = sha1::digest(&input[info.span.clone()]);
        let announce = bounded_string(root.dict_get(b"announce"), MAX_SOURCE_URL_LENGTH);
        let announce_list = parse_announce_list(root.dict_get(b"announce-list"));
        let comment = bounded_string(root.dict_get(b"comment"), MAX_COMMENT_LENGTH);
        let created_by = bounded_string(root.dict_get(b"created by"), MAX_CREATED_BY_LENGTH);
        let creation_date = root
            .dict_get(b"creation date")
            .and_then(|node| node.as_i64());
        let web_seeds = parse_url_list(root.dict_get(b"url-list"));
        Self::from_info_node(
            info,
            info_hash,
            announce,
            announce_list,
            comment,
            created_by,
            creation_date,
            web_seeds,
        )
    }

    pub fn from_info_bytes(
        info_bytes: &[u8],
        announce: Option<String>,
        announce_list: Vec<Vec<String>>,
        web_seeds: Vec<String>,
    ) -> Result<Self, String> {
        let info = bencode::parse(info_bytes)?;
        let info_hash = sha1::digest(info_bytes);
        Self::from_info_node(
            &info,
            info_hash,
            announce,
            announce_list,
            None,
            None,
            None,
            web_seeds,
        )
    }

    fn from_info_node(
        info: &bencode::BencodeNode,
        info_hash: [u8; 20],
        announce: Option<String>,
        announce_list: Vec<Vec<String>>,
        comment: Option<String>,
        created_by: Option<String>,
        creation_date: Option<i64>,
        web_seeds: Vec<String>,
    ) -> Result<Self, String> {
        let name = info
            .dict_get(b"name")
            .and_then(|node| node.as_str_lossy())
            .ok_or_else(|| "info dictionary is missing name".to_string())?;
        validate_path_component(&name)?;
        let piece_length = info
            .dict_get(b"piece length")
            .and_then(|node| node.as_i64())
            .ok_or_else(|| "info dictionary is missing piece length".to_string())?
            .try_into()
            .map_err(|_| "piece length cannot be negative".to_string())?;
        if piece_length == 0 {
            return Err("piece length must be greater than zero".to_string());
        }
        let pieces_raw = info
            .dict_get(b"pieces")
            .and_then(|node| node.as_bytes())
            .ok_or_else(|| "info dictionary is missing pieces".to_string())?;
        if pieces_raw.len() % 20 != 0 {
            return Err("pieces length must be a multiple of 20".to_string());
        }
        let pieces = pieces_raw
            .chunks_exact(20)
            .map(|chunk| chunk.try_into().expect("exact 20-byte chunk"))
            .collect::<Vec<[u8; 20]>>();
        let private = info
            .dict_get(b"private")
            .and_then(|node| node.as_i64())
            .is_some_and(|value| value == 1);
        let files = parse_files(info, &name)?;
        let total_length = files.iter().try_fold(0u64, |total, file| {
            total
                .checked_add(file.length)
                .ok_or_else(|| "torrent total length is too large".to_string())
        })?;
        let expected_piece_count = if total_length == 0 {
            0
        } else {
            ((total_length - 1) / piece_length) + 1
        };
        let actual_piece_count = u64::try_from(pieces.len())
            .map_err(|_| "torrent contains too many piece hashes".to_string())?;
        if actual_piece_count != expected_piece_count {
            return Err(format!(
                "piece hash count does not match torrent size: expected {expected_piece_count}, found {actual_piece_count}"
            ));
        }

        Ok(Self {
            announce,
            announce_list,
            comment,
            created_by,
            creation_date,
            web_seeds,
            info_hash,
            name,
            piece_length,
            pieces,
            private,
            files,
            total_length,
        })
    }

    pub fn tracker_urls(&self) -> Vec<String> {
        let mut urls = Vec::new();
        if let Some(announce) = self.announce.as_ref() {
            if announce.len() <= MAX_SOURCE_URL_LENGTH {
                urls.push(announce.clone());
            }
        }
        for tier in &self.announce_list {
            for tracker in tier {
                if urls.len() >= MAX_TRACKER_URLS {
                    return urls;
                }
                if tracker.len() > MAX_SOURCE_URL_LENGTH {
                    continue;
                }
                if !urls.iter().any(|existing| existing == tracker) {
                    urls.push(tracker.clone());
                }
            }
        }
        urls
    }
}

fn parse_files(info: &bencode::BencodeNode, name: &str) -> Result<Vec<TorrentFile>, String> {
    if let Some(length) = info.dict_get(b"length").and_then(|node| node.as_i64()) {
        let length = length
            .try_into()
            .map_err(|_| "single-file length cannot be negative".to_string())?;
        return Ok(vec![TorrentFile {
            name: name.to_string(),
            components: vec![name.to_string()],
            length,
            included: true,
        }]);
    }

    let files = info
        .dict_get(b"files")
        .and_then(|node| node.as_list())
        .ok_or_else(|| "multi-file torrent is missing files list".to_string())?;
    if files.len() > MAX_TORRENT_FILES {
        return Err(format!(
            "torrent contains too many files: maximum is {MAX_TORRENT_FILES}"
        ));
    }

    let files = files
        .iter()
        .map(|file| {
            let length = file
                .dict_get(b"length")
                .and_then(|node| node.as_i64())
                .ok_or_else(|| "file entry is missing length".to_string())?
                .try_into()
                .map_err(|_| "file length cannot be negative".to_string())?;
            let path = file
                .dict_get(b"path")
                .and_then(|node| node.as_list())
                .ok_or_else(|| "file entry is missing path".to_string())?;
            if path.len() > MAX_PATH_COMPONENTS {
                return Err(format!(
                    "torrent file path contains too many components: maximum is {MAX_PATH_COMPONENTS}"
                ));
            }
            let components = path
                .iter()
                .map(|node| {
                    node.as_str_lossy()
                        .ok_or_else(|| "path component is not a byte string".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            if components.is_empty() {
                return Err("file path cannot be empty".to_string());
            }
            for component in &components {
                validate_path_component(component)?;
            }
            Ok(TorrentFile {
                name: components.join("/"),
                components,
                length,
                included: true,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let mut normalized_paths = HashSet::with_capacity(files.len());
    for file in &files {
        let normalized = file
            .components
            .iter()
            .map(|component| component.to_lowercase())
            .collect::<Vec<_>>()
            .join("\\");
        if !normalized_paths.insert(normalized) {
            return Err("torrent contains duplicate Windows file paths".to_string());
        }
    }
    Ok(files)
}

fn validate_path_component(component: &str) -> Result<(), String> {
    if component.is_empty() || component == "." || component == ".." {
        return Err("torrent path component is not safe".to_string());
    }
    if component.ends_with([' ', '.'])
        || component.encode_utf16().count() > 255
        || component.chars().any(|ch| {
            ch.is_control()
                || matches!(
                    ch,
                    '/' | '\\' | ':' | '\0' | '<' | '>' | '"' | '|' | '?' | '*'
                )
        })
    {
        return Err("torrent path component contains an unsafe character".to_string());
    }
    let stem = component.split('.').next().unwrap_or(component);
    if matches!(
        stem.to_ascii_uppercase().as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    ) {
        return Err("torrent path component uses a reserved Windows name".to_string());
    }
    Ok(())
}

fn parse_announce_list(node: Option<&bencode::BencodeNode>) -> Vec<Vec<String>> {
    let Some(node) = node else {
        return Vec::new();
    };
    let Some(tiers) = node.as_list() else {
        return Vec::new();
    };
    let mut output = Vec::new();
    let mut remaining = MAX_TRACKER_URLS;
    for tier in tiers {
        if remaining == 0 {
            break;
        }
        let Some(items) = tier.as_list() else {
            continue;
        };
        let trackers = items
            .iter()
            .filter_map(|item| bounded_string(Some(item), MAX_SOURCE_URL_LENGTH))
            .take(remaining)
            .collect::<Vec<_>>();
        remaining = remaining.saturating_sub(trackers.len());
        if !trackers.is_empty() {
            output.push(trackers);
        }
    }
    output
}

fn parse_url_list(node: Option<&bencode::BencodeNode>) -> Vec<String> {
    let Some(node) = node else {
        return Vec::new();
    };
    match &node.value {
        BencodeValue::Bytes(_) => bounded_string(Some(node), MAX_TORRENT_FILE_BYTES_FOR_URL_LIST)
            .map(|value| collect_split_urls(std::iter::once(value)))
            .unwrap_or_default(),
        BencodeValue::List(items) => collect_split_urls(
            items
                .iter()
                .filter_map(|item| bounded_string(Some(item), MAX_TORRENT_FILE_BYTES_FOR_URL_LIST)),
        ),
        _ => Vec::new(),
    }
}

const MAX_TORRENT_FILE_BYTES_FOR_URL_LIST: usize = 32 * MAX_SOURCE_URL_LENGTH;

fn collect_split_urls(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut output = Vec::new();
    for value in values {
        for url in value.split(',').map(str::trim) {
            if output.len() >= MAX_WEB_SEED_URLS {
                return output;
            }
            if !url.is_empty() && url.len() <= MAX_SOURCE_URL_LENGTH {
                output.push(url.to_string());
            }
        }
    }
    output
}

fn bounded_string(node: Option<&bencode::BencodeNode>, max_length: usize) -> Option<String> {
    let bytes = node?.as_bytes()?;
    (bytes.len() <= max_length).then(|| String::from_utf8_lossy(bytes).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_bytes(output: &mut Vec<u8>, value: &[u8]) {
        output.extend_from_slice(value.len().to_string().as_bytes());
        output.push(b':');
        output.extend_from_slice(value);
    }

    fn push_integer(output: &mut Vec<u8>, value: i64) {
        output.push(b'i');
        output.extend_from_slice(value.to_string().as_bytes());
        output.push(b'e');
    }

    fn synthetic_multi_file_metainfo() -> Vec<u8> {
        let mut output = vec![b'd'];

        push_bytes(&mut output, b"announce");
        push_bytes(&mut output, b"udp://tracker.example:6969/announce");
        push_bytes(&mut output, b"announce-list");
        output.extend_from_slice(b"ll");
        push_bytes(&mut output, b"udp://backup.example:6969/announce");
        output.extend_from_slice(b"ee");
        push_bytes(&mut output, b"comment");
        push_bytes(&mut output, b"synthetic test metadata");
        push_bytes(&mut output, b"created by");
        push_bytes(&mut output, b"NovaTorrent tests");
        push_bytes(&mut output, b"creation date");
        push_integer(&mut output, 1_700_000_000);
        push_bytes(&mut output, b"info");
        output.push(b'd');
        push_bytes(&mut output, b"files");
        output.push(b'l');
        for (length, path) in [(3, ["dir", "one.bin"]), (5, ["dir", "two.bin"])] {
            output.push(b'd');
            push_bytes(&mut output, b"length");
            push_integer(&mut output, length);
            push_bytes(&mut output, b"path");
            output.push(b'l');
            for component in path {
                push_bytes(&mut output, component.as_bytes());
            }
            output.extend_from_slice(b"ee");
        }
        output.push(b'e');
        push_bytes(&mut output, b"name");
        push_bytes(&mut output, b"example");
        push_bytes(&mut output, b"piece length");
        push_integer(&mut output, 4);
        push_bytes(&mut output, b"pieces");
        push_bytes(&mut output, &[0x11; 40]);
        push_bytes(&mut output, b"private");
        push_integer(&mut output, 1);
        output.push(b'e');
        push_bytes(&mut output, b"url-list");
        output.push(b'l');
        push_bytes(&mut output, b"https://seed.example/files/");
        output.extend_from_slice(b"ee");
        output
    }

    #[test]
    fn parses_synthetic_multi_file_metainfo() {
        let bytes = synthetic_multi_file_metainfo();
        let meta = Metainfo::from_bytes(&bytes).expect("synthetic metainfo parses");

        assert_eq!(meta.name, "example");
        assert_eq!(meta.piece_length, 4);
        assert_eq!(meta.pieces, vec![[0x11; 20], [0x11; 20]]);
        assert_eq!(meta.total_length, 8);
        assert_eq!(meta.files.len(), 2);
        assert_eq!(meta.files[0].components, ["dir", "one.bin"]);
        assert_eq!(meta.files[1].name, "dir/two.bin");
        assert!(meta.private);
        assert_eq!(meta.comment.as_deref(), Some("synthetic test metadata"));
        assert_eq!(meta.created_by.as_deref(), Some("NovaTorrent tests"));
        assert_eq!(meta.creation_date, Some(1_700_000_000));
        assert_eq!(meta.web_seeds, ["https://seed.example/files/"]);
        assert_eq!(
            meta.tracker_urls(),
            [
                "udp://tracker.example:6969/announce",
                "udp://backup.example:6969/announce"
            ]
        );
    }

    #[test]
    fn parses_raw_info_dictionary_from_magnet_metadata() {
        let info = b"d6:lengthi11e4:name11:example.txt12:piece lengthi4e6:pieces60:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaae";
        let meta = Metainfo::from_info_bytes(
            info,
            Some("udp://tracker.example:6969/announce".to_string()),
            Vec::new(),
            Vec::new(),
        )
        .expect("raw info dictionary parses");

        assert_eq!(meta.name, "example.txt");
        assert_eq!(meta.total_length, 11);
        assert_eq!(meta.piece_length, 4);
        assert_eq!(meta.pieces.len(), 3);
        assert_eq!(
            meta.announce.as_deref(),
            Some("udp://tracker.example:6969/announce")
        );
        assert_eq!(meta.info_hash, sha1::digest(info));
    }

    #[test]
    fn rejects_unsafe_file_paths() {
        let input = b"d4:infod5:filesld6:lengthi1e4:pathl2:..8:evil.txteee4:name4:root12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
        assert!(Metainfo::from_bytes(input).is_err());
    }

    #[test]
    fn rejects_zero_piece_length() {
        let input =
            b"d4:infod6:lengthi1e4:name1:a12:piece lengthi0e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
        assert!(Metainfo::from_bytes(input).is_err());
    }

    #[test]
    fn rejects_inconsistent_piece_hash_count() {
        let input = b"d4:infod6:lengthi10e4:name1:a12:piece lengthi4e6:pieces40:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaee";
        let error = Metainfo::from_bytes(input).expect_err("piece count must be validated");
        assert!(error.contains("piece hash count"));
    }

    #[test]
    fn rejects_duplicate_windows_paths() {
        let input = b"d4:infod5:filesld6:lengthi1e4:pathl5:A.txteed6:lengthi1e4:pathl5:a.txteee4:name4:root12:piece lengthi2e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
        let error = Metainfo::from_bytes(input).expect_err("duplicate paths must be rejected");
        assert!(error.contains("duplicate Windows file paths"));
    }

    #[test]
    fn rejects_reserved_windows_names() {
        let input = b"d4:infod6:lengthi1e4:name7:CON.txt12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
        let error = Metainfo::from_bytes(input).expect_err("reserved names must be rejected");
        assert!(error.contains("reserved Windows name"));
    }
}
