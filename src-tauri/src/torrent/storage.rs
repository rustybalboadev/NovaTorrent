use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use crate::torrent::metainfo::TorrentFile;
use crate::torrent::sha1;

const PARTIAL_CHECKPOINT_PIECES: usize = 16;
const PARTIAL_CHECKPOINT_BYTES: u64 = 16 * 1024 * 1024;
const PARTIAL_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentWriteSummary {
    pub paths: Vec<PathBuf>,
    pub bytes_written: u64,
    pub files_written: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredTorrentCheck {
    pub pieces: Vec<bool>,
    pub bytes_verified: u64,
    pub complete: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct VerifiedByteRange {
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PartialPieceState {
    version: u32,
    total_length: u64,
    piece_length: u64,
    pieces: Vec<bool>,
}

#[derive(Debug)]
pub struct PartialPieceStore {
    data_path: PathBuf,
    state_path: PathBuf,
    data_file: File,
    state: PartialPieceState,
    piece_hashes: Vec<[u8; 20]>,
    dirty_pieces: usize,
    dirty_bytes: u64,
    last_checkpoint: Instant,
}

impl PartialPieceStore {
    pub fn open(
        output_root: &Path,
        key: &str,
        total_length: u64,
        piece_length: u64,
        piece_hashes: &[[u8; 20]],
    ) -> Result<Self, String> {
        Self::open_inner(
            output_root,
            key,
            total_length,
            piece_length,
            piece_hashes,
            true,
        )?
        .ok_or_else(|| "partial store was not created".to_string())
    }

    pub fn open_existing(
        output_root: &Path,
        key: &str,
        total_length: u64,
        piece_length: u64,
        piece_hashes: &[[u8; 20]],
    ) -> Result<Option<Self>, String> {
        Self::open_inner(
            output_root,
            key,
            total_length,
            piece_length,
            piece_hashes,
            false,
        )
    }

    fn open_inner(
        output_root: &Path,
        key: &str,
        total_length: u64,
        piece_length: u64,
        piece_hashes: &[[u8; 20]],
        create_missing: bool,
    ) -> Result<Option<Self>, String> {
        validate_path_component(key)?;
        if piece_length == 0 {
            return Err("piece length cannot be zero".to_string());
        }
        let expected_piece_count = total_length.div_ceil(piece_length) as usize;
        if expected_piece_count != piece_hashes.len() {
            return Err(format!(
                "partial store piece count mismatch: expected {expected_piece_count}, got {}",
                piece_hashes.len()
            ));
        }

        let store_dir = output_root.join(".novatorrent");
        let data_path = store_dir.join(format!("{key}.part"));
        let state_path = store_dir.join(format!("{key}.json"));
        if !create_missing && (!data_path.exists() || !state_path.exists()) {
            return Ok(None);
        }
        if create_missing {
            fs::create_dir_all(&store_dir)
                .map_err(|err| format!("could not create partial store directory: {err}"))?;
        }
        let expected_state = PartialPieceState {
            version: 1,
            total_length,
            piece_length,
            pieces: vec![false; piece_hashes.len()],
        };
        let mut state = fs::read(&state_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<PartialPieceState>(&bytes).ok())
            .filter(|state| {
                state.version == expected_state.version
                    && state.total_length == total_length
                    && state.piece_length == piece_length
                    && state.pieces.len() == piece_hashes.len()
            })
            .unwrap_or_else(|| expected_state.clone());

        let data_file = fs::OpenOptions::new()
            .create(create_missing)
            .read(true)
            .write(true)
            .open(&data_path)
            .map_err(|err| format!("could not open partial torrent data: {err}"))?;
        let existing_length = data_file
            .metadata()
            .map_err(|err| format!("could not inspect partial torrent data: {err}"))?
            .len();
        if existing_length != total_length {
            if !create_missing {
                return Err(format!(
                    "partial torrent data length mismatch: expected {total_length}, got {existing_length}"
                ));
            }
            data_file
                .set_len(total_length)
                .map_err(|err| format!("could not size partial torrent data: {err}"))?;
            state.pieces.fill(false);
        }

        let mut store = Self {
            data_path,
            state_path,
            data_file,
            state,
            piece_hashes: piece_hashes.to_vec(),
            dirty_pieces: 0,
            dirty_bytes: 0,
            last_checkpoint: Instant::now(),
        };
        store.recheck_marked_pieces()?;
        store.persist_state()?;
        Ok(Some(store))
    }

    pub fn verified_pieces(&self) -> &[bool] {
        &self.state.pieces
    }

    #[cfg(test)]
    fn data_path(&self) -> &Path {
        &self.data_path
    }

    pub fn write_piece(&mut self, index: u32, bytes: &[u8]) -> Result<(), String> {
        let (offset, expected_length) = self.piece_bounds(index)?;
        if bytes.len() as u64 != expected_length {
            return Err(format!(
                "partial piece {index} length mismatch: expected {expected_length}, got {}",
                bytes.len()
            ));
        }
        let expected_hash = self
            .piece_hashes
            .get(index as usize)
            .ok_or_else(|| format!("partial piece index is out of range: {index}"))?;
        if sha1::digest(bytes) != *expected_hash {
            return Err(format!("partial piece {index} failed SHA-1 verification"));
        }

        self.data_file
            .seek(SeekFrom::Start(offset))
            .map_err(|err| format!("could not seek partial torrent data: {err}"))?;
        self.data_file
            .write_all(bytes)
            .map_err(|err| format!("could not write partial torrent piece {index}: {err}"))?;
        self.state.pieces[index as usize] = true;
        self.dirty_pieces = self.dirty_pieces.saturating_add(1);
        self.dirty_bytes = self.dirty_bytes.saturating_add(bytes.len() as u64);
        if self.dirty_pieces >= PARTIAL_CHECKPOINT_PIECES
            || self.dirty_bytes >= PARTIAL_CHECKPOINT_BYTES
            || self.last_checkpoint.elapsed() >= PARTIAL_CHECKPOINT_INTERVAL
        {
            self.checkpoint()?;
        }
        Ok(())
    }

    pub fn checkpoint(&mut self) -> Result<(), String> {
        if self.dirty_pieces == 0 {
            return Ok(());
        }
        self.data_file
            .sync_data()
            .map_err(|err| format!("could not checkpoint partial torrent data: {err}"))?;
        self.persist_state()?;
        self.dirty_pieces = 0;
        self.dirty_bytes = 0;
        self.last_checkpoint = Instant::now();
        Ok(())
    }

    pub fn read_complete(&mut self) -> Result<Vec<u8>, String> {
        if !self.state.pieces.iter().all(|piece| *piece) {
            return Err("partial torrent is not complete".to_string());
        }
        self.checkpoint()?;
        let length = usize::try_from(self.state.total_length)
            .map_err(|_| "torrent is too large for the current assembly buffer".to_string())?;
        let mut bytes = vec![0u8; length];
        self.data_file
            .seek(SeekFrom::Start(0))
            .map_err(|err| format!("could not seek partial torrent data: {err}"))?;
        self.data_file
            .read_exact(&mut bytes)
            .map_err(|err| format!("could not read complete partial torrent: {err}"))?;
        Ok(bytes)
    }

    pub fn read_verified_range(&mut self, offset: u64, length: u64) -> Result<Vec<u8>, String> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| "partial torrent byte range overflow".to_string())?;
        if end > self.state.total_length {
            return Err("partial torrent byte range exceeds total length".to_string());
        }
        let start_piece = offset / self.state.piece_length;
        let end_piece = end.saturating_sub(1) / self.state.piece_length;
        for piece_index in start_piece..=end_piece {
            if !self
                .state
                .pieces
                .get(piece_index as usize)
                .copied()
                .unwrap_or(false)
            {
                return Err("partial torrent byte range is not verified yet".to_string());
            }
        }
        let read_len = usize::try_from(length)
            .map_err(|_| "partial torrent byte range is too large for this platform".to_string())?;
        let mut bytes = vec![0u8; read_len];
        self.data_file
            .seek(SeekFrom::Start(offset))
            .map_err(|err| format!("could not seek partial torrent data: {err}"))?;
        self.data_file
            .read_exact(&mut bytes)
            .map_err(|err| format!("could not read partial torrent data: {err}"))?;
        Ok(bytes)
    }

    pub fn read_verified_file_range(
        &mut self,
        files: &[TorrentFile],
        file_index: usize,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, String> {
        let (file_start, file_length) = file_torrent_offset(files, file_index)?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| "stream byte range overflow".to_string())?;
        if end > file_length {
            return Err("stream byte range exceeds file length".to_string());
        }
        let ranges = verified_file_ranges_from_pieces(
            files,
            file_index,
            self.state.piece_length,
            &self.state.pieces,
        )?;
        if !is_range_verified(&ranges, offset, length) {
            return Err("stream byte range is not verified yet".to_string());
        }

        let absolute_offset = file_start
            .checked_add(offset)
            .ok_or_else(|| "stream byte range overflow".to_string())?;
        let read_len = usize::try_from(length)
            .map_err(|_| "stream byte range is too large for this platform".to_string())?;
        let mut bytes = vec![0u8; read_len];
        self.data_file
            .seek(SeekFrom::Start(absolute_offset))
            .map_err(|err| format!("could not seek partial torrent stream data: {err}"))?;
        self.data_file
            .read_exact(&mut bytes)
            .map_err(|err| format!("could not read partial torrent stream data: {err}"))?;
        Ok(bytes)
    }

    pub fn write_complete_to_files(
        &mut self,
        output_root: &Path,
        torrent_name: &str,
        files: &[TorrentFile],
        overwrite: bool,
    ) -> Result<TorrentWriteSummary, String> {
        if !self.state.pieces.iter().all(|piece| *piece) {
            return Err("partial torrent is not complete".to_string());
        }
        let total_length = total_file_length(files)?;
        if total_length != self.state.total_length {
            return Err(format!(
                "partial torrent length mismatch: expected {}, got {total_length}",
                self.state.total_length
            ));
        }
        let wrap_in_torrent_folder = requires_torrent_name_folder(files);
        validate_path_component(torrent_name)?;
        fs::create_dir_all(output_root)
            .map_err(|err| format!("could not create torrent output directory: {err}"))?;

        let output_paths = files
            .iter()
            .map(|file| {
                if !file.included {
                    return Ok(None);
                }
                let path =
                    output_path_for_file(output_root, torrent_name, file, wrap_in_torrent_folder)?;
                if path.exists() && !overwrite {
                    return Err(format!(
                        "output file already exists and overwrite is disabled: {}",
                        path.to_string_lossy()
                    ));
                }
                Ok(Some(path))
            })
            .collect::<Result<Vec<_>, String>>()?;

        self.data_file
            .seek(SeekFrom::Start(0))
            .map_err(|err| format!("could not seek partial torrent data: {err}"))?;

        let mut summary = TorrentWriteSummary {
            paths: Vec::new(),
            bytes_written: 0,
            files_written: 0,
        };
        let mut buffer = [0u8; 64 * 1024];

        for (file, output_path) in files.iter().zip(output_paths) {
            match output_path {
                Some(path) => {
                    if let Some(parent) = path.parent() {
                        fs::create_dir_all(parent).map_err(|err| {
                            format!("could not create torrent file directory: {err}")
                        })?;
                    }
                    let mut out = File::create(&path)
                        .map_err(|err| format!("could not create torrent output file: {err}"))?;
                    copy_exact(&mut self.data_file, &mut out, file.length, &mut buffer)?;
                    summary.bytes_written += file.length;
                    summary.files_written += 1;
                    summary.paths.push(path);
                }
                None => {
                    skip_exact(&mut self.data_file, file.length, &mut buffer)?;
                }
            }
        }

        Ok(summary)
    }

    pub fn clear(self) -> Result<(), String> {
        let data_path = self.data_path.clone();
        let state_path = self.state_path.clone();
        let store_dir = data_path.parent().map(PathBuf::from);
        drop(self);
        for path in [&data_path, &state_path] {
            if path.exists() {
                fs::remove_file(path).map_err(|err| {
                    format!(
                        "could not remove partial store file {}: {err}",
                        path.to_string_lossy()
                    )
                })?;
            }
        }
        if let Some(store_dir) = store_dir {
            let is_empty = fs::read_dir(&store_dir)
                .map(|mut entries| entries.next().is_none())
                .unwrap_or(false);
            if is_empty {
                let _ = fs::remove_dir(store_dir);
            }
        }
        Ok(())
    }

    fn recheck_marked_pieces(&mut self) -> Result<(), String> {
        for index in 0..self.state.pieces.len() {
            if !self.state.pieces[index] {
                continue;
            }
            let (offset, length) = self.piece_bounds(index as u32)?;
            let mut bytes = vec![0u8; length as usize];
            self.data_file
                .seek(SeekFrom::Start(offset))
                .map_err(|err| format!("could not seek partial piece {index}: {err}"))?;
            self.data_file
                .read_exact(&mut bytes)
                .map_err(|err| format!("could not read partial piece {index}: {err}"))?;
            if sha1::digest(&bytes) != self.piece_hashes[index] {
                self.state.pieces[index] = false;
            }
        }
        Ok(())
    }

    fn persist_state(&self) -> Result<(), String> {
        let bytes = serde_json::to_vec_pretty(&self.state)
            .map_err(|err| format!("could not encode partial torrent state: {err}"))?;
        let mut state_file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.state_path)
            .map_err(|err| format!("could not open partial torrent state: {err}"))?;
        state_file
            .write_all(&bytes)
            .map_err(|err| format!("could not write partial torrent state: {err}"))?;
        state_file
            .sync_all()
            .map_err(|err| format!("could not flush partial torrent state: {err}"))
    }

    fn piece_bounds(&self, index: u32) -> Result<(u64, u64), String> {
        if index as usize >= self.state.pieces.len() {
            return Err(format!("partial piece index is out of range: {index}"));
        }
        let offset = (index as u64)
            .checked_mul(self.state.piece_length)
            .ok_or_else(|| "partial piece offset overflow".to_string())?;
        let length = self
            .state
            .total_length
            .saturating_sub(offset)
            .min(self.state.piece_length);
        Ok((offset, length))
    }
}

impl Drop for PartialPieceStore {
    fn drop(&mut self) {
        let _ = self.checkpoint();
    }
}

pub fn write_torrent_bytes(
    output_root: &Path,
    torrent_name: &str,
    files: &[TorrentFile],
    bytes: &[u8],
    overwrite: bool,
) -> Result<TorrentWriteSummary, String> {
    let total_length = total_file_length(files)?;
    if bytes.len() as u64 != total_length {
        return Err(format!(
            "downloaded length mismatch: expected {total_length}, got {}",
            bytes.len()
        ));
    }

    let wrap_in_torrent_folder = requires_torrent_name_folder(files);
    validate_path_component(torrent_name)?;
    fs::create_dir_all(output_root)
        .map_err(|err| format!("could not create torrent output directory: {err}"))?;

    let mut cursor = 0usize;
    let mut summary = TorrentWriteSummary {
        paths: Vec::new(),
        bytes_written: 0,
        files_written: 0,
    };

    for file in files {
        let file_len = usize::try_from(file.length)
            .map_err(|_| format!("file is too large for this platform: {}", file.name))?;
        let next = cursor
            .checked_add(file_len)
            .ok_or_else(|| "torrent byte cursor overflow".to_string())?;
        let file_bytes = bytes.get(cursor..next).ok_or_else(|| {
            "downloaded bytes ended before all torrent files were mapped".to_string()
        })?;
        cursor = next;

        if !file.included {
            continue;
        }

        let path = output_path_for_file(output_root, torrent_name, file, wrap_in_torrent_folder)?;
        if path.exists() && !overwrite {
            return Err(format!(
                "output file already exists and overwrite is disabled: {}",
                path.to_string_lossy()
            ));
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("could not create torrent file directory: {err}"))?;
        }
        let mut out = File::create(&path)
            .map_err(|err| format!("could not create torrent output file: {err}"))?;
        out.write_all(file_bytes)
            .map_err(|err| format!("could not write torrent output file: {err}"))?;

        summary.bytes_written += file.length;
        summary.files_written += 1;
        summary.paths.push(path);
    }

    Ok(summary)
}

pub fn read_torrent_bytes(
    output_root: &Path,
    torrent_name: &str,
    files: &[TorrentFile],
) -> Result<Vec<u8>, String> {
    let total_length = total_file_length(files)?;
    let capacity = usize::try_from(total_length)
        .map_err(|_| "torrent is too large for this platform".to_string())?;
    let wrap_in_torrent_folder = requires_torrent_name_folder(files);
    validate_path_component(torrent_name)?;

    let mut out = Vec::with_capacity(capacity);
    for file in files {
        let path = output_path_for_file(output_root, torrent_name, file, wrap_in_torrent_folder)?;
        let mut input = File::open(&path).map_err(|err| {
            format!(
                "could not open torrent file {}: {err}",
                path.to_string_lossy()
            )
        })?;
        let before = out.len();
        input.read_to_end(&mut out).map_err(|err| {
            format!(
                "could not read torrent file {}: {err}",
                path.to_string_lossy()
            )
        })?;
        let read_len = out.len() - before;
        if read_len as u64 != file.length {
            return Err(format!(
                "stored file length mismatch for {}: expected {}, got {read_len}",
                file.name, file.length
            ));
        }
    }
    Ok(out)
}

pub fn read_torrent_range(
    output_root: &Path,
    torrent_name: &str,
    files: &[TorrentFile],
    offset: u64,
    length: u64,
) -> Result<Vec<u8>, String> {
    let total_length = total_file_length(files)?;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| "torrent byte range overflow".to_string())?;
    if end > total_length {
        return Err("torrent byte range exceeds total length".to_string());
    }
    let read_len = usize::try_from(length)
        .map_err(|_| "torrent byte range is too large for this platform".to_string())?;
    let wrap_in_torrent_folder = requires_torrent_name_folder(files);
    validate_path_component(torrent_name)?;

    let mut out = Vec::with_capacity(read_len);
    let mut cursor = 0u64;
    for file in files {
        let file_start = cursor;
        let file_end = file_start
            .checked_add(file.length)
            .ok_or_else(|| "torrent file offset overflow".to_string())?;
        cursor = file_end;
        let overlap_start = offset.max(file_start);
        let overlap_end = end.min(file_end);
        if overlap_start >= overlap_end {
            continue;
        }
        let path = output_path_for_file(output_root, torrent_name, file, wrap_in_torrent_folder)?;
        let metadata = fs::metadata(&path).map_err(|err| {
            format!(
                "could not inspect torrent file {}: {err}",
                path.to_string_lossy()
            )
        })?;
        if metadata.len() != file.length {
            return Err(format!(
                "stored file length mismatch for {}: expected {}, got {}",
                file.name,
                file.length,
                metadata.len()
            ));
        }
        let mut input = File::open(&path).map_err(|err| {
            format!(
                "could not open torrent file {}: {err}",
                path.to_string_lossy()
            )
        })?;
        input
            .seek(SeekFrom::Start(overlap_start - file_start))
            .map_err(|err| {
                format!(
                    "could not seek torrent file {}: {err}",
                    path.to_string_lossy()
                )
            })?;
        let chunk_len = usize::try_from(overlap_end - overlap_start)
            .map_err(|_| "torrent file range is too large for this platform".to_string())?;
        let before = out.len();
        out.resize(before + chunk_len, 0);
        input.read_exact(&mut out[before..]).map_err(|err| {
            format!(
                "could not read torrent file {}: {err}",
                path.to_string_lossy()
            )
        })?;
    }
    if out.len() != read_len {
        return Err(format!(
            "torrent byte range read returned {} byte(s), expected {read_len}",
            out.len()
        ));
    }
    Ok(out)
}

pub fn verify_stored_torrent(
    output_root: &Path,
    torrent_name: &str,
    files: &[TorrentFile],
    piece_length: u64,
    piece_hashes: &[[u8; 20]],
) -> Result<StoredTorrentCheck, String> {
    if piece_length == 0 {
        return Err("piece length cannot be zero".to_string());
    }
    let bytes = read_torrent_bytes(output_root, torrent_name, files)?;
    let expected_pieces = (bytes.len() as u64).div_ceil(piece_length) as usize;
    if expected_pieces != piece_hashes.len() {
        return Err(format!(
            "stored torrent piece count mismatch: expected {expected_pieces}, got {}",
            piece_hashes.len()
        ));
    }

    let mut pieces = Vec::with_capacity(piece_hashes.len());
    let mut bytes_verified = 0u64;
    for (index, expected_hash) in piece_hashes.iter().enumerate() {
        let start = index as u64 * piece_length;
        let end = (start + piece_length).min(bytes.len() as u64);
        let piece = &bytes[start as usize..end as usize];
        let verified = sha1::digest(piece) == *expected_hash;
        if verified {
            bytes_verified += piece.len() as u64;
        }
        pieces.push(verified);
    }

    Ok(StoredTorrentCheck {
        complete: pieces.iter().all(|piece| *piece),
        pieces,
        bytes_verified,
    })
}

pub fn file_progress_from_pieces(
    files: &[TorrentFile],
    piece_length: u64,
    pieces: &[bool],
) -> Result<Vec<u64>, String> {
    if piece_length == 0 {
        return Err("piece length cannot be zero".to_string());
    }

    let mut file_ranges = Vec::with_capacity(files.len());
    let mut file_start = 0u64;
    for file in files {
        let file_end = file_start
            .checked_add(file.length)
            .ok_or_else(|| "file offset overflow".to_string())?;
        file_ranges.push((file_start, file_end));
        file_start = file_end;
    }

    let mut progress = vec![0u64; files.len()];
    let mut file_cursor = 0usize;
    for (piece_index, verified) in pieces.iter().enumerate() {
        if !verified {
            continue;
        }
        let piece_start = (piece_index as u64)
            .checked_mul(piece_length)
            .ok_or_else(|| "piece offset overflow".to_string())?;
        let piece_end = piece_start
            .checked_add(piece_length)
            .ok_or_else(|| "piece offset overflow".to_string())?;

        while file_cursor < file_ranges.len() && file_ranges[file_cursor].1 <= piece_start {
            file_cursor += 1;
        }
        let mut file_index = file_cursor;
        while file_index < file_ranges.len() {
            let (range_start, range_end) = file_ranges[file_index];
            if range_start >= piece_end {
                break;
            }
            let overlap_start = range_start.max(piece_start);
            let overlap_end = range_end.min(piece_end);
            if overlap_start < overlap_end {
                progress[file_index] = progress[file_index]
                    .saturating_add(overlap_end - overlap_start)
                    .min(files[file_index].length);
            }
            file_index += 1;
        }
    }
    Ok(progress)
}

pub fn verified_file_ranges_from_pieces(
    files: &[TorrentFile],
    file_index: usize,
    piece_length: u64,
    pieces: &[bool],
) -> Result<Vec<VerifiedByteRange>, String> {
    if piece_length == 0 {
        return Err("piece length cannot be zero".to_string());
    }
    let (file_start, file_length) = file_torrent_offset(files, file_index)?;
    let file_end = file_start
        .checked_add(file_length)
        .ok_or_else(|| "file offset overflow".to_string())?;
    let total_length = total_file_length(files)?;
    let mut ranges = Vec::<VerifiedByteRange>::new();

    for (piece_index, verified) in pieces.iter().enumerate() {
        if !verified {
            continue;
        }
        let piece_start = (piece_index as u64)
            .checked_mul(piece_length)
            .ok_or_else(|| "piece offset overflow".to_string())?;
        let piece_end = piece_start
            .checked_add(piece_length)
            .ok_or_else(|| "piece offset overflow".to_string())?
            .min(total_length);
        let overlap_start = file_start.max(piece_start);
        let overlap_end = file_end.min(piece_end);
        if overlap_start >= overlap_end {
            continue;
        }
        let offset = overlap_start - file_start;
        let length = overlap_end - overlap_start;
        if let Some(last) = ranges.last_mut() {
            if last.offset + last.length == offset {
                last.length += length;
                continue;
            }
        }
        ranges.push(VerifiedByteRange { offset, length });
    }

    Ok(ranges)
}

/// Reads a file range using a live, already SHA-1-verified piece map.
///
/// The persisted partial-store map is checkpointed in batches and can lag active
/// downloads. Streaming callers use this path after the session has observed the
/// completed write, avoiding both stale availability and an expensive full recheck.
pub fn read_verified_partial_file_range(
    output_root: &Path,
    key: &str,
    total_length: u64,
    files: &[TorrentFile],
    file_index: usize,
    piece_length: u64,
    verified: &[bool],
    offset: u64,
    length: u64,
) -> Result<Vec<u8>, String> {
    validate_path_component(key)?;
    if piece_length == 0 {
        return Err("piece length cannot be zero".to_string());
    }
    let expected_piece_count = total_length.div_ceil(piece_length) as usize;
    if verified.len() != expected_piece_count {
        return Err(format!(
            "live verified piece count mismatch: expected {expected_piece_count}, got {}",
            verified.len()
        ));
    }
    let (file_start, file_length) = file_torrent_offset(files, file_index)?;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| "stream byte range overflow".to_string())?;
    if end > file_length {
        return Err("stream byte range exceeds file length".to_string());
    }
    let ranges = verified_file_ranges_from_pieces(files, file_index, piece_length, verified)?;
    if !is_range_verified(&ranges, offset, length) {
        return Err("stream byte range is not verified yet".to_string());
    }

    let data_path = output_root.join(".novatorrent").join(format!("{key}.part"));
    let metadata = fs::symlink_metadata(&data_path)
        .map_err(|err| format!("could not inspect partial torrent data: {err}"))?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err("partial torrent streaming requires a regular, non-symlink file".to_string());
    }
    if metadata.len() != total_length {
        return Err(format!(
            "partial torrent data length mismatch: expected {total_length}, got {}",
            metadata.len()
        ));
    }

    let absolute_offset = file_start
        .checked_add(offset)
        .ok_or_else(|| "stream byte range overflow".to_string())?;
    let read_len = usize::try_from(length)
        .map_err(|_| "stream byte range is too large for this platform".to_string())?;
    let mut bytes = vec![0u8; read_len];
    let mut input = fs::File::open(&data_path)
        .map_err(|err| format!("could not open partial torrent data: {err}"))?;
    input
        .seek(SeekFrom::Start(absolute_offset))
        .map_err(|err| format!("could not seek partial torrent stream data: {err}"))?;
    input
        .read_exact(&mut bytes)
        .map_err(|err| format!("could not read partial torrent stream data: {err}"))?;
    Ok(bytes)
}

pub fn file_torrent_offset(files: &[TorrentFile], file_index: usize) -> Result<(u64, u64), String> {
    let file = files
        .get(file_index)
        .ok_or_else(|| format!("torrent file index is out of range: {file_index}"))?;
    let offset = files
        .iter()
        .take(file_index)
        .try_fold(0u64, |total, file| {
            total
                .checked_add(file.length)
                .ok_or_else(|| "file offset overflow".to_string())
        })?;
    Ok((offset, file.length))
}

pub fn is_range_verified(ranges: &[VerifiedByteRange], offset: u64, length: u64) -> bool {
    if length == 0 {
        return true;
    }
    let Some(end) = offset.checked_add(length) else {
        return false;
    };
    ranges.iter().any(|range| {
        let range_end = range.offset.saturating_add(range.length);
        range.offset <= offset && end <= range_end
    })
}

pub fn output_path_for_file(
    output_root: &Path,
    torrent_name: &str,
    file: &TorrentFile,
    wrap_in_torrent_folder: bool,
) -> Result<PathBuf, String> {
    let mut path = PathBuf::from(output_root);
    if wrap_in_torrent_folder {
        validate_path_component(torrent_name)?;
        path.push(torrent_name);
    }
    for component in &file.components {
        validate_path_component(component)?;
        path.push(component);
    }
    Ok(path)
}

pub fn torrent_files_exist(
    output_root: &Path,
    torrent_name: &str,
    files: &[TorrentFile],
) -> Result<bool, String> {
    let wrap_in_torrent_folder = requires_torrent_name_folder(files);
    for file in files {
        let path = output_path_for_file(output_root, torrent_name, file, wrap_in_torrent_folder)?;
        if !path.is_file() {
            return Ok(false);
        }
    }
    Ok(!files.is_empty())
}

pub fn existing_output_paths(
    output_root: &Path,
    torrent_name: &str,
    files: &[TorrentFile],
) -> Result<Vec<PathBuf>, String> {
    let wrap_in_torrent_folder = requires_torrent_name_folder(files);
    files
        .iter()
        .filter(|file| file.included)
        .map(|file| output_path_for_file(output_root, torrent_name, file, wrap_in_torrent_folder))
        .filter_map(|path| match path {
            Ok(path) if path.exists() => Some(Ok(path)),
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        })
        .collect()
}

pub fn total_file_length(files: &[TorrentFile]) -> Result<u64, String> {
    files.iter().try_fold(0u64, |total, file| {
        total
            .checked_add(file.length)
            .ok_or_else(|| "torrent total length overflow".to_string())
    })
}

pub fn requires_torrent_name_folder(files: &[TorrentFile]) -> bool {
    let is_multi_file =
        files.len() > 1 || files.first().is_some_and(|file| file.components.len() > 1);
    if !is_multi_file {
        return false;
    }

    let Some(first_folder) = files
        .first()
        .and_then(|file| file.components.first())
        .filter(|_| files[0].components.len() > 1)
    else {
        return true;
    };

    !files
        .iter()
        .all(|file| file.components.len() > 1 && file.components.first() == Some(first_folder))
}

fn copy_exact(
    input: &mut File,
    output: &mut File,
    mut bytes: u64,
    buffer: &mut [u8],
) -> Result<(), String> {
    while bytes > 0 {
        let chunk = bytes.min(buffer.len() as u64) as usize;
        input
            .read_exact(&mut buffer[..chunk])
            .map_err(|err| format!("could not read partial torrent data: {err}"))?;
        output
            .write_all(&buffer[..chunk])
            .map_err(|err| format!("could not write torrent output file: {err}"))?;
        bytes -= chunk as u64;
    }
    Ok(())
}

fn skip_exact(input: &mut File, mut bytes: u64, buffer: &mut [u8]) -> Result<(), String> {
    while bytes > 0 {
        let chunk = bytes.min(buffer.len() as u64) as usize;
        input
            .read_exact(&mut buffer[..chunk])
            .map_err(|err| format!("could not skip unchecked partial torrent data: {err}"))?;
        bytes -= chunk as u64;
    }
    Ok(())
}

fn validate_path_component(component: &str) -> Result<(), String> {
    if component.is_empty() || component == "." || component == ".." {
        return Err("torrent path component is not safe".to_string());
    }
    if component
        .chars()
        .any(|ch| matches!(ch, '/' | '\\' | ':' | '\0'))
    {
        return Err("torrent path component contains an unsafe character".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn file(name: &str, components: &[&str], length: u64, included: bool) -> TorrentFile {
        TorrentFile {
            name: name.to_string(),
            components: components
                .iter()
                .map(|component| component.to_string())
                .collect(),
            length,
            included,
        }
    }

    #[test]
    fn writes_single_file_torrent() {
        let root = temp_dir("single");
        let files = vec![file("payload.bin", &["payload.bin"], 5, true)];

        let summary = write_torrent_bytes(&root, "payload.bin", &files, b"hello", false)
            .expect("single file writes");

        assert_eq!(summary.bytes_written, 5);
        assert_eq!(summary.files_written, 1);
        assert_eq!(
            fs::read(root.join("payload.bin")).expect("file reads"),
            b"hello"
        );
        remove_temp_dir(root);
    }

    #[test]
    fn writes_and_reads_multi_file_torrent_hierarchy() {
        let root = temp_dir("multi");
        let files = vec![
            file("dir/one.bin", &["dir", "one.bin"], 3, true),
            file("two.bin", &["two.bin"], 4, true),
        ];

        let summary = write_torrent_bytes(&root, "Example", &files, b"abcdefg", false)
            .expect("multi file writes");

        assert_eq!(summary.bytes_written, 7);
        assert_eq!(summary.files_written, 2);
        assert_eq!(
            fs::read(root.join("Example").join("dir").join("one.bin")).expect("first file reads"),
            b"abc"
        );
        assert_eq!(
            fs::read(root.join("Example").join("two.bin")).expect("second file reads"),
            b"defg"
        );
        assert_eq!(
            read_torrent_bytes(&root, "Example", &files).expect("torrent reads"),
            b"abcdefg"
        );
        assert_eq!(
            read_torrent_range(&root, "Example", &files, 2, 4).expect("cross-file range reads"),
            b"cdef"
        );
        assert!(read_torrent_range(&root, "Example", &files, 6, 2).is_err());
        remove_temp_dir(root);
    }

    #[test]
    fn preserves_an_existing_common_top_level_folder_without_duplicate_wrapper() {
        let root = temp_dir("existing-root");
        let files = vec![
            file("Big Buck/video.mp4", &["Big Buck", "video.mp4"], 3, true),
            file(
                "Big Buck/subtitles.srt",
                &["Big Buck", "subtitles.srt"],
                4,
                true,
            ),
        ];

        let summary = write_torrent_bytes(&root, "Big Buck", &files, b"abcdefg", false)
            .expect("existing root folder writes");

        assert_eq!(summary.paths[0], root.join("Big Buck").join("video.mp4"));
        assert_eq!(
            summary.paths[1],
            root.join("Big Buck").join("subtitles.srt")
        );
        assert!(!root.join("Big Buck").join("Big Buck").exists());
        assert_eq!(
            read_torrent_bytes(&root, "Big Buck", &files).expect("torrent reads"),
            b"abcdefg"
        );
        remove_temp_dir(root);
    }

    #[test]
    fn wraps_loose_multi_file_payload_in_the_torrent_name() {
        let root = temp_dir("loose-root");
        let files = vec![
            file("video.mp4", &["video.mp4"], 3, true),
            file("subtitles.srt", &["subtitles.srt"], 4, true),
        ];

        write_torrent_bytes(&root, "Big Buck", &files, b"abcdefg", false)
            .expect("loose files write");

        assert_eq!(
            fs::read(root.join("Big Buck").join("video.mp4")).unwrap(),
            b"abc"
        );
        assert_eq!(
            fs::read(root.join("Big Buck").join("subtitles.srt")).unwrap(),
            b"defg"
        );
        remove_temp_dir(root);
    }

    #[test]
    fn reports_only_selected_existing_output_conflicts() {
        let root = temp_dir("existing-conflicts");
        let folder = root.join("Example");
        fs::create_dir_all(&folder).expect("torrent folder creates");
        fs::write(folder.join("one.bin"), b"old").expect("existing file writes");
        let files = vec![
            file("one.bin", &["one.bin"], 3, true),
            file("two.bin", &["two.bin"], 4, false),
        ];

        assert_eq!(
            existing_output_paths(&root, "Example", &files).expect("conflicts resolve"),
            vec![folder.join("one.bin")]
        );
        remove_temp_dir(root);
    }

    #[test]
    fn skips_unwanted_files_but_advances_offsets() {
        let root = temp_dir("skip");
        let files = vec![
            file("one.bin", &["one.bin"], 3, true),
            file("two.bin", &["two.bin"], 4, false),
            file("three.bin", &["three.bin"], 2, true),
        ];

        let summary = write_torrent_bytes(&root, "Example", &files, b"abcdefghi", false)
            .expect("partial file writes");

        assert_eq!(summary.bytes_written, 5);
        assert_eq!(summary.files_written, 2);
        assert_eq!(
            fs::read(root.join("Example").join("one.bin")).expect("one reads"),
            b"abc"
        );
        assert!(!root.join("Example").join("two.bin").exists());
        assert_eq!(
            fs::read(root.join("Example").join("three.bin")).expect("three reads"),
            b"hi"
        );
        remove_temp_dir(root);
    }

    #[test]
    fn protects_existing_files_without_overwrite() {
        let root = temp_dir("overwrite");
        let files = vec![file("payload.bin", &["payload.bin"], 5, true)];
        fs::write(root.join("payload.bin"), b"prior").expect("seed existing file");

        let err = write_torrent_bytes(&root, "payload.bin", &files, b"hello", false)
            .expect_err("overwrite is rejected");
        assert!(err.contains("overwrite is disabled"));

        write_torrent_bytes(&root, "payload.bin", &files, b"hello", true)
            .expect("overwrite allowed");
        assert_eq!(
            fs::read(root.join("payload.bin")).expect("file reads"),
            b"hello"
        );
        remove_temp_dir(root);
    }

    #[test]
    fn detects_complete_torrent_file_layout() {
        let root = temp_dir("exists");
        let files = vec![
            file("one.bin", &["one.bin"], 3, true),
            file("two.bin", &["two.bin"], 4, true),
        ];

        assert!(!torrent_files_exist(&root, "Example", &files).expect("missing layout checks"));
        write_torrent_bytes(&root, "Example", &files, b"abcdefg", false).expect("layout writes");
        assert!(torrent_files_exist(&root, "Example", &files).expect("complete layout checks"));
        remove_temp_dir(root);
    }

    #[test]
    fn partial_piece_store_persists_and_rechecks_verified_pieces() {
        let root = temp_dir("partial-store");
        let data = b"abcdefghi";
        let hashes = data.chunks(4).map(sha1::digest).collect::<Vec<_>>();
        let key = "00112233445566778899aabbccddeeff00112233";
        let data_path;

        {
            let mut store = PartialPieceStore::open(&root, key, data.len() as u64, 4, &hashes)
                .expect("partial store opens");
            assert_eq!(store.verified_pieces(), &[false, false, false]);
            assert!(store.write_piece(0, b"bad!").is_err());
            store.write_piece(0, b"abcd").expect("first piece persists");
            store.write_piece(2, b"i").expect("last piece persists");
            assert_eq!(store.verified_pieces(), &[true, false, true]);
            data_path = store.data_path().to_path_buf();
        }

        {
            let mut store = PartialPieceStore::open(&root, key, data.len() as u64, 4, &hashes)
                .expect("partial store reopens");
            assert_eq!(store.verified_pieces(), &[true, false, true]);
            store
                .write_piece(1, b"efgh")
                .expect("middle piece persists");
            assert_eq!(store.read_complete().expect("complete data reads"), data);
        }

        {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .open(&data_path)
                .expect("partial data opens for corruption");
            file.seek(SeekFrom::Start(0)).expect("partial data seeks");
            file.write_all(b"x").expect("partial data corrupts");
        }

        let mut store = PartialPieceStore::open(&root, key, data.len() as u64, 4, &hashes)
            .expect("corrupt partial store reopens");
        assert_eq!(store.verified_pieces(), &[false, true, true]);
        store
            .write_piece(0, b"abcd")
            .expect("corrupt piece repairs");
        assert_eq!(store.read_complete().expect("repaired data reads"), data);
        store.clear().expect("partial store clears");
        assert!(!data_path.exists());
        assert!(!root
            .join(".novatorrent")
            .join(format!("{key}.json"))
            .exists());
        remove_temp_dir(root);
    }

    #[test]
    fn partial_piece_store_batches_state_until_checkpoint() {
        let root = temp_dir("partial-checkpoint-batch");
        let data = b"abcdefghijkl";
        let hashes = data.chunks(4).map(sha1::digest).collect::<Vec<_>>();
        let mut store = PartialPieceStore::open(&root, "batch", data.len() as u64, 4, &hashes)
            .expect("partial store opens");

        store
            .write_piece(0, &data[..4])
            .expect("first piece writes");
        store
            .write_piece(1, &data[4..8])
            .expect("second piece writes");
        assert_eq!(store.dirty_pieces, 2);
        let before = serde_json::from_slice::<PartialPieceState>(
            &fs::read(&store.state_path).expect("checkpoint state reads"),
        )
        .expect("checkpoint state parses");
        assert_eq!(before.pieces, vec![false, false, false]);

        store.checkpoint().expect("partial store checkpoints");
        assert_eq!(store.dirty_pieces, 0);
        let after = serde_json::from_slice::<PartialPieceState>(
            &fs::read(&store.state_path).expect("updated checkpoint state reads"),
        )
        .expect("updated checkpoint state parses");
        assert_eq!(after.pieces, vec![true, true, false]);

        drop(store);
        fs::remove_dir_all(root).expect("temp directory removes");
    }

    #[test]
    fn live_verified_map_streams_before_checkpoint() {
        let root = temp_dir("live-stream-before-checkpoint");
        let data = b"abcdefghijkl";
        let hashes = data.chunks(4).map(sha1::digest).collect::<Vec<_>>();
        let files = vec![file("video.bin", &["video.bin"], data.len() as u64, true)];
        let mut store = PartialPieceStore::open(&root, "live", data.len() as u64, 4, &hashes)
            .expect("partial store opens");

        store
            .write_piece(0, &data[..4])
            .expect("first piece writes");
        let verified = store.verified_pieces().to_vec();
        let bytes = read_verified_partial_file_range(
            &root,
            "live",
            data.len() as u64,
            &files,
            0,
            4,
            &verified,
            0,
            4,
        )
        .expect("live verified range reads before checkpoint");
        assert_eq!(bytes, b"abcd");

        let checkpoint = serde_json::from_slice::<PartialPieceState>(
            &fs::read(&store.state_path).expect("checkpoint state reads"),
        )
        .expect("checkpoint state parses");
        assert_eq!(checkpoint.pieces, vec![false, false, false]);
        drop(store);
        remove_temp_dir(root);
    }

    #[test]
    fn partial_piece_store_streams_complete_output_files() {
        let root = temp_dir("partial-stream");
        let output = root.join("out");
        let data = b"abcdefghi";
        let hashes = data.chunks(4).map(sha1::digest).collect::<Vec<_>>();
        let files = vec![
            file("one.bin", &["dir", "one.bin"], 3, true),
            file("two.bin", &["two.bin"], 4, false),
            file("three.bin", &["three.bin"], 2, true),
        ];
        let key = "10112233445566778899aabbccddeeff00112233";
        let mut store = PartialPieceStore::open(&root, key, data.len() as u64, 4, &hashes)
            .expect("store opens");
        store.write_piece(0, b"abcd").expect("piece 0 writes");
        store.write_piece(1, b"efgh").expect("piece 1 writes");
        store.write_piece(2, b"i").expect("piece 2 writes");

        let summary = store
            .write_complete_to_files(&output, "Example", &files, false)
            .expect("complete output streams");

        assert_eq!(summary.bytes_written, 5);
        assert_eq!(summary.files_written, 2);
        assert_eq!(
            fs::read(output.join("Example").join("dir").join("one.bin")).expect("first file reads"),
            b"abc"
        );
        assert!(!output.join("Example").join("two.bin").exists());
        assert_eq!(
            fs::read(output.join("Example").join("three.bin")).expect("third file reads"),
            b"hi"
        );
        store.clear().expect("partial store clears");
        remove_temp_dir(root);
    }

    #[test]
    fn partial_piece_store_reads_only_verified_file_ranges() {
        let root = temp_dir("partial-range-stream");
        let data = b"abcdefghi";
        let hashes = data.chunks(4).map(sha1::digest).collect::<Vec<_>>();
        let files = vec![
            file("one.bin", &["one.bin"], 3, true),
            file("two.bin", &["two.bin"], 4, true),
            file("three.bin", &["three.bin"], 2, true),
        ];
        let key = "20112233445566778899aabbccddeeff00112233";
        let mut store = PartialPieceStore::open(&root, key, data.len() as u64, 4, &hashes)
            .expect("store opens");
        store.write_piece(0, b"abcd").expect("piece 0 writes");

        assert_eq!(
            store
                .read_verified_file_range(&files, 1, 0, 1)
                .expect("verified file byte reads"),
            b"d"
        );
        assert!(store.read_verified_file_range(&files, 1, 1, 1).is_err());

        store.write_piece(1, b"efgh").expect("piece 1 writes");
        assert_eq!(
            store
                .read_verified_file_range(&files, 1, 0, 4)
                .expect("verified file range reads"),
            b"defg"
        );
        remove_temp_dir(root);
    }

    #[test]
    fn verifies_stored_torrent_pieces() {
        let root = temp_dir("verify");
        let files = vec![
            file("one.bin", &["one.bin"], 3, true),
            file("two.bin", &["two.bin"], 4, true),
        ];
        let bytes = b"abcdefg";
        let hashes = [sha1::digest(b"abcd"), sha1::digest(b"efg")];
        write_torrent_bytes(&root, "Example", &files, bytes, false).expect("files write");

        let check = verify_stored_torrent(&root, "Example", &files, 4, &hashes)
            .expect("stored data verifies");

        assert_eq!(
            check,
            StoredTorrentCheck {
                pieces: vec![true, true],
                bytes_verified: 7,
                complete: true,
            }
        );
        remove_temp_dir(root);
    }

    #[test]
    fn reports_corrupt_stored_piece() {
        let root = temp_dir("corrupt");
        let files = vec![
            file("one.bin", &["one.bin"], 3, true),
            file("two.bin", &["two.bin"], 4, true),
        ];
        let hashes = [sha1::digest(b"abcd"), sha1::digest(b"efg")];
        write_torrent_bytes(&root, "Example", &files, b"abcxefg", false).expect("files write");

        let check = verify_stored_torrent(&root, "Example", &files, 4, &hashes)
            .expect("stored data checks");

        assert_eq!(check.pieces, vec![false, true]);
        assert_eq!(check.bytes_verified, 3);
        assert!(!check.complete);
        remove_temp_dir(root);
    }

    #[test]
    fn maps_verified_pieces_to_file_progress() {
        let files = vec![
            file("one.bin", &["one.bin"], 3, true),
            file("two.bin", &["two.bin"], 4, true),
            file("three.bin", &["three.bin"], 3, true),
        ];

        let progress =
            file_progress_from_pieces(&files, 4, &[true, false, true]).expect("progress maps");

        assert_eq!(progress, vec![3, 1, 2]);
    }

    #[test]
    fn maps_verified_pieces_across_empty_files_in_one_pass() {
        let files = vec![
            file("empty-a.bin", &["empty-a.bin"], 0, true),
            file("one.bin", &["one.bin"], 2, true),
            file("empty-b.bin", &["empty-b.bin"], 0, true),
            file("two.bin", &["two.bin"], 3, true),
        ];

        let progress = file_progress_from_pieces(&files, 4, &[true, false])
            .expect("progress maps around empty files");

        assert_eq!(progress, vec![0, 2, 0, 2]);
    }

    #[test]
    fn rejects_unsafe_components() {
        let root = temp_dir("unsafe");
        let files = vec![file("bad", &["..", "bad"], 1, true)];

        assert!(write_torrent_bytes(&root, "Example", &files, b"x", false).is_err());
        remove_temp_dir(root);
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "novatorrent-storage-{label}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("temp dir creates");
        path
    }

    fn remove_temp_dir(path: PathBuf) {
        fs::remove_dir_all(path).expect("temp dir removes");
    }
}
