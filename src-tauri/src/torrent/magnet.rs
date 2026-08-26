use crate::torrent::sha1;

#[derive(Debug, Clone)]
pub struct MagnetLink {
    pub display_name: Option<String>,
    pub info_hash: [u8; 20],
    pub trackers: Vec<String>,
    pub web_seeds: Vec<String>,
}

impl MagnetLink {
    pub fn parse(input: &str) -> Result<Self, String> {
        if !input.starts_with("magnet:?") {
            return Err("magnet link must start with magnet:?".to_string());
        }

        let mut display_name = None;
        let mut info_hash = None;
        let mut trackers = Vec::new();
        let mut web_seeds = Vec::new();

        for pair in input["magnet:?".len()..].split('&') {
            let Some((key, value)) = pair.split_once('=') else {
                continue;
            };
            let decoded = percent_decode(value)?;
            match key {
                "dn" => display_name = Some(decoded),
                "tr" => trackers.push(decoded),
                "ws" => web_seeds.push(decoded),
                "xt" => {
                    if let Some(hash) = decoded.strip_prefix("urn:btih:") {
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
        })
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
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=Example&tr=http%3A%2F%2Ftracker.test%2Fannounce&ws=https%3A%2F%2Fmirror.test%2Ffile.bin",
        )
        .expect("magnet parses");
        assert_eq!(magnet.display_name.as_deref(), Some("Example"));
        assert_eq!(magnet.trackers, vec!["http://tracker.test/announce"]);
        assert_eq!(magnet.web_seeds, vec!["https://mirror.test/file.bin"]);
        assert_eq!(sha1::hex(&magnet.info_hash), "0123456789abcdef0123456789abcdef01234567");
    }
}
