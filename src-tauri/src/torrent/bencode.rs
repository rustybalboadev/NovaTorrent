use std::ops::Range;

#[derive(Debug, Clone)]
pub struct BencodeNode {
    pub value: BencodeValue,
    pub span: Range<usize>,
}

#[derive(Debug, Clone)]
pub enum BencodeValue {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<BencodeNode>),
    Dict(Vec<(Vec<u8>, BencodeNode)>),
}

impl BencodeNode {
    pub fn dict_get(&self, key: &[u8]) -> Option<&BencodeNode> {
        let BencodeValue::Dict(entries) = &self.value else {
            return None;
        };
        entries
            .iter()
            .find_map(|(entry_key, value)| (entry_key.as_slice() == key).then_some(value))
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match &self.value {
            BencodeValue::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub fn as_str_lossy(&self) -> Option<String> {
        self.as_bytes()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self.value {
            BencodeValue::Int(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[BencodeNode]> {
        match &self.value {
            BencodeValue::List(items) => Some(items),
            _ => None,
        }
    }
}

pub fn parse(input: &[u8]) -> Result<BencodeNode, String> {
    let mut parser = Parser { input, cursor: 0 };
    let node = parser.parse_node()?;
    if parser.cursor != input.len() {
        return Err("trailing data after bencode value".to_string());
    }
    Ok(node)
}

pub fn parse_prefix(input: &[u8]) -> Result<BencodeNode, String> {
    let mut parser = Parser { input, cursor: 0 };
    parser.parse_node()
}

struct Parser<'a> {
    input: &'a [u8],
    cursor: usize,
}

impl<'a> Parser<'a> {
    fn parse_node(&mut self) -> Result<BencodeNode, String> {
        let start = self.cursor;
        let Some(byte) = self.peek() else {
            return Err("unexpected end of input".to_string());
        };

        let value = match byte {
            b'i' => self.parse_int()?,
            b'l' => self.parse_list()?,
            b'd' => self.parse_dict()?,
            b'0'..=b'9' => self.parse_bytes()?,
            _ => return Err(format!("unexpected bencode byte: {byte}")),
        };

        Ok(BencodeNode {
            value,
            span: start..self.cursor,
        })
    }

    fn parse_int(&mut self) -> Result<BencodeValue, String> {
        self.expect(b'i')?;
        let start = self.cursor;
        while self.peek() != Some(b'e') {
            self.advance()?;
        }
        let raw = std::str::from_utf8(&self.input[start..self.cursor])
            .map_err(|_| "integer is not valid ascii".to_string())?;
        if raw.starts_with("-0") || (raw.len() > 1 && raw.starts_with('0')) {
            return Err("invalid bencode integer leading zero".to_string());
        }
        let value = raw
            .parse::<i64>()
            .map_err(|err| format!("invalid bencode integer: {err}"))?;
        self.expect(b'e')?;
        Ok(BencodeValue::Int(value))
    }

    fn parse_list(&mut self) -> Result<BencodeValue, String> {
        self.expect(b'l')?;
        let mut items = Vec::new();
        while self.peek() != Some(b'e') {
            items.push(self.parse_node()?);
        }
        self.expect(b'e')?;
        Ok(BencodeValue::List(items))
    }

    fn parse_dict(&mut self) -> Result<BencodeValue, String> {
        self.expect(b'd')?;
        let mut entries = Vec::new();
        let mut previous_key: Option<Vec<u8>> = None;
        while self.peek() != Some(b'e') {
            let key_node = self.parse_node()?;
            let BencodeValue::Bytes(key) = key_node.value else {
                return Err("dictionary key is not a byte string".to_string());
            };
            if previous_key
                .as_ref()
                .is_some_and(|previous| previous.as_slice() > key.as_slice())
            {
                return Err("dictionary keys are not sorted".to_string());
            }
            previous_key = Some(key.clone());
            let value = self.parse_node()?;
            entries.push((key, value));
        }
        self.expect(b'e')?;
        Ok(BencodeValue::Dict(entries))
    }

    fn parse_bytes(&mut self) -> Result<BencodeValue, String> {
        let length_start = self.cursor;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.advance()?;
        }
        if self.cursor == length_start {
            return Err("missing byte string length".to_string());
        }
        if self.input[length_start] == b'0' && self.cursor - length_start > 1 {
            return Err("invalid byte string leading zero".to_string());
        }
        let length = std::str::from_utf8(&self.input[length_start..self.cursor])
            .map_err(|_| "invalid byte string length".to_string())?
            .parse::<usize>()
            .map_err(|err| format!("invalid byte string length: {err}"))?;
        self.expect(b':')?;
        let end = self
            .cursor
            .checked_add(length)
            .ok_or_else(|| "byte string length overflow".to_string())?;
        if end > self.input.len() {
            return Err("byte string extends past input".to_string());
        }
        let bytes = self.input[self.cursor..end].to_vec();
        self.cursor = end;
        Ok(BencodeValue::Bytes(bytes))
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.cursor).copied()
    }

    fn advance(&mut self) -> Result<u8, String> {
        let byte = self
            .peek()
            .ok_or_else(|| "unexpected end of input".to_string())?;
        self.cursor += 1;
        Ok(byte)
    }

    fn expect(&mut self, expected: u8) -> Result<(), String> {
        let actual = self.advance()?;
        if actual != expected {
            return Err(format!("expected byte {expected}, got {actual}"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_dictionary_and_spans() {
        let input = b"d3:cow3:moo4:spam4:eggse";
        let node = parse(input).expect("bencode parses");
        assert_eq!(node.span, 0..input.len());
        assert_eq!(node.dict_get(b"cow").and_then(BencodeNode::as_str_lossy), Some("moo".to_string()));
        assert_eq!(node.dict_get(b"spam").and_then(BencodeNode::as_str_lossy), Some("eggs".to_string()));
    }

    #[test]
    fn rejects_unsorted_dictionary_keys() {
        assert!(parse(b"d4:spam4:eggs3:cow3:mooe").is_err());
    }

    #[test]
    fn parses_prefix_with_trailing_bytes() {
        let input = b"d1:ai1eeextra";
        let node = parse_prefix(input).expect("prefix parses");

        assert_eq!(node.span, 0..8);
        assert_eq!(node.dict_get(b"a").and_then(BencodeNode::as_i64), Some(1));
    }
}
