use std::{
    collections::HashSet,
    net::{Ipv6Addr, ToSocketAddrs, UdpSocket},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::torrent::{
    bencode::{self, BencodeNode},
    peer::PeerInfo,
    sha1,
    tracker,
};

pub const DHT_BUCKET_COUNT: usize = 160;
pub const DHT_BUCKET_SIZE: usize = 8;
pub const DHT_GOOD_WINDOW_MS: u128 = 15 * 60 * 1_000;
pub const DHT_MAX_CONSECUTIVE_FAILURES: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DhtNode {
    pub id: [u8; 20],
    pub address: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DhtContact {
    pub address: String,
    pub port: u16,
}

#[derive(Debug, Clone)]
pub struct DhtResponse {
    pub transaction_id: Vec<u8>,
    pub node_id: Option<[u8; 20]>,
    pub token: Option<Vec<u8>>,
    pub nodes: Vec<DhtNode>,
    pub nodes6: Vec<DhtNode>,
    pub peers: Vec<PeerInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DhtError {
    pub transaction_id: Vec<u8>,
    pub code: i64,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DhtQuery {
    pub transaction_id: Vec<u8>,
    pub node_id: [u8; 20],
    pub kind: DhtQueryKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DhtQueryKind {
    Ping,
    FindNode {
        target: [u8; 20],
    },
    GetPeers {
        info_hash: [u8; 20],
    },
    AnnouncePeer {
        info_hash: [u8; 20],
        port: u16,
        token: Vec<u8>,
        implied_port: bool,
    },
}

#[derive(Debug, Clone)]
pub struct DhtLookupOptions {
    pub timeout: Duration,
    pub max_queries: usize,
}

#[derive(Debug, Clone)]
pub struct DhtLookupResult {
    pub peers: Vec<PeerInfo>,
    pub announce_targets: Vec<DhtAnnounceTarget>,
    pub queried_nodes: usize,
    pub discovered_nodes: usize,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DhtAnnounceTarget {
    pub contact: DhtContact,
    pub token: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct DhtRoutingTable {
    own_id: [u8; 20],
    bucket_size: usize,
    buckets: Vec<DhtRoutingBucket>,
}

#[derive(Debug, Clone)]
struct DhtRoutingBucket {
    entries: Vec<DhtRoutingEntry>,
    pending: Option<DhtRoutingEntry>,
    last_changed_ms: u128,
    last_refresh_ms: u128,
}

#[derive(Debug, Clone)]
struct DhtRoutingEntry {
    node: DhtNode,
    last_response_ms: Option<u128>,
    last_query_ms: Option<u128>,
    consecutive_failures: u8,
    discovered_ms: u128,
    persistable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DhtRefreshJob {
    pub target: [u8; 20],
    pub contact: DhtContact,
}

impl DhtRoutingTable {
    pub fn new(own_id: [u8; 20]) -> Self {
        Self::with_bucket_size_at(own_id, DHT_BUCKET_SIZE, dht_timestamp_ms())
    }

    pub fn with_bucket_size(own_id: [u8; 20], bucket_size: usize) -> Self {
        Self::with_bucket_size_at(own_id, bucket_size, dht_timestamp_ms())
    }

    fn with_bucket_size_at(own_id: [u8; 20], bucket_size: usize, now_ms: u128) -> Self {
        Self {
            own_id,
            bucket_size: bucket_size.max(1),
            buckets: vec![
                DhtRoutingBucket {
                    entries: Vec::new(),
                    pending: None,
                    last_changed_ms: now_ms,
                    last_refresh_ms: now_ms,
                };
                DHT_BUCKET_COUNT
            ],
        }
    }

    pub fn len(&self) -> usize {
        self.buckets.iter().map(|bucket| bucket.entries.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(&mut self, node: DhtNode) -> bool {
        self.record_response_at(node, dht_timestamp_ms())
    }

    pub fn observe_query(&mut self, node: DhtNode) -> bool {
        self.observe_query_at(node, dht_timestamp_ms())
    }

    pub fn insert_candidate(&mut self, node: DhtNode) -> bool {
        self.insert_candidate_at(node, dht_timestamp_ms())
    }

    pub fn insert_persisted_candidate(&mut self, node: DhtNode) -> bool {
        self.upsert(
            node,
            dht_timestamp_ms(),
            RoutingObservation::PersistedCandidate,
        )
    }

    fn record_response_at(&mut self, node: DhtNode, now_ms: u128) -> bool {
        self.upsert(node, now_ms, RoutingObservation::Response)
    }

    fn observe_query_at(&mut self, node: DhtNode, now_ms: u128) -> bool {
        self.upsert(node, now_ms, RoutingObservation::Query)
    }

    fn insert_candidate_at(&mut self, node: DhtNode, now_ms: u128) -> bool {
        self.upsert(node, now_ms, RoutingObservation::Candidate)
    }

    fn upsert(
        &mut self,
        node: DhtNode,
        now_ms: u128,
        observation: RoutingObservation,
    ) -> bool {
        if node.port == 0 {
            return false;
        }
        let Some(index) = bucket_index(&self.own_id, &node.id) else {
            return false;
        };

        for bucket_index in 0..self.buckets.len() {
            let existing_position = self.buckets[bucket_index].entries.iter().position(|entry| {
                entry.node.id == node.id
                    || (entry.node.address == node.address && entry.node.port == node.port)
            });
            if let Some(position) = existing_position {
                if bucket_index != index {
                    if self.buckets[index].entries.len() >= self.bucket_size {
                        return false;
                    }
                    let entry = self.buckets[bucket_index].entries.remove(position);
                    self.buckets[bucket_index].last_changed_ms = now_ms;
                    self.buckets[index].entries.push(entry);
                }
                let entry = self.buckets[index]
                    .entries
                    .iter_mut()
                    .find(|entry| {
                        entry.node.id == node.id
                            || (entry.node.address == node.address && entry.node.port == node.port)
                    })
                    .expect("routing entry moved or found");
                entry.node = node;
                apply_observation(entry, observation, now_ms);
                if matches!(observation, RoutingObservation::Response) {
                    self.buckets[index].last_changed_ms = now_ms;
                    if self.buckets[index]
                        .entries
                        .iter()
                        .all(|entry| entry.is_good_at(now_ms))
                    {
                        self.buckets[index].pending = None;
                    }
                }
                return false;
            }
        }

        let bucket = &mut self.buckets[index];
        if bucket.entries.len() >= self.bucket_size {
            if bucket.entries.iter().any(|entry| !entry.is_good_at(now_ms)) {
                let mut pending = DhtRoutingEntry {
                    node,
                    last_response_ms: None,
                    last_query_ms: None,
                    consecutive_failures: 0,
                    discovered_ms: now_ms,
                    persistable: false,
                };
                apply_observation(&mut pending, observation, now_ms);
                bucket.pending = Some(pending);
            }
            return false;
        }
        let mut entry = DhtRoutingEntry {
            node,
            last_response_ms: None,
            last_query_ms: None,
            consecutive_failures: 0,
            discovered_ms: now_ms,
            persistable: false,
        };
        apply_observation(&mut entry, observation, now_ms);
        bucket.entries.push(entry);
        bucket.last_changed_ms = now_ms;
        true
    }

    pub fn closest_nodes(&self, target: [u8; 20], limit: usize) -> Vec<DhtNode> {
        self.closest_nodes_at(target, limit, dht_timestamp_ms())
    }

    fn closest_nodes_at(&self, target: [u8; 20], limit: usize, now_ms: u128) -> Vec<DhtNode> {
        let mut nodes = self
            .buckets
            .iter()
            .flat_map(|bucket| bucket.entries.iter())
            .filter(|entry| entry.is_good_at(now_ms))
            .map(|entry| entry.node.clone())
            .collect::<Vec<_>>();
        nodes.sort_by(|left, right| {
            xor_distance(&left.id, &target)
                .cmp(&xor_distance(&right.id, &target))
                .then_with(|| left.address.cmp(&right.address))
                .then_with(|| left.port.cmp(&right.port))
        });
        nodes.truncate(limit);
        nodes
    }

    pub fn questionable_contacts(&self, limit: usize) -> Vec<DhtContact> {
        self.questionable_contacts_at(dht_timestamp_ms(), limit)
    }

    fn questionable_contacts_at(&self, now_ms: u128, limit: usize) -> Vec<DhtContact> {
        let mut entries = self
            .buckets
            .iter()
            .flat_map(|bucket| bucket.entries.iter())
            .filter(|entry| !entry.is_good_at(now_ms))
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.last_activity_ms());
        entries
            .into_iter()
            .take(limit)
            .map(|entry| DhtContact {
                address: entry.node.address.clone(),
                port: entry.node.port,
            })
            .collect()
    }

    pub fn record_failure(&mut self, contact: &DhtContact) -> bool {
        self.record_failure_at(contact, dht_timestamp_ms())
    }

    fn record_failure_at(&mut self, contact: &DhtContact, now_ms: u128) -> bool {
        for bucket in &mut self.buckets {
            let Some(position) = bucket.entries.iter().position(|entry| {
                entry.node.address == contact.address && entry.node.port == contact.port
            }) else {
                continue;
            };
            let entry = &mut bucket.entries[position];
            entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
            if entry.consecutive_failures >= DHT_MAX_CONSECUTIVE_FAILURES {
                bucket.entries.remove(position);
                if let Some(replacement) = bucket.pending.take() {
                    bucket.entries.push(replacement);
                }
                bucket.last_changed_ms = now_ms;
                return true;
            }
            return false;
        }
        false
    }

    pub fn take_refresh_jobs(&mut self, limit: usize) -> Vec<DhtRefreshJob> {
        self.take_refresh_jobs_at(dht_timestamp_ms(), DHT_GOOD_WINDOW_MS, limit)
    }

    fn take_refresh_jobs_at(
        &mut self,
        now_ms: u128,
        refresh_interval_ms: u128,
        limit: usize,
    ) -> Vec<DhtRefreshJob> {
        let due = self
            .buckets
            .iter()
            .enumerate()
            .filter(|(_, bucket)| !bucket.entries.is_empty())
            .filter(|(_, bucket)| {
                now_ms.saturating_sub(bucket.last_changed_ms.max(bucket.last_refresh_ms))
                    >= refresh_interval_ms
            })
            .map(|(index, _)| index)
            .take(limit)
            .collect::<Vec<_>>();
        let mut jobs = Vec::new();
        for index in due {
            let target = refresh_target(self.own_id, index, now_ms);
            let Some(node) = self.closest_nodes_at(target, 1, now_ms).into_iter().next() else {
                continue;
            };
            self.buckets[index].last_refresh_ms = now_ms;
            jobs.push(DhtRefreshJob {
                target,
                contact: DhtContact {
                    address: node.address,
                    port: node.port,
                },
            });
        }
        jobs
    }

    pub fn persistable_nodes(&self, limit: usize) -> Vec<DhtNode> {
        let mut entries = self
            .buckets
            .iter()
            .flat_map(|bucket| bucket.entries.iter())
            .filter(|entry| entry.persistable)
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.last_activity_ms()));
        entries
            .into_iter()
            .take(limit)
            .map(|entry| entry.node.clone())
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
enum RoutingObservation {
    Candidate,
    PersistedCandidate,
    Query,
    Response,
}

impl DhtRoutingEntry {
    fn is_good_at(&self, now_ms: u128) -> bool {
        let response_is_recent = self
            .last_response_ms
            .is_some_and(|seen| now_ms.saturating_sub(seen) < DHT_GOOD_WINDOW_MS);
        let responded_before_and_query_is_recent = self.last_response_ms.is_some()
            && self
                .last_query_ms
                .is_some_and(|seen| now_ms.saturating_sub(seen) < DHT_GOOD_WINDOW_MS);
        response_is_recent || responded_before_and_query_is_recent
    }

    fn last_activity_ms(&self) -> u128 {
        self.last_response_ms
            .into_iter()
            .chain(self.last_query_ms)
            .max()
            .unwrap_or(self.discovered_ms)
    }
}

fn apply_observation(entry: &mut DhtRoutingEntry, observation: RoutingObservation, now_ms: u128) {
    match observation {
        RoutingObservation::Candidate => {}
        RoutingObservation::PersistedCandidate => entry.persistable = true,
        RoutingObservation::Query => entry.last_query_ms = Some(now_ms),
        RoutingObservation::Response => {
            entry.last_response_ms = Some(now_ms);
            entry.consecutive_failures = 0;
            entry.persistable = true;
        }
    }
}

pub fn build_ping_query(transaction_id: &[u8], node_id: [u8; 20]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"d1:ad2:id");
    write_bytes(&mut out, &node_id);
    out.extend_from_slice(b"e1:q4:ping1:t");
    write_bytes(&mut out, transaction_id);
    out.extend_from_slice(b"1:y1:qe");
    out
}

pub fn build_find_node_query(transaction_id: &[u8], node_id: [u8; 20], target: [u8; 20]) -> Vec<u8> {
    build_find_node_query_with_want(transaction_id, node_id, target, &[])
}

pub fn build_find_node_query_with_want(
    transaction_id: &[u8],
    node_id: [u8; 20],
    target: [u8; 20],
    want: &[&str],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"d1:ad2:id");
    write_bytes(&mut out, &node_id);
    out.extend_from_slice(b"6:target");
    write_bytes(&mut out, &target);
    write_want(&mut out, want);
    out.extend_from_slice(b"e1:q9:find_node1:t");
    write_bytes(&mut out, transaction_id);
    out.extend_from_slice(b"1:y1:qe");
    out
}

pub fn build_get_peers_query(transaction_id: &[u8], node_id: [u8; 20], info_hash: [u8; 20]) -> Vec<u8> {
    build_get_peers_query_with_want(transaction_id, node_id, info_hash, &[])
}

pub fn build_get_peers_query_with_want(
    transaction_id: &[u8],
    node_id: [u8; 20],
    info_hash: [u8; 20],
    want: &[&str],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"d1:ad2:id");
    write_bytes(&mut out, &node_id);
    out.extend_from_slice(b"9:info_hash");
    write_bytes(&mut out, &info_hash);
    write_want(&mut out, want);
    out.extend_from_slice(b"e1:q9:get_peers1:t");
    write_bytes(&mut out, transaction_id);
    out.extend_from_slice(b"1:y1:qe");
    out
}

pub fn build_announce_peer_query(
    transaction_id: &[u8],
    node_id: [u8; 20],
    info_hash: [u8; 20],
    port: u16,
    token: &[u8],
    implied_port: bool,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"d1:ad2:id");
    write_bytes(&mut out, &node_id);
    out.extend_from_slice(b"12:implied_porti");
    out.extend_from_slice(if implied_port { b"1" } else { b"0" });
    out.extend_from_slice(b"e9:info_hash");
    write_bytes(&mut out, &info_hash);
    out.extend_from_slice(format!("4:porti{port}e5:token").as_bytes());
    write_bytes(&mut out, token);
    out.extend_from_slice(b"e1:q13:announce_peer1:t");
    write_bytes(&mut out, transaction_id);
    out.extend_from_slice(b"1:y1:qe");
    out
}

pub fn parse_dht_query(input: &[u8]) -> Result<DhtQuery, String> {
    let root = bencode::parse(input)?;
    if root.dict_get(b"y").and_then(BencodeNode::as_bytes) != Some(&b"q"[..]) {
        return Err("KRPC message is not a query".to_string());
    }
    let transaction_id = root
        .dict_get(b"t")
        .and_then(BencodeNode::as_bytes)
        .ok_or_else(|| "KRPC query is missing transaction id".to_string())?
        .to_vec();
    if transaction_id.is_empty() || transaction_id.len() > 16 {
        return Err("KRPC query transaction id length is invalid".to_string());
    }
    let method = root
        .dict_get(b"q")
        .and_then(BencodeNode::as_bytes)
        .ok_or_else(|| "KRPC query is missing method".to_string())?;
    let args = root
        .dict_get(b"a")
        .ok_or_else(|| "KRPC query is missing arguments".to_string())?;
    let node_id = args
        .dict_get(b"id")
        .and_then(BencodeNode::as_bytes)
        .ok_or_else(|| "KRPC query is missing node id".to_string())
        .and_then(bytes_to_20)?;

    let required_hash = |key: &[u8]| {
        args.dict_get(key)
            .and_then(BencodeNode::as_bytes)
            .ok_or_else(|| format!("KRPC query is missing {}", String::from_utf8_lossy(key)))
            .and_then(bytes_to_20)
    };
    let kind = match method {
        b"ping" => DhtQueryKind::Ping,
        b"find_node" => DhtQueryKind::FindNode {
            target: required_hash(b"target")?,
        },
        b"get_peers" => DhtQueryKind::GetPeers {
            info_hash: required_hash(b"info_hash")?,
        },
        b"announce_peer" => {
            let port = args
                .dict_get(b"port")
                .and_then(BencodeNode::as_i64)
                .ok_or_else(|| "announce_peer is missing port".to_string())?;
            let port = u16::try_from(port)
                .ok()
                .filter(|port| *port > 0)
                .ok_or_else(|| "announce_peer port is out of range".to_string())?;
            let token = args
                .dict_get(b"token")
                .and_then(BencodeNode::as_bytes)
                .ok_or_else(|| "announce_peer is missing token".to_string())?
                .to_vec();
            if token.is_empty() || token.len() > 64 {
                return Err("announce_peer token length is invalid".to_string());
            }
            let implied_port = match args
                .dict_get(b"implied_port")
                .and_then(BencodeNode::as_i64)
                .unwrap_or(0)
            {
                0 => false,
                1 => true,
                _ => return Err("announce_peer implied_port must be 0 or 1".to_string()),
            };
            DhtQueryKind::AnnouncePeer {
                info_hash: required_hash(b"info_hash")?,
                port,
                token,
                implied_port,
            }
        }
        _ => return Err(format!("unknown DHT method: {}", String::from_utf8_lossy(method))),
    };
    Ok(DhtQuery {
        transaction_id,
        node_id,
        kind,
    })
}

pub fn transaction_id_from_message(input: &[u8]) -> Option<Vec<u8>> {
    bencode::parse(input)
        .ok()?
        .dict_get(b"t")?
        .as_bytes()
        .map(Vec::from)
}

pub fn build_id_response(transaction_id: &[u8], node_id: [u8; 20]) -> Vec<u8> {
    let mut out = b"d1:rd2:id".to_vec();
    write_bytes(&mut out, &node_id);
    out.extend_from_slice(b"e1:t");
    write_bytes(&mut out, transaction_id);
    out.extend_from_slice(b"1:y1:re");
    out
}

pub fn build_find_node_response(
    transaction_id: &[u8],
    node_id: [u8; 20],
    nodes: &[DhtNode],
) -> Result<Vec<u8>, String> {
    let mut compact = Vec::with_capacity(nodes.len() * 26);
    for node in nodes {
        compact.extend_from_slice(&build_compact_node(node)?);
    }
    let mut out = b"d1:rd2:id".to_vec();
    write_bytes(&mut out, &node_id);
    out.extend_from_slice(b"5:nodes");
    write_bytes(&mut out, &compact);
    out.extend_from_slice(b"e1:t");
    write_bytes(&mut out, transaction_id);
    out.extend_from_slice(b"1:y1:re");
    Ok(out)
}

pub fn build_get_peers_response(
    transaction_id: &[u8],
    node_id: [u8; 20],
    token: &[u8],
    peers: &[PeerInfo],
    nodes: &[DhtNode],
) -> Result<Vec<u8>, String> {
    let mut out = b"d1:rd2:id".to_vec();
    write_bytes(&mut out, &node_id);
    if peers.is_empty() {
        let mut compact = Vec::with_capacity(nodes.len() * 26);
        for node in nodes {
            compact.extend_from_slice(&build_compact_node(node)?);
        }
        out.extend_from_slice(b"5:nodes");
        write_bytes(&mut out, &compact);
        out.extend_from_slice(b"5:token");
        write_bytes(&mut out, token);
    } else {
        out.extend_from_slice(b"5:token");
        write_bytes(&mut out, token);
        out.extend_from_slice(b"6:valuesl");
        for peer in peers.iter().take(50) {
            write_bytes(&mut out, &compact_peer(peer)?);
        }
        out.push(b'e');
    }
    out.extend_from_slice(b"e1:t");
    write_bytes(&mut out, transaction_id);
    out.extend_from_slice(b"1:y1:re");
    Ok(out)
}

pub fn build_krpc_error(transaction_id: &[u8], code: i64, message: &str) -> Vec<u8> {
    let message = message.as_bytes();
    let mut out = format!("d1:eli{code}e").into_bytes();
    write_bytes(&mut out, message);
    out.extend_from_slice(b"e1:t");
    write_bytes(&mut out, transaction_id);
    out.extend_from_slice(b"1:y1:ee");
    out
}

pub fn default_bootstrap_nodes() -> Vec<DhtContact> {
    vec![
        DhtContact {
            address: "dht.transmissionbt.com".to_string(),
            port: 6881,
        },
        DhtContact {
            address: "router.bittorrent.com".to_string(),
            port: 6881,
        },
        DhtContact {
            address: "router.utorrent.com".to_string(),
            port: 6881,
        },
    ]
}

pub fn lookup_peers(
    seeds: &[DhtContact],
    node_id: [u8; 20],
    info_hash: [u8; 20],
    options: DhtLookupOptions,
) -> Result<DhtLookupResult, String> {
    if seeds.is_empty() {
        return Err("DHT lookup needs at least one bootstrap node".to_string());
    }

    let mut candidates = seeds
        .iter()
        .cloned()
        .map(|contact| DhtCandidate { contact, node_id: None })
        .collect::<Vec<_>>();
    let mut seen_nodes = HashSet::new();
    for candidate in &candidates {
        seen_nodes.insert(candidate.contact.key());
    }
    let mut routing_table = DhtRoutingTable::new(node_id);
    let mut queried_nodes = HashSet::new();
    let mut peers = Vec::new();
    let mut announce_targets = Vec::<DhtAnnounceTarget>::new();
    let mut errors = Vec::new();
    let max_queries = options.max_queries.max(1);

    while queried_nodes.len() < max_queries {
        let Some(index) = select_next_candidate(&candidates, &queried_nodes, &info_hash) else {
            break;
        };
        let candidate = candidates.remove(index);
        let endpoint = candidate.contact.endpoint();
        if !queried_nodes.insert(candidate.contact.key()) {
            continue;
        }

        let transaction_id = lookup_transaction_id(queried_nodes.len() as u16);
        let packet = build_get_peers_query(&transaction_id, node_id, info_hash);
        let response = match send_krpc_query(&endpoint, &packet, options.timeout) {
            Ok(response) => response,
            Err(err) => {
                errors.push(format!("{endpoint}: {err}"));
                continue;
            }
        };

        let response = match parse_dht_response(&response) {
            Ok(response) => response,
            Err(response_err) => {
                let message = parse_dht_error(&response)
                    .map(|err| format!("DHT error {}: {}", err.code, err.message))
                    .unwrap_or(response_err);
                errors.push(format!("{endpoint}: {message}"));
                continue;
            }
        };

        if response.transaction_id != transaction_id {
            errors.push(format!("{endpoint}: DHT transaction ID mismatch"));
            continue;
        }

        if let Some(token) = response.token.clone() {
            if let Some(existing) = announce_targets
                .iter_mut()
                .find(|target| target.contact == candidate.contact)
            {
                existing.token = token;
            } else {
                announce_targets.push(DhtAnnounceTarget {
                    contact: candidate.contact.clone(),
                    token,
                });
            }
        }

        merge_lookup_peers(&mut peers, response.peers);
        for node in response.nodes {
            if node.port == 0 {
                continue;
            }
            let contact = DhtContact {
                address: node.address.clone(),
                port: node.port,
            };
            if routing_table.insert(node.clone()) && seen_nodes.insert(contact.key()) {
                candidates.push(DhtCandidate {
                    contact,
                    node_id: Some(node.id),
                });
            }
        }
        let ignored_ipv6_nodes = response
            .nodes6
            .into_iter()
            .filter(|node| node.port != 0)
            .count();
        if ignored_ipv6_nodes > 0 {
            errors.push(format!(
                "ignored {ignored_ipv6_nodes} IPv6 DHT nodes because dual-stack routing is not active"
            ));
        }

        if !peers.is_empty() {
            break;
        }
    }

    Ok(DhtLookupResult {
        peers,
        announce_targets,
        queried_nodes: queried_nodes.len(),
        discovered_nodes: routing_table.len(),
        errors,
    })
}

pub fn announce_peer(
    target: &DhtAnnounceTarget,
    transaction_id: &[u8],
    node_id: [u8; 20],
    info_hash: [u8; 20],
    port: u16,
    timeout: Duration,
) -> Result<DhtResponse, String> {
    if port == 0 {
        return Err("DHT announce_peer requires a non-zero listening port".to_string());
    }
    let packet = build_announce_peer_query(
        transaction_id,
        node_id,
        info_hash,
        port,
        &target.token,
        false,
    );
    let endpoint = target.contact.endpoint();
    let response_bytes = send_krpc_query(&endpoint, &packet, timeout)?;
    let response = parse_dht_response(&response_bytes).map_err(|parse_error| {
        parse_dht_error(&response_bytes)
            .map(|error| format!("DHT error {}: {}", error.code, error.message))
            .unwrap_or(parse_error)
    })?;
    if response.transaction_id != transaction_id {
        return Err(format!("{endpoint}: DHT transaction ID mismatch"));
    }
    Ok(response)
}

pub fn parse_dht_response(input: &[u8]) -> Result<DhtResponse, String> {
    let root = bencode::parse(input)?;
    let y = root
        .dict_get(b"y")
        .and_then(BencodeNode::as_bytes)
        .ok_or_else(|| "KRPC message is missing y".to_string())?;
    if y != b"r" {
        return Err("KRPC message is not a response".to_string());
    }
    let transaction_id = root
        .dict_get(b"t")
        .and_then(BencodeNode::as_bytes)
        .ok_or_else(|| "KRPC response is missing transaction id".to_string())?
        .to_vec();
    let response = root
        .dict_get(b"r")
        .ok_or_else(|| "KRPC response is missing r dictionary".to_string())?;
    let node_id = response
        .dict_get(b"id")
        .and_then(BencodeNode::as_bytes)
        .map(bytes_to_20)
        .transpose()?;
    let token = response
        .dict_get(b"token")
        .and_then(BencodeNode::as_bytes)
        .map(Vec::from);
    let nodes = response
        .dict_get(b"nodes")
        .and_then(BencodeNode::as_bytes)
        .map(parse_compact_nodes)
        .transpose()?
        .unwrap_or_default();
    let nodes6 = response
        .dict_get(b"nodes6")
        .and_then(BencodeNode::as_bytes)
        .map(parse_compact_nodes6)
        .transpose()?
        .unwrap_or_default();
    let peers = response
        .dict_get(b"values")
        .and_then(BencodeNode::as_list)
        .map(parse_peer_values)
        .transpose()?
        .unwrap_or_default();

    Ok(DhtResponse {
        transaction_id,
        node_id,
        token,
        nodes,
        nodes6,
        peers,
    })
}

pub fn parse_dht_error(input: &[u8]) -> Result<DhtError, String> {
    let root = bencode::parse(input)?;
    let y = root
        .dict_get(b"y")
        .and_then(BencodeNode::as_bytes)
        .ok_or_else(|| "KRPC message is missing y".to_string())?;
    if y != b"e" {
        return Err("KRPC message is not an error".to_string());
    }
    let transaction_id = root
        .dict_get(b"t")
        .and_then(BencodeNode::as_bytes)
        .ok_or_else(|| "KRPC error is missing transaction id".to_string())?
        .to_vec();
    let error = root
        .dict_get(b"e")
        .and_then(BencodeNode::as_list)
        .ok_or_else(|| "KRPC error is missing error list".to_string())?;
    if error.len() != 2 {
        return Err("KRPC error list must contain code and message".to_string());
    }
    let code = error[0]
        .as_i64()
        .ok_or_else(|| "KRPC error code is not an integer".to_string())?;
    let message = error[1]
        .as_str_lossy()
        .ok_or_else(|| "KRPC error message is not a byte string".to_string())?;

    Ok(DhtError {
        transaction_id,
        code,
        message,
    })
}

pub fn send_krpc_query(
    address: &str,
    packet: &[u8],
    timeout: Duration,
) -> Result<Vec<u8>, String> {
    let destination = address
        .to_socket_addrs()
        .map_err(|err| format!("could not resolve DHT node: {err}"))?
        .next()
        .ok_or_else(|| "DHT node address did not resolve".to_string())?;
    let socket = UdpSocket::bind("0.0.0.0:0")
        .map_err(|err| format!("could not bind DHT UDP socket: {err}"))?;
    socket
        .set_read_timeout(Some(timeout))
        .map_err(|err| format!("could not set DHT read timeout: {err}"))?;
    socket
        .send_to(packet, destination)
        .map_err(|err| format!("could not send DHT query: {err}"))?;
    let mut buffer = vec![0u8; 2048];
    let (length, _) = socket
        .recv_from(&mut buffer)
        .map_err(|err| format!("could not receive DHT response: {err}"))?;
    buffer.truncate(length);
    Ok(buffer)
}

pub fn ping_node(
    address: &str,
    transaction_id: &[u8],
    node_id: [u8; 20],
) -> Result<DhtResponse, String> {
    ping_node_with_timeout(
        address,
        transaction_id,
        node_id,
        Duration::from_secs(5),
    )
}

pub fn ping_node_with_timeout(
    address: &str,
    transaction_id: &[u8],
    node_id: [u8; 20],
    timeout: Duration,
) -> Result<DhtResponse, String> {
    let packet = build_ping_query(transaction_id, node_id);
    let response = send_krpc_query(address, &packet, timeout)?;
    let response = parse_dht_response(&response)?;
    if response.transaction_id != transaction_id {
        return Err(format!("{address}: DHT transaction ID mismatch"));
    }
    Ok(response)
}

pub fn find_node(
    address: &str,
    transaction_id: &[u8],
    node_id: [u8; 20],
    target: [u8; 20],
    timeout: Duration,
) -> Result<DhtResponse, String> {
    let packet = build_find_node_query(transaction_id, node_id, target);
    let response = send_krpc_query(address, &packet, timeout)?;
    let response = parse_dht_response(&response)?;
    if response.transaction_id != transaction_id {
        return Err(format!("{address}: DHT transaction ID mismatch"));
    }
    Ok(response)
}

pub fn parse_compact_nodes(bytes: &[u8]) -> Result<Vec<DhtNode>, String> {
    if bytes.len() % 26 != 0 {
        return Err("compact DHT node list length must be a multiple of 26".to_string());
    }
    let mut nodes = Vec::new();
    for chunk in bytes.chunks_exact(26) {
        let id = bytes_to_20(&chunk[..20])?;
        let address = format!("{}.{}.{}.{}", chunk[20], chunk[21], chunk[22], chunk[23]);
        let port = u16::from_be_bytes([chunk[24], chunk[25]]);
        if port != 0 {
            nodes.push(DhtNode { id, address, port });
        }
    }
    Ok(nodes)
}

pub fn parse_compact_nodes6(bytes: &[u8]) -> Result<Vec<DhtNode>, String> {
    if bytes.len() % 38 != 0 {
        return Err("compact IPv6 DHT node list length must be a multiple of 38".to_string());
    }
    let mut nodes = Vec::new();
    for chunk in bytes.chunks_exact(38) {
        let id = bytes_to_20(&chunk[..20])?;
        let address = Ipv6Addr::from(
            <[u8; 16]>::try_from(&chunk[20..36])
                .expect("sixteen-byte IPv6 compact node slice"),
        )
        .to_string();
        let port = u16::from_be_bytes([chunk[36], chunk[37]]);
        if port != 0 {
            nodes.push(DhtNode { id, address, port });
        }
    }
    Ok(nodes)
}

pub fn build_compact_node(node: &DhtNode) -> Result<Vec<u8>, String> {
    if node.port == 0 {
        return Err("DHT node port cannot be zero".to_string());
    }
    let mut out = Vec::with_capacity(26);
    out.extend_from_slice(&node.id);
    let octets = node
        .address
        .split('.')
        .map(|part| {
            part.parse::<u8>()
                .map_err(|err| format!("DHT node IPv4 octet is invalid: {err}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let octets: [u8; 4] = octets
        .try_into()
        .map_err(|_| "DHT node address must be an IPv4 address".to_string())?;
    out.extend_from_slice(&octets);
    out.extend_from_slice(&node.port.to_be_bytes());
    Ok(out)
}

#[cfg(test)]
fn build_compact_node6(node: &DhtNode) -> Result<Vec<u8>, String> {
    if node.port == 0 {
        return Err("DHT node port cannot be zero".to_string());
    }
    let mut out = Vec::with_capacity(38);
    out.extend_from_slice(&node.id);
    let address = node
        .address
        .parse::<Ipv6Addr>()
        .map_err(|err| format!("DHT node IPv6 address is invalid: {err}"))?;
    out.extend_from_slice(&address.octets());
    out.extend_from_slice(&node.port.to_be_bytes());
    Ok(out)
}

impl DhtContact {
    fn endpoint(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }

    fn key(&self) -> String {
        self.endpoint()
    }
}

#[derive(Debug, Clone)]
struct DhtCandidate {
    contact: DhtContact,
    node_id: Option<[u8; 20]>,
}

fn select_next_candidate(
    candidates: &[DhtCandidate],
    queried_nodes: &HashSet<String>,
    target: &[u8; 20],
) -> Option<usize> {
    candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| !queried_nodes.contains(&candidate.contact.key()))
        .min_by(|(_, left), (_, right)| compare_candidate_distance(left, right, target))
        .map(|(index, _)| index)
}

fn compare_candidate_distance(
    left: &DhtCandidate,
    right: &DhtCandidate,
    target: &[u8; 20],
) -> std::cmp::Ordering {
    match (left.node_id, right.node_id) {
        (Some(left_id), Some(right_id)) => xor_distance(&left_id, target)
            .cmp(&xor_distance(&right_id, target))
            .then_with(|| left.contact.endpoint().cmp(&right.contact.endpoint())),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.contact.endpoint().cmp(&right.contact.endpoint()),
    }
}

fn xor_distance(left: &[u8; 20], right: &[u8; 20]) -> [u8; 20] {
    let mut distance = [0u8; 20];
    for (index, byte) in distance.iter_mut().enumerate() {
        *byte = left[index] ^ right[index];
    }
    distance
}

fn dht_timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn refresh_target(own_id: [u8; 20], bucket: usize, entropy_value: u128) -> [u8; 20] {
    let mut entropy_source = own_id.to_vec();
    entropy_source.extend_from_slice(&(bucket as u16).to_be_bytes());
    entropy_source.extend_from_slice(&entropy_value.to_be_bytes());
    let entropy = sha1::digest(&entropy_source);
    let mut target = own_id;
    set_id_bit(&mut target, bucket, !id_bit(&own_id, bucket));
    for bit in bucket + 1..DHT_BUCKET_COUNT {
        let entropy_bit = (bit - bucket - 1) % DHT_BUCKET_COUNT;
        set_id_bit(&mut target, bit, id_bit(&entropy, entropy_bit));
    }
    target
}

fn id_bit(id: &[u8; 20], bit: usize) -> bool {
    id[bit / 8] & (0x80 >> (bit % 8)) != 0
}

fn set_id_bit(id: &mut [u8; 20], bit: usize, value: bool) {
    let mask = 0x80 >> (bit % 8);
    if value {
        id[bit / 8] |= mask;
    } else {
        id[bit / 8] &= !mask;
    }
}

fn bucket_index(own_id: &[u8; 20], node_id: &[u8; 20]) -> Option<usize> {
    let distance = xor_distance(own_id, node_id);
    for (byte_index, byte) in distance.iter().enumerate() {
        if *byte != 0 {
            return Some(byte_index * 8 + byte.leading_zeros() as usize);
        }
    }
    None
}

fn lookup_transaction_id(index: u16) -> [u8; 2] {
    index.to_be_bytes()
}

fn merge_lookup_peers(existing: &mut Vec<PeerInfo>, next: Vec<PeerInfo>) {
    for peer in next {
        let seen = existing
            .iter()
            .any(|current| current.address == peer.address && current.port == peer.port);
        if !seen {
            existing.push(peer);
        }
    }
}

fn compact_peer(peer: &PeerInfo) -> Result<Vec<u8>, String> {
    let octets = peer
        .address
        .split('.')
        .map(|part| {
            part.parse::<u8>()
                .map_err(|err| format!("DHT peer IPv4 octet is invalid: {err}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let octets: [u8; 4] = octets
        .try_into()
        .map_err(|_| "DHT compact peer requires an IPv4 address".to_string())?;
    let mut out = Vec::with_capacity(6);
    out.extend_from_slice(&octets);
    out.extend_from_slice(&peer.port.to_be_bytes());
    Ok(out)
}

fn parse_peer_values(values: &[BencodeNode]) -> Result<Vec<PeerInfo>, String> {
    let mut peers = Vec::new();
    for value in values {
        let bytes = value
            .as_bytes()
            .ok_or_else(|| "DHT peer value is not compact bytes".to_string())?;
        peers.extend(parse_compact_peer_value(bytes)?);
    }
    Ok(peers)
}

fn parse_compact_peer_value(bytes: &[u8]) -> Result<Vec<PeerInfo>, String> {
    match bytes.len() {
        6 => tracker::parse_compact_peers(bytes),
        18 => {
            let port = u16::from_be_bytes([bytes[16], bytes[17]]);
            if port == 0 {
                return Ok(Vec::new());
            }
            Ok(vec![PeerInfo {
                address: Ipv6Addr::from(
                    <[u8; 16]>::try_from(&bytes[..16]).expect("sixteen-byte IPv6 peer slice"),
                )
                .to_string(),
                port,
                client: None,
                progress: 0.0,
                download_speed: 0,
                upload_speed: 0,
                connection: "DHT discovered".to_string(),
            }])
        }
        len if len % 6 == 0 => tracker::parse_compact_peers(bytes),
        _ => Err("DHT compact peer value must be 6-byte IPv4 or 18-byte IPv6 contact data".to_string()),
    }
}

fn bytes_to_20(bytes: &[u8]) -> Result<[u8; 20], String> {
    bytes.try_into()
        .map_err(|_| "DHT node id must be 20 bytes".to_string())
}

fn write_want(out: &mut Vec<u8>, want: &[&str]) {
    if want.is_empty() {
        return;
    }
    out.extend_from_slice(b"4:wantl");
    for flag in want {
        write_bytes(out, flag.as_bytes());
    }
    out.push(b'e');
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::UdpSocket, thread};

    #[test]
    fn builds_ping_query_matching_bep5_shape() {
        let query = build_ping_query(b"aa", *b"abcdefghij0123456789");

        assert_eq!(
            query,
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe".to_vec()
        );
    }

    #[test]
    fn builds_find_node_query() {
        let query = build_find_node_query(
            b"aa",
            *b"abcdefghij0123456789",
            *b"mnopqrstuvwxyz123456",
        );

        assert_eq!(
            query,
            b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe".to_vec()
        );
    }

    #[test]
    fn builds_get_peers_query() {
        let query = build_get_peers_query(
            b"aa",
            *b"abcdefghij0123456789",
            *b"mnopqrstuvwxyz123456",
        );

        assert_eq!(
            query,
            b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe".to_vec()
        );
    }

    #[test]
    fn builds_bep32_want_queries() {
        let find_node = build_find_node_query_with_want(
            b"aa",
            *b"abcdefghij0123456789",
            *b"mnopqrstuvwxyz123456",
            &["n4", "n6"],
        );
        assert_eq!(
            find_node,
            b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz1234564:wantl2:n42:n6ee1:q9:find_node1:t2:aa1:y1:qe".to_vec()
        );

        let get_peers = build_get_peers_query_with_want(
            b"bb",
            *b"abcdefghij0123456789",
            *b"mnopqrstuvwxyz123456",
            &["n6"],
        );
        assert_eq!(
            get_peers,
            b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:wantl2:n6ee1:q9:get_peers1:t2:bb1:y1:qe".to_vec()
        );
    }

    #[test]
    fn builds_announce_peer_query() {
        let query = build_announce_peer_query(
            b"aa",
            *b"abcdefghij0123456789",
            *b"mnopqrstuvwxyz123456",
            6881,
            b"aoeusnth",
            true,
        );

        assert_eq!(
            query,
            b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe".to_vec()
        );
        assert_eq!(
            parse_dht_query(&query).expect("announce query parses"),
            DhtQuery {
                transaction_id: b"aa".to_vec(),
                node_id: *b"abcdefghij0123456789",
                kind: DhtQueryKind::AnnouncePeer {
                    info_hash: *b"mnopqrstuvwxyz123456",
                    port: 6881,
                    token: b"aoeusnth".to_vec(),
                    implied_port: true,
                },
            }
        );
    }

    #[test]
    fn builds_inbound_get_peers_values_and_protocol_error_responses() {
        let peer = PeerInfo {
            address: "127.0.0.1".to_string(),
            port: 6881,
            client: None,
            progress: 0.0,
            download_speed: 0,
            upload_speed: 0,
            connection: "DHT".to_string(),
        };
        let response = build_get_peers_response(
            b"aa",
            *b"abcdefghij0123456789",
            b"token",
            &[peer],
            &[],
        )
        .expect("get_peers response builds");
        let parsed = parse_dht_response(&response).expect("get_peers response parses");
        assert_eq!(parsed.token.as_deref(), Some(&b"token"[..]));
        assert_eq!(parsed.peers[0].address, "127.0.0.1");
        assert_eq!(parsed.peers[0].port, 6881);

        assert_eq!(
            parse_dht_error(&build_krpc_error(b"aa", 203, "bad token"))
                .expect("error response parses")
                .message,
            "bad token"
        );
    }

    #[test]
    fn parses_compact_nodes() {
        let node = DhtNode {
            id: [7; 20],
            address: "127.0.0.1".to_string(),
            port: 6881,
        };
        let mut compact = [9; 20].to_vec();
        compact.extend_from_slice(&[127, 0, 0, 2]);
        compact.extend_from_slice(&0u16.to_be_bytes());
        compact.extend_from_slice(&build_compact_node(&node).expect("node compacts"));

        assert_eq!(parse_compact_nodes(&compact).expect("nodes parse"), vec![node]);
    }

    #[test]
    fn parses_bep32_nodes6_and_hybrid_peer_values() {
        let node6 = DhtNode {
            id: [8; 20],
            address: "2001:db8::1".to_string(),
            port: 49002,
        };
        let compact_node6 = build_compact_node6(&node6).expect("IPv6 node compacts");
        let mut zero_port_node6 = [9; 20].to_vec();
        zero_port_node6.extend_from_slice(
            &"2001:db8::2"
                .parse::<Ipv6Addr>()
                .expect("IPv6 parses")
                .octets(),
        );
        zero_port_node6.extend_from_slice(&0u16.to_be_bytes());
        let mut compact_nodes6 = zero_port_node6;
        compact_nodes6.extend_from_slice(&compact_node6);
        assert_eq!(
            parse_compact_nodes6(&compact_nodes6).expect("nodes6 parses"),
            vec![node6.clone()]
        );

        let ipv4_peer = vec![127, 0, 0, 1, 0x1a, 0xe1];
        let zero_port_ipv4_peer = vec![127, 0, 0, 2, 0, 0];
        let mut ipv6_peer = "2001:db8::5"
            .parse::<Ipv6Addr>()
            .expect("IPv6 peer address parses")
            .octets()
            .to_vec();
        ipv6_peer.extend_from_slice(&51413u16.to_be_bytes());
        let mut zero_port_ipv6_peer = "2001:db8::6"
            .parse::<Ipv6Addr>()
            .expect("IPv6 peer address parses")
            .octets()
            .to_vec();
        zero_port_ipv6_peer.extend_from_slice(&0u16.to_be_bytes());

        let mut response = b"d1:rd2:id".to_vec();
        write_bytes(&mut response, b"abcdefghij0123456789");
        response.extend_from_slice(b"6:nodes6");
        write_bytes(&mut response, &compact_node6);
        response.extend_from_slice(b"5:token2:tk6:valuesl");
        write_bytes(&mut response, &ipv4_peer);
        write_bytes(&mut response, &zero_port_ipv4_peer);
        write_bytes(&mut response, &ipv6_peer);
        write_bytes(&mut response, &zero_port_ipv6_peer);
        response.extend_from_slice(b"ee1:t2:aa1:y1:re");

        let parsed = parse_dht_response(&response).expect("BEP 32 response parses");

        assert_eq!(parsed.nodes6, vec![node6]);
        assert_eq!(parsed.peers.len(), 2);
        assert_eq!(parsed.peers[0].address, "127.0.0.1");
        assert_eq!(parsed.peers[0].port, 6881);
        assert_eq!(parsed.peers[1].address, "2001:db8::5");
        assert_eq!(parsed.peers[1].port, 51413);
    }

    #[test]
    fn routing_table_skips_own_node_and_deduplicates_contacts() {
        let own_id = [0u8; 20];
        let mut table = DhtRoutingTable::new(own_id);

        assert!(!table.insert(DhtNode {
            id: own_id,
            address: "127.0.0.1".to_string(),
            port: 6881,
        }));
        assert!(table.insert(DhtNode {
            id: node_id_with_last_byte(1),
            address: "127.0.0.1".to_string(),
            port: 6881,
        }));
        assert!(!table.insert(DhtNode {
            id: node_id_with_last_byte(2),
            address: "127.0.0.1".to_string(),
            port: 6881,
        }));

        assert_eq!(table.len(), 1);
        assert_eq!(table.closest_nodes([0u8; 20], 1)[0].id, node_id_with_last_byte(2));
    }

    #[test]
    fn routing_table_caps_each_bucket() {
        let mut table = DhtRoutingTable::with_bucket_size([0u8; 20], 2);

        assert!(table.insert(DhtNode {
            id: node_id_with_first_byte(0x80),
            address: "10.0.0.1".to_string(),
            port: 6881,
        }));
        assert!(table.insert(DhtNode {
            id: node_id_with_first_byte(0x81),
            address: "10.0.0.2".to_string(),
            port: 6881,
        }));
        assert!(!table.insert(DhtNode {
            id: node_id_with_first_byte(0x82),
            address: "10.0.0.3".to_string(),
            port: 6881,
        }));

        assert_eq!(table.len(), 2);
    }

    #[test]
    fn routing_table_returns_closest_nodes_by_xor_distance() {
        let mut table = DhtRoutingTable::new([0u8; 20]);
        let target = node_id_with_last_byte(10);
        for (last_byte, port) in [(20, 7000), (11, 7001), (200, 7002)] {
            assert!(table.insert(DhtNode {
                id: node_id_with_last_byte(last_byte),
                address: "127.0.0.1".to_string(),
                port,
            }));
        }

        let closest = table.closest_nodes(target, 2);

        assert_eq!(closest.len(), 2);
        assert_eq!(closest[0].id, node_id_with_last_byte(11));
        assert_eq!(closest[1].id, node_id_with_last_byte(20));
    }

    #[test]
    fn routing_health_requires_response_evidence_and_uses_recent_queries() {
        let mut table = DhtRoutingTable::with_bucket_size_at([0u8; 20], 8, 100);
        let node = DhtNode {
            id: node_id_with_last_byte(1),
            address: "127.0.0.1".to_string(),
            port: 7000,
        };

        assert!(table.observe_query_at(node.clone(), 100));
        assert!(table.closest_nodes_at(node.id, 1, 100).is_empty());
        assert_eq!(table.questionable_contacts_at(100, 1).len(), 1);

        assert!(!table.record_response_at(node.clone(), 200));
        assert_eq!(table.closest_nodes_at(node.id, 1, 200), vec![node.clone()]);

        let stale_at = 200 + DHT_GOOD_WINDOW_MS;
        assert!(table.closest_nodes_at(node.id, 1, stale_at).is_empty());
        assert!(!table.observe_query_at(node.clone(), stale_at));
        assert_eq!(table.closest_nodes_at(node.id, 1, stale_at), vec![node]);
    }

    #[test]
    fn routing_evicts_after_two_failures_and_promotes_pending_good_node() {
        let mut table = DhtRoutingTable::with_bucket_size_at([0u8; 20], 1, 100);
        let first = DhtNode {
            id: node_id_with_first_byte(0x80),
            address: "10.0.0.1".to_string(),
            port: 7001,
        };
        let replacement = DhtNode {
            id: node_id_with_first_byte(0x81),
            address: "10.0.0.2".to_string(),
            port: 7002,
        };
        assert!(table.record_response_at(first.clone(), 100));
        let stale_at = 100 + DHT_GOOD_WINDOW_MS;
        assert!(!table.record_response_at(replacement.clone(), stale_at));
        let contact = DhtContact {
            address: first.address,
            port: first.port,
        };

        assert!(!table.record_failure_at(&contact, stale_at + 1));
        assert_eq!(table.len(), 1);
        assert!(table.record_failure_at(&contact, stale_at + 2));
        assert_eq!(table.len(), 1);
        assert_eq!(
            table.closest_nodes_at(replacement.id, 1, stale_at + 2),
            vec![replacement]
        );
    }

    #[test]
    fn stale_bucket_refresh_target_stays_in_range_and_is_throttled() {
        let own_id = [0u8; 20];
        let mut table = DhtRoutingTable::with_bucket_size_at(own_id, 8, 100);
        let node = DhtNode {
            id: node_id_with_first_byte(0x80),
            address: "10.0.0.1".to_string(),
            port: 7001,
        };
        assert!(table.record_response_at(node.clone(), 100));
        let refresh_at = 100 + DHT_GOOD_WINDOW_MS;
        assert!(!table.observe_query_at(node, refresh_at - 1));

        assert!(table
            .take_refresh_jobs_at(refresh_at - 1, DHT_GOOD_WINDOW_MS, 1)
            .is_empty());
        let jobs = table.take_refresh_jobs_at(refresh_at, DHT_GOOD_WINDOW_MS, 1);
        assert_eq!(jobs.len(), 1);
        assert_eq!(bucket_index(&own_id, &jobs[0].target), Some(0));
        assert_eq!(jobs[0].contact.address, "10.0.0.1");
        assert!(table
            .take_refresh_jobs_at(refresh_at, DHT_GOOD_WINDOW_MS, 1)
            .is_empty());
    }

    #[test]
    fn parses_get_peers_response_with_peers() {
        let response = b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:\x7f\x00\x00\x01\x1a\xe1ee1:t2:aa1:y1:re";
        let parsed = parse_dht_response(response).expect("DHT response parses");

        assert_eq!(parsed.transaction_id, b"aa");
        assert_eq!(parsed.node_id, Some(*b"abcdefghij0123456789"));
        assert_eq!(parsed.token.as_deref(), Some(&b"aoeusnth"[..]));
        assert_eq!(parsed.peers[0].address, "127.0.0.1");
        assert_eq!(parsed.peers[0].port, 6881);
    }

    #[test]
    fn parses_get_peers_response_with_nodes() {
        let node = DhtNode {
            id: [5; 20],
            address: "10.0.0.8".to_string(),
            port: 49001,
        };
        let compact = build_compact_node(&node).expect("node compacts");
        let mut response = b"d1:rd2:id20:abcdefghij01234567895:nodes".to_vec();
        write_bytes(&mut response, &compact);
        response.extend_from_slice(b"5:token2:tke1:t2:aa1:y1:re");

        let parsed = parse_dht_response(&response).expect("DHT response parses");

        assert_eq!(parsed.nodes, vec![node]);
        assert_eq!(parsed.token.as_deref(), Some(&b"tk"[..]));
    }

    #[test]
    fn parses_krpc_error() {
        let parsed = parse_dht_error(b"d1:eli203e8:bad datae1:t2:aa1:y1:ee").expect("error parses");

        assert_eq!(
            parsed,
            DhtError {
                transaction_id: b"aa".to_vec(),
                code: 203,
                message: "bad data".to_string(),
            }
        );
    }

    #[test]
    fn pings_local_dht_node_over_udp() {
        let server = UdpSocket::bind("127.0.0.1:0").expect("local UDP socket binds");
        let address = server.local_addr().expect("server has address").to_string();
        let query_node_id = *b"abcdefghij0123456789";
        let response_node_id = *b"mnopqrstuvwxyz123456";

        let handle = thread::spawn(move || {
            let mut buffer = [0u8; 2048];
            let (length, client) = server.recv_from(&mut buffer).expect("query arrives");
            assert_eq!(
                &buffer[..length],
                build_ping_query(b"aa", query_node_id).as_slice()
            );
            server
                .send_to(
                    b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re",
                    client,
                )
                .expect("response sends");
        });

        let response = ping_node(&address, b"aa", query_node_id).expect("ping response parses");

        handle.join().expect("server exits");
        assert_eq!(response.transaction_id, b"aa");
        assert_eq!(response.node_id, Some(response_node_id));
    }

    #[test]
    fn finds_compact_nodes_over_udp() {
        let server = UdpSocket::bind("127.0.0.1:0").expect("local UDP socket binds");
        let address = server.local_addr().expect("server has address").to_string();
        let query_node_id = *b"abcdefghij0123456789";
        let response_node_id = *b"mnopqrstuvwxyz123456";
        let target = [42u8; 20];
        let returned = DhtNode {
            id: [43u8; 20],
            address: "127.0.0.1".to_string(),
            port: 49000,
        };
        let expected = returned.clone();
        let handle = thread::spawn(move || {
            let mut buffer = [0u8; 2048];
            let (length, client) = server.recv_from(&mut buffer).expect("query arrives");
            let query = parse_dht_query(&buffer[..length]).expect("find_node query parses");
            assert_eq!(query.transaction_id, b"fn");
            assert_eq!(query.node_id, query_node_id);
            assert_eq!(query.kind, DhtQueryKind::FindNode { target });
            server
                .send_to(
                    &build_find_node_response(b"fn", response_node_id, &[returned])
                        .expect("find_node response builds"),
                    client,
                )
                .expect("response sends");
        });

        let response = find_node(
            &address,
            b"fn",
            query_node_id,
            target,
            Duration::from_secs(2),
        )
        .expect("find_node succeeds");
        assert_eq!(response.node_id, Some(response_node_id));
        assert_eq!(response.nodes, vec![expected]);
        handle.join().expect("server exits");
    }

    #[test]
    fn announces_peer_to_token_issuing_node_over_udp() {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("DHT node binds");
        let target = DhtAnnounceTarget {
            contact: DhtContact {
                address: "127.0.0.1".to_string(),
                port: socket.local_addr().expect("DHT node address").port(),
            },
            token: b"issued-token".to_vec(),
        };
        let local_node_id = *b"abcdefghij0123456789";
        let response_node_id = *b"server-node-id-00000";
        let info_hash = *b"mnopqrstuvwxyz123456";
        let server = thread::spawn(move || {
            let mut buffer = [0u8; 2048];
            let (length, client) = socket.recv_from(&mut buffer).expect("announce query arrives");
            let root = bencode::parse(&buffer[..length]).expect("announce query parses");
            assert_eq!(root.dict_get(b"y").and_then(BencodeNode::as_bytes), Some(&b"q"[..]));
            assert_eq!(
                root.dict_get(b"q").and_then(BencodeNode::as_bytes),
                Some(&b"announce_peer"[..])
            );
            let args = root.dict_get(b"a").expect("announce has arguments");
            assert_eq!(
                args.dict_get(b"info_hash").and_then(BencodeNode::as_bytes),
                Some(&info_hash[..])
            );
            assert_eq!(args.dict_get(b"port").and_then(BencodeNode::as_i64), Some(6999));
            assert_eq!(
                args.dict_get(b"implied_port").and_then(BencodeNode::as_i64),
                Some(0)
            );
            assert_eq!(
                args.dict_get(b"token").and_then(BencodeNode::as_bytes),
                Some(&b"issued-token"[..])
            );
            let transaction_id = root
                .dict_get(b"t")
                .and_then(BencodeNode::as_bytes)
                .expect("announce has transaction ID");
            let mut response = b"d1:rd2:id".to_vec();
            write_bytes(&mut response, &response_node_id);
            response.extend_from_slice(b"e1:t");
            write_bytes(&mut response, transaction_id);
            response.extend_from_slice(b"1:y1:re");
            socket.send_to(&response, client).expect("announce response sends");
        });

        let response = announce_peer(
            &target,
            b"ap",
            local_node_id,
            info_hash,
            6999,
            Duration::from_secs(2),
        )
        .expect("announce_peer succeeds");
        server.join().expect("DHT node exits");
        assert_eq!(response.transaction_id, b"ap");
        assert_eq!(response.node_id, Some(response_node_id));
    }

    #[test]
    fn lookup_peers_walks_compact_nodes_until_peer_values() {
        let seed = UdpSocket::bind("127.0.0.1:0").expect("seed socket binds");
        let closer = UdpSocket::bind("127.0.0.1:0").expect("closer socket binds");
        let seed_contact = DhtContact {
            address: "127.0.0.1".to_string(),
            port: seed.local_addr().expect("seed address").port(),
        };
        let closer_contact = DhtContact {
            address: "127.0.0.1".to_string(),
            port: closer.local_addr().expect("closer address").port(),
        };
        let query_node_id = *b"abcdefghij0123456789";
        let info_hash = *b"mnopqrstuvwxyz123456";
        let closer_node = DhtNode {
            id: *b"mnopqrstuvwxyz123000",
            address: closer_contact.address.clone(),
            port: closer_contact.port,
        };

        let seed_handle = thread::spawn(move || {
            let mut buffer = [0u8; 2048];
            let (length, client) = seed.recv_from(&mut buffer).expect("seed query arrives");
            let transaction_id = assert_get_peers_query(&buffer[..length], info_hash);
            let response = build_nodes_response(&transaction_id, *b"seednode-abcdefghijk", &[closer_node], b"tk");
            seed.send_to(&response, client).expect("seed response sends");
        });

        let closer_handle = thread::spawn(move || {
            let mut buffer = [0u8; 2048];
            let (length, client) = closer.recv_from(&mut buffer).expect("closer query arrives");
            let transaction_id = assert_get_peers_query(&buffer[..length], info_hash);
            let response = build_values_response(
                &transaction_id,
                *b"closenodeabcdefghijk",
                &[PeerInfo {
                    address: "127.0.0.1".to_string(),
                    port: 51413,
                    client: None,
                    progress: 0.0,
                    download_speed: 0,
                    upload_speed: 0,
                    connection: "Discovered".to_string(),
                }],
                b"tk",
            );
            closer.send_to(&response, client).expect("closer response sends");
        });

        let result = lookup_peers(
            &[seed_contact.clone()],
            query_node_id,
            info_hash,
            DhtLookupOptions {
                timeout: Duration::from_secs(2),
                max_queries: 4,
            },
        )
        .expect("lookup succeeds");

        seed_handle.join().expect("seed exits");
        closer_handle.join().expect("closer exits");
        assert_eq!(result.queried_nodes, 2);
        assert_eq!(result.discovered_nodes, 1);
        assert_eq!(result.peers.len(), 1);
        assert_eq!(result.peers[0].address, "127.0.0.1");
        assert_eq!(result.peers[0].port, 51413);
        assert_eq!(result.announce_targets.len(), 2);
        assert_eq!(result.announce_targets[0].contact, seed_contact);
        assert_eq!(result.announce_targets[1].contact, closer_contact);
        assert!(result
            .announce_targets
            .iter()
            .all(|target| target.token == b"tk"));
        assert!(result.errors.is_empty());
    }

    fn assert_get_peers_query(input: &[u8], expected_info_hash: [u8; 20]) -> Vec<u8> {
        let root = bencode::parse(input).expect("query bencode parses");
        assert_eq!(root.dict_get(b"y").and_then(BencodeNode::as_bytes), Some(&b"q"[..]));
        assert_eq!(
            root.dict_get(b"q").and_then(BencodeNode::as_bytes),
            Some(&b"get_peers"[..])
        );
        let args = root.dict_get(b"a").expect("query has arguments");
        assert_eq!(
            args.dict_get(b"info_hash").and_then(BencodeNode::as_bytes),
            Some(&expected_info_hash[..])
        );
        root.dict_get(b"t")
            .and_then(BencodeNode::as_bytes)
            .expect("query has transaction id")
            .to_vec()
    }

    fn build_nodes_response(transaction_id: &[u8], node_id: [u8; 20], nodes: &[DhtNode], token: &[u8]) -> Vec<u8> {
        let mut compact = Vec::new();
        for node in nodes {
            compact.extend_from_slice(&build_compact_node(node).expect("node compacts"));
        }
        let mut out = b"d1:rd2:id".to_vec();
        write_bytes(&mut out, &node_id);
        out.extend_from_slice(b"5:nodes");
        write_bytes(&mut out, &compact);
        out.extend_from_slice(b"5:token");
        write_bytes(&mut out, token);
        out.extend_from_slice(b"e1:t");
        write_bytes(&mut out, transaction_id);
        out.extend_from_slice(b"1:y1:re");
        out
    }

    fn build_values_response(transaction_id: &[u8], node_id: [u8; 20], peers: &[PeerInfo], token: &[u8]) -> Vec<u8> {
        let mut out = b"d1:rd2:id".to_vec();
        write_bytes(&mut out, &node_id);
        out.extend_from_slice(b"5:token");
        write_bytes(&mut out, token);
        out.extend_from_slice(b"6:valuesl");
        for peer in peers {
            write_bytes(&mut out, &compact_peer(peer));
        }
        out.extend_from_slice(b"ee1:t");
        write_bytes(&mut out, transaction_id);
        out.extend_from_slice(b"1:y1:re");
        out
    }

    fn compact_peer(peer: &PeerInfo) -> Vec<u8> {
        let mut out = Vec::new();
        for part in peer.address.split('.') {
            out.push(part.parse::<u8>().expect("IPv4 octet parses"));
        }
        out.extend_from_slice(&peer.port.to_be_bytes());
        out
    }

    fn node_id_with_first_byte(byte: u8) -> [u8; 20] {
        let mut id = [0u8; 20];
        id[0] = byte;
        id
    }

    fn node_id_with_last_byte(byte: u8) -> [u8; 20] {
        let mut id = [0u8; 20];
        id[19] = byte;
        id
    }
}
