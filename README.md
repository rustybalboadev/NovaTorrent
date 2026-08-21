# NovaTorrent

NovaTorrent is a desktop BitTorrent client built with a Rust/Tauri backend and a Next.js, React, TypeScript frontend.

## Stack

- Rust + Tauri v2 for the native app, filesystem, deep links, file associations, and torrent session commands.
- Manual BitTorrent protocol modules in Rust for bencode, metainfo, magnet parsing, tracker announces, DHT KRPC packets and peer lookup, peer handshakes, peer-wire downloads, DHT `PORT` messages, magnet metadata messages, webseeds, multi-file storage, and session state.
- Next.js static export for Tauri packaging.
- React + TypeScript for the UI.
- Tailwind CSS with shadcn-style Radix UI primitives.

## Features in this scaffold

- Modern light/dark UI with a torrent grid and live summary counters.
- Custom right-click menu on torrents for pause, continue, announce trackers, query DHT, resolve magnet metadata, fetch metadata, download, recheck, remove, delete files, and options.
- Add-torrent window route at `/add/` with torrent file, magnet link, destination, and advanced options.
- Native Tauri commands for adding, previewing, listing, pausing, resuming, explicitly announcing trackers, querying DHT, resolving magnet metadata, downloading from discovered peers, deleting, updating file selections, reading backend logs, and showing the backend log file path.
- Unpaused additions and Continue launch independent background torrent workers. Each worker verifies existing files, resolves magnet metadata, discovers peers, downloads from peers, and uses a verified webseed fallback when available; duplicate workers for the same torrent are suppressed.
- Pause cancellation is propagated into manual peer-wire and HTTP webseed transfers between protocol reads/piece requests.
- Manual BEP 3 peer-wire downloader that validates bitfields, retains peer availability, assigns missing pieces rarest-first, respects each torrent's connection ceiling while using at most eight outbound peers concurrently, pipelines eight 16 KiB block requests per piece, accepts out-of-order responses, cancels pending requests on pause, reassigns work after peer failure, and verifies every accepted piece. Near completion it races scarce remaining pieces across eligible peers, cancels losing block pipelines, and reuses healthy sockets for the next tail piece.
- Manual peer-ID decoding for common Azureus-style client IDs from tracker dictionaries and live peer handshakes, so the Peers inspector can show readable client names such as qBittorrent, Transmission, Deluge, or NovaTorrent when peers identify themselves.
- App-level inbound TCP listener that tries ports 6881-6889, admits at most 64 active sockets before creating workers, routes handshakes by info hash, verifies stored data, advertises available pieces, and serves valid BEP 3 block requests through four upload slots per torrent. Interested peers wait in FIFO order while all slots are occupied; active peers yield ten-second leases when a waiter exists, then rejoin the queue for fair optimistic rotation.
- Shared per-torrent download and upload limiters reserve byte windows across concurrent connections, cover peer-wire plus HTTP webseed downloads, wake in short cancellation-aware intervals, and can be changed while the session is running. Sequential mode switches piece assignment from rarest-first to ascending index order, and completed torrents reject new upload sessions after their configured seed ratio is reached.
- Tracker announces use the listener's actual bound port and a background scheduler honors tracker intervals. Runtime announces follow `started`, regular, `completed`, and `stopped` lifecycle events; pause/delete send `stopped`, and inbound uploads update torrent totals, ratio, peer state, and readable logs.
- Manual BEP 5 DHT `get_peers` lookup that walks compact node responses, deduplicates compact peers, feeds discovered peers into the torrent session, retains node-specific tokens, and advertises completed public torrents with `announce_peer` using the active listener port.
- Long-lived inbound BEP 5 UDP node for `ping`, `find_node`, `get_peers`, and `announce_peer`. It learns IPv4 contacts, returns closest verified nodes or known compact peers, rejects private torrent info hashes, validates rotating five-minute IP-bound tokens, and keeps a bounded 30-minute peer cache.
- Health-aware DHT routing with BEP 5 good/questionable evidence, two-failure eviction, pending replacement for full buckets, bounded one-minute maintenance, startup self-lookup for the local node ID, 15-minute in-range `find_node` refreshes, and concurrent liveness checks.
- Versioned `novatorrent-dht.json` persistence keeps a stable node ID and up to 256 vetted IPv4 contacts. Restored contacts remain questionable and are never served until they answer a new ping.
- Live BEP 5 peer-wire DHT negotiation for outbound downloads, inbound seeding, and BEP 9 metadata connections. NovaTorrent exchanges `PORT` only after mutual capability advertisement, suppresses DHT signaling for known private torrents, and concurrently pings peer-advertised UDP endpoints before admitting their returned node IDs to routing.
- Manual BEP 9/BEP 10 magnet metadata fetch from extension-capable peers, with info-hash verification before populating a magnet torrent.
- Explicit magnet resolver that chains DHT peer lookup into BEP 9 metadata fetch for magnet links.
- Multi-file torrent storage that recreates the torrent folder hierarchy, protects existing files unless overwrite is enabled, and skips unchecked files while preserving torrent byte offsets.
- Recheck files action that hashes stored data back into piece and file progress for resume and future seeding.
- Manual plain-HTTP BEP 19 webseed downloads for single-file and multi-file torrents, with cross-file piece hash verification, selected-file output, cancellation, and shared bandwidth limiting. The single-file path is verified live against the Alpine minirootfs torrent.
- Collapsible file/folder hierarchy with folder and file checkboxes.
- Concurrent multi-torrent download workers through NovaTorrent's Rust session API, verified with two simultaneous localhost swarms.
- Piece-level multi-peer swarm assembly, verified with one peer disconnecting after its first contribution and a second peer supplying the missing pieces.
- Concurrent outbound swarm scheduling is verified with delayed localhost peers: transfers overlap, and a healthy open connection takes over an abandoned piece after another worker disconnects.
- Durable `.novatorrent` partial-piece storage keyed by info hash. Each piece is SHA-1 checked before its random-access write is recorded, marked pieces are rechecked on reopen, missing pieces resume after a session/app restart, and completed peer downloads stream selected output files from the verified store without allocating a whole-torrent output buffer.
- Versioned `novatorrent-session.json` persistence for torrent sources, destinations, paused state, file selection, overwrite/tracker options, connection and bandwidth limits, sequential mode, seed ratio, and stable IDs. Valid entries restore at launch and unpaused entries restart background workers.
- Bottom resizable inspector for status, general info, peers, trackers, web seeds, files, security, options, and logs.
- Privacy-preserving file reputation workflow for completed selected files: NovaTorrent streams a local SHA-256 digest, never uploads the file, stores no VirusTotal API key, and opens the public VirusTotal hash report only after an explicit user action.
- Editable Options inspector for connection limits, peer download/upload limits, piece ordering, and seed ratio. The torrent context menu selects the torrent and opens this pane directly.
- Readable `novatorrent.log` file written beside the default NovaTorrent download folder for bug review.
- `.torrent` file association plus validated startup and second-instance handling for `magnet:`, `novatorrent:`, and real `.torrent` paths. Windows NSIS/WiX installer definitions register app-specific protocol handlers without a deep-link plugin; `magnet:` remains an explicit Windows Default Apps choice instead of silently replacing another client.

## Local Development

Install dependencies:

```bash
npm install
```

Run the frontend only:

```bash
npm run dev
```

Run the Tauri app:

```bash
npm run tauri:dev
```

Build the frontend static export:

```bash
npm run build
```

Build the desktop app:

```bash
npm run tauri:build
```

Run the Rust protocol tests:

```bash
cd src-tauri
cargo test
```

Run the live safe webseed download test:

```bash
cd src-tauri
cargo test downloads_alpine_safe_fixture_from_http_webseed -- --ignored --nocapture
```

## Rust Toolchain

This project needs Rust 1.77.2 or newer for Tauri v2 and its plugins. Install Rust from https://rustup.rs, then restart the terminal so `cargo` is on PATH.

## Website/Open-With Integration

NovaTorrent is configured for:

- `magnet:?xt=urn:btih:...`
- `novatorrent://open?magnet=...`
- `.torrent` files with MIME type `application/x-bittorrent`

Installed desktop builds register `.torrent` through the Tauri bundle configuration. The Windows installers register NovaTorrent as an available `magnet:` handler and directly register the private `novatorrent:` scheme. Windows still lets the user choose the default magnet client. Incoming arguments are length-limited, validated, deduplicated, and resolved against the launching process directory; malformed magnets, unsupported wrapper routes, control characters, missing files, and non-torrent paths are rejected before opening the Add Torrent window.

## Test Fixture

`test serum torrent.torrent` is used as a parser fixture only. Do not use it for download tests unless you own or are authorized to download that content.

For fast protocol testing, use `fixtures/safe/alpine-minirootfs-3.23.3-x86_64.tar.gz.torrent`. The payload is about 3.6 MiB. NovaTorrent's live ignored webseed test downloads it through the manual HTTP webseed path and verifies every BEP 3 piece hash before accepting the file. The Debian netinst fixture remains available as a larger official-distribution fallback. These fixtures are metadata only; NovaTorrent should not auto-download payloads unless a developer explicitly starts an integration test.

See `docs/plan.md` for the live implementation plan, feature research, and verification checklist.

## File Reputation Safety

The Security inspector hashes only completed, selected regular files under the verified torrent output root. It rejects symlinks, path escapes, unexpected file sizes, and incomplete torrents before reading. Checking a hash is entirely local. Choosing **Open report** then shares only the 64-character SHA-256 value with VirusTotal; NovaTorrent does not upload files or submit them for scanning.
