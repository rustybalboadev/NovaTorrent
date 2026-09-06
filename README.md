<p align="center">
  <img src="public/novatorrent-logo.png" alt="NovaTorrent" width="160">
</p>

<h1 align="center">NovaTorrent</h1>

<p align="center">
  <img src="public/novatorrent-dash.png" alt="NovaTorrent" width="1000">
</p>


<p align="center">
  A fast, modern BitTorrent client for Windows, Linux, and macOS with built-in media streaming.
</p>

<p align="center">
  <a href="https://github.com/rustybalboadev/NovaTorrent/releases/latest">Download</a>
  ·
  <a href="#features">Features</a>
  ·
  <a href="#run-from-source">Run from source</a>
  ·
  <a href="#build-a-desktop-release">Build</a>
</p>

<p align="center">
  NovaTorrent is a cross-platform BitTorrent client built with Rust, Tauri, React, and Next.js. It downloads torrents from magnet links or .torrent files, supports resumable downloads and per-file controls, and can play media while it downloads.
</p>

## Download and install

Download the current release from [GitHub Releases](https://github.com/rustybalboadev/NovaTorrent/releases/latest).

1. Expand **Assets** on the latest release.
2. Download the package for your system:
   - **Windows x64:** `NovaTorrent_*_x64-setup.exe` is recommended for most users; the `.msi` is intended for managed installation.
   - **Linux x64:** use the `.AppImage` for a portable app or the `.deb` package on Debian and Ubuntu-based systems.
   - **macOS:** use the `.dmg` matching Apple Silicon (`aarch64`) or Intel (`x64`).

NovaTorrent releases are not currently signed with a trusted developer identity or notarized. Windows may show a SmartScreen warning, and macOS may require manually allowing NovaTorrent in **System Settings > Privacy & Security** after the first launch. The macOS build uses only the ad-hoc signature required to produce a runnable app; it does not establish publisher trust. Only install builds downloaded from this repository, or build the application from source.

## Features

- Add magnet links and `.torrent` files with a complete file preview before downloading.
- Discover peers through HTTP, HTTPS, and UDP trackers, DHT, peer exchange, local peer discovery, and magnet peer hints.
- Download from multiple peers concurrently with adaptive request pipelining, rarest-first selection, endgame requests, connection reuse, and stalled-peer recovery.
- Verify every completed piece against the torrent's SHA-1 piece hashes.
- Resume verified partial downloads after pausing, closing, or restarting NovaTorrent.
- Set per-torrent connection limits, bandwidth limits, sequential mode, seed ratios, file selection, and per-file priority.
- Inspect torrent state, peers, trackers, web seeds, individual file progress, piece availability, options, and filterable logs.
- Stream supported media before the torrent finishes, with seek-aware piece priority, remembered playback positions, a playable-file queue, and subtitle selection.
- Recheck existing files and preserve safe, predictable download folder layouts.
- Seed completed torrents through a bounded inbound listener with fair upload-slot rotation.
- Open `.torrent` files from supported desktop integrations and register `magnet:` links on Windows.

## Run from source

### Development requirements

Building NovaTorrent requires:

- [Git](https://git-scm.com/download/win).
- [Node.js](https://nodejs.org/) 20.9 or newer. npm is included with Node.js.
- [Rust](https://www.rust-lang.org/tools/install) 1.77.2 or newer.
- The platform prerequisites for [Windows](https://v2.tauri.app/start/prerequisites/#windows), [Linux](https://v2.tauri.app/start/prerequisites/#linux), or [macOS](https://v2.tauri.app/start/prerequisites/#macos).

The official [Tauri prerequisites guide](https://v2.tauri.app/start/prerequisites/) contains detailed setup instructions for each operating system.

### Clone and start NovaTorrent

Open PowerShell and run:

```powershell
git clone https://github.com/rustybalboadev/NovaTorrent.git
Set-Location NovaTorrent
npm ci
npm run tauri:dev
```

`npm ci` installs the exact dependency versions recorded in `package-lock.json`. The first Tauri launch can take several minutes because Rust dependencies must be compiled.

## Build a desktop release

Install all [development requirements](#development-requirements), then run:

```powershell
npm ci
npm run tauri:build
```

Tauri automatically selects the platform configuration and creates the appropriate packages:

```text
Windows: .exe and .msi
Linux:  .AppImage and .deb
macOS:  .app and .dmg
```

Desktop packages must be built on their corresponding operating system. Tagged releases are built for all supported targets by GitHub Actions. Linux AppImages include the media framework needed for playback, so they are larger than the Debian package and the other platform downloads.

## License

NovaTorrent is released under the [MIT License](LICENSE).
