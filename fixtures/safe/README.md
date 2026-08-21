# Safe Torrent Fixtures

These fixtures are legal test metadata used for NovaTorrent parser and tracker development. They are `.torrent` files only; the payload content is not included.

## Alpine Minirootfs

- File: `alpine-minirootfs-3.23.3-x86_64.tar.gz.torrent`
- Torrent metadata source: `https://fosstorrents.com/files/download.php?file=alpine-minirootfs-3.23.3-x86_64.tar.gz.torrent`
- Torrent metadata SHA-256: `8DABC8875DE14A68C587F32F07AF4B667ABC56327B35F1A7748E08A690B855A8`
- Payload name: `alpine-minirootfs-3.23.3-x86_64.tar.gz`
- Payload size: `3713234` bytes
- Official Alpine payload SHA-256: `42d0e6d8de5521e7bf92e075e032b5690c1d948fa9775efa32a51a38b25460fb`
- Announce URL: `udp://fosstorrents.com:6969/announce`
- Notes: This is the preferred quick integration fixture because the payload is only about 3.6 MiB. The torrent metadata is from FOSS Torrents and includes Alpine mirror web seeds; verify the downloaded payload against Alpine's official checksum.

## Debian Netinst

- File: `debian-13.6.0-amd64-netinst.iso.torrent`
- Source: `https://cdimage.debian.org/debian-cd/current/amd64/bt-cd/debian-13.6.0-amd64-netinst.iso.torrent`
- Fixture SHA-256: `763E5F84C8AFF61DA94F20604E078900825AB8C4D44DC66D6B9DE73C5BE29976`
- Payload name: `debian-13.6.0-amd64-netinst.iso`
- Payload size: `791674880` bytes
- Announce URL: `http://bttracker.debian.org:6969/announce`

Use the Alpine fixture for fast parser, tracker announce, peer handshake, and eventual end-to-end download tests. Keep Debian as a larger official-distribution fallback. Tests should still avoid auto-downloading payloads unless a developer explicitly starts an integration test.
