# macOS startup investigation

## Scope and reproduction

Investigated upstream `768aa43` on macOS 26.5.1 (25F80), Apple silicon,
using Node 24.16.0 and the installed Rust nightly toolchain. Dependencies remain
locked; no framework or application dependency versions were changed.

The GitHub v1.0.2 release inspected on September 7, 2026 contains only an EXE
and MSI. Its release workflow is Windows-only. The originally reported DMG was
not available, so reproduction used a locally packaged release build from the
current source. This distinction matters: the findings establish a reproducible
source-level failure, not the provenance or signing status of that original DMG.

The first `cargo test --locked` on macOS failed in `tauri::generate_context!()`:
`failed to open icon .../src-tauri/icons/icon.png: No such file or directory`.
Only `icon.ico` was tracked. After supplying a PNG derived from that ICO, the
otherwise unchanged application could be packaged with
`npm run tauri -- build --bundles app,dmg`.

Launching that baseline bundle through macOS reproduced the reported behavior:
the process remained alive, backend listeners started, and CoreGraphics listed
its 1280×820 main window, but the window was not on screen. A one-second stack
sample showed the main thread idle in the AppKit event loop, rather than blocked
in backend setup. The committed smoke check also failed against the baseline
app copied out of its DMG: no persistent visible main window within 30 seconds.

## Cause and changes

The main window is configured with `visible: false`. Its native reveal waits for
`PageLoadEvent::Finished`; a second reveal in React waits for initialization and
`requestAnimationFrame`. Both depend on webview progress. On the tested macOS
system those paths did not reveal the initially hidden window. Showing the
native window from setup resolves the reproduced failure. This evidence does
not establish a specific WebKit internal scheduling defect.

- Native macOS setup now shows, restores, and focuses the main window without
  waiting for page load or animation frames. Errors propagate through setup.
- Add Torrent and media windows start visible on macOS for the same reason.
  Their existing page-load handling and Windows visibility behavior are retained.
- A shared helper restores an existing main window or recreates it from the
  original Tauri configuration if it was closed while another window remained.
  macOS Dock/Launch Services reopen events call it. Secondary launches without
  torrent sources also restore the main window instead of silently returning.
- Added PNG and ICNS assets generated with `sips` and `tauri icon` from the
  existing ICO artwork. The original Windows icon is unchanged.
- Added the automatically loaded `tauri.macos.conf.json` with app/DMG targets
  and macOS icons, plus an explicit ad-hoc bundle signing identity (`-`).
  The default build only linker-signs the executable and leaves the bundle
  resources unsigned; the explicit identity allows bundle integrity verification.
  Windows installer configuration remains in the base config.
- CI now runs Rust tests on macOS and builds a DMG, mounts it read-only, copies
  out the app, and launches that copy through Launch Services. The native smoke
  check requires a substantial on-screen window belonging to the launched PID
  for two seconds, with a 30-second deadline. It refuses an already running app
  to avoid a false pass. A logged-in graphical desktop session is required.
- Release tags now also build a universal macOS app/DMG containing Apple silicon
  and Intel executable slices. README documents native and universal builds.

## Validation

- Baseline app copied from DMG: visibility regression check **failed**, as expected.
- Fixed native app copied from DMG: visibility regression check **passed**.
- Dashboard verified through the macOS accessibility tree: torrent list, status
  filter, search, theme control, and Add Torrent controls rendered.
- Clicking Add Torrent opened a visible secondary window, exercising frontend
  interaction and the Rust window-creation command.
- Minimized main window reported `AXMinimized=true`; reopening the same app
  through Launch Services restored it to `AXMinimized=false`.
- Closed the main window with Add Torrent still open; reopening recreated the
  dashboard while retaining Add Torrent.
- `cargo test --locked --manifest-path src-tauri/Cargo.toml`: **202 passed**.
- `npm run lint`, TypeScript validation during the production Next.js build,
  and `cargo fmt --all --check`: passed. Existing Rust dead-code warnings remain.
- Native app/DMG and universal app/DMG release builds succeeded.
- Final universal app copied from its DMG passed the visibility check and
  `codesign --verify --deep --strict`; `lipo -archs` reports `x86_64 arm64`.

## Limits and follow-up

The smoke check establishes native visibility, not a complete torrent or media
playback test. No external torrent was downloaded during validation. Intel
hardware runtime behavior and Windows runtime behavior require their respective
machines; the universal build validates both macOS compilation targets.

These bundles are ad-hoc signed by the build tooling, not Developer ID signed
or notarized. This change does not bypass Gatekeeper or add signing credentials.
Distribution signing/notarization is a separate release-owner task. No public
release has been published as part of this fix.
