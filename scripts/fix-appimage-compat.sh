#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <AppImage>" >&2
  exit 2
fi

appimage="$(readlink -f "$1")"
if [[ ! -f "$appimage" ]]; then
  echo "AppImage not found: $appimage" >&2
  exit 2
fi

work_dir="$(mktemp -d)"
cp "$appimage" "$work_dir/original.AppImage"
chmod +x "$work_dir/original.AppImage"
cd "$work_dir"
./original.AppImage --appimage-extract >/dev/null

lib_dir="$work_dir/squashfs-root/usr/lib"
if [[ ! -d "$lib_dir" ]]; then
  echo "AppImage library directory not found: $lib_dir" >&2
  exit 1
fi

patterns=(
  'libwayland-*.so*'
  'libglib-2.0.so*'
  'libgio-2.0.so*'
  'libgobject-2.0.so*'
  'libgmodule-2.0.so*'
  'libmount.so*'
  'libblkid.so*'
  'libselinux.so*'
  'libpcre2-8.so*'
  'libzstd.so*'
  'libelf.so*'
  'libffi.so*'
)

for pattern in "${patterns[@]}"; do
  find "$lib_dir" -maxdepth 1 -type f -name "$pattern" -print -delete
done

curl --fail --location --silent --show-error \
  https://github.com/AppImage/appimagetool/releases/download/continuous/appimagetool-x86_64.AppImage \
  --output appimagetool.AppImage
chmod +x appimagetool.AppImage
ARCH=x86_64 APPIMAGE_EXTRACT_AND_RUN=1 ./appimagetool.AppImage squashfs-root rebuilt.AppImage
chmod +x rebuilt.AppImage
mv --force rebuilt.AppImage "$appimage"

echo "Repacked AppImage with host-coupled libraries excluded: $appimage"
