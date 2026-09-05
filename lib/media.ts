const playableMediaExtensions = new Set(["mp4", "m4v", "mov", "webm", "mkv", "ogv", "avi"]);

export function isPlayableMediaName(name: string) {
  const leafName = name.split(/[\\/]/).at(-1) ?? name;
  const extension = leafName.split(".").pop()?.toLowerCase();
  return Boolean(extension && playableMediaExtensions.has(extension));
}
