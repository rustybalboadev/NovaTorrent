import type { TorrentDetails } from "@/lib/torrent-types";

export const mockTorrents: TorrentDetails[] = [
  {
    id: 1,
    info_hash: "0f3a6cf4a0c4dbceee5f5ff623672540ac5dbb78",
    name: "Ubuntu 26.04 Desktop ISO",
    output_folder: "C:\\Users\\rusty\\Downloads\\NovaTorrent",
    stats: {
      state: "live",
      progress_bytes: 2382364672,
      uploaded_bytes: 882901811,
      total_bytes: 5368709120,
      finished: false,
      live: {
        download_speed: 918552,
        upload_speed: 245812,
        time_remaining: 3228
      }
    },
    files: [
      {
        name: "ubuntu-26.04-desktop-amd64.iso",
        components: ["ubuntu-26.04-desktop-amd64.iso"],
        length: 5368709120,
        included: true
      }
    ]
  },
  {
    id: 2,
    info_hash: "ef93a76e91c9acb654db22f7c706551d7f7ad182",
    name: "Creative Commons Media Pack",
    output_folder: "D:\\Media\\Downloads",
    stats: {
      state: "paused",
      progress_bytes: 1300234240,
      uploaded_bytes: 19005440,
      total_bytes: 3292528640,
      finished: false,
      live: null
    },
    files: [
      {
        name: "Media Pack/Video/Launch Film.mp4",
        components: ["Media Pack", "Video", "Launch Film.mp4"],
        length: 1505759232,
        included: true
      },
      {
        name: "Media Pack/Audio/Interview.flac",
        components: ["Media Pack", "Audio", "Interview.flac"],
        length: 824180736,
        included: true
      },
      {
        name: "Media Pack/Extras/Posters/poster-a.png",
        components: ["Media Pack", "Extras", "Posters", "poster-a.png"],
        length: 3596610,
        included: false
      },
      {
        name: "Media Pack/Extras/Subtitles/en.srt",
        components: ["Media Pack", "Extras", "Subtitles", "en.srt"],
        length: 67042,
        included: true
      }
    ]
  },
  {
    id: 3,
    info_hash: "39d91f87f80ec1417f47982be2537a896a2299b0",
    name: "Public Domain Archive",
    output_folder: "C:\\Users\\rusty\\Downloads\\NovaTorrent",
    stats: {
      state: "complete",
      progress_bytes: 812646400,
      uploaded_bytes: 521273344,
      total_bytes: 812646400,
      finished: true,
      live: {
        download_speed: 0,
        upload_speed: 86192,
        time_remaining: null
      }
    },
    files: [
      {
        name: "Archive/books/alice.txt",
        components: ["Archive", "books", "alice.txt"],
        length: 174355,
        included: true
      },
      {
        name: "Archive/images/source-map.tif",
        components: ["Archive", "images", "source-map.tif"],
        length: 812472045,
        included: true
      }
    ]
  }
];

export const mockPreview: TorrentDetails = {
  id: null,
  info_hash: "5e5c81a2eec16b31a4d2d37d15bd20f2a2df7631",
  name: "Example Collection",
  output_folder: "C:\\Users\\rusty\\Downloads\\NovaTorrent",
  files: [
    {
      name: "Example Collection/readme.txt",
      components: ["Example Collection", "readme.txt"],
      length: 8192,
      included: true
    },
    {
      name: "Example Collection/source/video.mp4",
      components: ["Example Collection", "source", "video.mp4"],
      length: 734003200,
      included: true
    },
    {
      name: "Example Collection/source/stills/frame-0001.png",
      components: ["Example Collection", "source", "stills", "frame-0001.png"],
      length: 2097152,
      included: true
    },
    {
      name: "Example Collection/extras/checksums.sha256",
      components: ["Example Collection", "extras", "checksums.sha256"],
      length: 4096,
      included: false
    }
  ],
  stats: null
};
