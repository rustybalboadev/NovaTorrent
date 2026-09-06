"use client";

import { AddTorrentPanel } from "@/components/add-torrent-panel";

export default function AddTorrentPage() {
  return (
    <main className="h-full min-h-0 bg-background p-4">
      <AddTorrentPanel windowMode />
    </main>
  );
}
