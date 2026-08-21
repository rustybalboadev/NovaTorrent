"use client";

import { AddTorrentPanel } from "@/components/add-torrent-panel";

export default function AddTorrentPage() {
  return (
    <main className="min-h-screen bg-background p-4">
      <AddTorrentPanel windowMode />
    </main>
  );
}
