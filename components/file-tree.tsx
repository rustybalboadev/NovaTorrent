"use client";

import * as React from "react";
import { ChevronRight, FileText, Folder, FolderOpen } from "lucide-react";
import { Checkbox } from "@/components/ui/checkbox";
import { cn, formatBytes } from "@/lib/utils";
import type { TorrentFile } from "@/lib/torrent-types";

type FileNode = {
  id: string;
  name: string;
  type: "folder" | "file";
  size: number;
  fileIds: number[];
  children: FileNode[];
};

type FileTreeProps = {
  files: TorrentFile[];
  selectedFileIds: Set<number>;
  onSelectionChange?: (selected: Set<number>) => void;
  readonly?: boolean;
  compact?: boolean;
};

export function FileTree({ files, selectedFileIds, onSelectionChange, readonly, compact }: FileTreeProps) {
  const tree = React.useMemo(() => buildTree(files), [files]);
  const [expanded, setExpanded] = React.useState<Set<string>>(() => new Set());

  const toggleExpanded = (id: string) => {
    setExpanded((current) => {
      const next = new Set(current);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  const toggleNode = (node: FileNode, checked: boolean) => {
    if (readonly || !onSelectionChange) return;
    const next = new Set(selectedFileIds);
    node.fileIds.forEach((fileId) => {
      if (checked) {
        next.add(fileId);
      } else {
        next.delete(fileId);
      }
    });
    onSelectionChange(next);
  };

  if (!files.length) {
    return (
      <div className="flex h-full items-center justify-center rounded-md border border-dashed p-6 text-sm text-muted-foreground">
        No file metadata yet.
      </div>
    );
  }

  return (
    <div className={cn("space-y-0.5", compact && "text-sm")}>
      {tree.children.map((node) => (
        <TreeNode
          key={node.id}
          node={node}
          depth={0}
          expanded={expanded}
          selectedFileIds={selectedFileIds}
          readonly={readonly}
          onToggleExpanded={toggleExpanded}
          onToggleNode={toggleNode}
        />
      ))}
    </div>
  );
}

function TreeNode({
  node,
  depth,
  expanded,
  selectedFileIds,
  readonly,
  onToggleExpanded,
  onToggleNode
}: {
  node: FileNode;
  depth: number;
  expanded: Set<string>;
  selectedFileIds: Set<number>;
  readonly?: boolean;
  onToggleExpanded: (id: string) => void;
  onToggleNode: (node: FileNode, checked: boolean) => void;
}) {
  const isExpanded = expanded.has(node.id);
  const checkboxState = getCheckboxState(node, selectedFileIds);
  const Icon = node.type === "folder" ? (isExpanded ? FolderOpen : Folder) : FileText;

  return (
    <div>
      <div
        className="grid min-h-8 grid-cols-[auto_1fr_auto] items-center gap-2 rounded-md px-2 hover:bg-secondary/65"
        style={{ paddingLeft: `${depth * 18 + 8}px` }}
      >
        <div className="flex items-center gap-1">
          {node.type === "folder" ? (
            <button
              type="button"
              aria-label={isExpanded ? "Collapse folder" : "Expand folder"}
              className="flex h-6 w-6 items-center justify-center rounded hover:bg-background"
              onClick={() => onToggleExpanded(node.id)}
            >
              <ChevronRight className={cn("h-4 w-4 transition-transform", isExpanded && "rotate-90")} />
            </button>
          ) : (
            <span className="h-6 w-6" />
          )}
          <Checkbox
            checked={checkboxState}
            disabled={readonly}
            onCheckedChange={(checked) => onToggleNode(node, checked === true)}
          />
        </div>
        <div className="flex min-w-0 items-center gap-2">
          <Icon className={cn("h-4 w-4 shrink-0", node.type === "folder" ? "text-accent" : "text-muted-foreground")} />
          <span className="truncate text-sm">{node.name}</span>
        </div>
        <span className="text-xs tabular-nums text-muted-foreground">{formatBytes(node.size)}</span>
      </div>
      {node.type === "folder" && isExpanded ? (
        <div>
          {node.children.map((child) => (
            <TreeNode
              key={child.id}
              node={child}
              depth={depth + 1}
              expanded={expanded}
              selectedFileIds={selectedFileIds}
              readonly={readonly}
              onToggleExpanded={onToggleExpanded}
              onToggleNode={onToggleNode}
            />
          ))}
        </div>
      ) : null}
    </div>
  );
}

function buildTree(files: TorrentFile[]): FileNode {
  const root: FileNode = {
    id: "root",
    name: "root",
    type: "folder",
    size: 0,
    fileIds: [],
    children: []
  };

  files.forEach((file, index) => {
    const components = file.components.length ? file.components : file.name.split(/[\\/]/).filter(Boolean);
    let cursor = root;
    components.forEach((component, componentIndex) => {
      const isFile = componentIndex === components.length - 1;
      const id = `${cursor.id}/${component}`;
      let child = cursor.children.find((node) => node.name === component && node.type === (isFile ? "file" : "folder"));
      if (!child) {
        child = {
          id,
          name: component,
          type: isFile ? "file" : "folder",
          size: 0,
          fileIds: [],
          children: []
        };
        cursor.children.push(child);
      }
      child.size += file.length;
      child.fileIds.push(index);
      cursor = child;
    });
    root.size += file.length;
    root.fileIds.push(index);
  });

  sortNode(root);
  return root;
}

function sortNode(node: FileNode) {
  node.children.sort((a, b) => {
    if (a.type !== b.type) return a.type === "folder" ? -1 : 1;
    return a.name.localeCompare(b.name);
  });
  node.children.forEach(sortNode);
}

function getCheckboxState(node: FileNode, selectedFileIds: Set<number>) {
  const selected = node.fileIds.filter((fileId) => selectedFileIds.has(fileId)).length;
  if (selected === 0) return false;
  if (selected === node.fileIds.length) return true;
  return "indeterminate";
}
