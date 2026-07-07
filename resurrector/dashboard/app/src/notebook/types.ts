// Notebook UI overhaul — core types (v0.8).
//
// The notebook model is the design DNA: every analysis capability is one
// more cell type, so the surface scales by adding cell renderers, not pages.

export type CellType = 'plot' | 'stats' | 'sync' | 'health' | 'image' | 'search' | 'scene' | 'transform'

export interface Cell {
  id: string
  type: CellType
  topic?: string
  topics?: string[]
  frame?: number
  query?: string
  // transform cell: derived-series op on one topic column
  op?: string
  column?: string
  expression?: string
}

export type HealthTier = 'good' | 'warn' | 'bad'

// A rail folder for organizing notebooks. In-memory only — the notebook
// model itself isn't persisted server-side, so neither is its grouping.
export interface Folder {
  id: string
  name: string
}

export interface Notebook {
  id: string
  title: string
  folderId?: string | null  // null / undefined = top level (ungrouped)
  bag: string          // bag filename shown in the rail + header chip
  bagId?: number       // backend bag id (wired in later PRs)
  health: number       // 0–100 score
  tier: HealthTier
  durationLabel: string // e.g. "42.8s"
  durationSec: number
  startNs: number       // bag start timestamp (for scene time queries)
  topicCount: number
  messageCount: number
  bagTopics: { name: string; messageType: string; messageCount: number }[]
  cells: Cell[]
}

const IMAGE_TYPES = new Set([
  'sensor_msgs/msg/Image', 'sensor_msgs/msg/CompressedImage',
])
const POINTCLOUD_TYPES = new Set(['sensor_msgs/msg/PointCloud2'])

// Image topics aren't line-plottable; everything else is offered in the
// plot/stats topic dropdown (numeric extraction happens after fetch).
export function plottableTopics(nb: Pick<Notebook, 'bagTopics'>): string[] {
  return nb.bagTopics.filter(t => !IMAGE_TYPES.has(t.messageType)).map(t => t.name)
}
export function imageTopics(nb: Pick<Notebook, 'bagTopics'>): string[] {
  return nb.bagTopics.filter(t => IMAGE_TYPES.has(t.messageType)).map(t => t.name)
}
export function pointcloudTopics(nb: Pick<Notebook, 'bagTopics'>): string[] {
  return nb.bagTopics.filter(t => POINTCLOUD_TYPES.has(t.messageType)).map(t => t.name)
}
export function topicMessageCount(nb: Pick<Notebook, 'bagTopics'>, topic: string): number {
  return nb.bagTopics.find(t => t.name === topic)?.messageCount ?? 0
}

export function tierForScore(score: number): HealthTier {
  if (score >= 90) return 'good'
  if (score >= 80) return 'warn'
  return 'bad'
}

// The mono command string shown in each cell header. Mirrors the
// pandas-like API the cell represents.
export function cellCommand(cell: Cell): string {
  switch (cell.type) {
    case 'health': return 'bf.health().report()'
    case 'plot': return `bf["${cell.topic ?? '/topic'}"].plot()`
    case 'stats': return `bf["${cell.topic ?? '/topic'}"].stats()`
    case 'sync': return `bf.sync([${(cell.topics ?? []).map(t => `"${t}"`).join(', ')}]).head()`
    case 'image': return `bf["${cell.topic ?? '/camera/rgb'}"].frame(${cell.frame ?? 0})`
    case 'search': return `search("${cell.query ?? ''}")`
    case 'scene': return `bf["${cell.topic ?? '/lidar/points'}"].scene()`
    case 'transform': {
      const t = cell.topic ?? '/topic'
      if (cell.expression) return `bf["${t}"].transform(${JSON.stringify(cell.expression)})`
      const col = cell.column ? `"${cell.column}"` : ''
      return `bf["${t}"].${cell.op ?? 'derivative'}(${col})`
    }
  }
}

let _cellSeq = 0
export function nextCellId(): string {
  return `cell-${Date.now()}-${_cellSeq++}`
}

let _folderSeq = 0
export function nextFolderId(): string {
  return `folder-${Date.now()}-${_folderSeq++}`
}
