// Notebook UI overhaul — core types (v0.8).
//
// The notebook model is the design DNA: every analysis capability is one
// more cell type, so the surface scales by adding cell renderers, not pages.

export type CellType = 'plot' | 'stats' | 'sync' | 'health' | 'image' | 'search' | 'scene'

export interface Cell {
  id: string
  type: CellType
  topic?: string
  topics?: string[]
  frame?: number
  query?: string
}

export type HealthTier = 'good' | 'warn' | 'bad'

export interface Notebook {
  id: string
  title: string
  bag: string          // bag filename shown in the rail + header chip
  bagId?: number       // backend bag id (wired in later PRs)
  health: number       // 0–100 score
  tier: HealthTier
  durationLabel: string // e.g. "42.8s"
  durationSec: number
  topicCount: number
  messageCount: number
  cells: Cell[]
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
  }
}

let _cellSeq = 0
export function nextCellId(): string {
  return `cell-${Date.now()}-${_cellSeq++}`
}
