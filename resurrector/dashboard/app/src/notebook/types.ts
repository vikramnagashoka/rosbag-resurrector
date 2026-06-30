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
