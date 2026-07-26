import { Bag } from '../api'
import { Notebook, tierForScore } from './types'

function basename(path: string): string {
  return path.split(/[/\\]/).pop() || path
}

function prettyTitle(filename: string): string {
  // "explain-demo.mcap" -> "explain-demo". Investigations get a default
  // title from the bag stem; the user can rename later.
  return filename.replace(/\.(mcap|bag|db3)$/i, '')
}

function formatDuration(sec: number): string {
  if (sec < 60) return `${sec.toFixed(1)}s`
  const m = Math.floor(sec / 60)
  const s = Math.round(sec % 60)
  return `${m}m ${String(s).padStart(2, '0')}s`
}

// The bag-derived fields of a notebook (everything except id / folderId /
// title / cells). Shared by the starter-notebook builder and by attaching a
// bag to a blank investigation.
export function bagFields(b: Bag) {
  const score = b.health_score ?? 0
  return {
    bag: basename(b.path),
    bagId: b.id,
    health: score,
    tier: tierForScore(score),
    durationLabel: formatDuration(b.duration_sec),
    durationSec: b.duration_sec,
    startNs: b.start_time_ns ?? 0,
    topicCount: b.topics.length,
    messageCount: b.message_count,
    bagTopics: b.topics.map(t => ({
      name: t.name, messageType: t.message_type, messageCount: t.message_count,
    })),
  }
}

// One starter notebook per indexed bag. The rail's "+" adds blank
// investigations on top of these.
export function notebooksFromBags(bags: Bag[]): Notebook[] {
  return bags.map(b => ({
    id: `nb-bag-${b.id}`,
    title: prettyTitle(basename(b.path)),
    ...bagFields(b),
    cells: [],
  }))
}

// Attach an indexed bag to an existing (usually blank) notebook, preserving
// its identity, folder, and any cells. A still-default title adopts the bag
// stem so the rail stops reading "Untitled investigation".
export function attachBagToNotebook(nb: Notebook, b: Bag): Notebook {
  const retitle = nb.title === 'Untitled investigation'
  return {
    ...nb,
    ...bagFields(b),
    title: retitle ? prettyTitle(basename(b.path)) : nb.title,
  }
}
