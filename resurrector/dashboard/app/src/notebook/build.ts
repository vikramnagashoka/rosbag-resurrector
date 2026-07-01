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

// One starter notebook per indexed bag. The rail's "+" adds blank
// investigations on top of these.
export function notebooksFromBags(bags: Bag[]): Notebook[] {
  return bags.map(b => {
    const file = basename(b.path)
    const score = b.health_score ?? 0
    return {
      id: `nb-bag-${b.id}`,
      title: prettyTitle(file),
      bag: file,
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
      cells: [],
    }
  })
}
