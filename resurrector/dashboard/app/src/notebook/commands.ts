import {
  Cell, Notebook, nextCellId,
  plottableTopics, imageTopics, pointcloudTopics,
} from './types'

// A runnable command in the palette. Each maps to a cell it appends.
export interface CommandEntry {
  id: string
  cmd: string          // display command, e.g. bf["/imu/data"].plot()
  kind: string         // cell kind tag
  description: string
  keywords: string     // extra match terms
  makeCell: () => Cell
}

// Build the topic-aware command catalog for the active notebook. Filtering
// happens in the palette against cmd + description + keywords.
export function buildCatalog(nb: Notebook | null): CommandEntry[] {
  if (!nb) return []
  const out: CommandEntry[] = []

  out.push({
    id: 'health', cmd: 'bf.health().report()', kind: 'health',
    description: 'Health report — score, checks, issues',
    keywords: 'health quality score checks report',
    makeCell: () => ({ id: nextCellId(), type: 'health' }),
  })

  for (const t of plottableTopics(nb)) {
    out.push({
      id: `plot:${t}`, cmd: `bf["${t}"].plot()`, kind: 'plot',
      description: `Line chart of ${t}`, keywords: `plot chart graph ${t}`,
      makeCell: () => ({ id: nextCellId(), type: 'plot', topic: t }),
    })
    out.push({
      id: `stats:${t}`, cmd: `bf["${t}"].stats()`, kind: 'stats',
      description: `min / mean / max / σ of ${t}`, keywords: `stats describe summary ${t}`,
      makeCell: () => ({ id: nextCellId(), type: 'stats', topic: t }),
    })
    out.push({
      id: `transform:${t}`, cmd: `bf["${t}"].derivative()`, kind: 'transform',
      description: `Derived signal from ${t} — derivative, integral, filter…`,
      keywords: `transform derivative integral derived filter moving average low pass expression ${t}`,
      makeCell: () => ({ id: nextCellId(), type: 'transform', topic: t, op: 'derivative' }),
    })
  }

  const two = plottableTopics(nb).slice(0, 2)
  if (two.length >= 2) {
    out.push({
      id: 'sync', cmd: `bf.sync([${two.map(t => `"${t}"`).join(', ')}]).head()`, kind: 'sync',
      description: 'Time-align topics', keywords: 'sync align synchronize merge',
      makeCell: () => ({ id: nextCellId(), type: 'sync', topics: two }),
    })
  }

  for (const t of imageTopics(nb)) {
    out.push({
      id: `image:${t}`, cmd: `bf["${t}"].frame(0)`, kind: 'image',
      description: `Camera frames from ${t}`, keywords: `image camera frame video ${t}`,
      makeCell: () => ({ id: nextCellId(), type: 'image', topic: t, frame: 0 }),
    })
  }

  for (const t of pointcloudTopics(nb)) {
    out.push({
      id: `scene:${t}`, cmd: `bf["${t}"].scene()`, kind: 'scene',
      description: `3D point cloud + TF for ${t}`, keywords: `scene 3d pointcloud lidar ${t}`,
      makeCell: () => ({ id: nextCellId(), type: 'scene', topic: t }),
    })
  }

  out.push({
    id: 'search', cmd: 'search("…")', kind: 'search',
    description: 'CLIP semantic frame search', keywords: 'search semantic clip find frame',
    makeCell: () => ({ id: nextCellId(), type: 'search' }),
  })

  return out
}

export function filterCatalog(catalog: CommandEntry[], query: string): CommandEntry[] {
  const q = query.trim().toLowerCase()
  if (!q) return catalog
  const terms = q.split(/\s+/)
  return catalog.filter(e => {
    const hay = `${e.cmd} ${e.kind} ${e.description} ${e.keywords}`.toLowerCase()
    return terms.every(t => hay.includes(t))
  })
}
