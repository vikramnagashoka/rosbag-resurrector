import React, { useEffect, useMemo, useState } from 'react'
import { api, Bag, CompareTopicsResponse } from '../../api'
import { SERIES_COLORS } from './series'

// `compare` cell body — overlay ONE topic across multiple bags on a shared
// relative-time axis. The notebook-native port of the classic Compare-runs
// page: pick 2+ bags + a topic + a value column, and each bag becomes a
// series. Uses /api/compare/topics; renders inline SVG (no Plotly) to match
// the plot/transform cells.

// Columns that are never useful to overlay (join keys + raw metadata).
const NON_VALUE = new Set(['bag_label', 'relative_t_sec', 'timestamp_ns'])
const META_RE = /(_offset$|^data_length$|sequence|frame_id|stamp_nsec|_ns$)/i

function basename(p: string): string { return p.split(/[/\\]/).pop() || p }

interface Props {
  topic?: string
  column?: string
  bagIds?: number[]
  allBags: Bag[]
  defaultBagId?: number
  onRuntime?: (ms: number) => void
  onPatch: (patch: { topic?: string; column?: string; bagIds?: number[] }) => void
}

export default function CompareCell({
  topic, column, bagIds, allBags, defaultBagId, onRuntime, onPatch,
}: Props) {
  const [resp, setResp] = useState<CompareTopicsResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  // Seed the selection: the active bag + the first other bag, if unset.
  useEffect(() => {
    if (bagIds && bagIds.length) return
    const seed: number[] = []
    if (defaultBagId != null) seed.push(defaultBagId)
    const other = allBags.find(b => b.id !== defaultBagId)
    if (other) seed.push(other.id)
    if (seed.length) onPatch({ bagIds: seed })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const selected = bagIds ?? []

  // Topics offered: union across the selected bags (so a topic present in at
  // least one bag is pickable). Falls back to the active bag's topics.
  const topicOptions = useMemo(() => {
    const names = new Set<string>()
    for (const b of allBags) {
      if (selected.includes(b.id)) b.topics.forEach(t => names.add(t.name))
    }
    return [...names].sort()
  }, [allBags, selected])

  // Fetch the overlay whenever the bag set or topic changes.
  useEffect(() => {
    if (selected.length < 2 || !topic) { setResp(null); return }
    let cancelled = false
    setLoading(true); setError(null)
    const t0 = performance.now()
    api.compareTopics({ bag_ids: selected, topic, max_points_per_bag: 120 })
      .then(r => { if (!cancelled) { setResp(r); onRuntime?.(performance.now() - t0) } })
      .catch(e => { if (!cancelled) { setError(String(e?.message ?? e)); setResp(null) } })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify(selected), topic])

  // Numeric value columns available in the response (metadata de-prioritized).
  const valueColumns = useMemo(() => {
    if (!resp) return []
    return resp.columns.filter(c => {
      if (NON_VALUE.has(c)) return false
      return resp.data.some(r => typeof r[c] === 'number')
    })
  }, [resp])
  const usefulColumns = valueColumns.filter(c => !META_RE.test(c))
  const valueCol = column && valueColumns.includes(column)
    ? column
    : (usefulColumns[0] ?? valueColumns[0])

  // One series per bag label: x = relative_t_sec, y = value column.
  const chart = useMemo(() => {
    if (!resp || !valueCol) return null
    const bySeries = resp.labels.map(label => {
      const rows = resp.data.filter(r => r.bag_label === label)
      const pts = rows.map(r => ({ t: Number(r.relative_t_sec), v: Number(r[valueCol]) }))
        .filter(p => Number.isFinite(p.t) && Number.isFinite(p.v))
      return { label, pts }
    }).filter(s => s.pts.length)
    if (!bySeries.length) return null
    let tMin = Infinity, tMax = -Infinity, vMin = Infinity, vMax = -Infinity
    for (const s of bySeries) for (const p of s.pts) {
      tMin = Math.min(tMin, p.t); tMax = Math.max(tMax, p.t)
      vMin = Math.min(vMin, p.v); vMax = Math.max(vMax, p.v)
    }
    const tSpan = (tMax - tMin) || 1
    const vSpan = (vMax - vMin) || 1
    const x = (t: number) => ((t - tMin) / tSpan) * 1000
    const y = (v: number) => 10 + (1 - (v - vMin) / vSpan) * 180
    const polylines = bySeries.map((s, i) => ({
      label: s.label,
      color: SERIES_COLORS[i % SERIES_COLORS.length],
      last: s.pts[s.pts.length - 1]?.v,
      points: s.pts.map(p => `${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`).join(' '),
    }))
    return { polylines, tSpan }
  }, [resp, valueCol])

  function toggleBag(id: number) {
    const next = selected.includes(id) ? selected.filter(x => x !== id) : [...selected, id]
    onPatch({ bagIds: next })
  }

  return (
    <div className="nb-compare">
      <div className="nb-cmp-bags">
        <span className="nb-cmp-lbl">Bags</span>
        {allBags.map(b => (
          <button
            key={b.id}
            className={`nb-cmp-bagchip${selected.includes(b.id) ? ' on' : ''}`}
            onClick={() => toggleBag(b.id)}
            title={b.path}
          >
            {basename(b.path)}
          </button>
        ))}
      </div>

      <div className="nb-cmp-controls">
        <label className="nb-tf-field">
          <span>Topic</span>
          <select value={topic ?? ''} onChange={e => onPatch({ topic: e.target.value })}>
            {!topic && <option value="">select…</option>}
            {topicOptions.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        </label>
        {valueColumns.length > 0 && (
          <label className="nb-tf-field">
            <span>Value</span>
            <select value={valueCol ?? ''} onChange={e => onPatch({ column: e.target.value })}>
              {valueColumns.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </label>
        )}
      </div>

      {selected.length < 2 ? (
        <div className="nb-cell-loading">Select at least two bags to overlay.</div>
      ) : error ? (
        <div className="nb-cell-loading">{error}</div>
      ) : !chart ? (
        <div className="nb-cell-loading">
          {loading ? 'Overlaying…' : `No numeric series to overlay for ${topic ?? 'this topic'}.`}
        </div>
      ) : (
        <>
          <div className="nb-plot-legend">
            {chart.polylines.map((p, i) => (
              <span className="nb-legend-chip" key={p.label + i}>
                <span className="nb-legend-swatch" style={{ background: p.color }} />
                {p.label}
                {p.last != null && Number.isFinite(p.last) && <span className="nb-legend-val">{p.last.toFixed(2)}</span>}
              </span>
            ))}
            <span className="nb-plot-meta">{valueCol}{loading && ' · updating…'}</span>
          </div>
          <div className="nb-chart-wrap">
            <svg className="nb-chart" viewBox="0 0 1000 200" preserveAspectRatio="none">
              {[50, 100, 150].map(gy => (
                <line key={gy} x1="0" x2="1000" y1={gy} y2={gy} stroke="#f0ebe1" strokeWidth="1" />
              ))}
              {chart.polylines.map((p, i) => (
                <polyline key={i} points={p.points} fill="none" stroke={p.color}
                  strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" vectorEffect="non-scaling-stroke" />
              ))}
            </svg>
            <div className="nb-chart-xaxis">
              {[0, 0.25, 0.5, 0.75, 1].map((f, i) => <span key={i}>{(chart.tSpan * f).toFixed(1)}s</span>)}
            </div>
          </div>
        </>
      )}
    </div>
  )
}
