import React, { useEffect, useState } from 'react'
import { api, TopicDataResponse } from '../../api'

// `sync` cell body — time-aligned head() of N topics via the existing
// /api/bags/:id/sync endpoint. Renders the first rows as a table. The
// linked-cursor row highlight lands in PR 5.

const HEAD_ROWS = 8

export default function SyncCell({
  bagId, topics, cursor, onRuntime,
}: {
  bagId?: number
  topics?: string[]
  cursor?: number | null
  onRuntime?: (ms: number) => void
}) {
  const [resp, setResp] = useState<TopicDataResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  const topicKey = (topics ?? []).join(',')
  useEffect(() => {
    if (bagId == null || !topics || topics.length < 2) return
    let cancelled = false
    setResp(null); setError(null)
    const t0 = performance.now()
    api.getSyncedData(bagId, topics, 'nearest', 50, HEAD_ROWS)
      .then(r => { if (!cancelled) { setResp(r); onRuntime?.(performance.now() - t0) } })
      .catch(e => { if (!cancelled) setError(String(e?.message ?? e)) })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bagId, topicKey])

  if (bagId == null || !topics || topics.length < 2)
    return <div className="nb-cell-loading">Sync needs at least two topics.</div>
  if (error) return <div className="nb-cell-loading">{error}</div>
  if (!resp) return <div className="nb-cell-loading">Aligning {topics.join(', ')}…</div>
  if (!resp.data.length) return <div className="nb-cell-loading">No aligned rows.</div>

  const cols = resp.columns
  const rows = resp.data.slice(0, HEAD_ROWS)

  // Row nearest the linked cursor — mapped across the shown rows' time span.
  let cursorRow = -1
  if (cursor != null && rows.length) {
    const ts = rows.map(r => Number(r.timestamp_ns))
    const tMin = Math.min(...ts), tMax = Math.max(...ts)
    const target = tMin + cursor * (tMax - tMin || 1)
    let bestDt = Infinity
    ts.forEach((t, i) => { const dt = Math.abs(t - target); if (dt < bestDt) { bestDt = dt; cursorRow = i } })
  }

  return (
    <div className="nb-table-scroll">
      <table className="nb-table">
        <thead>
          <tr>{cols.map(c => <th key={c}>{c.replace(/_/g, '.')}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className={i === cursorRow ? 'nb-row-cursor' : undefined}>
              {cols.map(c => {
                const v = row[c]
                return <td key={c}>{typeof v === 'number' ? v.toFixed(3) : String(v ?? '—')}</td>
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
