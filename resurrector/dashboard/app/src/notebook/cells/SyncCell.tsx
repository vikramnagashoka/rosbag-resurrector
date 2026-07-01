import React, { useEffect, useState } from 'react'
import { api, TopicDataResponse } from '../../api'

// `sync` cell body — time-aligned head() of N topics via the existing
// /api/bags/:id/sync endpoint. Renders the first rows as a table. The
// linked-cursor row highlight lands in PR 5.

const HEAD_ROWS = 8

export default function SyncCell({
  bagId, topics, onRuntime,
}: { bagId?: number; topics?: string[]; onRuntime?: (ms: number) => void }) {
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
  return (
    <div className="nb-table-scroll">
      <table className="nb-table">
        <thead>
          <tr>{cols.map(c => <th key={c}>{c.replace(/_/g, '.')}</th>)}</tr>
        </thead>
        <tbody>
          {resp.data.slice(0, HEAD_ROWS).map((row, i) => (
            <tr key={i}>
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
