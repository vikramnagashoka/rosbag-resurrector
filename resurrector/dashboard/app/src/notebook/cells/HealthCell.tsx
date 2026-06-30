import React, { useEffect, useState } from 'react'
import { api, HealthReport } from '../../api'
import { tierForScore } from '../types'

// `health` cell body — the conic-gradient score ring + issues list,
// wired to the existing /api/bags/:id/health endpoint.

const TIER_COLOR: Record<string, string> = {
  good: '#2f8f5f', warn: '#bf8a2c', bad: '#c75c4b',
}
const SEV_COLOR: Record<string, string> = {
  error: '#c75c4b', warning: '#bf8a2c', info: '#3f6fb0',
}

export default function HealthCell({
  bagId, onRuntime,
}: { bagId?: number; onRuntime?: (ms: number) => void }) {
  const [report, setReport] = useState<HealthReport | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (bagId == null) return
    let cancelled = false
    const t0 = performance.now()
    api.getBagHealth(bagId)
      .then(r => {
        if (cancelled) return
        setReport(r)
        onRuntime?.(performance.now() - t0)
      })
      .catch(e => { if (!cancelled) setError(String(e?.message ?? e)) })
    return () => { cancelled = true }
    // onRuntime intentionally omitted — a fresh closure each render would
    // otherwise re-fire the fetch.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bagId])

  if (bagId == null) return <div className="nb-cell-loading">No bag bound to this notebook.</div>
  if (error) return <div className="nb-cell-loading">{error}</div>
  if (!report) return <div className="nb-cell-loading">Running health report…</div>

  const color = TIER_COLOR[tierForScore(report.score)]
  const deg = Math.round((report.score / 100) * 360)

  return (
    <div className="nb-health">
      <div
        className="nb-ring"
        style={{ background: `conic-gradient(${color} ${deg}deg, #ece6da 0)` }}
        role="img"
        aria-label={`Health score ${report.score} of 100`}
      >
        <div className="nb-ring-inner">{report.score}</div>
      </div>
      <div className="nb-issues">
        {report.issues.length === 0 ? (
          <div className="nb-issue-none">No issues found — clean recording.</div>
        ) : (
          report.issues.slice(0, 8).map((iss, i) => (
            <div className="nb-issue" key={i}>
              <span
                className="nb-issue-dot"
                style={{ background: SEV_COLOR[iss.severity] ?? '#bf8a2c' }}
              />
              <span>
                {iss.topic && <strong>{iss.topic}</strong>}
                {iss.topic ? ' — ' : ''}
                {iss.message}
              </span>
            </div>
          ))
        )}
      </div>
    </div>
  )
}
