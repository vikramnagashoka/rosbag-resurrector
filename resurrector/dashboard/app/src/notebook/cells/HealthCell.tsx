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

function scoreColor(score: number): string {
  if (score >= 90) return '#2f8f5f'
  if (score >= 80) return '#bf8a2c'
  return '#c75c4b'
}

// "message_rate_stability" -> "Rate stability"
function prettyCheck(name: string): string {
  const s = name.replace(/^message_/, '').replace(/_/g, ' ')
  return s.charAt(0).toUpperCase() + s.slice(1)
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
  const summary = report.summary
  const checks = report.checks ?? []
  // Worst topics first — surface the problem children, not the healthy ones.
  const worstTopics = Object.entries(report.topic_scores)
    .sort((a, b) => a[1].score - b[1].score)
    .slice(0, 6)
    .filter(([, v]) => v.score < 100)

  return (
    <div>
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

      {/* Aggregate summary */}
      {summary && (
        <div className="nb-health-summary">
          <span><b style={{ color: '#c75c4b' }}>{summary.errors}</b> errors</span>
          <span><b style={{ color: '#bf8a2c' }}>{summary.warnings}</b> warnings</span>
          <span><b>{summary.topics_checked}</b> topics checked</span>
        </div>
      )}

      {/* Per-check breakdown — the 5 health dimensions + their scores */}
      {checks.length > 0 && (
        <>
          <div className="nb-health-section-label">CHECKS</div>
          <div className="nb-checks">
            {checks.map(c => (
              <span className="nb-check" key={c.check} title={`${c.issue_count} issue(s)`}>
                <span className="nb-check-name">{prettyCheck(c.check)}</span>
                <span className="nb-check-score" style={{ color: scoreColor(c.score) }}>
                  {c.score}
                </span>
              </span>
            ))}
          </div>
        </>
      )}

      {/* Worst topics */}
      {worstTopics.length > 0 && (
        <>
          <div className="nb-health-section-label">TOPICS NEEDING ATTENTION</div>
          <div className="nb-topic-scores">
            {worstTopics.map(([topic, v]) => (
              <span className="nb-topic-score" key={topic}>
                {topic} <span style={{ color: scoreColor(v.score) }}>{v.score}</span>
              </span>
            ))}
          </div>
        </>
      )}

      {/* Recommendations */}
      {report.recommendations.length > 0 && (
        <>
          <div className="nb-health-section-label">RECOMMENDATIONS</div>
          <div className="nb-recos">
            {report.recommendations.map((r, i) => (
              <div className="nb-reco" key={i}>{r}</div>
            ))}
          </div>
        </>
      )}
    </div>
  )
}
