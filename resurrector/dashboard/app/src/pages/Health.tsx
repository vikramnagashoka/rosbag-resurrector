import React, { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import HealthBadge from '../components/HealthBadge'
import { api, HealthReport } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'

const severityColors: Record<string, string> = {
  info: '#8b949e',
  warning: '#d29922',
  error: '#f85149',
  critical: '#da3633',
}

export default function Health() {
  const { id } = useParams<{ id: string }>()
  const [report, setReport] = useState<HealthReport | null>(null)
  const [loading, setLoading] = useState(true)
  const toast = useErrorToast()

  useEffect(() => {
    runWithToast(toast, () => api.getBagHealth(Number(id))).then(r => {
      if (r) setReport(r)
      setLoading(false)
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id])

  if (loading || !report) return <p style={{ color: '#8b949e' }}>Loading health report...</p>

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px' }}>
        <div>
          <Link to={`/bag/${id}`} style={{ color: '#8b949e', fontSize: '13px' }}>Back to Explorer</Link>
          <h1 style={{ fontSize: '24px', fontWeight: 600, marginTop: '4px' }}>Health Report</h1>
        </div>
        <HealthBadge score={report.score} size="large" />
      </div>

      {/* Recommendations */}
      {report.recommendations.length > 0 && (
        <div style={{
          background: '#1c1c0e',
          border: '1px solid #d29922',
          borderRadius: '8px',
          padding: '16px',
          marginBottom: '24px',
        }}>
          <h3 style={{ fontSize: '14px', fontWeight: 600, marginBottom: '8px', color: '#d29922' }}>Recommendations</h3>
          <ul style={{ paddingLeft: '20px', color: '#e1e4e8', fontSize: '14px' }}>
            {report.recommendations.map((rec, i) => (
              <li key={i} style={{ marginBottom: '4px' }}>{rec}</li>
            ))}
          </ul>
        </div>
      )}

      {/* Per-topic scores */}
      <div style={{ marginBottom: '24px' }}>
        <h3 style={{ fontSize: '16px', fontWeight: 600, marginBottom: '12px' }}>Per-Topic Scores</h3>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(250px, 1fr))', gap: '12px' }}>
          {Object.entries(report.topic_scores).sort(([, a], [, b]) => a.score - b.score).map(([topic, ts]) => (
            <div key={topic} style={{
              background: '#161b22',
              border: '1px solid #30363d',
              borderRadius: '8px',
              padding: '12px',
            }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span style={{ color: '#58a6ff', fontSize: '13px', fontWeight: 500 }}>{topic}</span>
                <HealthBadge score={ts.score} size="small" />
              </div>
              {ts.issue_count > 0 && (
                <div style={{ color: '#8b949e', fontSize: '12px', marginTop: '4px' }}>
                  {ts.issue_count} issue(s)
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Issues table */}
      {report.issues.length > 0 && (
        <div>
          <h3 style={{ fontSize: '16px', fontWeight: 600, marginBottom: '12px' }}>Issues ({report.issues.length})</h3>
          <table style={{
            width: '100%',
            borderCollapse: 'collapse',
            background: '#161b22',
            borderRadius: '8px',
            overflow: 'hidden',
          }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #30363d' }}>
                <th style={{ padding: '10px 12px', textAlign: 'left', color: '#8b949e', fontSize: '13px' }}>Severity</th>
                <th style={{ padding: '10px 12px', textAlign: 'left', color: '#8b949e', fontSize: '13px' }}>Topic</th>
                <th style={{ padding: '10px 12px', textAlign: 'left', color: '#8b949e', fontSize: '13px' }}>Check</th>
                <th style={{ padding: '10px 12px', textAlign: 'left', color: '#8b949e', fontSize: '13px' }}>Message</th>
                <th style={{ padding: '10px 12px', textAlign: 'right', color: '#8b949e', fontSize: '13px' }}>Time</th>
              </tr>
            </thead>
            <tbody>
              {report.issues.map((issue, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #21262d' }}>
                  <td style={{ padding: '8px 12px' }}>
                    <span style={{
                      color: severityColors[issue.severity] || '#8b949e',
                      fontWeight: 600,
                      fontSize: '12px',
                      textTransform: 'uppercase',
                    }}>
                      {issue.severity}
                    </span>
                  </td>
                  <td style={{ padding: '8px 12px', color: '#58a6ff', fontSize: '13px' }}>{issue.topic || '-'}</td>
                  <td style={{ padding: '8px 12px', color: '#8b949e', fontSize: '13px' }}>{issue.check}</td>
                  <td style={{ padding: '8px 12px', fontSize: '13px' }}>{issue.message}</td>
                  <td style={{ padding: '8px 12px', textAlign: 'right', color: '#8b949e', fontSize: '13px' }}>
                    {issue.start_time != null ? `${issue.start_time.toFixed(2)}s` : '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {report.issues.length === 0 && (
        <div style={{
          background: '#0d2818',
          border: '1px solid #238636',
          borderRadius: '8px',
          padding: '24px',
          textAlign: 'center',
          color: '#3fb950',
          fontSize: '16px',
        }}>
          All checks passed. No issues detected.
        </div>
      )}
    </div>
  )
}
