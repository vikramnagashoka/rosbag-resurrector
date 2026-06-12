import React, { useEffect, useState } from 'react'
import { api, ExplainResult } from '../api'

interface Props {
  bagId: number
  startSec: number
  endSec: number
  onClose: () => void
}

// "Ask your bag" — grounded explanation of a brushed time window.
// Shows the narrative plus the evidence it was built from, so every claim
// is traceable. The evidence table IS the citation: nothing in the prose
// should reference a topic/number that isn't in the table below it.
export default function ExplainPanel({ bagId, startSec, endSec, onClose }: Props) {
  const [result, setResult] = useState<ExplainResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    api.explainTimeRange(bagId, startSec, endSec)
      .then(r => { if (!cancelled) setResult(r) })
      .catch(e => { if (!cancelled) setError(String(e?.message ?? e)) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [bagId, startSec, endSec])

  return (
    <div
      style={{
        position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.6)',
        display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000,
      }}
      onClick={onClose}
    >
      <div
        role="dialog"
        aria-label="Explain time range"
        onClick={e => e.stopPropagation()}
        style={{
          background: '#161b22', border: '1px solid #30363d', borderRadius: 10,
          padding: 24, width: 'min(720px, 92vw)', maxHeight: '85vh', overflowY: 'auto',
          color: '#e1e4e8',
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <h2 style={{ fontSize: 18, fontWeight: 600, margin: 0 }}>
            Explain {startSec.toFixed(2)}s – {endSec.toFixed(2)}s
          </h2>
          <button
            onClick={onClose}
            aria-label="Close"
            style={{
              background: '#21262d', border: '1px solid #30363d', borderRadius: 6,
              color: '#e1e4e8', cursor: 'pointer', padding: '4px 10px', fontSize: 13,
            }}
          >Close</button>
        </div>

        {loading && <div style={{ color: '#8b949e' }}>Analyzing window…</div>}
        {error && (
          <div style={{ background: '#3a1a1a', border: '1px solid #f85149', borderRadius: 8, padding: 16, color: '#f85149' }}>
            {error}
          </div>
        )}

        {result && (
          <>
            <div style={{ marginBottom: 8 }}>
              <span style={{
                fontSize: 11, padding: '2px 8px', borderRadius: 10,
                background: result.source === 'llm' ? '#1f6feb33' : '#21262d',
                border: '1px solid #30363d', color: '#8b949e',
              }}>
                {result.source === 'llm' ? 'AI narrative' : 'rule-based summary'}
              </span>
            </div>

            <div style={{
              whiteSpace: 'pre-wrap', lineHeight: 1.55, fontSize: 14,
              background: '#0d1117', border: '1px solid #30363d', borderRadius: 8,
              padding: 16, marginBottom: 16,
            }}>
              {result.narrative}
            </div>

            <h3 style={{ fontSize: 13, color: '#8b949e', margin: '0 0 8px' }}>
              Evidence ({result.evidence.totals.messages_in_window.toLocaleString()} msgs,{' '}
              {result.evidence.totals.active_topics} active topics)
            </h3>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr style={{ color: '#8b949e', textAlign: 'left' }}>
                  <th style={{ padding: '4px 8px' }}>Topic</th>
                  <th style={{ padding: '4px 8px', textAlign: 'right' }}>Count</th>
                  <th style={{ padding: '4px 8px', textAlign: 'right' }}>Rate (window)</th>
                  <th style={{ padding: '4px 8px', textAlign: 'right' }}>Rate (overall)</th>
                </tr>
              </thead>
              <tbody>
                {result.evidence.topics.map(t => (
                  <tr key={t.topic} style={{ borderTop: '1px solid #21262d' }}>
                    <td style={{ padding: '4px 8px', color: '#58a6ff', fontFamily: 'monospace' }}>{t.topic}</td>
                    <td style={{ padding: '4px 8px', textAlign: 'right' }}>{t.count_in_window.toLocaleString()}</td>
                    <td style={{ padding: '4px 8px', textAlign: 'right' }}>{t.rate_in_window_hz.toFixed(1)} Hz</td>
                    <td style={{ padding: '4px 8px', textAlign: 'right', color: '#8b949e' }}>
                      {t.overall_rate_hz != null ? `${t.overall_rate_hz.toFixed(1)} Hz` : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            {result.evidence.health_issues.length > 0 && (
              <>
                <h3 style={{ fontSize: 13, color: '#8b949e', margin: '16px 0 8px' }}>
                  Health findings in window
                </h3>
                {result.evidence.health_issues.map((h, i) => (
                  <div key={i} style={{ fontSize: 12, color: '#d29922', marginBottom: 4 }}>
                    {h.severity.toUpperCase()} {h.check}
                    {h.topic ? ` [${h.topic}]` : ''}: {h.message}
                  </div>
                ))}
              </>
            )}
          </>
        )}
      </div>
    </div>
  )
}
