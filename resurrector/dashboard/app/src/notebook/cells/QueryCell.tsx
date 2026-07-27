import React, { useEffect, useRef, useState } from 'react'
import { api, TransformPreviewResponse } from '../../api'
import { extractSeries } from './series'

// `query` cell body — the free-form exploration cell. The user writes their
// own Polars expression against one topic and runs it on demand (button or
// ⌘⏎). Powered by the same server-side sandboxed evaluator the transform
// cell uses (/api/transforms/preview): pl.col / pl.lit, arithmetic, chained
// methods; imports, dunders, and IO are rejected server-side.
//
// Output is a chart of the resulting series plus a head-of-the-data table —
// chart for shape, table for exact values. Unlike the transform cell there
// is no op dropdown: the expression IS the interface.

const QUERY_COLOR = '#3f6fb0'
const TABLE_ROWS = 8

interface Props {
  bagId?: number
  topic?: string
  expression?: string
  onRuntime?: (ms: number) => void
  onPatch: (patch: { expression?: string }) => void
}

export default function QueryCell({ bagId, topic, expression, onRuntime, onPatch }: Props) {
  const [draft, setDraft] = useState(expression ?? '')
  const [columns, setColumns] = useState<string[] | null>(null)
  const [resp, setResp] = useState<TransformPreviewResponse | null>(null)
  const [running, setRunning] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const taRef = useRef<HTMLTextAreaElement>(null)
  const ranOnce = useRef(false)

  // Discover the topic's numeric columns — shown as clickable chips so the
  // user can see what's queryable without guessing field names.
  useEffect(() => {
    if (bagId == null || !topic) return
    let cancelled = false
    setColumns(null)
    api.getTopicData(bagId, topic, { maxPoints: 3 })
      .then(r => { if (!cancelled) setColumns(extractSeries(r).map(s => s.label)) })
      .catch(() => { if (!cancelled) setColumns([]) })
    return () => { cancelled = true }
  }, [bagId, topic])

  function run(expr?: string) {
    const e = (expr ?? draft).trim()
    if (bagId == null || !topic || !e) return
    onPatch({ expression: e })
    setRunning(true); setError(null)
    const t0 = performance.now()
    api.previewTransform({ bag_id: bagId, topic, expression: e, max_points: 400 })
      .then(r => { setResp(r); onRuntime?.(performance.now() - t0) })
      .catch(err => { setError(String(err?.message ?? err)); setResp(null) })
      .finally(() => setRunning(false))
  }

  // A cell created from the command bar arrives with an expression already
  // typed — run it once so the user sees a result, not a second input box.
  useEffect(() => {
    if (!ranOnce.current && expression?.trim() && bagId != null && topic) {
      ranOnce.current = true
      run(expression)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bagId, topic])

  function insertCol(col: string) {
    const snippet = `pl.col("${col}")`
    const ta = taRef.current
    if (!ta) { setDraft(d => d + snippet); return }
    const a = ta.selectionStart ?? draft.length
    const b = ta.selectionEnd ?? draft.length
    const next = draft.slice(0, a) + snippet + draft.slice(b)
    setDraft(next)
    requestAnimationFrame(() => {
      ta.focus()
      ta.selectionStart = ta.selectionEnd = a + snippet.length
    })
  }

  function onKeyDown(e: React.KeyboardEvent) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); run() }
  }

  // Chart + table derived from the run result.
  const label = resp?.label
  const points = resp && label
    ? resp.data
        .map(r => ({ t: Number(r.timestamp_ns), v: Number(r[label]) }))
        .filter(p => Number.isFinite(p.v))
    : []
  let chart: { poly: string; spanSec: number } | null = null
  if (points.length) {
    let tMin = Infinity, tMax = -Infinity, vMin = Infinity, vMax = -Infinity
    for (const p of points) {
      tMin = Math.min(tMin, p.t); tMax = Math.max(tMax, p.t)
      vMin = Math.min(vMin, p.v); vMax = Math.max(vMax, p.v)
    }
    const tSpan = tMax - tMin || 1, vSpan = vMax - vMin || 1
    const poly = points
      .map(p => `${(((p.t - tMin) / tSpan) * 1000).toFixed(1)},${(10 + (1 - (p.v - vMin) / vSpan) * 180).toFixed(1)}`)
      .join(' ')
    chart = { poly, spanSec: tSpan / 1e9 }
  }
  const t0 = points.length ? points[0].t : 0

  if (bagId == null || !topic) return <div className="nb-cell-loading">No topic selected.</div>

  return (
    <div className="nb-query">
      <div className="nb-query-cols">
        <span className="nb-cmp-lbl">Columns</span>
        {columns == null && <span className="nb-query-colhint">loading…</span>}
        {columns?.length === 0 && <span className="nb-query-colhint">no numeric columns on {topic}</span>}
        {columns?.map(c => (
          <button key={c} className="nb-query-col" title={`Insert pl.col("${c}")`} onClick={() => insertCol(c)}>
            {c}
          </button>
        ))}
      </div>

      <textarea
        ref={taRef}
        className="nb-query-editor"
        value={draft}
        spellCheck={false}
        placeholder={'pl.col("linear_acceleration.z").rolling_mean(25)'}
        onChange={e => setDraft(e.target.value)}
        onKeyDown={onKeyDown}
        aria-label="Polars expression"
      />
      <div className="nb-query-actions">
        <span className="nb-query-hint">
          Polars: <code>pl.col()</code>, <code>pl.lit()</code>, arithmetic, <code>.abs()</code>, <code>.diff()</code>, <code>.rolling_mean(N)</code>, <code>.pow(2)</code>… (imports + IO blocked)
        </span>
        <button className="nb-search-go" onClick={() => run()} disabled={running || !draft.trim()}>
          {running ? 'Running…' : 'Run ⌘⏎'}
        </button>
      </div>

      {error && <div className="nb-cell-loading nb-query-error">{error}</div>}

      {resp && !error && (
        <div className="nb-query-out">
          <div className="nb-tf-legend">
            <span className="nb-legend-swatch" style={{ background: QUERY_COLOR }} />
            {resp.label}
            <span className="nb-plot-meta">
              {resp.total.toLocaleString()} msgs{resp.downsampled ? ` · ↓${resp.data.length}` : ''}
            </span>
          </div>
          {chart ? (
            <div className="nb-chart-wrap">
              <svg className="nb-chart" viewBox="0 0 1000 200" preserveAspectRatio="none">
                {[50, 100, 150].map(gy => (
                  <line key={gy} x1="0" x2="1000" y1={gy} y2={gy} stroke="#f0ebe1" strokeWidth="1" />
                ))}
                <polyline points={chart.poly} fill="none" stroke={QUERY_COLOR}
                  strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" vectorEffect="non-scaling-stroke" />
              </svg>
              <div className="nb-chart-xaxis">
                {[0, 0.25, 0.5, 0.75, 1].map((f, i) => <span key={i}>{(chart!.spanSec * f).toFixed(1)}s</span>)}
              </div>
            </div>
          ) : (
            <div className="nb-cell-loading">Result has no numeric values to chart.</div>
          )}
          {points.length > 0 && (
            <table className="nb-table nb-query-table">
              <thead><tr><th>t (s)</th><th>{resp.label}</th></tr></thead>
              <tbody>
                {points.slice(0, TABLE_ROWS).map((p, i) => (
                  <tr key={i}>
                    <td>{((p.t - t0) / 1e9).toFixed(3)}</td>
                    <td>{p.v.toFixed(6)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  )
}
