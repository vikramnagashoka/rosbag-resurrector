import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api, TransformOp, TransformPreviewResponse } from '../../api'
import { extractSeries } from './series'

// `transform` cell body — the notebook home for the classic Transform editor.
// Apply a math op (derivative / integral / MA / low-pass / scale / abs /
// shift) to one numeric column of a topic, or a free-form Polars expression,
// and render the derived series inline. Live-previews via
// /api/transforms/preview. Unlike the classic modal there's no "add to plot"
// — the transform cell *is* the plot of the derived series.

const OP_LABELS: Record<TransformOp, string> = {
  derivative: 'Derivative (d/dt)',
  integral: 'Integral (∫ dt)',
  moving_average: 'Moving average',
  low_pass: 'Low-pass filter',
  scale: 'Scale (multiply)',
  abs: 'Absolute value',
  shift: 'Shift (lag/lead)',
}
const OPS = Object.keys(OP_LABELS) as TransformOp[]

const DERIVED_COLOR = '#5a57d6'

interface Props {
  bagId?: number
  topic?: string
  op?: string
  column?: string
  expression?: string
  onRuntime?: (ms: number) => void
  onPatch: (patch: { op?: string; column?: string; expression?: string }) => void
}

export default function TransformCell({
  bagId, topic, op, column, expression, onRuntime, onPatch,
}: Props) {
  const [mode, setMode] = useState<'common' | 'expression'>(expression ? 'expression' : 'common')
  const [columns, setColumns] = useState<string[] | null>(null)
  const [colErr, setColErr] = useState<string | null>(null)

  // Op-specific params (kept local — they don't need to survive collapse).
  const [scaleFactor, setScaleFactor] = useState(1.0)
  const [maWindow, setMaWindow] = useState(5)
  const [lpAlpha, setLpAlpha] = useState(0.1)
  const [shiftPeriods, setShiftPeriods] = useState(1)

  const [expr, setExpr] = useState(expression ?? '')
  const [preview, setPreview] = useState<TransformPreviewResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const curOp = (op as TransformOp) ?? 'derivative'

  // Discover the topic's numeric columns (same extraction the plot cell uses).
  useEffect(() => {
    if (bagId == null || !topic) return
    let cancelled = false
    setColumns(null); setColErr(null)
    // max_points floor is 3 on the topic-data endpoint; we only need the
    // column names, so ask for the minimum.
    api.getTopicData(bagId, topic, { maxPoints: 3 })
      .then(r => {
        if (cancelled) return
        const cols = extractSeries(r).map(s => s.label)
        setColumns(cols)
        // Seed a column if none chosen yet.
        if (!column && cols.length) onPatch({ column: cols[0] })
        if (!op) onPatch({ op: 'derivative' })
        if (!expr && cols.length) setExpr(`pl.col("${cols[0]}") * 2`)
      })
      .catch(e => { if (!cancelled) setColErr(String(e?.message ?? e)) })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bagId, topic])

  const body = useMemo(() => {
    if (mode === 'expression') {
      return { bag_id: bagId!, topic: topic!, expression: expr, max_points: 400 }
    }
    const params: Record<string, number> = {}
    if (curOp === 'scale') params.factor = scaleFactor
    if (curOp === 'moving_average') params.window = maWindow
    if (curOp === 'low_pass') params.alpha = lpAlpha
    if (curOp === 'shift') params.periods = shiftPeriods
    return { bag_id: bagId!, topic: topic!, op: curOp, column: column ?? '', params, max_points: 400 }
  }, [mode, bagId, topic, expr, curOp, column, scaleFactor, maWindow, lpAlpha, shiftPeriods])

  // Debounced live preview.
  const timer = useRef<number | undefined>(undefined)
  useEffect(() => {
    if (bagId == null || !topic) return
    if (mode === 'common' && !column) return
    if (mode === 'expression' && !expr.trim()) return
    window.clearTimeout(timer.current)
    timer.current = window.setTimeout(() => {
      setLoading(true); setError(null)
      const t0 = performance.now()
      api.previewTransform(body)
        .then(r => { setPreview(r); onRuntime?.(performance.now() - t0) })
        .catch(e => { setError(String(e?.message ?? e)); setPreview(null) })
        .finally(() => setLoading(false))
    }, 300)
    return () => window.clearTimeout(timer.current)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [body])

  // Inline single-series chart from the preview data.
  const chart = useMemo(() => {
    if (!preview || !preview.data.length) return null
    const key = preview.label
    const pts = preview.data.map(r => ({ t: Number(r.timestamp_ns), v: Number(r[key]) }))
      .filter(p => Number.isFinite(p.v))
    if (!pts.length) return null
    let tMin = Infinity, tMax = -Infinity, vMin = Infinity, vMax = -Infinity
    for (const p of pts) {
      tMin = Math.min(tMin, p.t); tMax = Math.max(tMax, p.t)
      vMin = Math.min(vMin, p.v); vMax = Math.max(vMax, p.v)
    }
    const tSpan = tMax - tMin || 1
    const vSpan = vMax - vMin || 1
    const x = (t: number) => ((t - tMin) / tSpan) * 1000
    const y = (v: number) => 10 + (1 - (v - vMin) / vSpan) * 180
    const points = pts.map(p => `${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`).join(' ')
    const spanSec = tSpan / 1e9
    return { points, spanSec, vMin, vMax }
  }, [preview])

  if (bagId == null || !topic) return <div className="nb-cell-loading">No topic selected.</div>

  return (
    <div className="nb-transform">
      <div className="nb-tf-tabs">
        <button className={`nb-tf-tab${mode === 'common' ? ' active' : ''}`}
          onClick={() => { setMode('common'); onPatch({ expression: undefined }) }}>Common</button>
        <button className={`nb-tf-tab${mode === 'expression' ? ' active' : ''}`}
          onClick={() => setMode('expression')}>Expression</button>
      </div>

      {mode === 'common' ? (
        <div className="nb-tf-controls">
          <label className="nb-tf-field">
            <span>Operation</span>
            <select value={curOp} onChange={e => onPatch({ op: e.target.value })}>
              {OPS.map(o => <option key={o} value={o}>{OP_LABELS[o]}</option>)}
            </select>
          </label>
          <label className="nb-tf-field">
            <span>Column</span>
            <select value={column ?? ''} onChange={e => onPatch({ column: e.target.value })}>
              {colErr && <option value="">— error —</option>}
              {columns == null && <option value="">loading…</option>}
              {columns?.length === 0 && <option value="">no numeric columns</option>}
              {columns?.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </label>
          {curOp === 'scale' && (
            <label className="nb-tf-field nb-tf-param">
              <span>Factor</span>
              <input type="number" step={0.1} value={scaleFactor}
                onChange={e => setScaleFactor(Number(e.target.value))} />
            </label>
          )}
          {curOp === 'moving_average' && (
            <label className="nb-tf-field nb-tf-param">
              <span>Window</span>
              <input type="number" min={1} value={maWindow}
                onChange={e => setMaWindow(Math.max(1, Number(e.target.value)))} />
            </label>
          )}
          {curOp === 'low_pass' && (
            <label className="nb-tf-field nb-tf-param">
              <span>Alpha</span>
              <input type="number" step={0.05} min={0.01} max={1} value={lpAlpha}
                onChange={e => setLpAlpha(Number(e.target.value))} />
            </label>
          )}
          {curOp === 'shift' && (
            <label className="nb-tf-field nb-tf-param">
              <span>Periods</span>
              <input type="number" step={1} value={shiftPeriods}
                onChange={e => setShiftPeriods(Number(e.target.value))} />
            </label>
          )}
        </div>
      ) : (
        <div className="nb-tf-expr">
          <input
            className="nb-tf-expr-input"
            value={expr}
            spellCheck={false}
            placeholder='pl.col("...") * 2'
            onChange={e => { setExpr(e.target.value); onPatch({ expression: e.target.value }) }}
          />
          <div className="nb-tf-expr-hint">
            Polars: <code>pl.col()</code>, <code>pl.lit()</code>, arithmetic, <code>.abs()</code>, <code>.rolling_mean(N)</code>… (IO + <code>__</code> blocked)
          </div>
        </div>
      )}

      <div className="nb-tf-preview">
        {error ? (
          <div className="nb-cell-loading">{error}</div>
        ) : chart ? (
          <>
            <div className="nb-tf-legend">
              <span className="nb-legend-swatch" style={{ background: DERIVED_COLOR }} />
              {preview!.label}
              <span className="nb-plot-meta">
                {preview!.total.toLocaleString()} msgs · ↓{preview!.data.length}
                {loading && ' · updating…'}
              </span>
            </div>
            <div className="nb-chart-wrap">
              <svg className="nb-chart" viewBox="0 0 1000 200" preserveAspectRatio="none">
                {[50, 100, 150].map(gy => (
                  <line key={gy} x1="0" x2="1000" y1={gy} y2={gy} stroke="#f0ebe1" strokeWidth="1" />
                ))}
                <polyline points={chart.points} fill="none" stroke={DERIVED_COLOR}
                  strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" vectorEffect="non-scaling-stroke" />
              </svg>
              <div className="nb-chart-xaxis">
                {[0, 0.25, 0.5, 0.75, 1].map((f, i) => <span key={i}>{(chart.spanSec * f).toFixed(1)}s</span>)}
              </div>
            </div>
          </>
        ) : (
          <div className="nb-cell-loading">{loading ? 'Computing transform…' : 'Adjust the controls to preview.'}</div>
        )}
      </div>
    </div>
  )
}
