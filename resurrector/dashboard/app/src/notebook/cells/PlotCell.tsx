import React, { useEffect, useMemo, useState } from 'react'
import { api, TopicDataResponse } from '../../api'
import { extractSeries, SERIES_COLORS } from './series'

// `plot` cell body — a multi-series line chart (inline SVG) from the
// downsampled-series endpoint. Hovering sets the linked time-cursor; when a
// cursor is present the chart draws a dashed vertical line, a dot on each
// series at that time, and a readout of the values. Region-select + Explain
// land in PR 6.

const MAX_POINTS = 120

interface Props {
  bagId?: number
  topic?: string
  cursor?: number | null              // 0–1 fraction across the time axis
  onRuntime?: (ms: number) => void
  onCursor?: (frac: number | null) => void
}

export default function PlotCell({ bagId, topic, cursor, onRuntime, onCursor }: Props) {
  const [resp, setResp] = useState<TopicDataResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (bagId == null || !topic) return
    let cancelled = false
    setResp(null); setError(null)
    const t0 = performance.now()
    api.getTopicData(bagId, topic, { maxPoints: MAX_POINTS })
      .then(r => { if (!cancelled) { setResp(r); onRuntime?.(performance.now() - t0) } })
      .catch(e => { if (!cancelled) setError(String(e?.message ?? e)) })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bagId, topic])

  const series = useMemo(() => (resp ? extractSeries(resp) : []), [resp])

  // Scales + polylines — recomputed only when the data changes, not on hover.
  const chart = useMemo(() => {
    if (!series.length) return null
    let tMin = Infinity, tMax = -Infinity, vMin = Infinity, vMax = -Infinity
    for (const s of series) {
      for (let i = 0; i < s.ts.length; i++) {
        tMin = Math.min(tMin, s.ts[i]); tMax = Math.max(tMax, s.ts[i])
        const v = s.values[i]
        if (Number.isFinite(v)) { vMin = Math.min(vMin, v); vMax = Math.max(vMax, v) }
      }
    }
    const tSpan = tMax - tMin || 1
    const vSpan = vMax - vMin || 1
    const x = (t: number) => ((t - tMin) / tSpan) * 1000
    const y = (v: number) => 10 + (1 - (v - vMin) / vSpan) * 180
    const polylines = series.map((s, i) => ({
      color: SERIES_COLORS[i % SERIES_COLORS.length],
      points: s.ts
        .map((t, j) => (Number.isFinite(s.values[j]) ? `${x(t).toFixed(1)},${y(s.values[j]).toFixed(1)}` : null))
        .filter(Boolean).join(' '),
    }))
    const spanSec = tSpan / 1e9
    const xLabels = [0, 0.25, 0.5, 0.75, 1].map(f => `${(spanSec * f).toFixed(1)}s`)
    return { tMin, tSpan, spanSec, y, polylines, xLabels }
  }, [series])

  // Cursor dots + readout — depends on the hovered fraction.
  const cursorInfo = useMemo(() => {
    if (!chart || cursor == null) return null
    const targetT = chart.tMin + cursor * chart.tSpan
    const dots = series.map((s, i) => {
      // Nearest sample to the cursor time.
      let best = 0, bestDt = Infinity
      for (let j = 0; j < s.ts.length; j++) {
        const dt = Math.abs(s.ts[j] - targetT)
        if (dt < bestDt) { bestDt = dt; best = j }
      }
      const v = s.values[best]
      return { color: SERIES_COLORS[i % SERIES_COLORS.length], label: s.label, value: v, y: chart.y(v) }
    })
    return { xPct: cursor * 100, tSec: cursor * chart.spanSec, dots }
  }, [chart, cursor, series])

  function handleMove(e: React.MouseEvent) {
    if (!onCursor) return
    const rect = e.currentTarget.getBoundingClientRect()
    const frac = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width))
    onCursor(frac)
  }

  if (bagId == null || !topic) return <div className="nb-cell-loading">No topic selected.</div>
  if (error) return <div className="nb-cell-loading">{error}</div>
  if (!resp) return <div className="nb-cell-loading">Loading {topic}…</div>
  if (!series.length || !chart) return <div className="nb-cell-loading">No numeric series to plot on {topic}.</div>

  return (
    <div>
      <div className="nb-plot-legend">
        {series.map((s, i) => {
          const cur = cursorInfo?.dots[i]?.value
          const last = cur != null && Number.isFinite(cur)
            ? cur
            : [...s.values].reverse().find(Number.isFinite)
          return (
            <span className="nb-legend-chip" key={s.label}>
              <span className="nb-legend-swatch" style={{ background: SERIES_COLORS[i % SERIES_COLORS.length] }} />
              {s.label}
              {last != null && <span className="nb-legend-val">{last.toFixed(2)}</span>}
            </span>
          )
        })}
        <span className="nb-plot-meta">{resp.total.toLocaleString()} msgs · ↓{resp.data.length}</span>
      </div>

      <div
        className="nb-chart-wrap"
        style={{ cursor: 'crosshair' }}
        onMouseMove={handleMove}
      >
        <svg className="nb-chart" viewBox="0 0 1000 200" preserveAspectRatio="none">
          {[50, 100, 150].map(gy => (
            <line key={gy} x1="0" x2="1000" y1={gy} y2={gy} stroke="#f0ebe1" strokeWidth="1" />
          ))}
          {chart.polylines.map((p, i) => (
            <polyline key={i} points={p.points} fill="none" stroke={p.color}
              strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" vectorEffect="non-scaling-stroke" />
          ))}
          {cursorInfo && (
            <>
              <line
                x1={cursorInfo.xPct * 10} x2={cursorInfo.xPct * 10} y1="0" y2="200"
                stroke="#2563c9" strokeWidth="1" strokeDasharray="4 3" vectorEffect="non-scaling-stroke"
              />
              {cursorInfo.dots.map((d, i) => (
                Number.isFinite(d.value) && (
                  <circle key={i} cx={cursorInfo.xPct * 10} cy={d.y} r="3.5"
                    fill={d.color} stroke="#fffdf9" strokeWidth="1" vectorEffect="non-scaling-stroke" />
                )
              ))}
            </>
          )}
        </svg>
        {cursorInfo && (
          <div className="nb-cursor-readout" style={{ left: `${cursorInfo.xPct}%` }}>
            {cursorInfo.tSec.toFixed(2)}s
          </div>
        )}
        <div className="nb-chart-xaxis">
          {chart.xLabels.map((l, i) => <span key={i}>{l}</span>)}
        </div>
      </div>
    </div>
  )
}
