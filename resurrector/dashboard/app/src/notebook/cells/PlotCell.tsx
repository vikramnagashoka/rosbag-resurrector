import React, { useEffect, useMemo, useState } from 'react'
import { api, TopicDataResponse } from '../../api'
import { extractSeries, SERIES_COLORS } from './series'

// `plot` cell body — a multi-series line chart rendered as inline SVG from
// the existing downsampled-series endpoint. Topic dropdown lives in the
// cell header (passed up via the workspace). Region-select + linked cursor
// land in later PRs.

const MAX_POINTS = 120

export default function PlotCell({
  bagId, topic, onRuntime,
}: { bagId?: number; topic?: string; onRuntime?: (ms: number) => void }) {
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

  const { polylines, gridY, xLabels } = useMemo(() => {
    if (!series.length) return { polylines: [], gridY: [], xLabels: [] as string[] }
    // Shared scales across all series (quick-look chart).
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
    const y = (v: number) => 10 + (1 - (v - vMin) / vSpan) * 180  // 10..190 in a 0..200 viewBox

    const polylines = series.map((s, i) => ({
      color: SERIES_COLORS[i % SERIES_COLORS.length],
      points: s.ts
        .map((t, j) => (Number.isFinite(s.values[j]) ? `${x(t).toFixed(1)},${y(s.values[j]).toFixed(1)}` : null))
        .filter(Boolean)
        .join(' '),
    }))
    const gridY = [50, 100, 150]
    const startSec = tMin / 1e9
    const span = tSpan / 1e9
    const xLabels = [0, 0.25, 0.5, 0.75, 1].map(f => `${(span * f).toFixed(1)}s`)
    void startSec
    return { polylines, gridY, xLabels }
  }, [series])

  if (bagId == null || !topic) return <div className="nb-cell-loading">No topic selected.</div>
  if (error) return <div className="nb-cell-loading">{error}</div>
  if (!resp) return <div className="nb-cell-loading">Loading {topic}…</div>
  if (!series.length) return <div className="nb-cell-loading">No numeric series to plot on {topic}.</div>

  return (
    <div>
      <div className="nb-plot-legend">
        {series.map((s, i) => {
          const last = [...s.values].reverse().find(Number.isFinite)
          return (
            <span className="nb-legend-chip" key={s.label}>
              <span className="nb-legend-swatch" style={{ background: SERIES_COLORS[i % SERIES_COLORS.length] }} />
              {s.label}
              {last != null && <span className="nb-legend-val">{last.toFixed(2)}</span>}
            </span>
          )
        })}
        <span className="nb-plot-meta">
          {resp.total.toLocaleString()} msgs · ↓{resp.data.length}
        </span>
      </div>

      <div className="nb-chart-wrap">
        <svg className="nb-chart" viewBox="0 0 1000 200" preserveAspectRatio="none">
          {gridY.map(gy => (
            <line key={gy} x1="0" x2="1000" y1={gy} y2={gy} stroke="#f0ebe1" strokeWidth="1" />
          ))}
          {polylines.map((p, i) => (
            <polyline
              key={i}
              points={p.points}
              fill="none"
              stroke={p.color}
              strokeWidth="2"
              strokeLinejoin="round"
              strokeLinecap="round"
              vectorEffect="non-scaling-stroke"
            />
          ))}
        </svg>
        <div className="nb-chart-xaxis">
          {xLabels.map((l, i) => <span key={i}>{l}</span>)}
        </div>
      </div>
    </div>
  )
}
