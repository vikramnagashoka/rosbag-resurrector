import React, { useEffect, useMemo, useState } from 'react'
import { api, TopicDataResponse } from '../../api'
import { extractSeries, seriesStats, SERIES_COLORS } from './series'

// `stats` cell body — min / mean / max / σ per numeric field. Computed
// client-side over a downsampled sample of the topic (there's no dedicated
// stats endpoint; the footer is honest that it's over the sampled points).

const SAMPLE_POINTS = 500

export default function StatsCell({
  bagId, topic, onRuntime,
}: { bagId?: number; topic?: string; onRuntime?: (ms: number) => void }) {
  const [resp, setResp] = useState<TopicDataResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (bagId == null || !topic) return
    let cancelled = false
    setResp(null); setError(null)
    const t0 = performance.now()
    api.getTopicData(bagId, topic, { maxPoints: SAMPLE_POINTS })
      .then(r => { if (!cancelled) { setResp(r); onRuntime?.(performance.now() - t0) } })
      .catch(e => { if (!cancelled) setError(String(e?.message ?? e)) })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bagId, topic])

  const series = useMemo(() => (resp ? extractSeries(resp) : []), [resp])

  if (bagId == null || !topic) return <div className="nb-cell-loading">No topic selected.</div>
  if (error) return <div className="nb-cell-loading">{error}</div>
  if (!resp) return <div className="nb-cell-loading">Computing stats for {topic}…</div>
  if (!series.length) return <div className="nb-cell-loading">No numeric fields on {topic}.</div>

  return (
    <div>
      <div className="nb-table-scroll">
        <table className="nb-table">
          <thead>
            <tr><th>field</th><th>min</th><th>mean</th><th>max</th><th>σ</th></tr>
          </thead>
          <tbody>
            {series.map((s, i) => {
              const st = seriesStats(s.values)
              return (
                <tr key={s.label}>
                  <td>
                    <span className="nb-table-field">
                      <span className="nb-table-swatch" style={{ background: SERIES_COLORS[i % SERIES_COLORS.length] }} />
                      {s.label}
                    </span>
                  </td>
                  <td>{st.min.toFixed(2)}</td>
                  <td>{st.mean.toFixed(2)}</td>
                  <td>{st.max.toFixed(2)}</td>
                  <td>{st.std.toFixed(2)}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      <div className="nb-table-foot">
        {resp.total.toLocaleString()} messages · over {resp.data.length} sampled points
      </div>
    </div>
  )
}
