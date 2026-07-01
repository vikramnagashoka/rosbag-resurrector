import React, { useEffect, useState } from 'react'
import { api, SceneTopics } from '../../api'

// `scene` cell body — a placeholder for the point cloud + TF tree, showing
// the live metadata line (topic · point count · TF frames) from the
// existing scene endpoints. The full 3D render is the classic Scene tab;
// here it's a compact summary card per the design.

export default function SceneCell({
  bagId, topic, onRuntime,
}: { bagId?: number; topic?: string; onRuntime?: (ms: number) => void }) {
  const [topics, setTopics] = useState<SceneTopics | null>(null)
  const [nPoints, setNPoints] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (bagId == null) return
    let cancelled = false
    setTopics(null); setNPoints(null); setError(null)
    const t0 = performance.now()
    api.listSceneTopics(bagId)
      .then(async st => {
        if (cancelled) return
        setTopics(st)
        onRuntime?.(performance.now() - t0)
        const pc = topic ?? st.pointclouds[0]
        if (pc) {
          try {
            const cloud = await api.getScenePointCloud(bagId, pc, { maxPoints: 20000 })
            if (!cancelled) setNPoints(cloud.n_points)
          } catch { /* point count is a nicety */ }
        }
      })
      .catch(e => { if (!cancelled) setError(String(e?.message ?? e)) })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bagId, topic])

  if (bagId == null) return <div className="nb-cell-loading">No bag bound.</div>
  if (error) return <div className="nb-cell-loading">{error}</div>
  if (!topics) return <div className="nb-cell-loading">Reading scene topics…</div>

  const pc = topic ?? topics.pointclouds[0]
  const frames = [...topics.tf, ...topics.tf_static]
  if (!pc) return <div className="nb-cell-loading">No point cloud topics on this bag.</div>

  const parts = [
    pc,
    nPoints != null ? `${nPoints.toLocaleString()} pts` : null,
    frames.length ? `TF ${frames.length} frame${frames.length === 1 ? '' : 's'}` : null,
  ].filter(Boolean)

  return (
    <div className="nb-scene-box">
      <div className="nb-scene-label">[ {parts.join(' · ')} ]</div>
    </div>
  )
}
