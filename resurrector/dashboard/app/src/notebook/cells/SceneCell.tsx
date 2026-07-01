import React, { useState } from 'react'
import SceneCanvas from './SceneCanvas'

// `scene` cell body — a live 3D render (point cloud + TF triads) sized to
// the cell, with a metadata caption. Uses the slim SceneCanvas (react-three-
// fiber) rather than the full classic SceneViewer chrome. Renders the scene
// at the bag's mid-time; drag to orbit.

export default function SceneCell({
  bagId, topic, timeNs, onRuntime,
}: {
  bagId?: number
  topic?: string
  timeNs: number
  onRuntime?: (ms: number) => void
}) {
  const [stats, setStats] = useState<{ nPoints: number; nFrames: number } | null>(null)

  if (bagId == null) return <div className="nb-cell-loading">No bag bound.</div>
  if (!topic) return <div className="nb-cell-loading">No point cloud topics on this bag.</div>

  const caption = [
    topic,
    stats ? `${stats.nPoints.toLocaleString()} pts` : null,
    stats && stats.nFrames ? `TF ${stats.nFrames} frame${stats.nFrames === 1 ? '' : 's'}` : null,
  ].filter(Boolean).join(' · ')

  return (
    <div>
      <div className="nb-scene-box nb-scene-live">
        <SceneCanvas
          bagId={bagId}
          topic={topic}
          timeNs={timeNs}
          onRuntime={onRuntime}
          onStats={setStats}
        />
      </div>
      <div className="nb-scene-caption">[ {caption} · drag to orbit ]</div>
    </div>
  )
}
