import React, { useState } from 'react'
import { api } from '../../api'

// `image` cell body — a camera-frame viewer with a scrubber over the
// topic's message range, using the existing frame-fetch URL endpoint.
// Linked-cursor "follow" mode lands in PR 5.

export default function ImageCell({
  bagId, topic, frameCount, frame, cursor, following, onSetFrame,
}: {
  bagId?: number
  topic?: string
  frameCount: number
  frame: number
  cursor?: number | null
  following?: boolean   // linked to the shared cursor (vs own-time slider)
  onSetFrame: (n: number) => void
}) {
  const [broken, setBroken] = useState(false)
  if (bagId == null || !topic) return <div className="nb-cell-loading">No image topic on this bag.</div>

  const maxIdx = Math.max(0, frameCount - 1)
  // When following the linked cursor, the frame tracks it; otherwise the
  // slider drives (own time).
  const followFrame = following && cursor != null
  const idx = followFrame
    ? Math.round(cursor * maxIdx)
    : Math.min(frame, maxIdx)
  const src = api.frameUrl(bagId, topic, idx, 480)

  return (
    <div>
      <div className="nb-image-frame">
        {broken
          ? <span className="nb-image-placeholder">frame {idx} unavailable</span>
          : <img src={src} alt={`${topic} frame ${idx}`} onError={() => setBroken(true)} />}
      </div>
      <div className="nb-image-controls">
        <input
          className="nb-image-range"
          type="range"
          min={0}
          max={maxIdx}
          value={idx}
          disabled={followFrame}
          onChange={e => { setBroken(false); onSetFrame(Number(e.target.value)) }}
          aria-label="Frame scrubber"
        />
        <span className="nb-image-count">
          {followFrame
            ? <span className="nb-image-follow">↳ following linked cursor</span>
            : `frame ${idx} / ${maxIdx}`}
        </span>
      </div>
    </div>
  )
}
