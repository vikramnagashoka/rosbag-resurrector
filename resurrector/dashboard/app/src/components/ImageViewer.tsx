import React, { useMemo, useState } from 'react'
import { api } from '../api'

interface Props {
  bagId: number
  topic: string
  totalFrames: number
}

export default function ImageViewer({ bagId, topic, totalFrames }: Props) {
  const [currentFrame, setCurrentFrame] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const url = useMemo(
    () => api.frameUrl(bagId, topic, currentFrame, 640),
    [bagId, topic, currentFrame],
  )

  // Thumbnail strip — 10 evenly-spaced frames across the recording.
  const thumbs = useMemo(() => {
    if (totalFrames < 2) return [0]
    return Array.from({ length: 10 }, (_, i) =>
      Math.min(totalFrames - 1, Math.floor((i / 9) * (totalFrames - 1))),
    )
  }, [totalFrames])

  return (
    <div
      style={{
        background: '#161b22',
        border: '1px solid #30363d',
        borderRadius: 8,
        padding: 16,
      }}
    >
      <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 12 }}>
        Image Viewer — {topic}
      </h3>

      <div
        style={{
          background: '#0d1117',
          borderRadius: 6,
          minHeight: 320,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          overflow: 'hidden',
          marginBottom: 12,
        }}
      >
        {error ? (
          <div style={{ color: '#f85149', fontSize: 13 }}>{error}</div>
        ) : (
          <img
            key={url}
            src={url}
            alt={`Frame ${currentFrame}`}
            style={{ maxWidth: '100%', maxHeight: 480 }}
            onLoad={() => {
              setLoading(false)
              setError(null)
            }}
            onLoadStart={() => setLoading(true)}
            onError={() => {
              setLoading(false)
              setError(`Could not load frame ${currentFrame}`)
            }}
          />
        )}
      </div>

      <div
        style={{
          display: 'flex',
          justifyContent: 'center',
          gap: 12,
          marginBottom: 12,
        }}
      >
        <button
          onClick={() => setCurrentFrame(Math.max(0, currentFrame - 1))}
          disabled={currentFrame === 0}
          style={{
            background: '#21262d',
            border: '1px solid #30363d',
            borderRadius: 6,
            padding: '6px 16px',
            color: '#e1e4e8',
            cursor: currentFrame === 0 ? 'not-allowed' : 'pointer',
          }}
        >
          Prev
        </button>
        <input
          type="range"
          min={0}
          max={totalFrames - 1}
          value={currentFrame}
          onChange={e => setCurrentFrame(Number(e.target.value))}
          style={{ flex: 1, maxWidth: 400 }}
        />
        <button
          onClick={() => setCurrentFrame(Math.min(totalFrames - 1, currentFrame + 1))}
          disabled={currentFrame >= totalFrames - 1}
          style={{
            background: '#21262d',
            border: '1px solid #30363d',
            borderRadius: 6,
            padding: '6px 16px',
            color: '#e1e4e8',
            cursor:
              currentFrame >= totalFrames - 1 ? 'not-allowed' : 'pointer',
          }}
        >
          Next
        </button>
      </div>

      <div style={{ fontSize: 12, color: '#8b949e', textAlign: 'center', marginBottom: 12 }}>
        Frame {currentFrame + 1} / {totalFrames}
        {loading && ' (loading...)'}
      </div>

      <div
        style={{
          display: 'flex',
          gap: 4,
          overflowX: 'auto',
          paddingBottom: 4,
        }}
      >
        {thumbs.map(idx => (
          <img
            key={idx}
            src={api.frameUrl(bagId, topic, idx, 120)}
            alt={`Thumb ${idx}`}
            onClick={() => setCurrentFrame(idx)}
            style={{
              width: 80,
              height: 60,
              objectFit: 'cover',
              cursor: 'pointer',
              border:
                currentFrame === idx
                  ? '2px solid #58a6ff'
                  : '1px solid #30363d',
              borderRadius: 4,
              opacity: currentFrame === idx ? 1 : 0.7,
            }}
          />
        ))}
      </div>
    </div>
  )
}
