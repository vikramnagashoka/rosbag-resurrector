import React, { useState } from 'react'

interface Props {
  bagId: number
  topic: string
  totalFrames: number
}

export default function ImageViewer({ bagId, topic, totalFrames }: Props) {
  const [currentFrame, setCurrentFrame] = useState(0)

  return (
    <div style={{
      background: '#161b22',
      border: '1px solid #30363d',
      borderRadius: '8px',
      padding: '16px',
    }}>
      <h3 style={{ fontSize: '14px', fontWeight: 600, marginBottom: '12px' }}>
        Image Viewer — {topic}
      </h3>
      <div style={{ textAlign: 'center', padding: '24px', color: '#8b949e' }}>
        Image viewing requires streaming image data from the API.
        <br />
        Frame {currentFrame + 1} of {totalFrames}
      </div>
      <div style={{ display: 'flex', justifyContent: 'center', gap: '12px', marginTop: '12px' }}>
        <button
          onClick={() => setCurrentFrame(Math.max(0, currentFrame - 1))}
          disabled={currentFrame === 0}
          style={{
            background: '#21262d',
            border: '1px solid #30363d',
            borderRadius: '6px',
            padding: '6px 16px',
            color: '#e1e4e8',
            cursor: 'pointer',
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
          style={{ flex: 1, maxWidth: '400px' }}
        />
        <button
          onClick={() => setCurrentFrame(Math.min(totalFrames - 1, currentFrame + 1))}
          disabled={currentFrame >= totalFrames - 1}
          style={{
            background: '#21262d',
            border: '1px solid #30363d',
            borderRadius: '6px',
            padding: '6px 16px',
            color: '#e1e4e8',
            cursor: 'pointer',
          }}
        >
          Next
        </button>
      </div>
    </div>
  )
}
