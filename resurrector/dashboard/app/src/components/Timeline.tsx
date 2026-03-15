import React from 'react'

interface Topic {
  name: string
  message_type: string
  message_count: number
  frequency_hz: number | null
  health_score: number | null
}

interface Props {
  topics: Topic[]
  selectedTopic: string | null
  onSelectTopic: (name: string) => void
}

function healthColor(score: number | null): string {
  if (score === null) return '#30363d'
  if (score >= 90) return '#238636'
  if (score >= 70) return '#9e6a03'
  return '#da3633'
}

export default function Timeline({ topics, selectedTopic, onSelectTopic }: Props) {
  const maxCount = Math.max(...topics.map(t => t.message_count), 1)

  return (
    <div style={{
      background: '#161b22',
      border: '1px solid #30363d',
      borderRadius: '8px',
      padding: '16px',
    }}>
      <h3 style={{ fontSize: '14px', fontWeight: 600, color: '#8b949e', marginBottom: '12px' }}>
        Topic Timeline
      </h3>
      {topics.map(topic => {
        const widthPct = (topic.message_count / maxCount) * 100
        const isSelected = selectedTopic === topic.name
        return (
          <div
            key={topic.name}
            onClick={() => onSelectTopic(topic.name)}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '12px',
              padding: '4px 8px',
              marginBottom: '4px',
              cursor: 'pointer',
              borderRadius: '4px',
              background: isSelected ? '#1f6feb22' : 'transparent',
            }}
          >
            <div style={{
              width: '200px',
              fontSize: '12px',
              color: '#58a6ff',
              flexShrink: 0,
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}>
              {topic.name}
            </div>
            <div style={{ flex: 1, height: '16px', background: '#0d1117', borderRadius: '4px', overflow: 'hidden' }}>
              <div style={{
                width: `${widthPct}%`,
                height: '100%',
                background: healthColor(topic.health_score),
                borderRadius: '4px',
                opacity: 0.8,
                transition: 'width 0.3s',
              }} />
            </div>
            <div style={{ width: '80px', fontSize: '11px', color: '#8b949e', textAlign: 'right', flexShrink: 0 }}>
              {topic.frequency_hz ? `${topic.frequency_hz.toFixed(0)}Hz` : ''}
            </div>
          </div>
        )
      })}
    </div>
  )
}
