import React, { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import TopicPlot from '../components/TopicPlot'
import Timeline from '../components/Timeline'
import HealthBadge from '../components/HealthBadge'

interface BagData {
  id: number
  path: string
  duration_sec: number
  size_bytes: number
  message_count: number
  health_score: number | null
  topics: { name: string; message_type: string; message_count: number; frequency_hz: number | null; health_score: number | null }[]
}

interface TopicData {
  topic: string
  total: number
  columns: string[]
  data: Record<string, any>[]
}

function formatSize(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let size = bytes
  for (const unit of units) {
    if (size < 1024) return `${size.toFixed(1)} ${unit}`
    size /= 1024
  }
  return `${size.toFixed(1)} PB`
}

function basename(path: string): string {
  return path.split(/[/\\]/).pop() || path
}

export default function Explorer() {
  const { id } = useParams<{ id: string }>()
  const [bag, setBag] = useState<BagData | null>(null)
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null)
  const [topicData, setTopicData] = useState<TopicData | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch(`/api/bags/${id}`)
      .then(r => r.json())
      .then(setBag)
      .finally(() => setLoading(false))
  }, [id])

  useEffect(() => {
    if (!selectedTopic) {
      setTopicData(null)
      return
    }
    const topicPath = selectedTopic.startsWith('/') ? selectedTopic.slice(1) : selectedTopic
    fetch(`/api/bags/${id}/topics/${topicPath}?limit=2000`)
      .then(r => r.json())
      .then(setTopicData)
  }, [id, selectedTopic])

  if (loading || !bag) return <p style={{ color: '#8b949e' }}>Loading...</p>

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px' }}>
        <div>
          <h1 style={{ fontSize: '24px', fontWeight: 600 }}>{basename(bag.path)}</h1>
          <div style={{ color: '#8b949e', fontSize: '14px', marginTop: '4px' }}>
            {bag.duration_sec?.toFixed(1)}s | {formatSize(bag.size_bytes)} | {bag.message_count?.toLocaleString()} messages
          </div>
        </div>
        <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
          <Link to={`/bag/${id}/health`} style={{
            background: '#21262d',
            border: '1px solid #30363d',
            borderRadius: '6px',
            padding: '6px 12px',
            color: '#e1e4e8',
            fontSize: '13px',
          }}>View Health Report</Link>
          <HealthBadge score={bag.health_score} />
        </div>
      </div>

      <Timeline topics={bag.topics} onSelectTopic={setSelectedTopic} selectedTopic={selectedTopic} />

      <div style={{ display: 'grid', gridTemplateColumns: '300px 1fr', gap: '24px', marginTop: '24px' }}>
        {/* Topic list */}
        <div style={{ background: '#161b22', border: '1px solid #30363d', borderRadius: '8px', padding: '12px' }}>
          <h3 style={{ fontSize: '14px', fontWeight: 600, marginBottom: '12px', color: '#8b949e' }}>Topics</h3>
          {bag.topics.map(topic => (
            <div
              key={topic.name}
              onClick={() => setSelectedTopic(topic.name)}
              style={{
                padding: '8px 12px',
                borderRadius: '6px',
                cursor: 'pointer',
                marginBottom: '4px',
                background: selectedTopic === topic.name ? '#1f6feb22' : 'transparent',
                border: selectedTopic === topic.name ? '1px solid #1f6feb' : '1px solid transparent',
              }}
            >
              <div style={{ fontSize: '13px', fontWeight: 500, color: '#58a6ff' }}>{topic.name}</div>
              <div style={{ fontSize: '12px', color: '#8b949e' }}>
                {topic.message_type} | {topic.message_count.toLocaleString()} msgs
                {topic.frequency_hz ? ` | ${topic.frequency_hz.toFixed(1)}Hz` : ''}
              </div>
            </div>
          ))}
        </div>

        {/* Topic data view */}
        <div>
          {topicData ? (
            <TopicPlot data={topicData} />
          ) : (
            <div style={{
              background: '#161b22',
              border: '1px solid #30363d',
              borderRadius: '8px',
              padding: '48px',
              textAlign: 'center',
              color: '#8b949e',
            }}>
              Select a topic to view its data
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
