import React, { useState } from 'react'

interface Props {
  bagId: number
  availableTopics: string[]
}

export default function SyncView({ bagId, availableTopics }: Props) {
  const [selectedTopics, setSelectedTopics] = useState<string[]>([])
  const [method, setMethod] = useState('nearest')
  const [data, setData] = useState<any>(null)
  const [loading, setLoading] = useState(false)

  function toggleTopic(topic: string) {
    setSelectedTopics(prev =>
      prev.includes(topic) ? prev.filter(t => t !== topic) : [...prev, topic]
    )
  }

  async function sync() {
    if (selectedTopics.length < 2) return
    setLoading(true)
    try {
      const topics = selectedTopics.join(',')
      const res = await fetch(`/api/bags/${bagId}/sync?topics=${encodeURIComponent(topics)}&method=${method}`)
      const result = await res.json()
      setData(result)
    } catch (err) {
      console.error('Sync failed:', err)
    }
    setLoading(false)
  }

  return (
    <div style={{
      background: '#161b22',
      border: '1px solid #30363d',
      borderRadius: '8px',
      padding: '16px',
    }}>
      <h3 style={{ fontSize: '14px', fontWeight: 600, marginBottom: '12px' }}>
        Synchronized Multi-Topic View
      </h3>

      <div style={{ display: 'flex', gap: '16px', marginBottom: '16px', flexWrap: 'wrap' }}>
        {availableTopics.map(topic => (
          <label key={topic} style={{
            display: 'flex', alignItems: 'center', gap: '6px',
            fontSize: '13px', cursor: 'pointer',
            color: selectedTopics.includes(topic) ? '#58a6ff' : '#8b949e',
          }}>
            <input
              type="checkbox"
              checked={selectedTopics.includes(topic)}
              onChange={() => toggleTopic(topic)}
            />
            {topic}
          </label>
        ))}
      </div>

      <div style={{ display: 'flex', gap: '12px', alignItems: 'center', marginBottom: '16px' }}>
        <select
          value={method}
          onChange={e => setMethod(e.target.value)}
          style={{
            background: '#0d1117',
            border: '1px solid #30363d',
            borderRadius: '6px',
            padding: '6px 12px',
            color: '#e1e4e8',
            fontSize: '13px',
          }}
        >
          <option value="nearest">Nearest</option>
          <option value="interpolate">Interpolate</option>
          <option value="sample_and_hold">Sample & Hold</option>
        </select>
        <button
          onClick={sync}
          disabled={selectedTopics.length < 2 || loading}
          style={{
            background: selectedTopics.length >= 2 ? '#238636' : '#21262d',
            color: '#fff',
            border: 'none',
            borderRadius: '6px',
            padding: '6px 16px',
            cursor: selectedTopics.length >= 2 ? 'pointer' : 'not-allowed',
            fontSize: '13px',
          }}
        >
          {loading ? 'Syncing...' : 'Synchronize'}
        </button>
      </div>

      {data && (
        <div>
          <div style={{ fontSize: '13px', color: '#8b949e', marginBottom: '8px' }}>
            {data.total} synchronized rows | {data.columns.length} columns
          </div>
          <div style={{ overflow: 'auto', maxHeight: '400px' }}>
            <table style={{ borderCollapse: 'collapse', fontSize: '12px', width: '100%' }}>
              <thead>
                <tr>
                  {data.columns.slice(0, 12).map((col: string) => (
                    <th key={col} style={{
                      padding: '4px 8px',
                      textAlign: 'right',
                      color: '#8b949e',
                      borderBottom: '1px solid #30363d',
                      whiteSpace: 'nowrap',
                    }}>
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.data.slice(0, 50).map((row: any, i: number) => (
                  <tr key={i}>
                    {data.columns.slice(0, 12).map((col: string) => (
                      <td key={col} style={{
                        padding: '2px 8px',
                        textAlign: 'right',
                        borderBottom: '1px solid #21262d',
                        whiteSpace: 'nowrap',
                      }}>
                        {typeof row[col] === 'number' ? row[col].toFixed(4) : String(row[col] ?? '')}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
