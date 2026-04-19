import React, { useState } from 'react'
import { api, TopicDataResponse } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'

interface Props {
  bagId: number
  availableTopics: string[]
}

export default function SyncView({ bagId, availableTopics }: Props) {
  const [selectedTopics, setSelectedTopics] = useState<string[]>([])
  const [method, setMethod] = useState<'nearest' | 'interpolate' | 'sample_and_hold'>('nearest')
  const [data, setData] = useState<TopicDataResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const toast = useErrorToast()

  function toggleTopic(topic: string) {
    setSelectedTopics(prev =>
      prev.includes(topic) ? prev.filter(t => t !== topic) : [...prev, topic],
    )
  }

  async function sync() {
    if (selectedTopics.length < 2) return
    setLoading(true)
    const r = await runWithToast(
      toast,
      () => api.getSyncedData(bagId, selectedTopics, method),
      { errorPrefix: 'Sync' },
    )
    if (r) setData(r)
    setLoading(false)
  }

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
        Synchronized Multi-Topic View
      </h3>

      <div style={{ display: 'flex', gap: 16, marginBottom: 16, flexWrap: 'wrap' }}>
        {availableTopics.map(topic => (
          <label
            key={topic}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 6,
              fontSize: 13,
              cursor: 'pointer',
              color: selectedTopics.includes(topic) ? '#58a6ff' : '#8b949e',
            }}
          >
            <input
              type="checkbox"
              checked={selectedTopics.includes(topic)}
              onChange={() => toggleTopic(topic)}
            />
            {topic}
          </label>
        ))}
      </div>

      <div style={{ display: 'flex', gap: 12, alignItems: 'center', marginBottom: 16 }}>
        <select
          value={method}
          onChange={e => setMethod(e.target.value as typeof method)}
          style={{
            background: '#0d1117',
            border: '1px solid #30363d',
            borderRadius: 6,
            padding: '6px 12px',
            color: '#e1e4e8',
            fontSize: 13,
          }}
        >
          <option value="nearest">Nearest</option>
          <option value="interpolate">Interpolate</option>
          <option value="sample_and_hold">Sample &amp; Hold</option>
        </select>
        <button
          onClick={sync}
          disabled={selectedTopics.length < 2 || loading}
          style={{
            background: selectedTopics.length >= 2 ? '#238636' : '#21262d',
            color: '#fff',
            border: 'none',
            borderRadius: 6,
            padding: '6px 16px',
            cursor: selectedTopics.length >= 2 ? 'pointer' : 'not-allowed',
            fontSize: 13,
          }}
        >
          {loading ? 'Syncing...' : 'Synchronize'}
        </button>
      </div>

      {data && (
        <div>
          <div style={{ fontSize: 13, color: '#8b949e', marginBottom: 8 }}>
            {data.total.toLocaleString()} synchronized rows · {data.columns.length} columns
          </div>
          <div style={{ overflow: 'auto', maxHeight: 400 }}>
            <table style={{ borderCollapse: 'collapse', fontSize: 12, width: '100%' }}>
              <thead>
                <tr>
                  {data.columns.slice(0, 12).map(col => (
                    <th
                      key={col}
                      style={{
                        padding: '4px 8px',
                        textAlign: 'right',
                        color: '#8b949e',
                        borderBottom: '1px solid #30363d',
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.data.slice(0, 50).map((row, i) => (
                  <tr key={i}>
                    {data.columns.slice(0, 12).map(col => (
                      <td
                        key={col}
                        style={{
                          padding: '2px 8px',
                          textAlign: 'right',
                          borderBottom: '1px solid #21262d',
                          whiteSpace: 'nowrap',
                        }}
                      >
                        {typeof row[col] === 'number'
                          ? Number(row[col]).toFixed(4)
                          : String(row[col] ?? '')}
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
