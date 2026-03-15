import React, { useState } from 'react'

interface Props {
  bagId: number
  availableTopics: string[]
  onClose: () => void
}

export default function ExportDialog({ bagId, availableTopics, onClose }: Props) {
  const [selectedTopics, setSelectedTopics] = useState<string[]>(availableTopics)
  const [format, setFormat] = useState('parquet')
  const [sync, setSync] = useState(false)
  const [exporting, setExporting] = useState(false)
  const [result, setResult] = useState<string | null>(null)

  function toggleTopic(topic: string) {
    setSelectedTopics(prev =>
      prev.includes(topic) ? prev.filter(t => t !== topic) : [...prev, topic]
    )
  }

  async function handleExport() {
    setExporting(true)
    try {
      const topics = selectedTopics.join(',')
      const res = await fetch(
        `/api/bags/${bagId}/export?topics=${encodeURIComponent(topics)}&format=${format}&sync=${sync}`,
        { method: 'POST' }
      )
      const data = await res.json()
      setResult(data.output_path || 'Export completed')
    } catch (err) {
      setResult('Export failed')
    }
    setExporting(false)
  }

  const overlayStyle: React.CSSProperties = {
    position: 'fixed',
    inset: 0,
    background: 'rgba(0,0,0,0.7)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 100,
  }

  const dialogStyle: React.CSSProperties = {
    background: '#161b22',
    border: '1px solid #30363d',
    borderRadius: '12px',
    padding: '24px',
    width: '500px',
    maxHeight: '80vh',
    overflow: 'auto',
  }

  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={dialogStyle} onClick={e => e.stopPropagation()}>
        <h2 style={{ fontSize: '18px', fontWeight: 600, marginBottom: '16px' }}>Export Data</h2>

        <div style={{ marginBottom: '16px' }}>
          <label style={{ fontSize: '13px', color: '#8b949e', display: 'block', marginBottom: '8px' }}>Format</label>
          <select
            value={format}
            onChange={e => setFormat(e.target.value)}
            style={{
              background: '#0d1117',
              border: '1px solid #30363d',
              borderRadius: '6px',
              padding: '8px 12px',
              color: '#e1e4e8',
              width: '100%',
            }}
          >
            <option value="parquet">Parquet</option>
            <option value="hdf5">HDF5</option>
            <option value="csv">CSV</option>
            <option value="numpy">NumPy (.npz)</option>
            <option value="zarr">Zarr</option>
          </select>
        </div>

        <div style={{ marginBottom: '16px' }}>
          <label style={{ fontSize: '13px', color: '#8b949e', display: 'block', marginBottom: '8px' }}>Topics</label>
          {availableTopics.map(topic => (
            <label key={topic} style={{
              display: 'flex', alignItems: 'center', gap: '6px',
              fontSize: '13px', marginBottom: '4px', cursor: 'pointer',
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

        <label style={{
          display: 'flex', alignItems: 'center', gap: '8px',
          fontSize: '13px', marginBottom: '16px', cursor: 'pointer',
        }}>
          <input type="checkbox" checked={sync} onChange={e => setSync(e.target.checked)} />
          Synchronize topics before export
        </label>

        {result && (
          <div style={{
            background: '#0d2818',
            border: '1px solid #238636',
            borderRadius: '6px',
            padding: '8px 12px',
            color: '#3fb950',
            fontSize: '13px',
            marginBottom: '16px',
          }}>
            {result}
          </div>
        )}

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '8px' }}>
          <button
            onClick={onClose}
            style={{
              background: '#21262d',
              border: '1px solid #30363d',
              borderRadius: '6px',
              padding: '8px 16px',
              color: '#e1e4e8',
              cursor: 'pointer',
            }}
          >
            Close
          </button>
          <button
            onClick={handleExport}
            disabled={exporting || selectedTopics.length === 0}
            style={{
              background: '#238636',
              border: 'none',
              borderRadius: '6px',
              padding: '8px 16px',
              color: '#fff',
              cursor: 'pointer',
              fontWeight: 600,
            }}
          >
            {exporting ? 'Exporting...' : 'Export'}
          </button>
        </div>
      </div>
    </div>
  )
}
