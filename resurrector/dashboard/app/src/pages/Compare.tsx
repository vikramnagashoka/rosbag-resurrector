import React, { useEffect, useState } from 'react'
import HealthBadge from '../components/HealthBadge'

interface BagEntry {
  id: number
  path: string
  duration_sec: number
  size_bytes: number
  message_count: number
  health_score: number | null
  topics: { name: string; message_type: string; message_count: number }[]
}

function basename(path: string): string {
  return path.split(/[/\\]/).pop() || path
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

export default function Compare() {
  const [bags, setBags] = useState<BagEntry[]>([])
  const [bag1Id, setBag1Id] = useState<number | null>(null)
  const [bag2Id, setBag2Id] = useState<number | null>(null)
  const [bag1, setBag1] = useState<BagEntry | null>(null)
  const [bag2, setBag2] = useState<BagEntry | null>(null)

  useEffect(() => {
    fetch('/api/bags').then(r => r.json()).then(setBags)
  }, [])

  useEffect(() => {
    if (bag1Id) fetch(`/api/bags/${bag1Id}`).then(r => r.json()).then(setBag1)
    else setBag1(null)
  }, [bag1Id])

  useEffect(() => {
    if (bag2Id) fetch(`/api/bags/${bag2Id}`).then(r => r.json()).then(setBag2)
    else setBag2(null)
  }, [bag2Id])

  const selectStyle: React.CSSProperties = {
    background: '#0d1117',
    border: '1px solid #30363d',
    borderRadius: '6px',
    padding: '8px 12px',
    color: '#e1e4e8',
    fontSize: '14px',
    minWidth: '300px',
  }

  const topics1 = new Set(bag1?.topics.map(t => t.name) || [])
  const topics2 = new Set(bag2?.topics.map(t => t.name) || [])
  const shared = bag1 && bag2 ? [...topics1].filter(t => topics2.has(t)) : []
  const only1 = bag1 && bag2 ? [...topics1].filter(t => !topics2.has(t)) : []
  const only2 = bag1 && bag2 ? [...topics2].filter(t => !topics1.has(t)) : []

  return (
    <div>
      <h1 style={{ fontSize: '24px', fontWeight: 600, marginBottom: '24px' }}>Compare Bags</h1>

      <div style={{ display: 'flex', gap: '24px', marginBottom: '32px' }}>
        <select style={selectStyle} onChange={e => setBag1Id(Number(e.target.value) || null)} value={bag1Id || ''}>
          <option value="">Select first bag...</option>
          {bags.map(b => <option key={b.id} value={b.id}>{basename(b.path)}</option>)}
        </select>
        <span style={{ color: '#8b949e', alignSelf: 'center', fontSize: '18px' }}>vs</span>
        <select style={selectStyle} onChange={e => setBag2Id(Number(e.target.value) || null)} value={bag2Id || ''}>
          <option value="">Select second bag...</option>
          {bags.map(b => <option key={b.id} value={b.id}>{basename(b.path)}</option>)}
        </select>
      </div>

      {bag1 && bag2 && (
        <>
          {/* Comparison table */}
          <table style={{ width: '100%', borderCollapse: 'collapse', marginBottom: '24px' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #30363d' }}>
                <th style={{ padding: '10px', textAlign: 'left', color: '#8b949e', width: '200px' }}>Property</th>
                <th style={{ padding: '10px', textAlign: 'left', color: '#58a6ff' }}>{basename(bag1.path)}</th>
                <th style={{ padding: '10px', textAlign: 'left', color: '#3fb950' }}>{basename(bag2.path)}</th>
              </tr>
            </thead>
            <tbody>
              {[
                ['Duration', `${bag1.duration_sec?.toFixed(1)}s`, `${bag2.duration_sec?.toFixed(1)}s`],
                ['Messages', bag1.message_count?.toLocaleString(), bag2.message_count?.toLocaleString()],
                ['Size', formatSize(bag1.size_bytes), formatSize(bag2.size_bytes)],
                ['Topics', String(bag1.topics.length), String(bag2.topics.length)],
              ].map(([prop, v1, v2]) => (
                <tr key={prop} style={{ borderBottom: '1px solid #21262d' }}>
                  <td style={{ padding: '8px 10px', color: '#8b949e', fontWeight: 500 }}>{prop}</td>
                  <td style={{ padding: '8px 10px' }}>{v1}</td>
                  <td style={{ padding: '8px 10px' }}>{v2}</td>
                </tr>
              ))}
              <tr style={{ borderBottom: '1px solid #21262d' }}>
                <td style={{ padding: '8px 10px', color: '#8b949e', fontWeight: 500 }}>Health</td>
                <td style={{ padding: '8px 10px' }}><HealthBadge score={bag1.health_score} size="small" /></td>
                <td style={{ padding: '8px 10px' }}><HealthBadge score={bag2.health_score} size="small" /></td>
              </tr>
            </tbody>
          </table>

          {/* Topic comparison */}
          {shared.length > 0 && (
            <div style={{ marginBottom: '24px' }}>
              <h3 style={{ fontSize: '16px', fontWeight: 600, marginBottom: '12px', color: '#8b949e' }}>
                Shared Topics ({shared.length})
              </h3>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr style={{ borderBottom: '1px solid #30363d' }}>
                    <th style={{ padding: '8px', textAlign: 'left', color: '#8b949e' }}>Topic</th>
                    <th style={{ padding: '8px', textAlign: 'right', color: '#58a6ff' }}>Count (Bag 1)</th>
                    <th style={{ padding: '8px', textAlign: 'right', color: '#3fb950' }}>Count (Bag 2)</th>
                  </tr>
                </thead>
                <tbody>
                  {shared.sort().map(topic => {
                    const t1 = bag1.topics.find(t => t.name === topic)!
                    const t2 = bag2.topics.find(t => t.name === topic)!
                    return (
                      <tr key={topic} style={{ borderBottom: '1px solid #21262d' }}>
                        <td style={{ padding: '6px 8px', color: '#58a6ff', fontSize: '13px' }}>{topic}</td>
                        <td style={{ padding: '6px 8px', textAlign: 'right', fontSize: '13px' }}>{t1.message_count.toLocaleString()}</td>
                        <td style={{ padding: '6px 8px', textAlign: 'right', fontSize: '13px' }}>{t2.message_count.toLocaleString()}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}

          {only1.length > 0 && (
            <p style={{ color: '#d29922', marginBottom: '8px' }}>
              Only in {basename(bag1.path)}: {only1.sort().join(', ')}
            </p>
          )}
          {only2.length > 0 && (
            <p style={{ color: '#d29922' }}>
              Only in {basename(bag2.path)}: {only2.sort().join(', ')}
            </p>
          )}
        </>
      )}
    </div>
  )
}
