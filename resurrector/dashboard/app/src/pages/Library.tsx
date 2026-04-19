import React, { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import HealthBadge from '../components/HealthBadge'

interface BagEntry {
  id: number
  path: string
  duration_sec: number
  size_bytes: number
  message_count: number
  health_score: number | null
  topics: { name: string; message_type: string; message_count: number }[]
  tags: { key: string; value: string }[]
}

const cardStyle: React.CSSProperties = {
  background: '#161b22',
  border: '1px solid #30363d',
  borderRadius: '8px',
  padding: '16px',
  marginBottom: '12px',
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'center',
  transition: 'border-color 0.2s',
}

const statStyle: React.CSSProperties = {
  color: '#8b949e',
  fontSize: '13px',
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

export default function Library() {
  const [bags, setBags] = useState<BagEntry[]>([])
  const [search, setSearch] = useState('')
  const [loading, setLoading] = useState(true)
  const [scanPath, setScanPath] = useState('')
  const [scanning, setScanning] = useState(false)
  const [scanMsg, setScanMsg] = useState<string | null>(null)

  useEffect(() => {
    fetchBags()
  }, [])

  async function fetchBags(query?: string) {
    setLoading(true)
    try {
      const url = query
        ? `/api/bags?search=${encodeURIComponent(query)}`
        : '/api/bags'
      const res = await fetch(url)
      const data = await res.json()
      setBags(data)
    } catch (err) {
      console.error('Failed to fetch bags:', err)
    }
    setLoading(false)
  }

  function handleSearch(e: React.FormEvent) {
    e.preventDefault()
    fetchBags(search || undefined)
  }

  async function handleScan(e: React.FormEvent) {
    e.preventDefault()
    if (!scanPath.trim()) return
    setScanning(true)
    setScanMsg(null)
    try {
      const res = await fetch(`/api/scan?path=${encodeURIComponent(scanPath)}`, {
        method: 'POST',
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }))
        throw new Error(err.detail || 'Scan failed')
      }
      const data = await res.json()
      setScanMsg(`Indexed ${data.indexed ?? '?'} bag(s). Reloading...`)
      await fetchBags()
    } catch (err: any) {
      setScanMsg(`Error: ${err.message || err}`)
    }
    setScanning(false)
  }

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px' }}>
        <h1 style={{ fontSize: '24px', fontWeight: 600 }}>Bag Library</h1>
        <form onSubmit={handleSearch} style={{ display: 'flex', gap: '8px' }}>
          <input
            type="text"
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="topic:/camera/rgb health:>80 after:2025-01"
            style={{
              background: '#0d1117',
              border: '1px solid #30363d',
              borderRadius: '6px',
              padding: '8px 12px',
              color: '#e1e4e8',
              width: '400px',
              fontSize: '14px',
            }}
          />
          <button
            type="submit"
            style={{
              background: '#238636',
              color: '#fff',
              border: 'none',
              borderRadius: '6px',
              padding: '8px 16px',
              cursor: 'pointer',
              fontSize: '14px',
            }}
          >
            Search
          </button>
        </form>
      </div>

      {loading ? (
        <p style={{ color: '#8b949e' }}>Loading...</p>
      ) : bags.length === 0 ? (
        <div style={{
          background: '#161b22',
          border: '1px solid #30363d',
          borderRadius: '8px',
          padding: '32px',
          textAlign: 'center',
        }}>
          <h2 style={{ fontSize: '18px', marginBottom: '8px' }}>No bags indexed yet</h2>
          <p style={{ color: '#8b949e', marginBottom: '24px' }}>
            Point at a folder of bag files to get started.
          </p>
          <form onSubmit={handleScan} style={{ display: 'flex', gap: '8px', justifyContent: 'center', marginBottom: '12px' }}>
            <input
              type="text"
              value={scanPath}
              onChange={e => setScanPath(e.target.value)}
              placeholder="/path/to/bags"
              style={{
                background: '#0d1117',
                border: '1px solid #30363d',
                borderRadius: '6px',
                padding: '8px 12px',
                color: '#e1e4e8',
                width: '400px',
                fontSize: '14px',
              }}
            />
            <button
              type="submit"
              disabled={scanning}
              style={{
                background: scanning ? '#21262d' : '#238636',
                color: '#fff',
                border: 'none',
                borderRadius: '6px',
                padding: '8px 16px',
                cursor: scanning ? 'not-allowed' : 'pointer',
                fontSize: '14px',
              }}
            >
              {scanning ? 'Scanning...' : 'Scan folder'}
            </button>
          </form>
          {scanMsg && (
            <p style={{ color: scanMsg.startsWith('Error') ? '#f85149' : '#3fb950', fontSize: '13px' }}>
              {scanMsg}
            </p>
          )}
          <p style={{ color: '#8b949e', fontSize: '13px', marginTop: '16px' }}>
            No data handy? Run <code style={{ background: '#0d1117', padding: '2px 6px', borderRadius: '4px' }}>resurrector demo</code> in a terminal to generate a sample bag.
          </p>
          <p style={{ color: '#8b949e', fontSize: '13px', marginTop: '8px' }}>
            Or from a terminal: <code style={{ background: '#0d1117', padding: '2px 6px', borderRadius: '4px' }}>resurrector scan /path/to/bags</code>
          </p>
        </div>
      ) : (
        bags.map(bag => (
          <Link key={bag.id} to={`/bag/${bag.id}`} style={{ textDecoration: 'none', color: 'inherit' }}>
            <div
              style={cardStyle}
              onMouseEnter={e => (e.currentTarget.style.borderColor = '#58a6ff')}
              onMouseLeave={e => (e.currentTarget.style.borderColor = '#30363d')}
            >
              <div>
                <div style={{ fontSize: '16px', fontWeight: 600, color: '#58a6ff', marginBottom: '4px' }}>
                  {basename(bag.path)}
                </div>
                <div style={{ display: 'flex', gap: '16px', ...statStyle }}>
                  <span>{bag.duration_sec?.toFixed(1)}s</span>
                  <span>{formatSize(bag.size_bytes)}</span>
                  <span>{bag.topics.length} topics</span>
                  <span>{bag.message_count?.toLocaleString()} msgs</span>
                </div>
                {bag.tags.length > 0 && (
                  <div style={{ marginTop: '4px', display: 'flex', gap: '6px' }}>
                    {bag.tags.map((t, i) => (
                      <span key={i} style={{
                        background: '#1f2937',
                        padding: '2px 8px',
                        borderRadius: '12px',
                        fontSize: '12px',
                        color: '#8b949e',
                      }}>
                        {t.key}:{t.value}
                      </span>
                    ))}
                  </div>
                )}
              </div>
              <HealthBadge score={bag.health_score} />
            </div>
          </Link>
        ))
      )}
    </div>
  )
}
