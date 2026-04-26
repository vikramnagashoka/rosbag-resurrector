import React, { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import HealthBadge from '../components/HealthBadge'
import { api, Bag } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'

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
  const [bags, setBags] = useState<Bag[]>([])
  const [search, setSearch] = useState('')
  const [loading, setLoading] = useState(true)
  const [scanPath, setScanPath] = useState('')
  const [scanning, setScanning] = useState(false)
  // Header scan form is collapsed by default once any bags exist —
  // it'd be loud to show a permanent input above the list. The
  // empty state already exposes the same form prominently.
  const [showHeaderScan, setShowHeaderScan] = useState(false)
  const toast = useErrorToast()

  useEffect(() => {
    fetchBags()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  async function fetchBags(query?: string) {
    setLoading(true)
    const result = await runWithToast(toast, () =>
      api.listBags(query ? { search: query } : undefined),
    )
    if (result) setBags(result)
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
    const result = await runWithToast(
      toast,
      () => api.triggerScan(scanPath),
      { errorPrefix: 'Scan failed' },
    )
    if (result) {
      toast.push('info', `Indexed ${result.indexed} of ${result.scanned} bag(s).`)
      await fetchBags()
    }
    setScanning(false)
  }

  async function handleGenerateDemo() {
    setScanning(true)
    const r = await runWithToast(
      toast,
      () =>
        api.generateDemoBag({
          name: `demo_${Date.now()}`,
          duration_sec: 5,
        }),
      { errorPrefix: 'Generate demo' },
    )
    if (r) {
      toast.push('info', `Generated demo bag at ${r.path}`)
      await fetchBags()
    }
    setScanning(false)
  }

  return (
    <div>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: '24px',
        }}
      >
        <h1 style={{ fontSize: '24px', fontWeight: 600 }}>Bag Library</h1>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <button
            onClick={() => setShowHeaderScan(prev => !prev)}
            title="Scan a folder for bag files and add them to the index"
            style={{
              background: showHeaderScan ? '#1f6feb' : '#21262d',
              color: '#fff',
              border: showHeaderScan ? '1px solid #1f6feb' : '1px solid #30363d',
              borderRadius: 6,
              padding: '8px 14px',
              cursor: 'pointer',
              fontSize: 14,
            }}
          >
            {showHeaderScan ? '✕ Close scan' : '+ Scan folder'}
          </button>
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
      </div>

      {/* Collapsible header-level scan input. Shown via the
          "+ Scan folder" toggle so users can index more bags without
          having to clear the library first. */}
      {showHeaderScan && (
        <div
          style={{
            background: '#161b22',
            border: '1px solid #1f6feb',
            borderRadius: 8,
            padding: 16,
            marginBottom: 16,
          }}
        >
          <form
            onSubmit={async e => {
              await handleScan(e)
              if (!scanning) setShowHeaderScan(false)
            }}
            style={{ display: 'flex', gap: 8, alignItems: 'center' }}
          >
            <input
              type="text"
              value={scanPath}
              onChange={e => setScanPath(e.target.value)}
              placeholder="/path/to/bags  (or a single .mcap file)"
              autoFocus
              style={{
                flex: 1,
                background: '#0d1117',
                border: '1px solid #30363d',
                borderRadius: 6,
                padding: '8px 12px',
                color: '#e1e4e8',
                fontSize: 14,
              }}
            />
            <button
              type="submit"
              disabled={scanning || !scanPath.trim()}
              style={{
                background: scanning || !scanPath.trim() ? '#21262d' : '#238636',
                color: '#fff',
                border: 'none',
                borderRadius: 6,
                padding: '8px 16px',
                cursor: scanning || !scanPath.trim() ? 'not-allowed' : 'pointer',
                fontSize: 14,
              }}
            >
              {scanning ? 'Scanning...' : 'Scan'}
            </button>
          </form>
          <div
            style={{
              marginTop: 10,
              display: 'flex',
              alignItems: 'center',
              gap: 8,
              fontSize: 12,
              color: '#8b949e',
            }}
          >
            No data handy?
            <button
              onClick={handleGenerateDemo}
              disabled={scanning}
              style={{
                background: scanning ? '#21262d' : '#21262d',
                color: scanning ? '#484f58' : '#58a6ff',
                border: '1px solid #30363d',
                borderRadius: 4,
                padding: '3px 10px',
                cursor: scanning ? 'not-allowed' : 'pointer',
                fontSize: 12,
              }}
            >
              {scanning ? 'Working...' : 'Generate demo bag'}
            </button>
            <span style={{ color: '#484f58' }}>
              · creates ~/.resurrector/demo_TIMESTAMP.mcap and indexes it
            </span>
          </div>
        </div>
      )}

      {loading ? (
        <p style={{ color: '#8b949e' }}>Loading...</p>
      ) : bags.length === 0 ? (
        <div
          style={{
            background: '#161b22',
            border: '1px solid #30363d',
            borderRadius: '8px',
            padding: '32px',
            textAlign: 'center',
          }}
        >
          <h2 style={{ fontSize: '18px', marginBottom: '8px' }}>
            No bags indexed yet
          </h2>
          <p style={{ color: '#8b949e', marginBottom: '24px' }}>
            Point at a folder of bag files to get started.
          </p>
          <form
            onSubmit={handleScan}
            style={{
              display: 'flex',
              gap: '8px',
              justifyContent: 'center',
              marginBottom: '12px',
            }}
          >
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
          <div style={{ marginTop: 16 }}>
            <p style={{ color: '#8b949e', fontSize: 13, marginBottom: 8 }}>
              No data handy? Generate a synthetic bag right here:
            </p>
            <button
              onClick={handleGenerateDemo}
              disabled={scanning}
              style={{
                background: scanning ? '#21262d' : '#1f6feb',
                color: '#fff',
                border: 'none',
                borderRadius: 6,
                padding: '8px 18px',
                cursor: scanning ? 'not-allowed' : 'pointer',
                fontSize: 14,
              }}
            >
              {scanning ? 'Working...' : 'Generate demo bag'}
            </button>
          </div>
        </div>
      ) : (
        bags.map(bag => (
          <Link
            key={bag.id}
            to={`/bag/${bag.id}`}
            style={{ textDecoration: 'none', color: 'inherit' }}
          >
            <div
              style={cardStyle}
              onMouseEnter={e => (e.currentTarget.style.borderColor = '#58a6ff')}
              onMouseLeave={e => (e.currentTarget.style.borderColor = '#30363d')}
            >
              <div>
                <div
                  style={{
                    fontSize: '16px',
                    fontWeight: 600,
                    color: '#58a6ff',
                    marginBottom: '4px',
                  }}
                >
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
                      <span
                        key={i}
                        style={{
                          background: '#1f2937',
                          padding: '2px 8px',
                          borderRadius: '12px',
                          fontSize: '12px',
                          color: '#8b949e',
                        }}
                      >
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
