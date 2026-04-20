import React, { useState } from 'react'
import { Link } from 'react-router-dom'
import { api, FrameSearchResult } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'

export default function Search() {
  const [query, setQuery] = useState('')
  const [clips, setClips] = useState(false)
  const [clipDuration, setClipDuration] = useState(5)
  const [minSimilarity, setMinSimilarity] = useState(0.15)
  const [topK, setTopK] = useState(20)
  const [result, setResult] = useState<FrameSearchResult | null>(null)
  const [loading, setLoading] = useState(false)
  const toast = useErrorToast()

  async function runSearch(e: React.FormEvent) {
    e.preventDefault()
    if (!query.trim()) return
    setLoading(true)
    setResult(null)
    const r = await runWithToast(
      toast,
      () =>
        api.searchFrames(query, {
          topK,
          clips,
          clipDuration,
          minSimilarity,
        }),
      { errorPrefix: 'Search' },
    )
    if (r) setResult(r)
    setLoading(false)
  }

  return (
    <div>
      <h1 style={{ fontSize: 24, fontWeight: 600, marginBottom: 4 }}>Semantic frame search</h1>
      <p style={{ color: '#8b949e', fontSize: 14, marginBottom: 20 }}>
        Describe what you're looking for in plain English. We match against CLIP
        embeddings of video frames indexed during <code>resurrector scan</code>.
      </p>

      <form onSubmit={runSearch} style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
          <input
            value={query}
            onChange={e => setQuery(e.target.value)}
            placeholder="robot dropping object, gripper collision with table, bright outdoor scene..."
            autoFocus
            style={{
              flex: 1,
              background: '#0d1117',
              border: '1px solid #30363d',
              borderRadius: 6,
              padding: '10px 14px',
              color: '#e1e4e8',
              fontSize: 14,
            }}
          />
          <button
            type="submit"
            disabled={loading || !query.trim()}
            style={{
              background: loading || !query.trim() ? '#21262d' : '#238636',
              color: '#fff',
              border: 'none',
              borderRadius: 6,
              padding: '10px 20px',
              cursor: loading || !query.trim() ? 'not-allowed' : 'pointer',
              fontSize: 14,
              fontWeight: 600,
            }}
          >
            {loading ? 'Searching...' : 'Search'}
          </button>
        </div>

        <div style={{ display: 'flex', gap: 16, alignItems: 'center', fontSize: 13, color: '#8b949e' }}>
          <label style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
            <input type="checkbox" checked={clips} onChange={e => setClips(e.target.checked)} />
            Return clips instead of single frames
          </label>
          {clips && (
            <label>
              Clip duration:{' '}
              <input
                type="number"
                value={clipDuration}
                onChange={e => setClipDuration(Number(e.target.value))}
                min={1}
                max={30}
                step={0.5}
                style={{ width: 60, background: '#0d1117', border: '1px solid #30363d', color: '#e1e4e8', padding: 4, borderRadius: 4 }}
              />
              s
            </label>
          )}
          <label>
            Top K:{' '}
            <input
              type="number"
              value={topK}
              onChange={e => setTopK(Number(e.target.value))}
              min={1}
              max={100}
              style={{ width: 60, background: '#0d1117', border: '1px solid #30363d', color: '#e1e4e8', padding: 4, borderRadius: 4 }}
            />
          </label>
          <label>
            Min similarity:{' '}
            <input
              type="number"
              value={minSimilarity}
              onChange={e => setMinSimilarity(Number(e.target.value))}
              min={0}
              max={1}
              step={0.05}
              style={{ width: 60, background: '#0d1117', border: '1px solid #30363d', color: '#e1e4e8', padding: 4, borderRadius: 4 }}
            />
          </label>
        </div>
      </form>

      {!result && !loading && (
        <div
          style={{
            background: '#161b22',
            border: '1px solid #30363d',
            borderRadius: 8,
            padding: 32,
            textAlign: 'center',
            color: '#8b949e',
          }}
        >
          Enter a query above to search your indexed frames.
        </div>
      )}

      {result && result.results.length === 0 && (
        <div
          style={{
            background: '#1c1c0e',
            border: '1px solid #d29922',
            borderRadius: 8,
            padding: 24,
            color: '#d29922',
            fontSize: 14,
          }}
        >
          <strong>No matches found.</strong>
          <div style={{ marginTop: 8 }}>
            Possible reasons:
            <ul style={{ paddingLeft: 20, marginTop: 4 }}>
              <li>No bags have indexed frames yet — run <code>resurrector index-frames /path/to/bags</code></li>
              <li>Lower the minimum similarity threshold</li>
              <li>Try a simpler phrasing</li>
            </ul>
          </div>
        </div>
      )}

      {result && result.results.length > 0 && result.mode === 'frames' && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(240px, 1fr))', gap: 12 }}>
          {result.results.map((r: any, i: number) => (
            <Link
              key={i}
              to={`/bag/${r.bag_id}`}
              style={{ textDecoration: 'none', color: 'inherit' }}
            >
              <div
                style={{
                  background: '#161b22',
                  border: '1px solid #30363d',
                  borderRadius: 8,
                  overflow: 'hidden',
                }}
              >
                <img
                  src={api.frameUrl(r.bag_id, r.topic, r.frame_index, 240)}
                  alt=""
                  style={{
                    width: '100%',
                    aspectRatio: '4/3',
                    objectFit: 'cover',
                    display: 'block',
                  }}
                />
                <div style={{ padding: 8, fontSize: 12 }}>
                  <div style={{ color: '#58a6ff', marginBottom: 2 }}>
                    {r.topic} · frame {r.frame_index}
                  </div>
                  <div style={{ color: '#8b949e' }}>
                    t={r.timestamp_sec?.toFixed(2)}s · sim={r.similarity?.toFixed(3)}
                  </div>
                </div>
              </div>
            </Link>
          ))}
        </div>
      )}

      {result && result.results.length > 0 && result.mode === 'clips' && (
        <div>
          {result.results.map((r: any, i: number) => (
            <Link
              key={i}
              to={`/bag/${r.bag_id}`}
              style={{ textDecoration: 'none', color: 'inherit' }}
            >
              <div
                style={{
                  background: '#161b22',
                  border: '1px solid #30363d',
                  borderRadius: 8,
                  padding: 12,
                  marginBottom: 8,
                  display: 'flex',
                  gap: 12,
                }}
              >
                <div style={{ flex: 1 }}>
                  <div style={{ color: '#58a6ff', fontSize: 13, fontWeight: 500 }}>
                    {r.topic}
                  </div>
                  <div style={{ color: '#8b949e', fontSize: 12, marginTop: 4 }}>
                    {r.start_timestamp_sec?.toFixed(2)}s → {r.end_timestamp_sec?.toFixed(2)}s
                    ({r.duration_sec?.toFixed(1)}s, {r.frame_count} frames)
                  </div>
                  <div style={{ color: '#8b949e', fontSize: 12 }}>
                    peak sim: {r.peak_similarity?.toFixed(3)} · avg: {r.avg_similarity?.toFixed(3)}
                  </div>
                </div>
              </div>
            </Link>
          ))}
        </div>
      )}
    </div>
  )
}
