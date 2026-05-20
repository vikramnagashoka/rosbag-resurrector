import React, { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { api, FrameSearchResult, SearchIndexStatus, SearchIndexBagEntry } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'
import { InstallBanner, useCapability } from '../components/InstallBanner'

export default function Search() {
  const [query, setQuery] = useState('')
  const [clips, setClips] = useState(false)
  const [clipDuration, setClipDuration] = useState(5)
  const [minSimilarity, setMinSimilarity] = useState(0.15)
  const [topK, setTopK] = useState(20)
  const [result, setResult] = useState<FrameSearchResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [status, setStatus] = useState<SearchIndexStatus | null>(null)
  const [statusError, setStatusError] = useState<string | null>(null)
  const visionCap = useCapability('vision')
  const toast = useErrorToast()

  useEffect(() => {
    let cancelled = false
    api.getSearchIndexStatus()
      .then(s => { if (!cancelled) setStatus(s) })
      .catch(e => { if (!cancelled) setStatusError(String(e?.message ?? e)) })
    return () => { cancelled = true }
  }, [])

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

  const noIndexedBags = status !== null && status.indexed_bags.length === 0
  const hasUnindexed = status !== null && status.unindexed_bags.length > 0
  const searchDisabled = loading || !query.trim() || noIndexedBags

  return (
    <div>
      <h1 style={{ fontSize: 24, fontWeight: 600, marginBottom: 4 }}>Semantic frame search</h1>
      <p style={{ color: '#8b949e', fontSize: 14, marginBottom: 20 }}>
        Describe what you're looking for in plain English. We match against CLIP
        embeddings of video frames indexed during <code>resurrector scan</code>.
      </p>

      {visionCap && !visionCap.available && (
        <InstallBanner
          capability={visionCap}
          title="Semantic search needs the vision extras."
          helperText="Install once to enable CLIP-powered frame search:"
        />
      )}

      {status && status.vision_available && noIndexedBags && (
        <EmptyIndexBanner
          unindexed={status.unindexed_bags}
          hasUnindexed={hasUnindexed}
        />
      )}

      {statusError && (
        <div style={bannerStyle('#3a1a1a', '#f85149')}>
          <strong>Couldn't check index status.</strong>
          <div style={{ marginTop: 6, fontSize: 13 }}>{statusError}</div>
        </div>
      )}

      <form onSubmit={runSearch} style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
          <input
            value={query}
            onChange={e => setQuery(e.target.value)}
            placeholder="robot dropping object, gripper collision with table, bright outdoor scene..."
            autoFocus
            disabled={noIndexedBags}
            style={{
              flex: 1,
              background: '#0d1117',
              border: '1px solid #30363d',
              borderRadius: 6,
              padding: '10px 14px',
              color: '#e1e4e8',
              fontSize: 14,
              opacity: noIndexedBags ? 0.5 : 1,
            }}
          />
          <button
            type="submit"
            disabled={searchDisabled}
            style={{
              background: searchDisabled ? '#21262d' : '#238636',
              color: '#fff',
              border: 'none',
              borderRadius: 6,
              padding: '10px 20px',
              cursor: searchDisabled ? 'not-allowed' : 'pointer',
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

      {!result && !loading && !noIndexedBags && status?.vision_available && (
        <div style={bannerStyle('#161b22', '#8b949e')}>
          Enter a query above to search your indexed frames.
          {status.indexed_bags.length > 0 && (
            <div style={{ marginTop: 8, fontSize: 12, color: '#6e7681' }}>
              Currently searching across {status.indexed_bags.length} indexed
              bag{status.indexed_bags.length === 1 ? '' : 's'} (
              {status.indexed_bags.reduce((s, b) => s + b.frame_count, 0).toLocaleString()} frames).
            </div>
          )}
        </div>
      )}

      {result && result.results.length === 0 && (
        <div style={bannerStyle('#1c1c0e', '#d29922')}>
          <strong>No matches found.</strong>
          <div style={{ marginTop: 8, fontSize: 13 }}>
            Try lowering the minimum similarity, or simplify the phrasing.
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

function bannerStyle(background: string, color: string): React.CSSProperties {
  return {
    background,
    border: `1px solid ${color}`,
    borderRadius: 8,
    padding: 24,
    color,
    fontSize: 14,
    marginBottom: 16,
  }
}

function EmptyIndexBanner({
  unindexed, hasUnindexed,
}: { unindexed: SearchIndexBagEntry[]; hasUnindexed: boolean }) {
  if (!hasUnindexed) {
    return (
      <div style={bannerStyle('#161b22', '#8b949e')}>
        <strong>No bags with image topics yet.</strong>
        <div style={{ marginTop: 8, fontSize: 13 }}>
          Scan a folder containing bag files with <code>sensor_msgs/Image</code> or
          <code> CompressedImage</code> topics from the Library page, then come back
          here to index them.
        </div>
      </div>
    )
  }

  return (
    <div style={bannerStyle('#0e1c1c', '#39d0a4')}>
      <strong>Pick a bag to index.</strong>
      <div style={{ marginTop: 8, fontSize: 13, color: '#e1e4e8' }}>
        {unindexed.length} bag{unindexed.length === 1 ? '' : 's'} with image topics
        haven't been indexed yet. Run the command below for one (or pass the
        whole folder) — first call downloads the CLIP model (~150 MB), subsequent
        bags are fast.
      </div>
      <div style={{
        marginTop: 12,
        maxHeight: 280,
        overflowY: 'auto',
        background: '#0d1117',
        border: '1px solid #30363d',
        borderRadius: 6,
      }}>
        {unindexed.map(b => (
          <div
            key={b.bag_id}
            style={{
              padding: '8px 12px',
              borderBottom: '1px solid #21262d',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              gap: 12,
            }}
          >
            <div style={{ minWidth: 0, flex: 1 }}>
              <div style={{ color: '#58a6ff', fontSize: 13, fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {b.name}
              </div>
              <div style={{ color: '#8b949e', fontSize: 11, fontFamily: 'monospace' }}>
                {b.image_topics.join(', ')}
              </div>
            </div>
            <CopyButton text={`resurrector index-frames "${b.path}"`} />
          </div>
        ))}
      </div>
    </div>
  )
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <button
      type="button"
      onClick={() => {
        navigator.clipboard.writeText(text)
        setCopied(true)
        setTimeout(() => setCopied(false), 1500)
      }}
      style={{
        background: '#21262d', color: '#e1e4e8', border: '1px solid #30363d',
        borderRadius: 4, padding: '4px 10px', fontSize: 11, cursor: 'pointer',
        fontFamily: 'monospace', whiteSpace: 'nowrap',
      }}
    >
      {copied ? 'Copied' : 'Copy'}
    </button>
  )
}

