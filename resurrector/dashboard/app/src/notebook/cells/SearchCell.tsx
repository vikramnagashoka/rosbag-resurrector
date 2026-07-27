import React, { useEffect, useMemo, useState } from 'react'
import { api, ApiError } from '../../api'
import { useCapability } from '../../components/InstallBanner'

// `search` cell body — CLIP semantic frame search via /api/search/frames.
// Query input + min-similarity slider + result grid. "Open frame" adds an
// image cell at that timestamp. Degrades gracefully when the vision extras
// aren't installed or no frames are indexed (the honest empty states).

interface Hit {
  bag_id: number; topic: string; timestamp_sec: number; frame_index: number; similarity: number
}

const SIM_MAX = 0.5

// Copyable install/setup command inside a cell banner.
function CmdRow({ cmd }: { cmd: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <div className="nb-cell-banner-cmd">
      <code>{cmd}</code>
      <button
        className="nb-cell-banner-copy"
        onClick={() => navigator.clipboard.writeText(cmd).then(() => {
          setCopied(true); setTimeout(() => setCopied(false), 1400)
        })}
      >{copied ? 'copied ✓' : 'copy'}</button>
    </div>
  )
}

export default function SearchCell({
  bagId, bagFile, query, onOpenFrame, onRuntime,
}: {
  bagId?: number
  bagFile?: string
  query?: string
  onOpenFrame: (bagId: number, topic: string, frameIndex: number) => void
  onRuntime?: (ms: number) => void
}) {
  const [q, setQ] = useState(query ?? '')
  const [hits, setHits] = useState<Hit[] | null>(null)
  const [minSim, setMinSim] = useState(0.15)
  const [status, setStatus] = useState<'idle' | 'loading' | 'ok' | 'no_vision' | 'no_frames' | 'error'>('idle')
  const [errMsg, setErrMsg] = useState<string | null>(null)
  const visionCap = useCapability('vision')

  // Pre-check on add (the classic Search page's install banner, in-cell):
  // vision extras missing → banner immediately, not after a failed search.
  // Vision present but this bag has no indexed frames → the index-frames
  // banner, same rule. The post-run error handling below stays as fallback.
  useEffect(() => {
    if (bagId == null) return
    let cancelled = false
    if (visionCap && !visionCap.available) {
      setStatus('no_vision')
      return
    }
    if (visionCap?.available) {
      api.getFrameIndexStatus(bagId)
        .then(r => {
          if (cancelled) return
          setStatus(prev =>
            !r.indexed ? 'no_frames'
              : (prev === 'no_frames' || prev === 'no_vision') ? 'idle' : prev)
        })
        .catch(() => { /* pre-check is best-effort; searching still works */ })
    }
    return () => { cancelled = true }
  }, [bagId, visionCap])

  // The pre-check states disable the input — searching would only fail.
  const blocked = status === 'no_vision' || status === 'no_frames'

  function run() {
    if (bagId == null || !q.trim()) return
    setStatus('loading'); setHits(null); setErrMsg(null)
    const t0 = performance.now()
    api.searchFrames(q, { bagId, minSimilarity: 0, topK: 24 })
      .then(r => {
        setHits((r.results as unknown as Hit[]) ?? [])
        setStatus('ok')
        onRuntime?.(performance.now() - t0)
      })
      .catch(e => {
        const kind = e instanceof ApiError && e.detail && typeof e.detail === 'object'
          ? (e.detail as any).detail?.kind : undefined
        if (kind === 'vision_not_installed') setStatus('no_vision')
        else if (kind === 'no_indexed_frames') setStatus('no_frames')
        else { setStatus('error'); setErrMsg(String(e?.message ?? e)) }
      })
  }

  const filtered = useMemo(
    () => (hits ?? []).filter(h => h.similarity >= minSim),
    [hits, minSim],
  )

  return (
    <div>
      <div className="nb-search-row">
        <input
          className="nb-search-input"
          value={q}
          disabled={blocked}
          onChange={e => setQ(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter') run() }}
          placeholder="describe a frame — e.g. gripper near the table, bright outdoor scene"
        />
        <button className="nb-search-go" onClick={run} disabled={blocked}>Search</button>
      </div>

      {status === 'no_vision' && (
        <div className="nb-cell-banner">
          <div className="nb-cell-banner-title">Semantic search needs the vision extras.</div>
          <div className="nb-cell-banner-body">
            CLIP runs locally — install the extra, restart the dashboard, and this cell comes alive.
          </div>
          <CmdRow cmd={visionCap?.install_command ?? "pip install 'rosbag-resurrector[vision]'"} />
        </div>
      )}
      {status === 'no_frames' && (
        <div className="nb-cell-banner">
          <div className="nb-cell-banner-title">This bag&rsquo;s frames aren&rsquo;t indexed yet.</div>
          <div className="nb-cell-banner-body">
            One-time step: compute CLIP embeddings for the camera frames, then search away.
          </div>
          <CmdRow cmd={`resurrector index-frames ${bagFile ?? '<bag>'}`} />
        </div>
      )}
      {status === 'error' && <div className="nb-search-msg">Search failed: {errMsg}</div>}
      {status === 'loading' && <div className="nb-search-msg">Searching…</div>}

      {status === 'ok' && (
        <>
          <div className="nb-search-controls">
            <span>{filtered.length} frame{filtered.length === 1 ? '' : 's'} matched</span>
            <label>
              min similarity {minSim.toFixed(2)}{' '}
              <input
                className="nb-search-slider" type="range" min={0} max={SIM_MAX} step={0.01}
                value={minSim} onChange={e => setMinSim(Number(e.target.value))}
                aria-label="Minimum similarity"
              />
            </label>
            <span className="nb-search-backend">CLIP ViT-B/32 · local</span>
          </div>
          {filtered.length === 0 ? (
            <div className="nb-search-msg">No frames above the similarity threshold.</div>
          ) : (
            <div className="nb-search-grid">
              {filtered.map((h, i) => (
                <div className="nb-search-card" key={i}>
                  <div className="nb-search-thumb">
                    <img src={api.frameUrl(h.bag_id, h.topic, h.frame_index, 240)} alt="" />
                    <span className="nb-search-sim">{h.similarity.toFixed(2)}</span>
                  </div>
                  <div className="nb-search-meta">
                    <div className="nb-search-topic">{h.topic}</div>
                    <div className="nb-search-bar">
                      <div className="nb-search-bar-fill" style={{ width: `${Math.min(100, (h.similarity / SIM_MAX) * 100)}%` }} />
                    </div>
                    <div className="nb-search-ts">t = {h.timestamp_sec?.toFixed(2)}s</div>
                    <button className="nb-search-open" onClick={() => onOpenFrame(h.bag_id, h.topic, h.frame_index)}>
                      Open frame →
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  )
}
