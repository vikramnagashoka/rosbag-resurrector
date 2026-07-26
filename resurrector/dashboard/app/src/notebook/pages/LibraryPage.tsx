import React, { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { api, Bag, ApiError } from '../../api'
import NotebookPageShell from '../NotebookPageShell'

// Warm-themed bag browser — the notebook-native Library. Scan a folder,
// browse indexed bags as cards, click one to open it in the notebook
// workspace (/n/nb-bag-<id>). Replaces the classic dark Library for /n users.

function basename(p: string): string { return p.split(/[/\\]/).pop() || p }
function fmtSize(bytes: number): string {
  const u = ['B', 'KB', 'MB', 'GB', 'TB']; let s = bytes
  for (const unit of u) { if (s < 1024) return `${s.toFixed(1)} ${unit}`; s /= 1024 }
  return `${s.toFixed(1)} PB`
}
function fmtDur(sec: number): string {
  if (sec < 60) return `${sec.toFixed(1)}s`
  const m = Math.floor(sec / 60); return `${m}m ${String(Math.round(sec % 60)).padStart(2, '0')}s`
}
function tier(score: number | null): 'good' | 'warn' | 'bad' {
  const s = score ?? 0; return s >= 90 ? 'good' : s >= 80 ? 'warn' : 'bad'
}

export default function LibraryPage() {
  const [bags, setBags] = useState<Bag[]>([])
  const [loading, setLoading] = useState(true)
  const [scanPath, setScanPath] = useState('')
  const [scanBusy, setScanBusy] = useState(false)
  const [scanMsg, setScanMsg] = useState<string | null>(null)
  const nav = useNavigate()

  async function refresh() {
    try { setBags(await api.listBags()) } catch { /* leave empty */ } finally { setLoading(false) }
  }
  useEffect(() => { refresh() }, [])

  async function runScan(e: React.FormEvent) {
    e.preventDefault()
    const path = scanPath.trim()
    if (!path || scanBusy) return
    setScanBusy(true); setScanMsg(null)
    try {
      const res = await api.triggerScan(path)
      await refresh()
      const failed = res.errors.length ? `, ${res.errors.length} failed` : ''
      setScanMsg(`Indexed ${res.indexed} of ${res.scanned} bag(s)${failed}.`)
      if (res.indexed > 0) setScanPath('')
    } catch (err) {
      setScanMsg(err instanceof ApiError ? err.message : `Scan failed: ${String(err)}`)
    } finally { setScanBusy(false) }
  }

  return (
    <NotebookPageShell
      title="Library"
      subtitle="Browse indexed bags and scan new folders. Open a bag to analyze it in the notebook."
    >
      <form className="nb-lib-scan" onSubmit={runScan}>
        <input
          className="nb-scan-input nb-lib-scan-input"
          value={scanPath}
          onChange={e => setScanPath(e.target.value)}
          placeholder="/path/to/bags — scan a folder to import"
          spellCheck={false}
        />
        <button className="nb-btn nb-btn-accent" type="submit" disabled={scanBusy || !scanPath.trim()}>
          {scanBusy ? 'Scanning…' : 'Scan folder'}
        </button>
        {scanMsg && <span className="nb-lib-scan-msg">{scanMsg}</span>}
      </form>

      {loading ? (
        <div className="nb-panel nb-panel-empty">Loading…</div>
      ) : bags.length === 0 ? (
        <div className="nb-panel nb-panel-empty">No indexed bags yet. Scan a folder above to import some.</div>
      ) : (
        <div className="nb-lib-grid">
          {bags.map(b => {
            const t = tier(b.health_score)
            return (
              <button key={b.id} className="nb-lib-card" onClick={() => nav(`/n/nb-bag-${b.id}`)}>
                <div className="nb-lib-card-head">
                  <span className="nb-lib-name">{basename(b.path)}</span>
                  <span className={`nb-lib-badge ${t}`}>{b.health_score ?? '—'}</span>
                </div>
                <div className="nb-lib-meta">
                  {fmtDur(b.duration_sec)} · {b.topics.length} topics · {b.message_count.toLocaleString()} msgs
                </div>
                <div className="nb-lib-sub">{fmtSize(b.size_bytes)}</div>
              </button>
            )
          })}
        </div>
      )}
    </NotebookPageShell>
  )
}
