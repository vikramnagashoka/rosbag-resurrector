import React, { useEffect, useState } from 'react'
import '../styles/notebook.css'
import { api } from '../api'
import { Notebook, HealthTier, CellType, Cell, nextCellId } from './types'
import { notebooksFromBags } from './build'
import CellFeed from './CellFeed'

// The notebook workspace: rail + header + cell feed + docked command bar.
// PR 1 wires it to real indexed bags and adds the cell framework + the
// `health` cell. Command palette (full), plot/stats/sync/image/search/scene
// cells, linked cursor, and Explain land in later PRs.

const TIER_VARS: Record<HealthTier, { color: string; bg: string }> = {
  good: { color: 'var(--nb-health-good)', bg: 'var(--nb-health-good-bg)' },
  warn: { color: 'var(--nb-health-warn)', bg: 'var(--nb-health-warn-bg)' },
  bad: { color: 'var(--nb-health-bad)', bg: 'var(--nb-health-bad-bg)' },
}

// Suggestion chips. `type` is the cell they add (null = not wired yet —
// those land in their respective PRs alongside the command palette).
const SUGGESTIONS: { label: string; type: CellType | null }[] = [
  { label: 'Health report', type: 'health' },
  { label: 'Plot /odom', type: null },
  { label: 'Stats /imu/data', type: null },
  { label: 'Synchronize', type: null },
  { label: 'Semantic search', type: null },
  { label: 'Camera frames', type: null },
]

export default function NotebookWorkspace() {
  const [notebooks, setNotebooks] = useState<Notebook[]>([])
  const [activeId, setActiveId] = useState<string | null>(null)
  const [query, setQuery] = useState('')
  const [loading, setLoading] = useState(true)
  // Per-cell UI state, keyed by cell id.
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({})
  const [runtime, setRuntime] = useState<Record<string, number>>({})

  useEffect(() => {
    let cancelled = false
    api.listBags()
      .then(bags => {
        if (cancelled) return
        const nbs = notebooksFromBags(bags)
        setNotebooks(nbs)
        setActiveId(nbs[0]?.id ?? null)
      })
      .catch(() => { /* empty rail shown on failure */ })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [])

  const active = notebooks.find(n => n.id === activeId) ?? null

  function newNotebook() {
    const id = `nb-${Date.now()}`
    const blank: Notebook = {
      id, title: 'Untitled investigation', bag: '—',
      health: 0, tier: 'warn', durationLabel: '—', durationSec: 0,
      topicCount: 0, messageCount: 0, cells: [],
    }
    setNotebooks(prev => [...prev, blank])
    setActiveId(id)
  }

  function addCell(type: CellType) {
    if (!active) return
    const cell: Cell = { id: nextCellId(), type }
    setNotebooks(prev => prev.map(n =>
      n.id === active.id ? { ...n, cells: [...n.cells, cell] } : n,
    ))
  }

  function removeCell(cellId: string) {
    if (!active) return
    setNotebooks(prev => prev.map(n =>
      n.id === active.id ? { ...n, cells: n.cells.filter(c => c.id !== cellId) } : n,
    ))
  }

  function toggleCollapse(cellId: string) {
    setCollapsed(prev => ({ ...prev, [cellId]: !prev[cellId] }))
  }

  function setCellRuntime(cellId: string, ms: number) {
    setRuntime(prev => (prev[cellId] != null ? prev : { ...prev, [cellId]: ms }))
  }

  return (
    <div className="nb">
      {/* ---------------------------------------------------------- Rail */}
      <aside className="nb-rail">
        <div className="nb-brand">
          <div className="nb-brand-mark"><span /></div>
          <div>
            <div className="nb-brand-name">Resurrector</div>
            <div className="nb-brand-sub">notebooks</div>
          </div>
        </div>

        <div className="nb-section-label">
          <span>INVESTIGATIONS</span>
          <button className="nb-new-btn" title="New notebook" onClick={newNotebook}>+</button>
        </div>

        <div className="nb-list">
          {notebooks.length === 0 && !loading && (
            <div style={{ padding: '10px 11px', fontSize: 12, color: 'var(--nb-text-faint)' }}>
              No indexed bags. Scan some from the classic Library, then reload.
            </div>
          )}
          {notebooks.map(nb => (
            <button
              key={nb.id}
              className={`nb-list-item${nb.id === activeId ? ' active' : ''}`}
              onClick={() => setActiveId(nb.id)}
            >
              <div className="nb-item-title">
                <span className="nb-dot" style={{ background: TIER_VARS[nb.tier].color }} />
                {nb.title}
              </div>
              <div className="nb-item-file">{nb.bag}</div>
              <div className="nb-item-cells">{nb.cells.length} cells</div>
            </button>
          ))}
        </div>

        <div className="nb-status">
          <div className="nb-status-row">
            <span className="nb-status-label">System status</span>
            <span className="nb-status-doctor">doctor</span>
          </div>
          <div className="nb-status-bar">
            {['#2f8f5f', '#2f8f5f', '#2f8f5f', '#bf8a2c', '#c75c4b', '#bf8a2c'].map((c, i) => (
              <span key={i} className="nb-status-seg" style={{ background: c }} />
            ))}
          </div>
          <div className="nb-status-meta">4 ready · 2 partial</div>
        </div>
      </aside>

      {/* -------------------------------------------------------- Column */}
      <div className="nb-col">
        <header className="nb-header">
          <div>
            <div className="nb-title">{active?.title ?? (loading ? 'Loading…' : 'No notebook')}</div>
            {active && (
              <div className="nb-header-meta">
                <span className="nb-bag-chip">{active.bag}</span>
                <span
                  className="nb-health-pill"
                  style={{ color: TIER_VARS[active.tier].color, background: TIER_VARS[active.tier].bg }}
                >
                  <span className="nb-dot" style={{ background: TIER_VARS[active.tier].color }} />
                  {active.health}
                </span>
                <span className="nb-duration">
                  {active.durationLabel} · {active.topicCount} topics · {active.messageCount.toLocaleString()} msgs
                </span>
              </div>
            )}
          </div>
          <div className="nb-header-actions">
            <button className="nb-btn">Share</button>
            <button className="nb-btn nb-btn-accent">Export ▾</button>
          </div>
        </header>

        <div className="nb-feed">
          <div className="nb-feed-inner">
            {!active || active.cells.length === 0 ? (
              <div className="nb-empty">
                <div className="nb-empty-title">Start your analysis</div>
                <div className="nb-empty-sub">
                  Type a command below, or pick a suggestion, to add your first cell.
                </div>
              </div>
            ) : (
              <CellFeed
                cells={active.cells}
                bagId={active.bagId}
                collapsed={collapsed}
                runtime={runtime}
                onToggleCollapse={toggleCollapse}
                onDelete={removeCell}
                onRuntime={setCellRuntime}
              />
            )}
          </div>
        </div>

        <div className="nb-cmdbar">
          <div className="nb-cmdbar-inner">
            <div className="nb-cmd-input-row">
              <span className="nb-cmd-caret">&gt;</span>
              <input
                className="nb-cmd-input"
                value={query}
                onChange={e => setQuery(e.target.value)}
                placeholder={'type a query — bf["/topic"].plot(), .sync([...]), .health(), search("…")'}
                spellCheck={false}
              />
              <span className="nb-cmd-keyhint">⏎ run</span>
            </div>
            <div className="nb-chips">
              {SUGGESTIONS.map(s => (
                <button
                  key={s.label}
                  className="nb-chip"
                  disabled={!s.type || !active}
                  title={s.type ? `Add a ${s.type} cell` : 'Lands in a later slice'}
                  style={!s.type ? { opacity: 0.5, cursor: 'not-allowed' } : undefined}
                  onClick={() => s.type && addCell(s.type)}
                >
                  <span className="nb-chip-plus">+</span> {s.label}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
