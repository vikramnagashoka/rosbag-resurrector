import React, { useState } from 'react'
import '../styles/notebook.css'
import { Notebook, HealthTier } from './types'
import { SAMPLE_NOTEBOOKS } from './sampleData'

// PR 0 — the workspace shell: rail + header + (empty) feed + docked
// command bar. Static data, no live cells yet. Later PRs add the cell
// framework, command palette, linked cursor, etc.

const TIER_VARS: Record<HealthTier, { color: string; bg: string }> = {
  good: { color: 'var(--nb-health-good)', bg: 'var(--nb-health-good-bg)' },
  warn: { color: 'var(--nb-health-warn)', bg: 'var(--nb-health-warn-bg)' },
  bad: { color: 'var(--nb-health-bad)', bg: 'var(--nb-health-bad-bg)' },
}

// Suggestion chips shown under the command input (static in PR 0).
const SUGGESTIONS = [
  'Plot /odom', 'Stats /imu/data', 'Synchronize', 'Health report',
  'Semantic search', 'Camera frames',
]

export default function NotebookWorkspace() {
  const [notebooks, setNotebooks] = useState<Notebook[]>(SAMPLE_NOTEBOOKS)
  const [activeId, setActiveId] = useState<string>(SAMPLE_NOTEBOOKS[0].id)
  const [query, setQuery] = useState('')

  const active = notebooks.find(n => n.id === activeId) ?? notebooks[0]

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
            <div className="nb-title">{active.title}</div>
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
          </div>
          <div className="nb-header-actions">
            <button className="nb-btn">Share</button>
            <button className="nb-btn nb-btn-accent">Export ▾</button>
          </div>
        </header>

        <div className="nb-feed">
          <div className="nb-feed-inner">
            {active.cells.length === 0 ? (
              <div className="nb-empty">
                <div className="nb-empty-title">Start your analysis</div>
                <div className="nb-empty-sub">
                  Type a command below, or pick a suggestion, to add your first cell.
                </div>
              </div>
            ) : (
              // Cell feed lands in PR 1.
              <div />
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
                <button key={s} className="nb-chip">
                  <span className="nb-chip-plus">+</span> {s}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
