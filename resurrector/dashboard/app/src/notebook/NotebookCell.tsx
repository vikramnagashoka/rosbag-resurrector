import React, { useState } from 'react'
import { CellGuide } from './guides'

// The shared cell shell — the design DNA. Every cell type is this header
// (chevron / index / command / runtime / [extras] / ? / delete) plus a body
// rendered by the specific cell type. Header extras (topic dropdown, time
// toggle) slot in via `headerExtras`. The ? toggle expands the cell type's
// inline guide (guides.ts) — help lives where the work is.

interface Props {
  index: number
  command: string
  runtimeMs?: number | null
  collapsed: boolean
  onToggleCollapse: () => void
  onDelete: () => void
  headerExtras?: React.ReactNode
  guide?: CellGuide
  children?: React.ReactNode
}

export default function NotebookCell({
  index, command, runtimeMs, collapsed,
  onToggleCollapse, onDelete, headerExtras, guide, children,
}: Props) {
  const [guideOpen, setGuideOpen] = useState(false)
  return (
    <div className="nb-cell">
      <div className={`nb-cell-header${collapsed ? ' collapsed' : ''}`}>
        <button
          className="nb-cell-chevron"
          onClick={onToggleCollapse}
          aria-label={collapsed ? 'Expand cell' : 'Collapse cell'}
        >
          {collapsed ? '▸' : '▾'}
        </button>
        <span className="nb-cell-index">[{index}]</span>
        <span className="nb-cell-cmd" title={command}>{command}</span>
        {runtimeMs != null && (
          <span className="nb-cell-runtime">{Math.round(runtimeMs)}ms</span>
        )}
        {headerExtras}
        {guide && (
          <button
            className={`nb-cell-help${guideOpen ? ' on' : ''}`}
            onClick={() => setGuideOpen(o => !o)}
            aria-label={guideOpen ? 'Hide cell guide' : 'Show cell guide'}
            aria-expanded={guideOpen}
            title="What this cell can do"
          >?</button>
        )}
        <button className="nb-cell-del" onClick={onDelete} aria-label="Delete cell">✕</button>
      </div>
      {!collapsed && guide && guideOpen && (
        <div className="nb-guide">
          <div className="nb-guide-what">{guide.what}</div>
          <dl className="nb-guide-rows">
            {guide.rows.map(r => (
              <React.Fragment key={r.label}>
                <dt>{r.label}</dt>
                <dd>{r.text}</dd>
              </React.Fragment>
            ))}
          </dl>
        </div>
      )}
      {!collapsed && <div className="nb-cell-body">{children}</div>}
    </div>
  )
}
