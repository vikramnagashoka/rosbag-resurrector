import React from 'react'

// The shared cell shell — the design DNA. Every cell type is this header
// (chevron / index / command / runtime / [extras] / delete) plus a body
// rendered by the specific cell type. Header extras (topic dropdown, time
// toggle) slot in via `headerExtras` in later PRs.

interface Props {
  index: number
  command: string
  runtimeMs?: number | null
  collapsed: boolean
  onToggleCollapse: () => void
  onDelete: () => void
  headerExtras?: React.ReactNode
  children?: React.ReactNode
}

export default function NotebookCell({
  index, command, runtimeMs, collapsed,
  onToggleCollapse, onDelete, headerExtras, children,
}: Props) {
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
        <button className="nb-cell-del" onClick={onDelete} aria-label="Delete cell">✕</button>
      </div>
      {!collapsed && <div className="nb-cell-body">{children}</div>}
    </div>
  )
}
