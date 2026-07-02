import React, { useEffect, useMemo, useRef, useState } from 'react'
import '../styles/notebook.css'
import { api, CapabilityMap } from '../api'
import {
  Notebook, Folder, HealthTier, CellType, Cell, nextCellId, nextFolderId,
  plottableTopics, imageTopics, pointcloudTopics, topicMessageCount,
} from './types'
import { notebooksFromBags } from './build'
import { buildCatalog, filterCatalog, CommandEntry } from './commands'
import CommandPalette from './CommandPalette'
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
  { label: 'Plot signal', type: 'plot' },
  { label: 'Statistics', type: 'stats' },
  { label: 'Health report', type: 'health' },
  { label: 'Synchronize', type: 'sync' },
  { label: 'Camera frames', type: 'image' },
  { label: '3D scene', type: 'scene' },
  { label: 'Semantic search', type: 'search' },
]

export default function NotebookWorkspace() {
  const [notebooks, setNotebooks] = useState<Notebook[]>([])
  const [folders, setFolders] = useState<Folder[]>([])
  const [collapsedFolders, setCollapsedFolders] = useState<Record<string, boolean>>({})
  const [renamingFolderId, setRenamingFolderId] = useState<string | null>(null)
  // The rail "+" popover (New notebook / New folder).
  const [addMenuOpen, setAddMenuOpen] = useState(false)
  const [activeId, setActiveId] = useState<string | null>(null)
  const [query, setQuery] = useState('')
  const [paletteOpen, setPaletteOpen] = useState(false)
  const [loading, setLoading] = useState(true)
  // Real system status for the rail footer (replaces the old fake bars).
  const [caps, setCaps] = useState<CapabilityMap | null>(null)
  const [copied, setCopied] = useState(false)
  // Per-cell UI state, keyed by cell id.
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({})
  const [runtime, setRuntime] = useState<Record<string, number>>({})
  // Linked time-cursor: a shared fraction 0–1 of the plot/bag time range.
  // Cells on "Own time" (in `unlinked`) use their own `localCursor` instead.
  const [cursor, setCursor] = useState<number | null>(null)
  const [localCursor, setLocalCursor] = useState<Record<string, number>>({})
  const [unlinked, setUnlinked] = useState<Record<string, boolean>>({})
  // Plot brush selection per cell (fractions), for the header command string.
  const [sel, setSel] = useState<Record<string, { a: number; b: number } | null>>({})
  const inputRef = useRef<HTMLInputElement>(null)

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

  // Real capability status for the rail footer.
  useEffect(() => {
    let cancelled = false
    api.getCapabilities()
      .then(c => { if (!cancelled) setCaps(c) })
      .catch(() => { /* footer degrades to "status unavailable" */ })
    return () => { cancelled = true }
  }, [])

  // ⌘K / Ctrl+K focuses the command bar from anywhere in the workspace.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault()
        inputRef.current?.focus()
        setPaletteOpen(true)
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [])

  const active = notebooks.find(n => n.id === activeId) ?? null

  const catalog = useMemo(() => buildCatalog(active), [active])
  const filtered = useMemo(() => filterCatalog(catalog, query), [catalog, query])

  function appendCell(cell: Cell) {
    if (!active) return
    setNotebooks(prev => prev.map(n =>
      n.id === active.id ? { ...n, cells: [...n.cells, cell] } : n,
    ))
  }

  function runEntry(e: CommandEntry) {
    appendCell(e.makeCell())
    setQuery('')
    setPaletteOpen(false)
  }

  function onInputKeyDown(ev: React.KeyboardEvent) {
    if (ev.key === 'Enter' && filtered.length) {
      ev.preventDefault()
      runEntry(filtered[0])
    } else if (ev.key === 'Escape') {
      setPaletteOpen(false)
      inputRef.current?.blur()
    }
  }

  function newNotebook(folderId: string | null = null) {
    const id = `nb-${Date.now()}`
    const blank: Notebook = {
      id, title: 'Untitled investigation', folderId, bag: '—',
      health: 0, tier: 'warn', durationLabel: '—', durationSec: 0, startNs: 0,
      topicCount: 0, messageCount: 0, bagTopics: [], cells: [],
    }
    setNotebooks(prev => [...prev, blank])
    setActiveId(id)
    setAddMenuOpen(false)
  }

  function newFolder() {
    const id = nextFolderId()
    setFolders(prev => [...prev, { id, name: 'New folder' }])
    setRenamingFolderId(id)   // open inline rename immediately
    setAddMenuOpen(false)
  }

  function commitFolderName(id: string, name: string) {
    const clean = name.trim() || 'New folder'
    setFolders(prev => prev.map(f => (f.id === id ? { ...f, name: clean } : f)))
    setRenamingFolderId(null)
  }

  // Delete a folder; its notebooks fall back to the top level (never lost).
  function deleteFolder(id: string) {
    setNotebooks(prev => prev.map(n => (n.folderId === id ? { ...n, folderId: null } : n)))
    setFolders(prev => prev.filter(f => f.id !== id))
  }

  function moveNotebook(nbId: string, folderId: string | null) {
    setNotebooks(prev => prev.map(n => (n.id === nbId ? { ...n, folderId } : n)))
  }

  function toggleFolder(id: string) {
    setCollapsedFolders(prev => ({ ...prev, [id]: !prev[id] }))
  }

  function addCell(type: CellType) {
    if (!active) return
    const cell: Cell = { id: nextCellId(), type }
    // Seed each cell type with a sensible default topic / topics.
    if (type === 'plot' || type === 'stats') cell.topic = plottableTopics(active)[0]
    else if (type === 'image') { cell.topic = imageTopics(active)[0]; cell.frame = 0 }
    else if (type === 'scene') cell.topic = pointcloudTopics(active)[0]
    else if (type === 'sync') cell.topics = plottableTopics(active).slice(0, 2)
    setNotebooks(prev => prev.map(n =>
      n.id === active.id ? { ...n, cells: [...n.cells, cell] } : n,
    ))
  }

  function patchCell(cellId: string, patch: Partial<Cell>) {
    if (!active) return
    setNotebooks(prev => prev.map(n =>
      n.id === active.id
        ? { ...n, cells: n.cells.map(c => (c.id === cellId ? { ...c, ...patch } : c)) }
        : n,
    ))
  }
  const setCellTopic = (id: string, topic: string) => patchCell(id, { topic })
  const setCellFrame = (id: string, frame: number) => patchCell(id, { frame })

  function removeCell(cellId: string) {
    if (!active) return
    setNotebooks(prev => prev.map(n =>
      n.id === active.id ? { ...n, cells: n.cells.filter(c => c.id !== cellId) } : n,
    ))
  }

  function toggleCollapse(cellId: string) {
    setCollapsed(prev => ({ ...prev, [cellId]: !prev[cellId] }))
  }

  // The cursor a given cell reads: its own when unlinked, else the shared one.
  const cursorForCell = (cellId: string): number | null =>
    unlinked[cellId] ? (localCursor[cellId] ?? null) : cursor

  // Hovering a cell writes the cursor — shared if linked, local if not.
  function moveCursor(cellId: string, frac: number | null) {
    if (unlinked[cellId]) {
      if (frac != null) setLocalCursor(prev => ({ ...prev, [cellId]: frac }))
    } else {
      setCursor(frac)
    }
  }

  // Flip a cell between Shared time and Own time. Switching to Own seeds its
  // local cursor from the current shared one so it doesn't jump.
  function toggleLink(cellId: string) {
    setUnlinked(prev => {
      const nowUnlinked = !prev[cellId]
      if (nowUnlinked && cursor != null) {
        setLocalCursor(lc => ({ ...lc, [cellId]: cursor }))
      }
      return { ...prev, [cellId]: nowUnlinked }
    })
  }

  const setCellSel = (cellId: string, s: { a: number; b: number } | null) =>
    setSel(prev => ({ ...prev, [cellId]: s }))

  // "Open frame" from a search result → append an image cell at that frame.
  function openFrame(_bagId: number, topic: string, frameIndex: number) {
    appendCell({ id: nextCellId(), type: 'image', topic, frame: frameIndex })
  }

  function setCellRuntime(cellId: string, ms: number) {
    // Updates on each fetch (a topic change re-measures). onRuntime only
    // fires after a completed fetch, and the cell effects don't depend on
    // runtime, so this can't loop.
    setRuntime(prev => ({ ...prev, [cellId]: ms }))
  }

  // One rail entry. A div (not a button) so the folder-move <select> can
  // nest without an invalid button-in-button. The select stops propagation
  // so changing folder doesn't also switch the active notebook.
  function renderItem(nb: Notebook) {
    return (
      <div
        key={nb.id}
        role="button"
        tabIndex={0}
        className={`nb-list-item${nb.id === activeId ? ' active' : ''}`}
        onClick={() => setActiveId(nb.id)}
        onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setActiveId(nb.id) } }}
      >
        <div className="nb-item-title">
          <span className="nb-dot" style={{ background: TIER_VARS[nb.tier].color }} />
          {nb.title}
        </div>
        <div className="nb-item-file">{nb.bag}</div>
        <div className="nb-item-row">
          <span className="nb-item-cells">{nb.cells.length} cells</span>
          {folders.length > 0 && (
            <select
              className="nb-move-select"
              title="Move to folder"
              value={nb.folderId ?? ''}
              onClick={e => e.stopPropagation()}
              onChange={e => moveNotebook(nb.id, e.target.value || null)}
            >
              <option value="">Top level</option>
              {folders.map(f => (
                <option key={f.id} value={f.id}>{f.name}</option>
              ))}
            </select>
          )}
        </div>
      </div>
    )
  }

  const topLevel = notebooks.filter(n => !n.folderId)

  // Copy the current workspace URL. Honest, in-scope "Share" for a
  // localhost dev tool — no server-side notebook persistence to link to yet.
  function shareLink() {
    const url = window.location.href
    const done = () => { setCopied(true); window.setTimeout(() => setCopied(false), 1500) }
    if (navigator.clipboard?.writeText) navigator.clipboard.writeText(url).then(done).catch(done)
    else done()
  }

  // Rail footer status derived from real capabilities.
  const capList = caps ? Object.values(caps) : []
  const readyCount = capList.filter(c => c.available).length

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
          <div className="nb-add-wrap">
            <button
              className="nb-new-btn"
              title="Add notebook or folder"
              aria-haspopup="menu"
              aria-expanded={addMenuOpen}
              onClick={() => setAddMenuOpen(o => !o)}
            >+</button>
            {addMenuOpen && (
              <>
                <div className="nb-add-backdrop" onClick={() => setAddMenuOpen(false)} />
                <div className="nb-add-menu" role="menu">
                  <button role="menuitem" onClick={() => newNotebook(null)}>
                    <span className="nb-add-glyph">▧</span> New notebook
                  </button>
                  <button role="menuitem" onClick={newFolder}>
                    <span className="nb-add-glyph">▤</span> New folder
                  </button>
                </div>
              </>
            )}
          </div>
        </div>

        <div className="nb-list">
          {notebooks.length === 0 && folders.length === 0 && !loading && (
            <div style={{ padding: '10px 11px', fontSize: 12, color: 'var(--nb-text-faint)' }}>
              No indexed bags. Scan some from the classic Library, then reload.
            </div>
          )}

          {/* Folder groups first, each collapsible with its own new-notebook + */}
          {folders.map(f => {
            const kids = notebooks.filter(n => n.folderId === f.id)
            const isCollapsed = !!collapsedFolders[f.id]
            return (
              <div className="nb-folder" key={f.id}>
                <div className="nb-folder-head">
                  <button
                    className="nb-folder-toggle"
                    onClick={() => toggleFolder(f.id)}
                    title={isCollapsed ? 'Expand' : 'Collapse'}
                  >
                    <span className="nb-folder-chevron">{isCollapsed ? '▸' : '▾'}</span>
                    {renamingFolderId === f.id ? (
                      <input
                        className="nb-folder-rename"
                        autoFocus
                        defaultValue={f.name}
                        onClick={e => e.stopPropagation()}
                        onKeyDown={e => {
                          if (e.key === 'Enter') commitFolderName(f.id, (e.target as HTMLInputElement).value)
                          else if (e.key === 'Escape') setRenamingFolderId(null)
                        }}
                        onBlur={e => commitFolderName(f.id, e.target.value)}
                      />
                    ) : (
                      <span
                        className="nb-folder-name"
                        onDoubleClick={e => { e.stopPropagation(); setRenamingFolderId(f.id) }}
                        title="Double-click to rename"
                      >{f.name}</span>
                    )}
                    <span className="nb-folder-count">{kids.length}</span>
                  </button>
                  <span className="nb-folder-actions">
                    <button className="nb-folder-btn" title="New notebook in folder" onClick={() => newNotebook(f.id)}>+</button>
                    <button className="nb-folder-btn" title="Delete folder (keeps notebooks)" onClick={() => deleteFolder(f.id)}>×</button>
                  </span>
                </div>
                {!isCollapsed && (
                  <div className="nb-folder-kids">
                    {kids.length === 0
                      ? <div className="nb-folder-empty">Empty — use + to add a notebook</div>
                      : kids.map(renderItem)}
                  </div>
                )}
              </div>
            )
          })}

          {/* Ungrouped notebooks below the folders */}
          {topLevel.map(renderItem)}
        </div>

        <div className="nb-status">
          <div className="nb-status-row">
            <span className="nb-status-label">System status</span>
            <span className="nb-status-doctor">capabilities</span>
          </div>
          {capList.length === 0 ? (
            <div className="nb-status-meta">{caps ? 'no capabilities reported' : 'checking…'}</div>
          ) : (
            <>
              <div className="nb-status-bar">
                {capList.map(c => (
                  <span
                    key={c.name}
                    className="nb-status-seg"
                    title={`${c.name}: ${c.available ? 'ready' : 'not installed'} — ${c.description}`}
                    style={{ background: c.available ? 'var(--nb-health-good)' : '#d8cfba' }}
                  />
                ))}
              </div>
              <div className="nb-status-meta">
                {readyCount} of {capList.length} ready
                {readyCount < capList.length && ` · ${capList.length - readyCount} optional not installed`}
              </div>
            </>
          )}
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
            <button
              className="nb-btn"
              title="Copy a link to this workspace"
              onClick={shareLink}
            >{copied ? 'Copied ✓' : 'Share'}</button>
            {/* Notebook-level export isn't wired yet — the real export path is
                per-cell (select a range on a plot → Export range). A phantom
                header button would be worse than none; add when it's real. */}
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
                plottableTopics={plottableTopics(active)}
                imageTopics={imageTopics(active)}
                pointcloudTopics={pointcloudTopics(active)}
                frameCountFor={t => topicMessageCount(active, t)}
                sceneTimeNs={active.startNs + Math.round(active.durationSec * 0.5 * 1e9)}
                collapsed={collapsed}
                runtime={runtime}
                cursorForCell={cursorForCell}
                isUnlinked={id => !!unlinked[id]}
                selForCell={id => sel[id] ?? null}
                durationSec={active.durationSec}
                onToggleCollapse={toggleCollapse}
                onDelete={removeCell}
                onRuntime={setCellRuntime}
                onSetTopic={setCellTopic}
                onSetFrame={setCellFrame}
                onCursor={moveCursor}
                onToggleLink={toggleLink}
                onSelect={setCellSel}
                onOpenFrame={openFrame}
              />
            )}
          </div>
        </div>

        <div className="nb-cmdbar">
          <div className="nb-cmdbar-inner">
            {paletteOpen && query.trim() && active && (
              <CommandPalette entries={filtered} onRun={runEntry} />
            )}
            <div className="nb-cmd-input-row">
              <span className="nb-cmd-caret">&gt;</span>
              <input
                ref={inputRef}
                className="nb-cmd-input"
                value={query}
                onChange={e => { setQuery(e.target.value); setPaletteOpen(true) }}
                onFocus={() => setPaletteOpen(true)}
                onBlur={() => setPaletteOpen(false)}
                onKeyDown={onInputKeyDown}
                placeholder={'type a query — bf["/topic"].plot(), .sync([...]), .health(), search("…")'}
                spellCheck={false}
              />
              <span className="nb-cmd-keyhint" title="Press ⌘K (Ctrl+K) to focus, ⏎ to run">
                {query.trim() ? '⏎ run' : '⌘K'}
              </span>
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
