import React from 'react'
import { Cell, cellCommand } from './types'
import NotebookCell from './NotebookCell'
import HealthCell from './cells/HealthCell'
import PlotCell from './cells/PlotCell'

// Renders the active notebook's cell list. Maps each cell to the shared
// NotebookCell shell wrapping its type-specific body + optional header
// extras (e.g. the plot topic dropdown). New cell types plug in here.

interface Props {
  cells: Cell[]
  bagId?: number
  plottableTopics: string[]
  collapsed: Record<string, boolean>
  runtime: Record<string, number>
  onToggleCollapse: (id: string) => void
  onDelete: (id: string) => void
  onRuntime: (id: string, ms: number) => void
  onSetTopic: (id: string, topic: string) => void
}

export default function CellFeed({
  cells, bagId, plottableTopics, collapsed, runtime,
  onToggleCollapse, onDelete, onRuntime, onSetTopic,
}: Props) {
  return (
    <>
      {cells.map((cell, i) => {
        let body: React.ReactNode
        let headerExtras: React.ReactNode = null

        switch (cell.type) {
          case 'health':
            body = <HealthCell bagId={bagId} onRuntime={ms => onRuntime(cell.id, ms)} />
            break
          case 'plot':
            body = <PlotCell bagId={bagId} topic={cell.topic} onRuntime={ms => onRuntime(cell.id, ms)} />
            headerExtras = (
              <select
                className="nb-cell-select"
                value={cell.topic ?? ''}
                onChange={e => onSetTopic(cell.id, e.target.value)}
                aria-label="Plot topic"
              >
                {plottableTopics.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            )
            break
          default:
            body = (
              <div className="nb-cell-loading">
                {cell.type} cell — coming in a later slice.
              </div>
            )
        }

        return (
          <NotebookCell
            key={cell.id}
            index={i + 1}
            command={cellCommand(cell)}
            runtimeMs={runtime[cell.id] ?? null}
            collapsed={!!collapsed[cell.id]}
            onToggleCollapse={() => onToggleCollapse(cell.id)}
            onDelete={() => onDelete(cell.id)}
            headerExtras={headerExtras}
          >
            {body}
          </NotebookCell>
        )
      })}
    </>
  )
}
