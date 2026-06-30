import React from 'react'
import { Cell, cellCommand } from './types'
import NotebookCell from './NotebookCell'
import HealthCell from './cells/HealthCell'

// Renders the active notebook's cell list. Maps each cell to the shared
// NotebookCell shell wrapping its type-specific body. New cell types plug
// in here as they land (plot, stats, sync, image, search, scene).

interface Props {
  cells: Cell[]
  bagId?: number
  collapsed: Record<string, boolean>
  runtime: Record<string, number>
  onToggleCollapse: (id: string) => void
  onDelete: (id: string) => void
  onRuntime: (id: string, ms: number) => void
}

export default function CellFeed({
  cells, bagId, collapsed, runtime, onToggleCollapse, onDelete, onRuntime,
}: Props) {
  return (
    <>
      {cells.map((cell, i) => {
        let body: React.ReactNode
        switch (cell.type) {
          case 'health':
            body = <HealthCell bagId={bagId} onRuntime={ms => onRuntime(cell.id, ms)} />
            break
          default:
            // Other cell types land in later PRs; show a placeholder so the
            // cell shell + command string are still demonstrable.
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
          >
            {body}
          </NotebookCell>
        )
      })}
    </>
  )
}
