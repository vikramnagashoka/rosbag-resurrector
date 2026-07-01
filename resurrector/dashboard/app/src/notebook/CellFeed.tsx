import React from 'react'
import { Cell, cellCommand } from './types'
import NotebookCell from './NotebookCell'
import HealthCell from './cells/HealthCell'
import PlotCell from './cells/PlotCell'
import StatsCell from './cells/StatsCell'
import SyncCell from './cells/SyncCell'
import ImageCell from './cells/ImageCell'
import SceneCell from './cells/SceneCell'

// Renders the active notebook's cell list. Maps each cell to the shared
// NotebookCell shell wrapping its type-specific body + optional header
// extras (topic dropdowns). New cell types plug in here.

interface Props {
  cells: Cell[]
  bagId?: number
  plottableTopics: string[]
  imageTopics: string[]
  pointcloudTopics: string[]
  frameCountFor: (topic: string) => number
  sceneTimeNs: number
  collapsed: Record<string, boolean>
  runtime: Record<string, number>
  cursorForCell: (id: string) => number | null
  isUnlinked: (id: string) => boolean
  onToggleCollapse: (id: string) => void
  onDelete: (id: string) => void
  onRuntime: (id: string, ms: number) => void
  onSetTopic: (id: string, topic: string) => void
  onSetFrame: (id: string, frame: number) => void
  onCursor: (id: string, frac: number | null) => void
  onToggleLink: (id: string) => void
}

// The clock pill in a time-based cell's header: Shared time ⇄ Own time.
function timeToggle(unlinked: boolean, onToggle: () => void) {
  return (
    <button
      className={`nb-time-toggle ${unlinked ? 'own' : 'shared'}`}
      onClick={onToggle}
      title={unlinked ? 'On its own time — click to relink' : 'Following the shared cursor — click to detach'}
    >
      ◷ {unlinked ? 'Own time' : 'Shared time'}
    </button>
  )
}

function topicSelect(
  value: string | undefined,
  options: string[],
  onChange: (v: string) => void,
) {
  return (
    <select
      className="nb-cell-select"
      value={value ?? ''}
      onChange={e => onChange(e.target.value)}
      aria-label="Cell topic"
    >
      {options.map(t => <option key={t} value={t}>{t}</option>)}
    </select>
  )
}

export default function CellFeed(props: Props) {
  const {
    cells, bagId, plottableTopics, imageTopics, pointcloudTopics, frameCountFor,
    sceneTimeNs, collapsed, runtime, cursorForCell, isUnlinked,
    onToggleCollapse, onDelete, onRuntime, onSetTopic, onSetFrame,
    onCursor, onToggleLink,
  } = props

  return (
    <>
      {cells.map((cell, i) => {
        let body: React.ReactNode
        let headerExtras: React.ReactNode = null
        const rt = (ms: number) => onRuntime(cell.id, ms)

        switch (cell.type) {
          case 'health':
            body = <HealthCell bagId={bagId} onRuntime={rt} />
            break
          case 'plot':
            body = (
              <PlotCell
                bagId={bagId} topic={cell.topic} onRuntime={rt}
                cursor={cursorForCell(cell.id)}
                onCursor={f => onCursor(cell.id, f)}
              />
            )
            headerExtras = (
              <>
                {topicSelect(cell.topic, plottableTopics, t => onSetTopic(cell.id, t))}
                {timeToggle(isUnlinked(cell.id), () => onToggleLink(cell.id))}
              </>
            )
            break
          case 'stats':
            body = <StatsCell bagId={bagId} topic={cell.topic} onRuntime={rt} />
            headerExtras = topicSelect(cell.topic, plottableTopics, t => onSetTopic(cell.id, t))
            break
          case 'sync':
            body = <SyncCell bagId={bagId} topics={cell.topics} onRuntime={rt} cursor={cursorForCell(cell.id)} />
            headerExtras = timeToggle(isUnlinked(cell.id), () => onToggleLink(cell.id))
            break
          case 'image':
            body = (
              <ImageCell
                bagId={bagId} topic={cell.topic}
                frameCount={cell.topic ? frameCountFor(cell.topic) : 0}
                frame={cell.frame ?? 0}
                onSetFrame={n => onSetFrame(cell.id, n)}
                cursor={cursorForCell(cell.id)}
                following={!isUnlinked(cell.id)}
              />
            )
            headerExtras = (
              <>
                {imageTopics.length > 1 ? topicSelect(cell.topic, imageTopics, t => onSetTopic(cell.id, t)) : null}
                {timeToggle(isUnlinked(cell.id), () => onToggleLink(cell.id))}
              </>
            )
            break
          case 'scene':
            body = <SceneCell bagId={bagId} topic={cell.topic} timeNs={sceneTimeNs} onRuntime={rt} />
            headerExtras = pointcloudTopics.length > 1
              ? topicSelect(cell.topic, pointcloudTopics, t => onSetTopic(cell.id, t))
              : null
            break
          default:
            body = <div className="nb-cell-loading">{cell.type} cell — coming in a later slice.</div>
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
