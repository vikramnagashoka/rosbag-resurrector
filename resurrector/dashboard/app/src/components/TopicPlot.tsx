// Plotly-based multi-topic plot with linked cursors, brush zoom,
// and user annotations.
//
// Architecture
// ════════════
//
//   parent Explorer
//     │ passes `topics: PlotSeries[]` — one per selected topic/column
//     ▼
//   TopicPlot
//     │
//     ├── Plotly figure with one subplot per series, shared x-axis
//     │
//     ├── onRelayout(xrange) ──▶ parent re-fetches topic data
//     │                         (downsampled inside new range)
//     │
//     ├── onHover(x) ──▶ rAF-throttled cursor lines drawn across
//     │                  every subplot (linked cursors)
//     │
//     └── onClick(x, y) ──▶ opens annotation popup; POST to
//                           /api/bags/{id}/annotations; re-render
//                           as pinned note.

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import Plot from 'react-plotly.js'
// Use the lighter cartesian bundle to keep page weight down.
// plotly.js-cartesian-dist-min is ~1MB gz vs ~3MB for full plotly.
// @ts-ignore — the package exports ES modules but has no types.
import Plotly from 'plotly.js-cartesian-dist-min'
import { api, Annotation } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'

export interface PlotSeries {
  label: string
  timestamps_ns: number[]
  values: number[]
}

interface Props {
  bagId: number
  topicName: string
  series: PlotSeries[]
  // Optional derived series (from the TransformEditor). They render as
  // an extra subplot at the bottom, dashed line so the user can tell
  // which is the original.
  derivedSeries?: PlotSeries[]
  onZoom?: (startSec: number | null, endSec: number | null) => void
  // Shift-drag range selection (separate from zoom). The parent uses
  // this to drive the trim-export popover.
  onRangeSelected?: (startSec: number, endSec: number) => void
  firstTimestampNs: number
  // Lifted annotations state — managed by the parent (Explorer) so
  // TopicPlot and BookmarksPanel see the same source of truth. When
  // either component mutates, the parent updates these and both
  // re-render together.
  annotations: Annotation[]
  onAnnotationsChanged: (next: Annotation[]) => void
  // When true, switch the chart drag mode from zoom to box-select so
  // the next horizontal drag fires onRangeSelected. Toggled by the
  // "Select range" button in the parent toolbar.
  selectMode?: boolean
}

function formatSec(ts_ns: number, first_ns: number): number {
  return (ts_ns - first_ns) / 1e9
}

export default function TopicPlot({
  bagId,
  topicName,
  series,
  derivedSeries,
  onZoom,
  onRangeSelected,
  firstTimestampNs,
  annotations,
  onAnnotationsChanged,
  selectMode = false,
}: Props) {
  const toast = useErrorToast()
  const [pendingNote, setPendingNote] = useState<{ ts_ns: number; label: string } | null>(null)
  const [noteText, setNoteText] = useState('')
  const hoverRef = useRef<number | null>(null) // rAF id

  const plotData = useMemo(() => {
    const all = [
      ...series.map((s, i) => ({
        type: 'scattergl' as const,
        mode: 'lines' as const,
        name: s.label,
        x: s.timestamps_ns.map(t => formatSec(t, firstTimestampNs)),
        y: s.values,
        xaxis: 'x',
        yaxis: i === 0 ? 'y' : `y${i + 1}`,
        hovertemplate: `<b>${s.label}</b><br>t=%{x:.3f}s<br>v=%{y:.4f}<extra></extra>`,
        line: { width: 1.2 },
      })),
      // Derived series live in their own subplot at the bottom so they
      // don't visually compete with the primary series.
      ...(derivedSeries ?? []).map((s, i) => ({
        type: 'scattergl' as const,
        mode: 'lines' as const,
        name: s.label,
        x: s.timestamps_ns.map(t => formatSec(t, firstTimestampNs)),
        y: s.values,
        xaxis: 'x',
        yaxis: `y${series.length + i + 1}`,
        hovertemplate: `<b>${s.label} (derived)</b><br>t=%{x:.3f}s<br>v=%{y:.4f}<extra></extra>`,
        line: { width: 1.2, dash: 'dot' as const },
      })),
    ]
    return all
  }, [series, derivedSeries, firstTimestampNs])

  const plotLayout = useMemo(() => {
    const totalRows = series.length + (derivedSeries?.length ?? 0)
    const axes: Record<string, unknown> = {}
    // Each subplot needs enough vertical space for at least the y-axis
    // title text. With many rows (e.g. /imu/data has ~13 numeric columns
    // plus a derived series) the per-row domain shrinks below the label
    // height and the label collides with the next subplot's plot area.
    // Cap title font + tick font sizes proportionally.
    const tightStack = totalRows >= 8
    const titleFontSize = tightStack ? 9 : 11
    const tickFontSize = tightStack ? 9 : 10
    const verticalGap = totalRows >= 6 ? 0.04 : 0.02
    const subplotHeight = 1 / Math.max(totalRows, 1)
    series.forEach((_, i) => {
      const domainTop = 1 - i * subplotHeight
      const domainBottom = 1 - (i + 1) * subplotHeight + verticalGap
      axes[`yaxis${i === 0 ? '' : i + 1}`] = {
        domain: [domainBottom, domainTop],
        title: { text: series[i].label, font: { size: titleFontSize } },
        tickfont: { size: tickFontSize },
        gridcolor: '#30363d',
        zerolinecolor: '#30363d',
        color: '#8b949e',
        automargin: true,
      }
    })
    // Derived series occupy the bottom slots.
    ;(derivedSeries ?? []).forEach((s, i) => {
      const slot = series.length + i
      const domainTop = 1 - slot * subplotHeight
      const domainBottom = 1 - (slot + 1) * subplotHeight + verticalGap
      axes[`yaxis${slot + 1}`] = {
        domain: [domainBottom, domainTop],
        title: { text: s.label, font: { size: titleFontSize, color: '#a371f7' } },
        tickfont: { size: tickFontSize },
        gridcolor: '#30363d',
        zerolinecolor: '#30363d',
        color: '#a371f7',
        automargin: true,
      }
    })
    // Translate annotations to shapes (vertical dashed lines).
    const shapes = annotations.map(a => {
      const x = formatSec(a.timestamp_ns, firstTimestampNs)
      return {
        type: 'line' as const,
        xref: 'x' as const,
        yref: 'paper' as const,
        x0: x,
        x1: x,
        y0: 0,
        y1: 1,
        line: { color: '#f85149', width: 1, dash: 'dash' as const },
      }
    })
    const annotationLabels = annotations.map(a => {
      const x = formatSec(a.timestamp_ns, firstTimestampNs)
      return {
        x,
        y: 1,
        xref: 'x' as const,
        yref: 'paper' as const,
        text: a.text.length > 40 ? a.text.slice(0, 37) + '…' : a.text,
        showarrow: true,
        arrowhead: 2,
        arrowsize: 0.7,
        ax: 0,
        ay: -20,
        font: { size: 10, color: '#f85149' },
        bgcolor: 'rgba(13,17,23,0.9)',
        bordercolor: '#f85149',
        borderpad: 3,
      }
    })
    // Per-subplot floor of 140px so even tight stacks stay legible. The
    // left margin widens with row count because tighter rows need more
    // automargin headroom for the y-axis labels.
    const perRowHeight = totalRows >= 8 ? 140 : 180
    const leftMargin = totalRows >= 8 ? 110 : 80
    return {
      autosize: true,
      paper_bgcolor: '#161b22',
      plot_bgcolor: '#0d1117',
      font: { color: '#e1e4e8', size: 11 },
      margin: { l: leftMargin, r: 12, t: 12, b: 40 },
      height: Math.max(200, totalRows * perRowHeight),
      hovermode: 'x unified' as const,
      xaxis: {
        title: { text: 'seconds', font: { size: 11 } },
        gridcolor: '#30363d',
        zerolinecolor: '#30363d',
        color: '#8b949e',
        rangeslider: totalRows === 1 ? { visible: false } : undefined,
      },
      ...axes,
      shapes,
      annotations: annotationLabels,
      showlegend: false,
      // Drag-mode is toggled by the parent's "Select range" button. In
      // 'zoom' (default) drag pans/zooms the time axis; in 'select' a
      // horizontal drag fires onSelected which the parent uses to open
      // the trim popover. Plotly's `selectdirection: 'h'` constrains
      // the brush to the time axis so y-coords don't matter.
      dragmode: (selectMode ? 'select' : 'zoom') as 'select' | 'zoom',
      selectdirection: 'h' as const,
    }
  }, [series, derivedSeries, annotations, firstTimestampNs, selectMode])

  // onRelayout fires on zoom/brush; we forward the range to the parent
  // so it can re-fetch a narrower slice. xaxis.autorange=true indicates
  // a reset (double-click) — send nulls.
  const handleRelayout = useCallback(
    (event: any) => {
      if (!onZoom) return
      if (event['xaxis.autorange']) {
        onZoom(null, null)
      } else if (
        'xaxis.range[0]' in event &&
        'xaxis.range[1]' in event
      ) {
        onZoom(Number(event['xaxis.range[0]']), Number(event['xaxis.range[1]']))
      }
    },
    [onZoom],
  )

  // Click-to-annotate: Plotly emits plotly_click events with x/y; we
  // translate back to nanoseconds.
  const handleClick = useCallback(
    (event: any) => {
      if (!event?.points?.length) return
      const p = event.points[0]
      const xSec = Number(p.x)
      if (!isFinite(xSec)) return
      const ts_ns = Math.round(xSec * 1e9) + firstTimestampNs
      setPendingNote({ ts_ns, label: p.data.name })
      setNoteText('')
    },
    [firstTimestampNs],
  )

  // Throttle hover events via rAF for linked-cursor performance.
  // We don't currently draw an overlay ourselves (Plotly's unified
  // hovermode provides linked behavior for subplots with shared x-axis)
  // but keeping the ref prevents any future listener from firing at
  // raw mouse-move rate.
  const handleHover = useCallback((_event: any) => {
    if (hoverRef.current != null) return
    hoverRef.current = requestAnimationFrame(() => {
      hoverRef.current = null
    })
  }, [])

  // Box-selection (shift-drag): forward the time range to the parent
  // for trim-export. Plotly returns null on deselect; we ignore that.
  const handleSelected = useCallback(
    (event: any) => {
      if (!onRangeSelected) return
      const xRange = event?.range?.x
      if (!Array.isArray(xRange) || xRange.length < 2) return
      const a = Number(xRange[0])
      const b = Number(xRange[1])
      if (!isFinite(a) || !isFinite(b)) return
      const start = Math.min(a, b)
      const end = Math.max(a, b)
      if (end - start < 1e-6) return
      onRangeSelected(start, end)
    },
    [onRangeSelected],
  )

  async function saveAnnotation() {
    if (!pendingNote || !noteText.trim()) {
      setPendingNote(null)
      return
    }
    const created = await runWithToast(toast, () =>
      api.createAnnotation(bagId, {
        timestamp_ns: pendingNote.ts_ns,
        text: noteText.trim(),
        topic: topicName,
      }),
    )
    if (created) {
      const next = [...annotations, created].sort(
        (a, b) => a.timestamp_ns - b.timestamp_ns,
      )
      onAnnotationsChanged(next)
    }
    setPendingNote(null)
  }

  // (Annotation deletion lives in BookmarksPanel; TopicPlot only adds.)

  if (series.length === 0) {
    return (
      <div
        style={{
          background: '#161b22',
          border: '1px solid #30363d',
          borderRadius: 8,
          padding: 24,
          color: '#8b949e',
          textAlign: 'center',
        }}
      >
        No numeric data to plot for this topic.
      </div>
    )
  }

  return (
    <div
      style={{
        background: '#161b22',
        border: '1px solid #30363d',
        borderRadius: 8,
        padding: 12,
      }}
    >
      <Plot
        data={plotData}
        layout={plotLayout}
        style={{ width: '100%' }}
        useResizeHandler
        config={{
          displaylogo: false,
          responsive: true,
          // Leave 'select2d' visible — paired with the "Select range"
          // toolbar button it gives users two ways to enter select mode.
          modeBarButtonsToRemove: ['lasso2d', 'autoScale2d'],
        }}
        onRelayout={handleRelayout}
        onClick={handleClick}
        onHover={handleHover}
        onSelected={handleSelected}
      />

      {/* Annotation list lives in the right-rail BookmarksPanel now —
          duplicate would show the same data twice. */}

      {pendingNote && (
        <div
          onClick={() => setPendingNote(null)}
          style={{
            position: 'fixed',
            inset: 0,
            background: 'rgba(0,0,0,0.7)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 100,
          }}
        >
          <div
            onClick={e => e.stopPropagation()}
            style={{
              background: '#161b22',
              border: '1px solid #30363d',
              borderRadius: 8,
              padding: 20,
              width: 360,
            }}
          >
            <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>Add annotation</h3>
            <div style={{ fontSize: 12, color: '#8b949e', marginBottom: 12 }}>
              t={formatSec(pendingNote.ts_ns, firstTimestampNs).toFixed(3)}s on{' '}
              <span style={{ color: '#58a6ff' }}>{pendingNote.label}</span>
            </div>
            <textarea
              autoFocus
              value={noteText}
              onChange={e => setNoteText(e.target.value)}
              placeholder="What happened here?"
              style={{
                width: '100%',
                background: '#0d1117',
                border: '1px solid #30363d',
                borderRadius: 6,
                padding: 8,
                color: '#e1e4e8',
                fontSize: 13,
                minHeight: 72,
                fontFamily: 'inherit',
              }}
            />
            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 12 }}>
              <button
                onClick={() => setPendingNote(null)}
                style={{
                  background: '#21262d',
                  border: '1px solid #30363d',
                  borderRadius: 6,
                  padding: '6px 14px',
                  color: '#e1e4e8',
                  cursor: 'pointer',
                }}
              >
                Cancel
              </button>
              <button
                onClick={saveAnnotation}
                style={{
                  background: '#238636',
                  border: 'none',
                  borderRadius: 6,
                  padding: '6px 14px',
                  color: '#fff',
                  cursor: 'pointer',
                }}
              >
                Save
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
