// DensityRibbon — per-topic message-density mini-heatmap above the chart.
//
// One row per topic. Each row shows the message count in N time bins
// across the bag's full duration. Visualizes drops (gaps), bursts, and
// uneven recordings at a glance — the "rqt_bag pattern that aged well"
// from the v0.4.0 plan.
//
// Renders as a Plotly heatmap with one trace, since we already pull
// plotly.js-cartesian-dist-min for the main chart.

import React, { useEffect, useMemo, useState } from 'react'
import Plot from 'react-plotly.js'
import { api, DensityResponse } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'

interface Props {
  bagId: number
  // Highlight one topic visually (the topic the user is currently
  // plotting). Other topics dim slightly so the active one stands out.
  highlightTopic?: string | null
  // When set, the parent (Explorer) is showing a zoomed-in time window;
  // we draw vertical lines at these seconds-from-start to mark the
  // viewport on the ribbon.
  zoomRangeSec?: { start: number; end: number } | null
  // Click-to-jump callback. Receives a relative-seconds value derived
  // from where the user clicked on the ribbon.
  onJumpToTimestampSec?: (relativeSec: number) => void
}

const RIBBON_BINS = 200

export default function DensityRibbon({
  bagId,
  highlightTopic,
  zoomRangeSec,
  onJumpToTimestampSec,
}: Props) {
  const toast = useErrorToast()
  const [data, setData] = useState<DensityResponse | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    runWithToast(toast, () => api.getDensity(bagId, { bins: RIBBON_BINS })).then(
      r => {
        if (r) setData(r)
        setLoading(false)
      },
    )
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bagId])

  const { topics, durationSec, z, hover } = useMemo(() => {
    if (!data)
      return {
        topics: [] as string[],
        durationSec: 0,
        z: [] as number[][],
        hover: [] as string[][],
      }
    // Sort topics so the highlighted one is on top, then alphabetical.
    const allTopics = Object.keys(data.density).sort((a, b) => {
      if (a === highlightTopic) return -1
      if (b === highlightTopic) return 1
      return a.localeCompare(b)
    })
    const sample = allTopics.length > 0 ? data.density[allTopics[0]] : null
    const dur = sample
      ? (sample.end_time_ns - sample.start_time_ns) / 1e9
      : 0
    // z is a 2D array — one row per topic, RIBBON_BINS columns.
    const z2d: number[][] = []
    const hoverTexts: string[][] = []
    for (const t of allTopics) {
      const info = data.density[t]
      const row = info.bins
      // Normalize to [0, 1] per row so each topic uses its full
      // dynamic range. Dim topics have a small max; busy topics fill.
      const max = Math.max(1, ...row)
      z2d.push(row.map(c => c / max))
      hoverTexts.push(
        row.map(
          (c, i) =>
            `${t}<br>${(((i + 0.5) / row.length) * dur).toFixed(2)}s<br>${c} msgs`,
        ),
      )
    }
    return { topics: allTopics, durationSec: dur, z: z2d, hover: hoverTexts }
  }, [data, highlightTopic])

  const shapes = useMemo(() => {
    if (!zoomRangeSec || !data) return []
    return [
      {
        type: 'rect' as const,
        xref: 'x' as const,
        yref: 'paper' as const,
        x0: zoomRangeSec.start,
        x1: zoomRangeSec.end,
        y0: 0,
        y1: 1,
        line: { color: '#58a6ff', width: 1 },
        fillcolor: 'rgba(88, 166, 255, 0.15)',
      },
    ]
  }, [zoomRangeSec, data])

  if (loading || !data || topics.length === 0) {
    return (
      <div
        style={{
          background: '#161b22',
          border: '1px solid #30363d',
          borderRadius: 6,
          padding: 8,
          color: '#8b949e',
          fontSize: 11,
          marginBottom: 8,
        }}
      >
        {loading ? 'Loading density…' : 'No density data available.'}
      </div>
    )
  }

  return (
    <div
      style={{
        background: '#161b22',
        border: '1px solid #30363d',
        borderRadius: 6,
        padding: 4,
        marginBottom: 8,
      }}
    >
      <Plot
        data={[
          {
            type: 'heatmap',
            z,
            // Pseudo-x: bin centers in seconds.
            x: Array.from(
              { length: RIBBON_BINS },
              (_, i) => ((i + 0.5) / RIBBON_BINS) * durationSec,
            ),
            y: topics,
            colorscale: [
              [0, '#0d1117'],
              [0.001, '#1f2937'],
              [0.5, '#1f6feb'],
              [1, '#58a6ff'],
            ],
            showscale: false,
            hoverinfo: 'text',
            // The plotly types declare text as string | string[], but
            // the runtime accepts 2D for heatmap; cast through unknown
            // to keep TS happy without losing the per-cell hover text.
            text: hover as unknown as string[],
            xgap: 0,
            ygap: 1,
          },
        ]}
        layout={{
          autosize: true,
          height: Math.min(220, 12 + topics.length * 18),
          margin: { l: 140, r: 8, t: 4, b: 24 },
          paper_bgcolor: '#161b22',
          plot_bgcolor: '#0d1117',
          font: { color: '#8b949e', size: 10 },
          xaxis: {
            title: { text: 'seconds from start', font: { size: 10 } },
            color: '#8b949e',
            gridcolor: '#30363d',
            zeroline: false,
            range: [0, durationSec],
          },
          yaxis: {
            color: '#8b949e',
            tickfont: { size: 10 },
            automargin: true,
          },
          shapes,
        }}
        style={{ width: '100%' }}
        useResizeHandler
        config={{
          displaylogo: false,
          responsive: true,
          displayModeBar: false,
        }}
        onClick={(event: any) => {
          if (!event?.points?.length || !onJumpToTimestampSec) return
          const x = Number(event.points[0].x)
          if (isFinite(x)) onJumpToTimestampSec(x)
        }}
      />
    </div>
  )
}
