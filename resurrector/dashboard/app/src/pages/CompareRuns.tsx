// CompareRuns — overlay the same topic across multiple bags.
//
// Workflow:
//   1. User picks 2+ bags from a multi-select chip list
//   2. We fetch the union of topics across them and show as another
//      multi-select; user picks ONE topic to overlay
//   3. We fetch /api/compare/topics and render one Plotly trace per bag,
//      colored by bag_label, on a shared relative-time axis
//   4. Per-bag offset sliders below the chart let the user nudge
//      alignment by an event (e.g., "the runs started 0.4s apart")

import React, { useEffect, useMemo, useState } from 'react'
import Plot from 'react-plotly.js'
import { api, Bag, CompareTopicsResponse } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'

function basename(path: string): string {
  return path.split(/[/\\]/).pop() || path
}

const TRACE_COLORS = [
  '#58a6ff',
  '#3fb950',
  '#f85149',
  '#d29922',
  '#a371f7',
  '#ff6cd0',
  '#39c5cf',
  '#ee9a3a',
]

// Columns we never auto-select for the y-axis even if they are
// nominally numeric — they're metadata (sequence numbers, raw
// nanosecond timestamps, frame ids) that produce visually useless
// step-function plots.
const METADATA_COLUMN_PATTERNS = [
  /^header\.stamp_/i,
  /^header\.frame_id$/i,
  /^header\.seq$/i,
  /^_pixel_data_offset$/,
  /^_compressed_data_offset$/,
  /^data_length$/,
  /^step$/,
  /^encoding$/i,
  /^is_bigendian$/i,
]

function isMetadataColumn(name: string): boolean {
  return METADATA_COLUMN_PATTERNS.some(p => p.test(name))
}

export default function CompareRuns() {
  const toast = useErrorToast()
  const [allBags, setAllBags] = useState<Bag[]>([])
  const [loadingBags, setLoadingBags] = useState(true)
  const [selectedBagIds, setSelectedBagIds] = useState<number[]>([])
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null)
  const [offsets, setOffsets] = useState<Record<number, number>>({})
  const [overlay, setOverlay] = useState<CompareTopicsResponse | null>(null)
  const [loadingOverlay, setLoadingOverlay] = useState(false)
  // User-selected column; null until overlay loads then defaults to
  // first non-metadata numeric column.
  const [selectedColumn, setSelectedColumn] = useState<string | null>(null)
  // When true and exactly 2 bags, render a third trace = bagB - bagA
  // sampled on the union of timestamps. Surfaces *where* runs diverge.
  const [showDiff, setShowDiff] = useState(false)

  useEffect(() => {
    runWithToast(toast, () => api.listBags()).then(r => {
      if (r) setAllBags(r)
      setLoadingBags(false)
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Topics shared across the selected bags. We compute the union (not
  // intersection) — the API returns an error if a bag is missing the
  // chosen topic, which is the right user feedback.
  const candidateTopics = useMemo(() => {
    if (selectedBagIds.length === 0) return [] as string[]
    const selectedBags = allBags.filter(b => selectedBagIds.includes(b.id))
    const counts = new Map<string, number>()
    for (const bag of selectedBags) {
      for (const t of bag.topics) {
        counts.set(t.name, (counts.get(t.name) ?? 0) + 1)
      }
    }
    // Sort: shared-by-all topics first (more meaningful for overlay),
    // then alphabetical.
    const all = [...counts.entries()]
    all.sort((a, b) => {
      const sharedA = a[1] === selectedBags.length ? 0 : 1
      const sharedB = b[1] === selectedBags.length ? 0 : 1
      if (sharedA !== sharedB) return sharedA - sharedB
      return a[0].localeCompare(b[0])
    })
    return all.map(([t]) => t)
  }, [selectedBagIds, allBags])

  function toggleBag(id: number) {
    setSelectedBagIds(prev => {
      const next = prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id]
      // Drop cached overlay when bag selection changes — it's now stale.
      setOverlay(null)
      return next
    })
  }

  async function loadOverlay() {
    if (selectedBagIds.length < 2 || !selectedTopic) return
    setLoadingOverlay(true)
    const offsetsArr = selectedBagIds.map(id => offsets[id] ?? 0)
    const r = await runWithToast(
      toast,
      () =>
        api.compareTopics({
          bag_ids: selectedBagIds,
          topic: selectedTopic,
          offsets_sec: offsetsArr,
        }),
      { errorPrefix: 'Compare' },
    )
    if (r) setOverlay(r)
    setLoadingOverlay(false)
  }

  // Auto-fetch when topic + bags are valid and user hasn't picked an
  // offset yet (offsets recompute manually via the "Apply offsets" button
  // to avoid a request per slider tick).
  useEffect(() => {
    if (selectedBagIds.length >= 2 && selectedTopic) {
      loadOverlay()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedBagIds, selectedTopic])

  const plotData = useMemo(() => {
    if (!overlay) return []
    // Group rows by bag_label and pick the first numeric column other
    // than relative_t_sec / timestamp_ns to plot.
    const valueCol = selectedColumn
    if (!valueCol) return []
    const traces: any[] = []
    // Per-bag traces.
    const perBagSeries: Array<{ label: string; xs: number[]; ys: number[] }> = []
    overlay.labels.forEach((label, i) => {
      const rows = overlay.data.filter(r => r.bag_label === label)
      const xs = rows.map(r => Number(r.relative_t_sec))
      const ys = rows.map(r => Number(r[valueCol]))
      perBagSeries.push({ label, xs, ys })
      traces.push({
        type: 'scattergl' as const,
        mode: 'lines' as const,
        name: label,
        x: xs,
        y: ys,
        line: { color: TRACE_COLORS[i % TRACE_COLORS.length], width: 1.4 },
        hovertemplate: `<b>${label}</b><br>t=%{x:.3f}s<br>v=%{y:.4f}<extra></extra>`,
      })
    })
    // Diff trace = B - A. Only sensible for exactly 2 bags. We resample
    // bag B onto bag A's timestamps via linear interpolation so the
    // subtraction is well-defined when the runs have different rates.
    if (showDiff && perBagSeries.length === 2) {
      const [a, b] = perBagSeries
      const diffs: number[] = []
      let bIdx = 0
      for (const ax of a.xs) {
        // Advance bIdx so b.xs[bIdx] <= ax < b.xs[bIdx+1]
        while (bIdx + 1 < b.xs.length && b.xs[bIdx + 1] < ax) bIdx++
        const x0 = b.xs[bIdx]
        const x1 = b.xs[bIdx + 1] ?? x0
        const y0 = b.ys[bIdx]
        const y1 = b.ys[bIdx + 1] ?? y0
        const span = x1 - x0
        const interp = span === 0 ? y0 : y0 + ((ax - x0) / span) * (y1 - y0)
        diffs.push(interp - a.ys[a.xs.indexOf(ax)])
      }
      traces.push({
        type: 'scattergl' as const,
        mode: 'lines' as const,
        name: `${b.label} − ${a.label}`,
        x: a.xs,
        y: diffs,
        line: { color: '#f85149', width: 1.4, dash: 'dash' as const },
        yaxis: 'y2',
        hovertemplate: `<b>diff</b><br>t=%{x:.3f}s<br>Δv=%{y:.4f}<extra></extra>`,
      })
    }
    return traces
  }, [overlay, selectedColumn, showDiff])

  // Available numeric columns + the auto-default we prefer (first
  // non-metadata one). This drives the column picker dropdown.
  const numericColumns = useMemo(() => {
    if (!overlay) return [] as string[]
    return overlay.columns.filter(c => {
      if (c === 'bag_label' || c === 'relative_t_sec' || c === 'timestamp_ns') return false
      const sample = overlay.data.find(r => typeof r[c] === 'number')
      return sample !== undefined
    })
  }, [overlay])

  const usefulColumns = useMemo(
    () => numericColumns.filter(c => !isMetadataColumn(c)),
    [numericColumns],
  )

  // Re-default the column when the overlay refreshes so we don't keep
  // pointing at a column the new payload doesn't have.
  useEffect(() => {
    if (!overlay) return
    if (selectedColumn && numericColumns.includes(selectedColumn)) return
    setSelectedColumn(usefulColumns[0] ?? numericColumns[0] ?? null)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [overlay])

  // Per-bag summary stats for the selected column. Mean / min / max /
  // std — the table that lives below the chart.
  const stats = useMemo(() => {
    if (!overlay || !selectedColumn) return [] as Array<{
      label: string; n: number; mean: number; min: number; max: number; std: number
    }>
    return overlay.labels.map(label => {
      const ys = overlay.data
        .filter(r => r.bag_label === label)
        .map(r => Number(r[selectedColumn]))
        .filter(v => Number.isFinite(v))
      const n = ys.length
      if (n === 0) {
        return { label, n: 0, mean: NaN, min: NaN, max: NaN, std: NaN }
      }
      const mean = ys.reduce((a, b) => a + b, 0) / n
      const min = Math.min(...ys)
      const max = Math.max(...ys)
      const variance = ys.reduce((a, b) => a + (b - mean) ** 2, 0) / n
      return { label, n, mean, min, max, std: Math.sqrt(variance) }
    })
  }, [overlay, selectedColumn])

  return (
    <div>
      <h1 style={{ fontSize: 24, fontWeight: 600, marginBottom: 4 }}>
        Compare runs
      </h1>
      <p style={{ color: '#8b949e', fontSize: 14, marginBottom: 24 }}>
        Pick two or more bags + a topic and overlay them on one chart, aligned by
        relative time. Use offsets to nudge alignment if runs started a beat apart.
      </p>

      <div
        style={{
          background: '#161b22',
          border: '1px solid #30363d',
          borderRadius: 8,
          padding: 16,
          marginBottom: 16,
        }}
      >
        <h3 style={{ fontSize: 13, color: '#8b949e', marginBottom: 8 }}>
          Step 1 — pick bags
        </h3>
        {loadingBags ? (
          <div style={{ color: '#8b949e', fontSize: 13 }}>Loading...</div>
        ) : allBags.length === 0 ? (
          <div style={{ color: '#8b949e', fontSize: 13 }}>
            No bags indexed. Open the Library page and use the "+ Scan folder"
            button to index a folder of bags.
          </div>
        ) : allBags.length === 1 ? (
          <div
            style={{
              background: '#1c1c0e',
              border: '1px solid #d29922',
              borderRadius: 8,
              padding: 12,
              color: '#d29922',
              fontSize: 13,
              marginBottom: 8,
            }}
          >
            <strong>Only 1 bag indexed</strong> — Compare runs needs at least 2.
            <div
              style={{
                marginTop: 10,
                display: 'flex',
                alignItems: 'center',
                gap: 10,
              }}
            >
              <button
                onClick={async () => {
                  const r = await runWithToast(
                    toast,
                    () =>
                      api.generateDemoBag({
                        name: `compare_demo_${Date.now()}`,
                        duration_sec: 5,
                      }),
                    { errorPrefix: 'Generate demo' },
                  )
                  if (r) {
                    toast.push('info', `Generated ${r.path}`)
                    // Refresh the bag list so the new bag appears.
                    const updated = await runWithToast(toast, () => api.listBags())
                    if (updated) setAllBags(updated)
                  }
                }}
                style={{
                  background: '#1f6feb',
                  color: '#fff',
                  border: 'none',
                  borderRadius: 6,
                  padding: '6px 14px',
                  cursor: 'pointer',
                  fontSize: 13,
                }}
              >
                Generate a demo bag now
              </button>
              <span style={{ color: '#8b949e', fontSize: 12 }}>
                creates ~/.resurrector/compare_demo_TIMESTAMP.mcap, indexes it,
                and refreshes this list
              </span>
            </div>
          </div>
        ) : null}
        {!loadingBags && allBags.length >= 1 && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
            {allBags.map(b => {
              const selected = selectedBagIds.includes(b.id)
              return (
                <button
                  key={b.id}
                  onClick={() => toggleBag(b.id)}
                  title={b.path}
                  style={{
                    background: selected ? '#1f6feb22' : '#21262d',
                    border: selected ? '1px solid #1f6feb' : '1px solid #30363d',
                    borderRadius: 16,
                    padding: '6px 14px',
                    color: '#e1e4e8',
                    cursor: 'pointer',
                    fontSize: 12,
                  }}
                >
                  {basename(b.path)}
                </button>
              )
            })}
          </div>
        )}
      </div>

      {selectedBagIds.length >= 1 && (
        <div
          style={{
            background: '#161b22',
            border: '1px solid #30363d',
            borderRadius: 8,
            padding: 16,
            marginBottom: 16,
          }}
        >
          <h3 style={{ fontSize: 13, color: '#8b949e', marginBottom: 8 }}>
            Step 2 — pick a topic to overlay
            {selectedBagIds.length < 2 && (
              <span style={{ color: '#d29922', marginLeft: 8 }}>
                (need at least 2 bags)
              </span>
            )}
          </h3>
          <select
            value={selectedTopic ?? ''}
            onChange={e => setSelectedTopic(e.target.value || null)}
            disabled={selectedBagIds.length < 2 || candidateTopics.length === 0}
            style={{
              background: '#0d1117',
              border: '1px solid #30363d',
              borderRadius: 6,
              padding: '8px 12px',
              color: '#e1e4e8',
              fontSize: 13,
              minWidth: 320,
            }}
          >
            <option value="">Choose a topic…</option>
            {candidateTopics.map(t => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </div>
      )}

      {selectedBagIds.length >= 2 && selectedTopic && (
        <div
          style={{
            background: '#161b22',
            border: '1px solid #30363d',
            borderRadius: 8,
            padding: 16,
            marginBottom: 16,
          }}
        >
          <h3 style={{ fontSize: 13, color: '#8b949e', marginBottom: 8 }}>
            Step 3 — overlay
          </h3>
          {loadingOverlay ? (
            <div style={{ color: '#8b949e', padding: 24, textAlign: 'center' }}>
              Loading overlay…
            </div>
          ) : !overlay || plotData.length === 0 ? (
            <div style={{ color: '#8b949e', padding: 24, textAlign: 'center' }}>
              {overlay
                ? 'No numeric columns to plot for this topic.'
                : 'Click a bag chip + topic to load the overlay.'}
            </div>
          ) : (
            <>
              {/* Column picker + diff toggle row */}
              <div
                style={{
                  display: 'flex',
                  flexWrap: 'wrap',
                  alignItems: 'center',
                  gap: 12,
                  marginBottom: 12,
                }}
              >
                <label
                  style={{
                    fontSize: 12,
                    color: '#8b949e',
                    display: 'flex',
                    alignItems: 'center',
                    gap: 6,
                  }}
                >
                  Column:
                  <select
                    value={selectedColumn ?? ''}
                    onChange={e => setSelectedColumn(e.target.value || null)}
                    style={{
                      background: '#0d1117',
                      border: '1px solid #30363d',
                      borderRadius: 6,
                      padding: '4px 10px',
                      color: '#e1e4e8',
                      fontSize: 12,
                      minWidth: 220,
                    }}
                  >
                    {usefulColumns.length > 0 && (
                      <optgroup label="Signals">
                        {usefulColumns.map(c => (
                          <option key={c} value={c}>{c}</option>
                        ))}
                      </optgroup>
                    )}
                    {numericColumns.filter(c => isMetadataColumn(c)).length > 0 && (
                      <optgroup label="Metadata (rarely useful)">
                        {numericColumns
                          .filter(c => isMetadataColumn(c))
                          .map(c => (
                            <option key={c} value={c}>{c}</option>
                          ))}
                      </optgroup>
                    )}
                  </select>
                </label>
                {selectedBagIds.length === 2 && (
                  <label
                    style={{
                      fontSize: 12,
                      color: '#8b949e',
                      display: 'flex',
                      alignItems: 'center',
                      gap: 6,
                      cursor: 'pointer',
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={showDiff}
                      onChange={e => setShowDiff(e.target.checked)}
                    />
                    Show diff (B − A)
                  </label>
                )}
                <span style={{ flex: 1 }} />
                <span style={{ fontSize: 11, color: '#484f58' }}>
                  Tip: pick a topic field with real signal — timestamp / frame_id are usually noise.
                </span>
              </div>

              <Plot
                data={plotData}
                layout={{
                  autosize: true,
                  height: 460,
                  paper_bgcolor: '#161b22',
                  plot_bgcolor: '#0d1117',
                  font: { color: '#e1e4e8', size: 11 },
                  // Wider left margin + automargin so y-axis tick + title
                  // labels never collide. Fixes the "00000004B" overlap
                  // seen on header.stamp_nsec columns.
                  margin: { l: 90, r: showDiff && selectedBagIds.length === 2 ? 80 : 12, t: 12, b: 40 },
                  xaxis: {
                    title: { text: 'seconds (relative to each bag start + offset)' },
                    color: '#8b949e',
                    gridcolor: '#30363d',
                  },
                  yaxis: {
                    title: { text: selectedColumn ?? '', standoff: 12 },
                    color: '#8b949e',
                    gridcolor: '#30363d',
                    automargin: true,
                    // Use SI tick format so huge values (e.g. nanosec
                    // timestamps) show as "1.7×10⁹" rather than "1.7B".
                    tickformat: '~s',
                    exponentformat: 'power',
                  },
                  yaxis2: {
                    title: { text: 'diff', standoff: 12, font: { color: '#f85149' } },
                    color: '#f85149',
                    gridcolor: '#30363d',
                    overlaying: 'y',
                    side: 'right',
                    automargin: true,
                    tickformat: '~s',
                    exponentformat: 'power',
                  },
                  legend: { orientation: 'h', y: -0.15 },
                  hovermode: 'x unified',
                }}
                style={{ width: '100%' }}
                useResizeHandler
                config={{
                  displaylogo: false,
                  responsive: true,
                  modeBarButtonsToRemove: ['lasso2d', 'select2d'],
                }}
              />

              {/* Per-bag summary stats. Same column the chart shows.
                  Quick way to compare runs numerically without eye-balling. */}
              {stats.length > 0 && selectedColumn && (
                <div style={{ marginTop: 12, overflow: 'auto' }}>
                  <table
                    style={{
                      borderCollapse: 'collapse',
                      width: '100%',
                      fontSize: 12,
                    }}
                  >
                    <thead>
                      <tr style={{ borderBottom: '1px solid #30363d' }}>
                        <th style={{ padding: '4px 8px', textAlign: 'left', color: '#8b949e' }}>
                          run
                        </th>
                        <th style={{ padding: '4px 8px', textAlign: 'right', color: '#8b949e' }}>
                          n
                        </th>
                        <th style={{ padding: '4px 8px', textAlign: 'right', color: '#8b949e' }}>
                          mean
                        </th>
                        <th style={{ padding: '4px 8px', textAlign: 'right', color: '#8b949e' }}>
                          min
                        </th>
                        <th style={{ padding: '4px 8px', textAlign: 'right', color: '#8b949e' }}>
                          max
                        </th>
                        <th style={{ padding: '4px 8px', textAlign: 'right', color: '#8b949e' }}>
                          std
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {stats.map((s, i) => (
                        <tr key={s.label} style={{ borderBottom: '1px solid #21262d' }}>
                          <td style={{
                            padding: '4px 8px',
                            color: TRACE_COLORS[i % TRACE_COLORS.length],
                          }}>
                            {s.label}
                          </td>
                          <td style={{ padding: '4px 8px', textAlign: 'right' }}>
                            {s.n.toLocaleString()}
                          </td>
                          <td style={{ padding: '4px 8px', textAlign: 'right' }}>
                            {Number.isFinite(s.mean) ? s.mean.toExponential(3) : '—'}
                          </td>
                          <td style={{ padding: '4px 8px', textAlign: 'right' }}>
                            {Number.isFinite(s.min) ? s.min.toExponential(3) : '—'}
                          </td>
                          <td style={{ padding: '4px 8px', textAlign: 'right' }}>
                            {Number.isFinite(s.max) ? s.max.toExponential(3) : '—'}
                          </td>
                          <td style={{ padding: '4px 8px', textAlign: 'right' }}>
                            {Number.isFinite(s.std) ? s.std.toExponential(3) : '—'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {/* Per-bag offset sliders */}
              <div style={{ marginTop: 16 }}>
                <h4 style={{ fontSize: 12, color: '#8b949e', marginBottom: 8 }}>
                  Per-bag offsets (seconds)
                </h4>
                {overlay.labels.map((label, i) => {
                  const bagId = selectedBagIds[i]
                  const value = offsets[bagId] ?? 0
                  return (
                    <div
                      key={bagId}
                      style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 12,
                        marginBottom: 6,
                        fontSize: 12,
                      }}
                    >
                      <span
                        style={{
                          color: TRACE_COLORS[i % TRACE_COLORS.length],
                          minWidth: 140,
                        }}
                      >
                        {label}
                      </span>
                      <input
                        type="range"
                        min={-10}
                        max={10}
                        step={0.05}
                        value={value}
                        onChange={e =>
                          setOffsets(prev => ({
                            ...prev,
                            [bagId]: Number(e.target.value),
                          }))
                        }
                        style={{ flex: 1 }}
                      />
                      <input
                        type="number"
                        value={value}
                        step={0.05}
                        onChange={e =>
                          setOffsets(prev => ({
                            ...prev,
                            [bagId]: Number(e.target.value),
                          }))
                        }
                        style={{
                          width: 72,
                          background: '#0d1117',
                          border: '1px solid #30363d',
                          borderRadius: 4,
                          padding: '2px 6px',
                          color: '#e1e4e8',
                          fontSize: 12,
                        }}
                      />
                    </div>
                  )
                })}
                <button
                  onClick={loadOverlay}
                  style={{
                    background: '#1f6feb',
                    color: '#fff',
                    border: 'none',
                    borderRadius: 6,
                    padding: '6px 14px',
                    cursor: 'pointer',
                    fontSize: 12,
                    marginTop: 8,
                  }}
                >
                  Apply offsets
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}
