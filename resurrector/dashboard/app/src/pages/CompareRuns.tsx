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

export default function CompareRuns() {
  const toast = useErrorToast()
  const [allBags, setAllBags] = useState<Bag[]>([])
  const [loadingBags, setLoadingBags] = useState(true)
  const [selectedBagIds, setSelectedBagIds] = useState<number[]>([])
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null)
  const [offsets, setOffsets] = useState<Record<number, number>>({})
  const [overlay, setOverlay] = useState<CompareTopicsResponse | null>(null)
  const [loadingOverlay, setLoadingOverlay] = useState(false)

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
    const numericCols = overlay.columns.filter(c => {
      if (c === 'bag_label' || c === 'relative_t_sec' || c === 'timestamp_ns') return false
      const sample = overlay.data.find(r => typeof r[c] === 'number')
      return sample !== undefined
    })
    const valueCol = numericCols[0]
    if (!valueCol) return []
    const traces: any[] = []
    overlay.labels.forEach((label, i) => {
      const rows = overlay.data.filter(r => r.bag_label === label)
      traces.push({
        type: 'scattergl' as const,
        mode: 'lines' as const,
        name: label,
        x: rows.map(r => Number(r.relative_t_sec)),
        y: rows.map(r => Number(r[valueCol])),
        line: { color: TRACE_COLORS[i % TRACE_COLORS.length], width: 1.4 },
        hovertemplate: `<b>${label}</b><br>t=%{x:.3f}s<br>v=%{y:.4f}<extra></extra>`,
      })
    })
    return traces
  }, [overlay])

  const valueColLabel = useMemo(() => {
    if (!overlay) return ''
    return (
      overlay.columns.find(
        c =>
          c !== 'bag_label' &&
          c !== 'relative_t_sec' &&
          c !== 'timestamp_ns' &&
          overlay.data.some(r => typeof r[c] === 'number'),
      ) ?? ''
    )
  }, [overlay])

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
              <Plot
                data={plotData}
                layout={{
                  autosize: true,
                  height: 420,
                  paper_bgcolor: '#161b22',
                  plot_bgcolor: '#0d1117',
                  font: { color: '#e1e4e8', size: 11 },
                  margin: { l: 60, r: 12, t: 12, b: 40 },
                  xaxis: {
                    title: { text: 'seconds (relative to each bag start + offset)' },
                    color: '#8b949e',
                    gridcolor: '#30363d',
                  },
                  yaxis: {
                    title: { text: valueColLabel },
                    color: '#8b949e',
                    gridcolor: '#30363d',
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
