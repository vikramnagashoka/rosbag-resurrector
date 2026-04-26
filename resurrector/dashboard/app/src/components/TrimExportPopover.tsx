// TrimExportPopover — appears when the user shift-drags a region on
// the chart to select a time window. Lets them export that slice to
// any supported format with one click.
//
// Reuses the v0.4.0 backend endpoint POST /api/bags/{id}/trim. The
// brush selection happens in TopicPlot via Plotly's onSelected event;
// the parent forwards the start/end seconds and the available topics
// for the format choices.

import React, { useState } from 'react'
import { api, TrimResponse } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'

interface Props {
  bagId: number
  startSec: number
  endSec: number
  availableTopics: string[]
  // Pre-checked topics (typically just the topic the user is plotting).
  defaultTopics?: string[]
  onClose: () => void
}

const FORMATS = [
  { value: 'mcap', label: 'MCAP — replayable bag file' },
  { value: 'parquet', label: 'Parquet — columnar table per topic' },
  { value: 'csv', label: 'CSV — quick inspection' },
  { value: 'hdf5', label: 'HDF5 — mixed numeric/image' },
  { value: 'numpy', label: 'NumPy (.npz) — Jupyter friendly' },
  { value: 'zarr', label: 'Zarr — chunked, very large' },
  { value: 'mp4', label: 'MP4 — single image topic only' },
] as const

type FormatStr = typeof FORMATS[number]['value']

export default function TrimExportPopover({
  bagId,
  startSec,
  endSec,
  availableTopics,
  defaultTopics,
  onClose,
}: Props) {
  const toast = useErrorToast()
  const [format, setFormat] = useState<FormatStr>('mcap')
  const [outputPath, setOutputPath] = useState('./trimmed.mcap')
  const [topics, setTopics] = useState<string[]>(
    defaultTopics ?? availableTopics,
  )
  const [busy, setBusy] = useState(false)
  const [result, setResult] = useState<TrimResponse | null>(null)
  // Editable start/end seeded from props so the user can fine-tune
  // after picking via select-drag, current-zoom, or manual open.
  const [start, setStart] = useState<number>(startSec)
  const [end, setEnd] = useState<number>(endSec)

  function toggleTopic(t: string) {
    setTopics(prev => (prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t]))
  }

  function setFormatWithDefault(newFormat: FormatStr) {
    setFormat(newFormat)
    // Update the output path extension to match the format.
    const ext = newFormat === 'numpy' ? 'npz' : newFormat
    setOutputPath(prev => {
      const stem = prev.replace(/\.[^.]+$/, '')
      // Parquet/CSV/HDF5/Zarr/NumPy are multi-file or directory outputs.
      // For simplicity, always set the extension on the path; the
      // backend treats the parent dir as the output for those formats.
      return `${stem || './trimmed'}.${ext}`
    })
  }

  async function handleExport() {
    if (end <= start) {
      toast.push('error', 'End must be greater than start')
      return
    }
    if (topics.length === 0) {
      toast.push('error', 'Select at least one topic')
      return
    }
    if (format === 'mp4' && topics.length !== 1) {
      toast.push('error', 'MP4 export requires exactly one image topic')
      return
    }
    setBusy(true)
    const r = await runWithToast(
      toast,
      () =>
        api.trimRange(bagId, {
          start_sec: start,
          end_sec: end,
          topics,
          format,
          output_path: outputPath,
        }),
      { errorPrefix: 'Trim export' },
    )
    if (r) {
      setResult(r)
      toast.push('info', `Exported to ${r.output}`)
    }
    setBusy(false)
  }

  const overlayStyle: React.CSSProperties = {
    position: 'fixed',
    inset: 0,
    background: 'rgba(0, 0, 0, 0.7)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 100,
  }
  const dialogStyle: React.CSSProperties = {
    background: '#161b22',
    border: '1px solid #30363d',
    borderRadius: 12,
    padding: 20,
    width: 520,
    maxHeight: '85vh',
    overflow: 'auto',
  }
  const inputStyle: React.CSSProperties = {
    background: '#0d1117',
    border: '1px solid #30363d',
    borderRadius: 6,
    padding: '6px 10px',
    color: '#e1e4e8',
    fontSize: 13,
  }

  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={dialogStyle} onClick={e => e.stopPropagation()}>
        <h2 style={{ fontSize: 18, fontWeight: 600, marginBottom: 4 }}>
          Trim &amp; export
        </h2>
        <p style={{ color: '#8b949e', fontSize: 13, marginBottom: 12 }}>
          Window:{' '}
          <strong style={{ color: '#e1e4e8' }}>
            {(end - start).toFixed(3)}s
          </strong>
        </p>

        <div style={{ display: 'flex', gap: 12, marginBottom: 16 }}>
          <label style={{ flex: 1 }}>
            <span style={{ fontSize: 13, color: '#8b949e' }}>Start (sec)</span>
            <input
              type="number"
              value={start}
              step={0.05}
              min={0}
              onChange={e => setStart(Number(e.target.value))}
              style={{ ...inputStyle, width: '100%', marginTop: 4 }}
            />
          </label>
          <label style={{ flex: 1 }}>
            <span style={{ fontSize: 13, color: '#8b949e' }}>End (sec)</span>
            <input
              type="number"
              value={end}
              step={0.05}
              min={0}
              onChange={e => setEnd(Number(e.target.value))}
              style={{ ...inputStyle, width: '100%', marginTop: 4 }}
            />
          </label>
        </div>

        <label style={{ display: 'block', marginBottom: 12 }}>
          <span style={{ fontSize: 13, color: '#8b949e' }}>Format</span>
          <select
            value={format}
            onChange={e => setFormatWithDefault(e.target.value as FormatStr)}
            style={{ ...inputStyle, width: '100%', marginTop: 4 }}
          >
            {FORMATS.map(f => (
              <option key={f.value} value={f.value}>
                {f.label}
              </option>
            ))}
          </select>
        </label>

        <label style={{ display: 'block', marginBottom: 12 }}>
          <span style={{ fontSize: 13, color: '#8b949e' }}>Output path</span>
          <input
            type="text"
            value={outputPath}
            onChange={e => setOutputPath(e.target.value)}
            style={{ ...inputStyle, width: '100%', marginTop: 4 }}
          />
        </label>

        <div style={{ marginBottom: 16 }}>
          <span style={{ fontSize: 13, color: '#8b949e', display: 'block', marginBottom: 6 }}>
            Topics
          </span>
          <div
            style={{
              maxHeight: 160,
              overflowY: 'auto',
              border: '1px solid #30363d',
              borderRadius: 6,
              padding: 8,
              background: '#0d1117',
            }}
          >
            {availableTopics.map(t => (
              <label
                key={t}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 6,
                  fontSize: 12,
                  marginBottom: 2,
                  cursor: 'pointer',
                }}
              >
                <input
                  type="checkbox"
                  checked={topics.includes(t)}
                  onChange={() => toggleTopic(t)}
                />
                {t}
              </label>
            ))}
          </div>
        </div>

        {result && (
          <div
            style={{
              background: '#0d2818',
              border: '1px solid #238636',
              borderRadius: 6,
              padding: '8px 12px',
              color: '#3fb950',
              fontSize: 13,
              marginBottom: 16,
              wordBreak: 'break-all',
            }}
          >
            ✓ {result.output}
          </div>
        )}

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
          <button
            onClick={onClose}
            style={{
              background: '#21262d',
              border: '1px solid #30363d',
              borderRadius: 6,
              padding: '8px 16px',
              color: '#e1e4e8',
              cursor: 'pointer',
            }}
          >
            Close
          </button>
          <button
            onClick={handleExport}
            disabled={busy}
            style={{
              background: busy ? '#21262d' : '#238636',
              color: '#fff',
              border: 'none',
              borderRadius: 6,
              padding: '8px 16px',
              cursor: busy ? 'not-allowed' : 'pointer',
              fontWeight: 600,
            }}
          >
            {busy ? 'Exporting...' : 'Export'}
          </button>
        </div>
      </div>
    </div>
  )
}
