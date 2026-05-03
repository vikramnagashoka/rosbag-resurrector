import React, { useEffect, useState } from 'react'
import { api, ExportPreset } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'

interface Props {
  bagId: number
  availableTopics: string[]
  onClose: () => void
}

// Image-message types — matches resurrector.core.export._IMAGE_MESSAGE_TYPES.
// Used client-side to apply preset topic filters ("images" / "non-images")
// without an extra API call. Approximate: we infer from topic name patterns
// when message type isn't passed in. For the dashboard's purpose
// (pre-checking the boxes), a name-based heuristic is good enough.
function isLikelyImageTopic(topic: string): boolean {
  const t = topic.toLowerCase()
  return (
    t.includes('camera') ||
    t.includes('image') ||
    t.includes('rgb') ||
    t.includes('depth')
  )
}

function applyTopicFilterClientSide(
  topics: string[],
  filter: string | null,
): string[] {
  if (!filter) return topics
  if (filter === 'images') return topics.filter(isLikelyImageTopic)
  if (filter === 'non-images') return topics.filter(t => !isLikelyImageTopic(t))
  return topics
}

export default function ExportDialog({ bagId, availableTopics, onClose }: Props) {
  const [presets, setPresets] = useState<ExportPreset[]>([])
  const [selectedPreset, setSelectedPreset] = useState<string>('')   // '' = manual
  const [selectedTopics, setSelectedTopics] = useState<string[]>(availableTopics)
  const [format, setFormat] = useState('parquet')
  const [sync, setSync] = useState(false)
  const [downsampleHz, setDownsampleHz] = useState<string>('') // string for empty-state UX
  const [outputDir, setOutputDir] = useState('./export')
  const [exporting, setExporting] = useState(false)
  const [result, setResult] = useState<string | null>(null)
  const toast = useErrorToast()

  // Fetch available presets once on mount
  useEffect(() => {
    let cancelled = false
    api.listExportPresets()
      .then(ps => { if (!cancelled) setPresets(ps) })
      .catch(() => { /* silent — presets are progressive enhancement */ })
    return () => { cancelled = true }
  }, [])

  // When a preset is selected, fill in its values (user can still override after)
  function applyPreset(presetName: string) {
    setSelectedPreset(presetName)
    if (!presetName) return
    const p = presets.find(x => x.name === presetName)
    if (!p) return
    setFormat(p.format)
    setSync(p.sync)
    setDownsampleHz(p.downsample_hz != null ? String(p.downsample_hz) : '')
    setSelectedTopics(applyTopicFilterClientSide(availableTopics, p.topic_filter))
  }

  function toggleTopic(topic: string) {
    setSelectedTopics(prev =>
      prev.includes(topic) ? prev.filter(t => t !== topic) : [...prev, topic],
    )
  }

  async function handleExport() {
    setExporting(true)
    const downsampleNum = downsampleHz.trim() ? parseFloat(downsampleHz) : undefined
    const r = await runWithToast(
      toast,
      () =>
        api.exportBag(bagId, {
          topics: selectedTopics,
          format,
          output_dir: outputDir,
          sync,
          downsample_hz: downsampleNum,
          // Pass the preset only if user picked one AND hasn't overridden everything;
          // backend uses preset to fill any unset values. Sending the preset
          // even when manual is fine — user-supplied values still win.
          preset: selectedPreset || undefined,
        }),
      { errorPrefix: 'Export failed' },
    )
    if (r) {
      setResult(r.output)
      toast.push('info', `Exported to ${r.output}`)
    }
    setExporting(false)
  }

  const overlayStyle: React.CSSProperties = {
    position: 'fixed',
    inset: 0,
    background: 'rgba(0,0,0,0.7)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 100,
  }

  const dialogStyle: React.CSSProperties = {
    background: '#161b22',
    border: '1px solid #30363d',
    borderRadius: 12,
    padding: 24,
    width: 540,
    maxHeight: '80vh',
    overflow: 'auto',
  }

  const labelStyle: React.CSSProperties = {
    fontSize: 13, color: '#8b949e', display: 'block', marginBottom: 8,
  }
  const fieldStyle: React.CSSProperties = {
    background: '#0d1117',
    border: '1px solid #30363d',
    borderRadius: 6,
    padding: '8px 12px',
    color: '#e1e4e8',
    width: '100%',
    fontSize: 13,
  }

  const selectedPresetMeta = selectedPreset
    ? presets.find(p => p.name === selectedPreset)
    : null

  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={dialogStyle} onClick={e => e.stopPropagation()}>
        <h2 style={{ fontSize: 18, fontWeight: 600, marginBottom: 16 }}>Export Data</h2>

        {/* Preset dropdown — appears only if presets loaded successfully */}
        {presets.length > 0 && (
          <div style={{ marginBottom: 16 }}>
            <label style={labelStyle}>Preset</label>
            <select
              value={selectedPreset}
              onChange={e => applyPreset(e.target.value)}
              style={fieldStyle}
            >
              <option value="">— Manual configuration —</option>
              {presets.map(p => (
                <option key={p.name} value={p.name} disabled={!p.available}>
                  {p.name}{!p.available ? ' (extras not installed)' : ''}
                </option>
              ))}
            </select>
            {selectedPresetMeta && (
              <div style={{ fontSize: 12, color: '#8b949e', marginTop: 6 }}>
                {selectedPresetMeta.description}
              </div>
            )}
          </div>
        )}

        <div style={{ marginBottom: 16 }}>
          <label style={labelStyle}>Format</label>
          <select
            value={format}
            onChange={e => setFormat(e.target.value)}
            style={fieldStyle}
          >
            <option value="parquet">Parquet</option>
            <option value="hdf5">HDF5</option>
            <option value="csv">CSV</option>
            <option value="numpy">NumPy (.npz)</option>
            <option value="zarr">Zarr</option>
            <option value="lerobot">LeRobot</option>
            <option value="rlds">RLDS</option>
          </select>
        </div>

        <div style={{ marginBottom: 16 }}>
          <label style={labelStyle}>Output directory</label>
          <input
            type="text"
            value={outputDir}
            onChange={e => setOutputDir(e.target.value)}
            style={fieldStyle}
          />
        </div>

        <div style={{ marginBottom: 16, display: 'flex', gap: 12 }}>
          <div style={{ flex: 1 }}>
            <label style={labelStyle}>
              Downsample (Hz, optional)
            </label>
            <input
              type="text"
              value={downsampleHz}
              placeholder="e.g. 50"
              onChange={e => setDownsampleHz(e.target.value)}
              style={fieldStyle}
            />
          </div>
        </div>

        <div style={{ marginBottom: 16 }}>
          <label style={labelStyle}>
            Topics ({selectedTopics.length} of {availableTopics.length} selected)
          </label>
          {availableTopics.map(topic => (
            <label
              key={topic}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 6,
                fontSize: 13,
                marginBottom: 4,
                cursor: 'pointer',
              }}
            >
              <input
                type="checkbox"
                checked={selectedTopics.includes(topic)}
                onChange={() => toggleTopic(topic)}
              />
              {topic}
            </label>
          ))}
        </div>

        <label
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            fontSize: 13,
            marginBottom: 16,
            cursor: 'pointer',
          }}
        >
          <input type="checkbox" checked={sync} onChange={e => setSync(e.target.checked)} />
          Synchronize topics before export
        </label>

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
            }}
          >
            {result}
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
            disabled={exporting || selectedTopics.length === 0}
            style={{
              background: exporting || selectedTopics.length === 0 ? '#21262d' : '#238636',
              border: 'none',
              borderRadius: 6,
              padding: '8px 16px',
              color: '#fff',
              cursor: exporting || selectedTopics.length === 0 ? 'not-allowed' : 'pointer',
              fontWeight: 600,
            }}
          >
            {exporting ? 'Exporting...' : 'Export'}
          </button>
        </div>
      </div>
    </div>
  )
}
