import React, { useEffect, useState } from 'react'
import { api, ExportPreset } from '../api'
import { useCapability } from '../components/InstallBanner'
import { runWithToast, useErrorToast } from '../ErrorToast'

// Warm-themed export dialog for the notebook. Same workflow as the classic
// ExportDialog (preset / format / topics / sync / downsample → /api/bags/:id/
// export) in the notebook paper palette. Opened from the notebook header.

function isLikelyImageTopic(topic: string): boolean {
  const t = topic.toLowerCase()
  return t.includes('camera') || t.includes('image') || t.includes('rgb') || t.includes('depth')
}
function applyTopicFilter(topics: string[], filter: string | null): string[] {
  if (!filter) return topics
  if (filter === 'images') return topics.filter(isLikelyImageTopic)
  if (filter === 'non-images') return topics.filter(t => !isLikelyImageTopic(t))
  return topics
}

export default function ExportDialog({
  bagId, availableTopics, onClose,
}: {
  bagId: number
  availableTopics: string[]
  onClose: () => void
}) {
  const [presets, setPresets] = useState<ExportPreset[]>([])
  const allExportsCap = useCapability('all_exports')
  const [selectedPreset, setSelectedPreset] = useState('')
  const [selectedTopics, setSelectedTopics] = useState<string[]>(availableTopics)
  const [format, setFormat] = useState('parquet')
  const [sync, setSync] = useState(false)
  const [downsampleHz, setDownsampleHz] = useState('')
  const [outputDir, setOutputDir] = useState('./export')
  const [exporting, setExporting] = useState(false)
  const [result, setResult] = useState<string | null>(null)
  const toast = useErrorToast()

  useEffect(() => {
    let cancelled = false
    api.listExportPresets()
      .then(ps => { if (!cancelled) setPresets(ps) })
      .catch(() => { /* presets are progressive enhancement */ })
    return () => { cancelled = true }
  }, [])

  function applyPreset(name: string) {
    setSelectedPreset(name)
    if (!name) return
    const p = presets.find(x => x.name === name)
    if (!p) return
    setFormat(p.format); setSync(p.sync)
    setDownsampleHz(p.downsample_hz != null ? String(p.downsample_hz) : '')
    setSelectedTopics(applyTopicFilter(availableTopics, p.topic_filter))
  }

  function toggleTopic(topic: string) {
    setSelectedTopics(prev => prev.includes(topic) ? prev.filter(t => t !== topic) : [...prev, topic])
  }

  async function handleExport() {
    setExporting(true)
    const downsampleNum = downsampleHz.trim() ? parseFloat(downsampleHz) : undefined
    const r = await runWithToast(
      toast,
      () => api.exportBag(bagId, {
        topics: selectedTopics, format, output_dir: outputDir,
        sync, downsample_hz: downsampleNum, preset: selectedPreset || undefined,
      }),
      { errorPrefix: 'Export failed' },
    )
    if (r) { setResult(r.output); toast.push('info', `Exported to ${r.output}`) }
    setExporting(false)
  }

  const presetMeta = selectedPreset ? presets.find(p => p.name === selectedPreset) : null
  const unavailable = presets.filter(p => !p.available).length

  return (
    <div className="nb-modal-backdrop" onClick={onClose}>
      <div className="nb-modal nb-export" onClick={e => e.stopPropagation()}>
        <h2 className="nb-panel-title">Export data</h2>

        {allExportsCap && !allExportsCap.available && unavailable > 0 && (
          <div className="nb-bridge-banner">
            <div className="nb-bridge-banner-title">{unavailable} preset(s) need the Zarr / RLDS extras.</div>
            <div className="nb-bridge-banner-body">
              Parquet, HDF5, and CSV work without them. Install for Zarr or LeRobot/RLDS output.
            </div>
            <code>{allExportsCap.install_command}</code>
          </div>
        )}

        {presets.length > 0 && (
          <label className="nb-modal-field">
            <span>Preset</span>
            <select value={selectedPreset} onChange={e => applyPreset(e.target.value)}>
              <option value="">— Manual configuration —</option>
              {presets.map(p => (
                <option key={p.name} value={p.name} disabled={!p.available}>
                  {p.name}{!p.available ? ' (extras not installed)' : ''}
                </option>
              ))}
            </select>
            {presetMeta && <div className="nb-export-hint">{presetMeta.description}</div>}
          </label>
        )}

        <div className="nb-export-row">
          <label className="nb-modal-field" style={{ flex: 1 }}>
            <span>Format</span>
            <select value={format} onChange={e => setFormat(e.target.value)}>
              <option value="parquet">Parquet</option>
              <option value="hdf5">HDF5</option>
              <option value="csv">CSV</option>
              <option value="numpy">NumPy (.npz)</option>
              <option value="zarr">Zarr</option>
              <option value="lerobot">LeRobot</option>
              <option value="rlds">RLDS</option>
            </select>
          </label>
          <label className="nb-modal-field" style={{ width: 140 }}>
            <span>Downsample (Hz)</span>
            <input value={downsampleHz} placeholder="e.g. 50" onChange={e => setDownsampleHz(e.target.value)} />
          </label>
        </div>

        <label className="nb-modal-field">
          <span>Output directory</span>
          <input value={outputDir} onChange={e => setOutputDir(e.target.value)} />
        </label>

        <div className="nb-modal-field">
          <span>Topics ({selectedTopics.length} of {availableTopics.length})</span>
          <div className="nb-export-topics">
            {availableTopics.map(topic => (
              <label key={topic} className="nb-export-topic">
                <input type="checkbox" checked={selectedTopics.includes(topic)} onChange={() => toggleTopic(topic)} />
                {topic}
              </label>
            ))}
          </div>
        </div>

        <label className="nb-export-sync">
          <input type="checkbox" checked={sync} onChange={e => setSync(e.target.checked)} />
          Synchronize topics before export
        </label>

        {result && <div className="nb-export-result">Exported to {result}</div>}

        <div className="nb-modal-actions">
          <button className="nb-btn" onClick={onClose}>Close</button>
          <button
            className="nb-btn nb-btn-accent"
            onClick={handleExport}
            disabled={exporting || selectedTopics.length === 0}
          >{exporting ? 'Exporting…' : 'Export'}</button>
        </div>
      </div>
    </div>
  )
}
