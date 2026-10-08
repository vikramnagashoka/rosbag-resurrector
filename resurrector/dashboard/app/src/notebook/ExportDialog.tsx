import React, { useEffect, useId, useState } from 'react'
import { api, ExportPreset } from '../api'
import { useCapabilities } from '../components/InstallBanner'
import { runWithToast, useErrorToast } from '../ErrorToast'
import {
  LEROBOT_DEFAULT_FPS,
  LEROBOT_SYNC_NOTE,
  extraGap,
  fpsRoundingHint,
  hasGap,
  installWontFix,
  isLerobot,
  presetOptionLabel,
  rateError,
  syncAndRateParams,
} from '../exportOptions'

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
  const caps = useCapabilities()
  const allExportsCap = caps?.all_exports ?? null
  const lerobotCap = caps?.lerobot ?? null
  const [selectedPreset, setSelectedPreset] = useState('')
  const [selectedTopics, setSelectedTopics] = useState<string[]>(availableTopics)
  const [format, setFormat] = useState('parquet')
  const [sync, setSync] = useState(false)
  const [downsampleHz, setDownsampleHz] = useState('')
  const [outputDir, setOutputDir] = useState('./export')
  const [exporting, setExporting] = useState(false)
  const [result, setResult] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const toast = useErrorToast()
  const presetId = useId()
  const stuckNoteId = useId()
  const rateId = useId()
  const rateNoteId = useId()
  const syncNoteId = useId()
  const lerobot = isLerobot(format)
  const rateErr = rateError(format, downsampleHz)
  const rateNote = rateErr ?? fpsRoundingHint(format, downsampleHz)

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
    setResult(null)
    setError(null)
    const r = await runWithToast(
      toast,
      () => api.exportBag(bagId, {
        topics: selectedTopics, format, output_dir: outputDir,
        ...syncAndRateParams(format, sync, downsampleHz), preset: selectedPreset || undefined,
      }),
      // Kept in the dialog too: a failed-columns error lists each column
      // and the fix, more than an 8-second toast can show.
      { errorPrefix: 'Export failed', onError: setError },
    )
    if (r) { setResult(r.output_path); toast.push('info', `Exported to ${r.output_path}`) }
    setExporting(false)
  }

  const presetMeta = selectedPreset ? presets.find(p => p.name === selectedPreset) : null
  // Split unavailable presets per extra so each banner names the install
  // command that actually unlocks them, and offers it only where it does.
  // Presets it can't unlock on this interpreter get their reason under the
  // preset picker instead.
  const allExportsGap = extraGap(presets, 'all-exports', caps)
  const lerobotGap = extraGap(presets, 'lerobot', caps)
  const stuckPresets = presets.filter(p => installWontFix(p, caps))
  const nAllExports = allExportsGap.fixable.length
  const nAllExportsStuck = allExportsGap.stuck.length

  return (
    <div className="nb-modal-backdrop" onClick={onClose}>
      <div className="nb-modal nb-export" onClick={e => e.stopPropagation()}>
        <h2 className="nb-panel-title">Export data</h2>

        {lerobotCap && !lerobotCap.available && hasGap(lerobotGap) && (
          <div className="nb-bridge-banner">
            <div className="nb-bridge-banner-title">
              {lerobotGap.fixable.length > 0
                ? 'The LeRobot preset needs the LeRobot extra.'
                : 'The LeRobot preset is unavailable on this interpreter.'}
            </div>
            <div className="nb-bridge-banner-body">
              {lerobotGap.stuck.length > 0
                ? lerobotCap.description
                : 'Writes a LeRobot v3 dataset (state, camera video) that loads directly in LeRobot. Needs Python 3.12+.'}
            </div>
            {lerobotGap.fixable.length > 0 && <code>{lerobotCap.install_command}</code>}
          </div>
        )}
        {allExportsCap && !allExportsCap.available && hasGap(allExportsGap) && (
          <div className="nb-bridge-banner">
            <div className="nb-bridge-banner-title">
              {nAllExportsStuck === 0
                ? `${nAllExports} preset(s) need the Zarr / RLDS extras.`
                : nAllExports === 0
                  ? `${nAllExportsStuck} preset(s) unavailable on this interpreter.`
                  : `${nAllExports} preset(s) need the [all-exports] extra; ${nAllExportsStuck} more can't run on this interpreter.`}
            </div>
            <div className="nb-bridge-banner-body">
              {nAllExportsStuck > 0
                // The backend's explanation of what the extra can't install here.
                ? allExportsCap.description
                : 'Parquet, HDF5, and CSV work without them. Install for Zarr or RLDS output.'}
            </div>
            {nAllExports > 0 && <code>{allExportsCap.install_command}</code>}
          </div>
        )}

        {presets.length > 0 && (
          // A div, not a wrapping <label>: the notes below must not become
          // part of the select's name.
          <div className="nb-modal-field">
            <label htmlFor={presetId}>Preset</label>
            <select
              id={presetId}
              value={selectedPreset}
              onChange={e => applyPreset(e.target.value)}
              aria-describedby={stuckPresets.length > 0 ? stuckNoteId : undefined}
            >
              <option value="">— Manual configuration —</option>
              {presets.map(p => (
                <option
                  key={p.name}
                  value={p.name}
                  disabled={!p.available}
                  title={p.unavailable_reason ?? undefined}
                >
                  {presetOptionLabel(p, caps)}
                </option>
              ))}
            </select>
            {presetMeta && <div className="nb-export-hint">{presetMeta.description}</div>}
            {stuckPresets.length > 0 && (
              <div id={stuckNoteId}>
                {stuckPresets.map(p => (
                  <div key={p.name} className="nb-export-hint">
                    <strong>{p.name}</strong>: {p.unavailable_reason}
                  </div>
                ))}
              </div>
            )}
          </div>
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
          {/* A div, not a wrapping <label>: the note below must describe the
              input (aria-describedby), not become part of its name. */}
          <div className="nb-modal-field" style={{ width: 140 }}>
            <label htmlFor={rateId}>{lerobot ? 'Frame rate (fps)' : 'Downsample (Hz)'}</label>
            <input
              id={rateId}
              value={downsampleHz}
              placeholder={lerobot ? `default ${LEROBOT_DEFAULT_FPS}` : 'e.g. 50'}
              onChange={e => setDownsampleHz(e.target.value)}
              aria-invalid={rateErr ? true : undefined}
              aria-describedby={rateNote ? rateNoteId : undefined}
            />
            {rateNote && (
              <div id={rateNoteId} className={rateErr ? 'nb-export-hint is-error' : 'nb-export-hint'}>
                {rateNote}
              </div>
            )}
          </div>
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

        <label className={lerobot ? 'nb-export-sync is-disabled' : 'nb-export-sync'}>
          <input
            type="checkbox"
            checked={lerobot || sync}
            disabled={lerobot}
            aria-describedby={lerobot ? syncNoteId : undefined}
            onChange={e => setSync(e.target.checked)}
          />
          Synchronize topics before export
        </label>
        {lerobot && <div id={syncNoteId} className="nb-export-hint">{LEROBOT_SYNC_NOTE}</div>}

        {result && <div className="nb-export-result" role="status">Exported to {result}</div>}
        {error && <div className="nb-export-error" role="alert">Export failed: {error}</div>}

        <div className="nb-modal-actions">
          <button className="nb-btn" onClick={onClose}>Close</button>
          <button
            className="nb-btn nb-btn-accent"
            onClick={handleExport}
            disabled={exporting || selectedTopics.length === 0 || rateErr !== null}
          >{exporting ? 'Exporting…' : 'Export'}</button>
        </div>
      </div>
    </div>
  )
}
