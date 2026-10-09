import React, { useEffect, useId, useState } from 'react'
import { api, ExportPreset } from '../api'
import { InstallBanner, useCapabilities } from './InstallBanner'
import ExportFailure, { CLASSIC_EXPORT_FAILURE_STYLE } from './ExportFailure'
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

// "Extras not installed" only for what installing them fixes; presets the
// extra can't unlock on this interpreter are counted apart.
function allExportsTitle(fixable: number, stuck: number): string {
  if (stuck === 0) return `${fixable} preset(s) unavailable — Zarr / RLDS extras not installed.`
  if (fixable === 0) return `${stuck} preset(s) unavailable on this interpreter.`
  return `${fixable} preset(s) unavailable — extras not installed. ` +
    `${stuck} more can't run on this interpreter.`
}

export default function ExportDialog({ bagId, availableTopics, onClose }: Props) {
  const [presets, setPresets] = useState<ExportPreset[]>([])
  const caps = useCapabilities()
  const allExportsCap = caps?.all_exports ?? null
  const lerobotCap = caps?.lerobot ?? null
  // Each banner offers only the install command that fixes something here;
  // presets it can't fix get their own reason under the preset picker.
  const allExportsGap = extraGap(presets, 'all-exports', caps)
  const lerobotGap = extraGap(presets, 'lerobot', caps)
  const stuckPresets = presets.filter(p => installWontFix(p, caps))
  const [selectedPreset, setSelectedPreset] = useState<string>('')   // '' = manual
  const [selectedTopics, setSelectedTopics] = useState<string[]>(availableTopics)
  const [format, setFormat] = useState('parquet')
  const [sync, setSync] = useState(false)
  const [downsampleHz, setDownsampleHz] = useState<string>('') // string for empty-state UX
  const [outputDir, setOutputDir] = useState('./export')
  const [exporting, setExporting] = useState(false)
  const [result, setResult] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const toast = useErrorToast()
  const outputDirId = useId()
  const stuckNoteId = useId()
  const rateId = useId()
  const rateNoteId = useId()
  const syncNoteId = useId()
  const lerobot = isLerobot(format)
  const rateErr = rateError(format, downsampleHz)
  const rateNote = rateErr ?? fpsRoundingHint(format, downsampleHz)
  const canExport = !exporting && selectedTopics.length > 0 && rateErr === null

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
    setResult(null)
    setError(null)
    const r = await runWithToast(
      toast,
      () =>
        api.exportBag(bagId, {
          topics: selectedTopics,
          format,
          output_dir: outputDir,
          ...syncAndRateParams(format, sync, downsampleHz),
          // Pass the preset only if user picked one AND hasn't overridden everything;
          // backend uses preset to fill any unset values. Sending the preset
          // even when manual is fine — user-supplied values still win.
          preset: selectedPreset || undefined,
        }),
      // Kept in the dialog too (ExportFailure): a failed-columns error
      // lists each column and what to do.
      { errorPrefix: 'Export failed', onError: setError },
    )
    if (r) {
      setResult(r.output_path)
      toast.push('info', `Exported to ${r.output_path}`)
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

        {lerobotCap && !lerobotCap.available && hasGap(lerobotGap) && (
          <InstallBanner
            capability={lerobotCap}
            installable={lerobotGap.fixable.length > 0}
            title={lerobotGap.fixable.length > 0
              ? 'LeRobot preset unavailable — LeRobot extra not installed (Python 3.12+).'
              : 'LeRobot preset unavailable on this interpreter.'}
            helperText={lerobotGap.stuck.length > 0
              ? lerobotCap.description
              : <>Writes a LeRobot v3 dataset (state, camera video) that loads directly in LeRobot.</>}
          />
        )}
        {allExportsCap && !allExportsCap.available && hasGap(allExportsGap) && (
          <InstallBanner
            capability={allExportsCap}
            installable={allExportsGap.fixable.length > 0}
            title={allExportsTitle(allExportsGap.fixable.length, allExportsGap.stuck.length)}
            helperText={allExportsGap.stuck.length > 0
              // The backend's explanation of what the extra can't install here.
              ? allExportsCap.description
              : <>Parquet, HDF5, and CSV exports work without this. Install if you need
                  Zarr or RLDS dataset output.</>}
          />
        )}

        {/* Preset dropdown — appears only if presets loaded successfully */}
        {presets.length > 0 && (
          <div style={{ marginBottom: 16 }}>
            <label style={labelStyle}>Preset</label>
            <select
              value={selectedPreset}
              onChange={e => applyPreset(e.target.value)}
              aria-describedby={stuckPresets.length > 0 ? stuckNoteId : undefined}
              style={fieldStyle}
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
            {selectedPresetMeta && (
              <div style={{ fontSize: 12, color: '#8b949e', marginTop: 6 }}>
                {selectedPresetMeta.description}
              </div>
            )}
            {stuckPresets.length > 0 && (
              <div id={stuckNoteId}>
                {stuckPresets.map(p => (
                  <div key={p.name} style={{ fontSize: 12, color: '#8b949e', marginTop: 6 }}>
                    <strong>{p.name}</strong>: {p.unavailable_reason}
                  </div>
                ))}
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
          <label htmlFor={outputDirId} style={labelStyle}>Output directory</label>
          <input
            id={outputDirId}
            type="text"
            value={outputDir}
            onChange={e => setOutputDir(e.target.value)}
            style={fieldStyle}
          />
        </div>

        <div style={{ marginBottom: 16, display: 'flex', gap: 12 }}>
          <div style={{ flex: 1 }}>
            <label htmlFor={rateId} style={labelStyle}>
              {lerobot
                ? `Frame rate (fps, default ${LEROBOT_DEFAULT_FPS})`
                : 'Downsample (Hz, optional)'}
            </label>
            <input
              id={rateId}
              type="text"
              value={downsampleHz}
              placeholder={lerobot ? String(LEROBOT_DEFAULT_FPS) : 'e.g. 50'}
              onChange={e => setDownsampleHz(e.target.value)}
              aria-invalid={rateErr ? true : undefined}
              aria-describedby={rateNote ? rateNoteId : undefined}
              // Swap the whole `border` shorthand. Layering `borderColor` on
              // it made React clear the shorthand's colour when the error
              // went away, leaving a browser-default border.
              style={rateErr ? { ...fieldStyle, border: '1px solid #f85149' } : fieldStyle}
            />
            {rateNote && (
              <div
                id={rateNoteId}
                style={{ fontSize: 12, color: rateErr ? '#f85149' : '#8b949e', marginTop: 6 }}
              >
                {rateNote}
              </div>
            )}
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
            marginBottom: lerobot ? 6 : 16,
            color: lerobot ? '#8b949e' : undefined,
            cursor: lerobot ? 'not-allowed' : 'pointer',
          }}
        >
          <input
            type="checkbox"
            checked={lerobot || sync}
            disabled={lerobot}
            aria-describedby={lerobot ? syncNoteId : undefined}
            onChange={e => setSync(e.target.checked)}
          />
          Synchronize topics before export
        </label>
        {lerobot && (
          <div id={syncNoteId} style={{ fontSize: 12, color: '#8b949e', marginBottom: 16 }}>
            {LEROBOT_SYNC_NOTE}
          </div>
        )}

        {/* No live role: the success toast is the copy that's announced. */}
        {result && (
          <div
            data-testid="export-result"
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
            Exported to {result}
          </div>
        )}

        {error && (
          <ExportFailure message={`Export failed: ${error}`} style={CLASSIC_EXPORT_FAILURE_STYLE} />
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
            disabled={!canExport}
            style={{
              background: canExport ? '#238636' : '#21262d',
              border: 'none',
              borderRadius: 6,
              padding: '8px 16px',
              color: '#fff',
              cursor: canExport ? 'pointer' : 'not-allowed',
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
