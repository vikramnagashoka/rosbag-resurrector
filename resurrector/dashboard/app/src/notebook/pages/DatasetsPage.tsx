import React, { useEffect, useState } from 'react'
import { api, Dataset } from '../../api'
import { runWithToast, useErrorToast } from '../../ErrorToast'
import NotebookPageShell from '../NotebookPageShell'

// Warm-themed port of the classic Datasets page. Same workflow (list /
// create / delete / versions / export) in the notebook paper palette.

export default function DatasetsPage() {
  const [datasets, setDatasets] = useState<Dataset[]>([])
  const [selected, setSelected] = useState<Dataset | null>(null)
  const [creating, setCreating] = useState(false)
  const [newName, setNewName] = useState('')
  const [newDesc, setNewDesc] = useState('')
  const [exportPath, setExportPath] = useState('./datasets')
  const toast = useErrorToast()

  async function refresh() {
    const r = await runWithToast(toast, () => api.listDatasets())
    if (r) setDatasets(r.datasets)
  }
  useEffect(() => { refresh() /* eslint-disable-next-line */ }, [])

  async function handleCreate(e: React.FormEvent) {
    e.preventDefault()
    if (!newName.trim()) return
    const r = await runWithToast(
      toast, () => api.createDataset({ name: newName.trim(), description: newDesc }),
      { errorPrefix: 'Create dataset' },
    )
    if (r) {
      toast.push('info', `Created "${newName}"`)
      setCreating(false); setNewName(''); setNewDesc(''); refresh()
    }
  }

  async function handleDelete(name: string) {
    if (!confirm(`Delete dataset "${name}" and all its versions?`)) return
    const r = await runWithToast(toast, () => api.deleteDataset(name), { errorPrefix: 'Delete' })
    if (r) {
      toast.push('info', `Deleted "${name}"`)
      if (selected?.name === name) setSelected(null)
      refresh()
    }
  }

  async function handleDeleteVersion(name: string, version: string) {
    if (!confirm(`Delete version ${version} of "${name}"?`)) return
    const r = await runWithToast(toast, () => api.deleteDatasetVersion(name, version))
    if (r) {
      toast.push('info', `Deleted ${name}@${version}`)
      if (selected?.name === name) {
        const updated = await runWithToast(toast, () => api.getDataset(name))
        if (updated) setSelected(updated)
      }
      refresh()
    }
  }

  async function handleExport(name: string, version: string) {
    const r = await runWithToast(
      toast, () => api.exportDatasetVersion(name, version, exportPath), { errorPrefix: 'Export' },
    )
    if (r) toast.push('info', `Exported to ${r.output}`)
  }

  return (
    <NotebookPageShell
      title="Datasets"
      subtitle="Versioned dataset collections for ML training pipelines."
      actions={<button className="nb-btn nb-btn-accent" onClick={() => setCreating(true)}>New dataset</button>}
    >
      <div className="nb-ds-grid">
        <div>
          {datasets.length === 0 ? (
            <div className="nb-panel nb-panel-empty">No datasets yet. Click “New dataset” to create one.</div>
          ) : (
            datasets.map(d => (
              <button
                key={d.id}
                className={`nb-ds-item${selected?.id === d.id ? ' active' : ''}`}
                onClick={() => setSelected(d)}
              >
                <div className="nb-ds-item-head">
                  <span className="nb-ds-name">{d.name}</span>
                  <span
                    className="nb-ds-del"
                    role="button"
                    title="Delete dataset"
                    onClick={e => { e.stopPropagation(); handleDelete(d.name) }}
                  >✕</span>
                </div>
                {d.description && <div className="nb-ds-desc">{d.description}</div>}
                <div className="nb-ds-meta">{d.versions?.length || 0} version(s)</div>
              </button>
            ))
          )}
        </div>

        <div>
          {selected ? (
            <div className="nb-panel">
              <h2 className="nb-panel-title">{selected.name}</h2>
              {selected.description && <p className="nb-panel-sub">{selected.description}</p>}

              <label className="nb-ds-exportrow">
                <span>Export output dir</span>
                <input className="nb-scan-input" value={exportPath} onChange={e => setExportPath(e.target.value)} />
              </label>

              <h3 className="nb-panel-h3">Versions</h3>
              {!selected.versions || selected.versions.length === 0 ? (
                <div className="nb-panel-empty nb-panel-empty-dashed">
                  No versions yet. Create one with{' '}
                  <code>resurrector dataset add-version {selected.name} 1.0 …</code>
                </div>
              ) : (
                <table className="nb-table">
                  <thead>
                    <tr><th>Version</th><th>Format</th><th>Created</th><th /></tr>
                  </thead>
                  <tbody>
                    {selected.versions.map(v => (
                      <tr key={v.version}>
                        <td className="nb-td-accent">{v.version}</td>
                        <td>{v.export_format}</td>
                        <td className="nb-td-muted">{v.created_at}</td>
                        <td className="nb-td-right">
                          <button className="nb-btn nb-btn-accent nb-btn-sm" onClick={() => handleExport(selected.name, v.version)}>Export</button>
                          <button className="nb-btn nb-btn-sm" onClick={() => handleDeleteVersion(selected.name, v.version)}>Delete</button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          ) : (
            <div className="nb-panel nb-panel-empty">Select a dataset to view its versions.</div>
          )}
        </div>
      </div>

      {creating && (
        <div className="nb-modal-backdrop" onClick={() => setCreating(false)}>
          <form className="nb-modal" onClick={e => e.stopPropagation()} onSubmit={handleCreate}>
            <h2 className="nb-panel-title">New dataset</h2>
            <label className="nb-modal-field">
              <span>Name</span>
              <input value={newName} autoFocus placeholder="pick-and-place-v1"
                onChange={e => setNewName(e.target.value)} />
            </label>
            <label className="nb-modal-field">
              <span>Description (optional)</span>
              <textarea value={newDesc} onChange={e => setNewDesc(e.target.value)} />
            </label>
            <div className="nb-modal-actions">
              <button type="button" className="nb-btn" onClick={() => setCreating(false)}>Cancel</button>
              <button type="submit" className="nb-btn nb-btn-accent">Create</button>
            </div>
          </form>
        </div>
      )}
    </NotebookPageShell>
  )
}
