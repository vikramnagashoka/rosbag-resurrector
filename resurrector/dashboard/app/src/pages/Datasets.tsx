import React, { useEffect, useState } from 'react'
import { api, Dataset } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'

export default function Datasets() {
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

  useEffect(() => {
    refresh()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  async function handleCreate(e: React.FormEvent) {
    e.preventDefault()
    if (!newName.trim()) return
    const r = await runWithToast(
      toast,
      () => api.createDataset({ name: newName.trim(), description: newDesc }),
      { errorPrefix: 'Create dataset' },
    )
    if (r) {
      toast.push('info', `Created "${newName}"`)
      setCreating(false)
      setNewName('')
      setNewDesc('')
      refresh()
    }
  }

  async function handleDelete(name: string) {
    if (!confirm(`Delete dataset "${name}" and all its versions?`)) return
    const r = await runWithToast(
      toast,
      () => api.deleteDataset(name),
      { errorPrefix: 'Delete' },
    )
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
      toast,
      () => api.exportDatasetVersion(name, version, exportPath),
      { errorPrefix: 'Export' },
    )
    if (r) toast.push('info', `Exported to ${r.output}`)
  }

  return (
    <div>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: 24,
        }}
      >
        <div>
          <h1 style={{ fontSize: 24, fontWeight: 600 }}>Datasets</h1>
          <p style={{ color: '#8b949e', fontSize: 14, marginTop: 4 }}>
            Versioned dataset collections for ML training pipelines.
          </p>
        </div>
        <button
          onClick={() => setCreating(true)}
          style={{
            background: '#238636',
            color: '#fff',
            border: 'none',
            borderRadius: 6,
            padding: '8px 16px',
            cursor: 'pointer',
            fontSize: 14,
          }}
        >
          New dataset
        </button>
      </div>

      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 2fr',
          gap: 24,
        }}
      >
        <div>
          {datasets.length === 0 ? (
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
              No datasets yet. Click "New dataset" to create one.
            </div>
          ) : (
            datasets.map(d => (
              <div
                key={d.id}
                onClick={() => setSelected(d)}
                style={{
                  background: '#161b22',
                  border:
                    selected?.id === d.id
                      ? '1px solid #1f6feb'
                      : '1px solid #30363d',
                  borderRadius: 8,
                  padding: 12,
                  marginBottom: 8,
                  cursor: 'pointer',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <strong style={{ color: '#58a6ff', fontSize: 14 }}>{d.name}</strong>
                  <button
                    onClick={e => {
                      e.stopPropagation()
                      handleDelete(d.name)
                    }}
                    style={{
                      background: 'transparent',
                      border: 'none',
                      color: '#8b949e',
                      cursor: 'pointer',
                    }}
                  >
                    ✕
                  </button>
                </div>
                {d.description && (
                  <div style={{ color: '#8b949e', fontSize: 12, marginTop: 4 }}>
                    {d.description}
                  </div>
                )}
                <div style={{ color: '#8b949e', fontSize: 11, marginTop: 4 }}>
                  {d.versions?.length || 0} version(s)
                </div>
              </div>
            ))
          )}
        </div>

        <div>
          {selected ? (
            <div
              style={{
                background: '#161b22',
                border: '1px solid #30363d',
                borderRadius: 8,
                padding: 16,
              }}
            >
              <h2 style={{ fontSize: 18, fontWeight: 600, marginBottom: 8 }}>
                {selected.name}
              </h2>
              {selected.description && (
                <p style={{ color: '#8b949e', fontSize: 14, marginBottom: 16 }}>
                  {selected.description}
                </p>
              )}

              <div style={{ marginBottom: 16 }}>
                <label style={{ fontSize: 13, color: '#8b949e', marginRight: 8 }}>
                  Export output dir:
                </label>
                <input
                  value={exportPath}
                  onChange={e => setExportPath(e.target.value)}
                  style={{
                    background: '#0d1117',
                    border: '1px solid #30363d',
                    borderRadius: 6,
                    padding: '4px 10px',
                    color: '#e1e4e8',
                    fontSize: 13,
                    width: 280,
                  }}
                />
              </div>

              <h3 style={{ fontSize: 14, color: '#8b949e', marginBottom: 8 }}>Versions</h3>
              {!selected.versions || selected.versions.length === 0 ? (
                <div
                  style={{
                    background: '#0d1117',
                    border: '1px dashed #30363d',
                    borderRadius: 6,
                    padding: 16,
                    color: '#8b949e',
                    fontSize: 13,
                    textAlign: 'center',
                  }}
                >
                  No versions yet. Create one with{' '}
                  <code>resurrector dataset add-version {selected.name} 1.0 ...</code>
                </div>
              ) : (
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
                  <thead>
                    <tr style={{ borderBottom: '1px solid #30363d' }}>
                      <th style={{ padding: '6px 8px', textAlign: 'left', color: '#8b949e' }}>Version</th>
                      <th style={{ padding: '6px 8px', textAlign: 'left', color: '#8b949e' }}>Format</th>
                      <th style={{ padding: '6px 8px', textAlign: 'left', color: '#8b949e' }}>Created</th>
                      <th style={{ padding: '6px 8px', textAlign: 'right' }} />
                    </tr>
                  </thead>
                  <tbody>
                    {selected.versions.map(v => (
                      <tr key={v.version} style={{ borderBottom: '1px solid #21262d' }}>
                        <td style={{ padding: '6px 8px', color: '#58a6ff' }}>{v.version}</td>
                        <td style={{ padding: '6px 8px' }}>{v.export_format}</td>
                        <td style={{ padding: '6px 8px', color: '#8b949e' }}>{v.created_at}</td>
                        <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                          <button
                            onClick={() => handleExport(selected.name, v.version)}
                            style={{
                              background: '#238636',
                              color: '#fff',
                              border: 'none',
                              borderRadius: 6,
                              padding: '4px 12px',
                              cursor: 'pointer',
                              fontSize: 12,
                              marginRight: 4,
                            }}
                          >
                            Export
                          </button>
                          <button
                            onClick={() => handleDeleteVersion(selected.name, v.version)}
                            style={{
                              background: 'transparent',
                              border: '1px solid #30363d',
                              borderRadius: 6,
                              padding: '4px 8px',
                              color: '#8b949e',
                              cursor: 'pointer',
                              fontSize: 12,
                            }}
                          >
                            Delete
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          ) : (
            <div
              style={{
                background: '#161b22',
                border: '1px solid #30363d',
                borderRadius: 8,
                padding: 32,
                color: '#8b949e',
                textAlign: 'center',
              }}
            >
              Select a dataset to view its versions.
            </div>
          )}
        </div>
      </div>

      {creating && (
        <div
          onClick={() => setCreating(false)}
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
          <form
            onClick={e => e.stopPropagation()}
            onSubmit={handleCreate}
            style={{
              background: '#161b22',
              border: '1px solid #30363d',
              borderRadius: 8,
              padding: 24,
              width: 420,
            }}
          >
            <h2 style={{ fontSize: 18, fontWeight: 600, marginBottom: 16 }}>New dataset</h2>
            <label style={{ display: 'block', marginBottom: 12 }}>
              <span style={{ fontSize: 13, color: '#8b949e' }}>Name</span>
              <input
                value={newName}
                onChange={e => setNewName(e.target.value)}
                autoFocus
                placeholder="pick-and-place-v1"
                style={{
                  width: '100%',
                  background: '#0d1117',
                  border: '1px solid #30363d',
                  borderRadius: 6,
                  padding: '8px 12px',
                  color: '#e1e4e8',
                  marginTop: 4,
                }}
              />
            </label>
            <label style={{ display: 'block', marginBottom: 16 }}>
              <span style={{ fontSize: 13, color: '#8b949e' }}>Description (optional)</span>
              <textarea
                value={newDesc}
                onChange={e => setNewDesc(e.target.value)}
                style={{
                  width: '100%',
                  background: '#0d1117',
                  border: '1px solid #30363d',
                  borderRadius: 6,
                  padding: '8px 12px',
                  color: '#e1e4e8',
                  marginTop: 4,
                  minHeight: 72,
                  fontFamily: 'inherit',
                }}
              />
            </label>
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
              <button
                type="button"
                onClick={() => setCreating(false)}
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
                type="submit"
                style={{
                  background: '#238636',
                  border: 'none',
                  borderRadius: 6,
                  padding: '6px 14px',
                  color: '#fff',
                  cursor: 'pointer',
                }}
              >
                Create
              </button>
            </div>
          </form>
        </div>
      )}
    </div>
  )
}
