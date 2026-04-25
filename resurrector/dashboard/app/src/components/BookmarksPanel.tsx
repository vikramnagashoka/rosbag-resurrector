// BookmarksPanel — right-rail list of all annotations on the current bag.
//
// Surfaces the existing /api/bags/{id}/annotations data as a searchable,
// click-to-jump sidebar. Click on a bookmark and the parent Explorer
// jumps to that timestamp by setting xRangeSec around it.
//
// Persistence is via the existing annotations API (built in v0.3.0); this
// component is purely a view + delete affordance.

import React, { useEffect, useMemo, useState } from 'react'
import { api, Annotation } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'

interface Props {
  bagId: number
  // First-message timestamp of the bag in nanoseconds; bookmarks are
  // timestamp_ns absolute, but we display them relative to first.
  firstTimestampNs: number
  // Callback when a user clicks a bookmark to jump to its location.
  // The parent decides how wide a window to render around the click.
  onJumpToTimestampSec?: (relativeSec: number) => void
}

export default function BookmarksPanel({
  bagId,
  firstTimestampNs,
  onJumpToTimestampSec,
}: Props) {
  const toast = useErrorToast()
  const [annotations, setAnnotations] = useState<Annotation[]>([])
  const [search, setSearch] = useState('')
  const [loading, setLoading] = useState(true)

  async function refresh() {
    const r = await runWithToast(toast, () => api.listAnnotations(bagId))
    if (r) setAnnotations(r.annotations)
    setLoading(false)
  }

  useEffect(() => {
    refresh()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bagId])

  const filtered = useMemo(() => {
    if (!search.trim()) return annotations
    const needle = search.toLowerCase()
    return annotations.filter(
      a =>
        a.text.toLowerCase().includes(needle) ||
        (a.topic ?? '').toLowerCase().includes(needle),
    )
  }, [annotations, search])

  async function handleDelete(id: number, e: React.MouseEvent) {
    e.stopPropagation()
    const r = await runWithToast(toast, () => api.deleteAnnotation(id))
    if (r) {
      setAnnotations(prev => prev.filter(a => a.id !== id))
      toast.push('info', 'Bookmark deleted')
    }
  }

  function handleJump(a: Annotation) {
    if (onJumpToTimestampSec) {
      onJumpToTimestampSec((a.timestamp_ns - firstTimestampNs) / 1e9)
    }
  }

  return (
    <div
      style={{
        background: '#161b22',
        border: '1px solid #30363d',
        borderRadius: 8,
        padding: 12,
        height: 'fit-content',
        position: 'sticky',
        top: 12,
      }}
    >
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: 8,
        }}
      >
        <h3 style={{ fontSize: 13, fontWeight: 600, color: '#8b949e', margin: 0 }}>
          Bookmarks
        </h3>
        <span style={{ fontSize: 11, color: '#8b949e' }}>
          {annotations.length}
        </span>
      </div>

      <input
        type="text"
        value={search}
        onChange={e => setSearch(e.target.value)}
        placeholder="Search bookmarks..."
        style={{
          width: '100%',
          background: '#0d1117',
          border: '1px solid #30363d',
          borderRadius: 6,
          padding: '6px 8px',
          color: '#e1e4e8',
          fontSize: 12,
          marginBottom: 8,
          boxSizing: 'border-box',
        }}
      />

      {loading ? (
        <div style={{ color: '#8b949e', fontSize: 12, padding: 8 }}>Loading...</div>
      ) : filtered.length === 0 ? (
        <div
          style={{
            color: '#8b949e',
            fontSize: 12,
            padding: 12,
            textAlign: 'center',
          }}
        >
          {annotations.length === 0
            ? 'No bookmarks yet. Click on the chart to add one.'
            : 'No bookmarks match your search.'}
        </div>
      ) : (
        <div style={{ maxHeight: 480, overflowY: 'auto' }}>
          {filtered.map(a => (
            <div
              key={a.id}
              onClick={() => handleJump(a)}
              style={{
                padding: '6px 8px',
                borderLeft: '3px solid #f85149',
                background: '#0d1117',
                borderRadius: 4,
                marginBottom: 4,
                cursor: 'pointer',
                position: 'relative',
              }}
              onMouseEnter={e => (e.currentTarget.style.background = '#1c2128')}
              onMouseLeave={e => (e.currentTarget.style.background = '#0d1117')}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span style={{ color: '#f85149', fontSize: 11, fontFamily: 'monospace' }}>
                  t={((a.timestamp_ns - firstTimestampNs) / 1e9).toFixed(3)}s
                </span>
                <button
                  onClick={e => handleDelete(a.id, e)}
                  style={{
                    background: 'transparent',
                    border: 'none',
                    color: '#8b949e',
                    cursor: 'pointer',
                    fontSize: 12,
                    padding: 0,
                  }}
                  title="Delete bookmark"
                >
                  ✕
                </button>
              </div>
              <div style={{ fontSize: 12, color: '#e1e4e8', marginTop: 2 }}>
                {a.text}
              </div>
              {a.topic && (
                <div style={{ fontSize: 10, color: '#58a6ff', marginTop: 2 }}>
                  {a.topic}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
