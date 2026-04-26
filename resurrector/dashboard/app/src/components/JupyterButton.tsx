// JupyterButton — one-click "open this selection in Jupyter."
//
// On click:
//   1. Trim the current selection to a Parquet file under the server's
//      ~/.resurrector cache (path resolved server-side, not guessed by
//      the browser — older versions sent ~/.resurrector verbatim and
//      the validator rejected the literal "~" prefix)
//   2. Copy a Python snippet to the user's clipboard that reads the
//      Parquet via Polars
//   3. Try to open localhost:8888 (default Jupyter URL); if Jupyter
//      isn't running, just toast the snippet so they can paste it
//      anywhere
//
// This is intentionally simple — auto-detection of arbitrary Jupyter
// servers is a rabbit hole. The 90% case is "I have Jupyter running
// already at the default port"; the 10% case can paste the snippet
// into any Python REPL.

import React, { useEffect, useState } from 'react'
import { api } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'

interface Props {
  bagId: number
  // Optional time window. If unset, exports the whole bag.
  startSec?: number
  endSec?: number
  // Topics to include in the Parquet export.
  topics: string[]
  disabled?: boolean
}

function joinPath(dir: string, name: string): string {
  // Server-supplied dir is always absolute (Path.resolve). Pick the
  // separator that's already in the dir so we don't break Windows
  // paths on Linux dashboards or vice versa.
  const sep = dir.includes('\\') && !dir.includes('/') ? '\\' : '/'
  const trimmed = dir.replace(/[\/\\]$/, '')
  return `${trimmed}${sep}${name}`
}

export default function JupyterButton({
  bagId,
  startSec,
  endSec,
  topics,
  disabled,
}: Props) {
  const toast = useErrorToast()
  const [busy, setBusy] = useState(false)
  const [cacheDir, setCacheDir] = useState<string | null>(null)

  // Resolve the server's ~/.resurrector path once on mount. Without
  // this we'd be sending the literal string "~/.resurrector" which the
  // path validator turns into "<cwd>/~/.resurrector" — exactly the
  // 403 the user hit.
  useEffect(() => {
    let active = true
    api
      .getSystemPaths()
      .then(p => {
        if (active) setCacheDir(p.resurrector_cache)
      })
      .catch(() => {
        // Non-fatal — handleClick will fall back to a clear error.
      })
    return () => {
      active = false
    }
  }, [])

  async function handleClick() {
    if (topics.length === 0) {
      toast.push('error', 'No topics selected to export')
      return
    }
    if (!cacheDir) {
      toast.push('error', 'Could not resolve the export directory; try again in a second')
      return
    }
    setBusy(true)
    try {
      const start = startSec ?? 0
      // Default "whole bag" is hard to know without metadata; pass a
      // sentinel large value and let the backend clamp via time_slice.
      const end = endSec ?? 1e12
      const stamp = Date.now()
      const path = joinPath(cacheDir, `jupyter_export_${bagId}_${stamp}.parquet`)
      const r = await runWithToast(
        toast,
        () =>
          api.trimRange(bagId, {
            start_sec: start,
            end_sec: end,
            topics,
            format: 'parquet',
            output_path: path,
          }),
        { errorPrefix: 'Jupyter export' },
      )
      if (!r) return

      const snippet = topics.length === 1
        ? `import polars as pl\ndf = pl.read_parquet(r"${r.output}/${topics[0].replace(/^\//, '').replace(/\//g, '_')}.parquet")\ndf.head()`
        : `import polars as pl\nfrom pathlib import Path\nfor f in Path(r"${r.output}").glob("*.parquet"):\n    print(f.name, pl.read_parquet(f).shape)`

      try {
        await navigator.clipboard.writeText(snippet)
        toast.push('info', 'Python snippet copied to clipboard')
      } catch {
        toast.push('warn', `Snippet (copy manually): ${snippet}`)
      }

      // Try to open Jupyter; opens regardless of whether it's running so
      // the user gets a clear "no server" page if it isn't.
      window.open('http://localhost:8888/', '_blank')
    } finally {
      setBusy(false)
    }
  }

  return (
    <button
      onClick={handleClick}
      disabled={disabled || busy || !cacheDir}
      title={
        cacheDir
          ? `Export selection as Parquet to ${cacheDir}, copy Polars snippet, open Jupyter`
          : 'Resolving export directory…'
      }
      style={{
        background: '#21262d',
        border: '1px solid #30363d',
        borderRadius: 6,
        padding: '6px 12px',
        color: disabled || busy || !cacheDir ? '#484f58' : '#e1e4e8',
        cursor: disabled || busy || !cacheDir ? 'not-allowed' : 'pointer',
        fontSize: 13,
        display: 'inline-flex',
        alignItems: 'center',
        gap: 6,
      }}
    >
      {/* Inline Jupyter "J" — keeps the bundle free of an icon font. */}
      <span style={{ color: '#f37726', fontWeight: 700, fontFamily: 'monospace' }}>
        J
      </span>
      {busy ? 'Exporting...' : 'Open in Jupyter'}
    </button>
  )
}
