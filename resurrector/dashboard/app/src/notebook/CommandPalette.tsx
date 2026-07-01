import React from 'react'
import { CommandEntry } from './commands'

// Autocomplete dropdown floating above the command input. The top row is
// what Enter runs; clicking any row (on mousedown, before the input blurs)
// runs it. Rendered only when open + there's a query.

export default function CommandPalette({
  entries, onRun,
}: {
  entries: CommandEntry[]
  onRun: (e: CommandEntry) => void
}) {
  return (
    <div className="nb-palette" role="listbox" aria-label="Command palette">
      {entries.length === 0 ? (
        <div className="nb-palette-empty">No matching commands.</div>
      ) : (
        entries.slice(0, 8).map((e, i) => (
          <button
            key={e.id}
            className={`nb-palette-row${i === 0 ? ' top' : ''}`}
            role="option"
            aria-selected={i === 0}
            // mousedown (not click) so it fires before the input's blur closes us.
            onMouseDown={ev => { ev.preventDefault(); onRun(e) }}
          >
            <span className="nb-palette-kind">{e.kind}</span>
            <span className="nb-palette-cmd">{e.cmd}</span>
            <span className="nb-palette-desc">{e.description}</span>
          </button>
        ))
      )}
    </div>
  )
}
