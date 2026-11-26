import React, { useId, useState } from 'react'
import { api, UndecodableTopicInfo } from '../api'

// Shown above a notebook's cells when its bag has topics whose message
// type has no decoder (GET /api/bags/{id}/decoders). Their fields can't be
// read, so the user chooses: load a folder of .msg definitions from this
// machine, or keep going with health only (the health checks need just
// timestamps and sizes). "Health only" folds the notice to one line that
// can be reopened. Python decoder files aren't offered here: the backend
// refuses them, since loading one runs it.

interface Props {
  topics: UndecodableTopicInfo[]
  healthOnly: boolean
  success: string | null
  onHealthOnly: () => void
  onReopen: () => void
  onDismissSuccess: () => void
  // Called with the load response's types once a folder loads; the parent
  // re-checks the bag and decides what the notice shows next.
  onLoaded: (path: string, messageTypes: string[]) => Promise<void>
}

export default function DecoderNotice({
  topics, healthOnly, success, onHealthOnly, onReopen, onDismissSuccess, onLoaded,
}: Props) {
  const [path, setPath] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  // Shown when a folder loaded but topics still can't be decoded (a type
  // they use is missing): the reasons in the list say which.
  const [note, setNote] = useState<string | null>(null)
  const inputId = useId()
  const errorId = useId()
  const n = topics.length
  const it = n === 1 ? 'it' : 'them'

  if (n === 0) {
    if (!success) return null
    return (
      <div className="nb-decoder-notice nb-decoder-ok" role="status">
        <span>{success}</span>
        <button className="nb-decoder-link" onClick={onDismissSuccess} aria-label="Dismiss">Dismiss</button>
      </div>
    )
  }

  if (healthOnly) {
    return (
      <div className="nb-decoder-notice nb-decoder-folded" role="status">
        <span>
          <span className="nb-decoder-mono">{topics.map(t => t.topic).join(', ')}</span>
          {': health only, no decoder.'}
        </span>
        <button className="nb-decoder-link" onClick={onReopen}>Load definitions…</button>
      </div>
    )
  }

  async function load(ev: React.FormEvent) {
    ev.preventDefault()
    const p = path.trim()
    if (!p) {
      setError('Enter the path of a folder of .msg files, e.g. ~/ros2_ws/src')
      return
    }
    setBusy(true)
    setError(null)
    setNote(null)
    try {
      const r = await api.loadDecoders(p)
      const count = r.message_types.length
      setNote(
        `Loaded ${count} message type${count === 1 ? '' : 's'} from ${r.path}, but the ` +
        'topics above still can\'t be decoded. Their reasons say what is missing.',
      )
      await onLoaded(r.path, r.message_types)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <section className="nb-decoder-notice" aria-label="Topics that can't be decoded">
      <div className="nb-decoder-title">
        {n} topic{n === 1 ? '' : 's'} can't be decoded
      </div>
      <ul className="nb-decoder-list">
        {topics.map(t => (
          <li key={t.topic}>
            <span className="nb-decoder-mono">{t.topic}</span>
            <span className="nb-decoder-type">{t.message_type}</span>
            <span className="nb-decoder-reason">{t.reason}</span>
          </li>
        ))}
      </ul>
      <p className="nb-decoder-body">
        Resurrector has no decoder for {n === 1 ? 'this message type' : 'these message types'}, so{' '}
        {n === 1 ? 'its' : 'their'} fields can't be plotted, queried or exported. Point it at a folder
        of .msg files (your ROS workspace's src/, or an installed share/ folder) to decode {it}, or keep
        going with health only: the health checks still cover {it}.
      </p>
      <form className="nb-decoder-form" onSubmit={load}>
        <label htmlFor={inputId} className="nb-decoder-label">Folder of .msg files</label>
        <input
          id={inputId}
          className="nb-decoder-input"
          value={path}
          onChange={e => setPath(e.target.value)}
          placeholder="~/ros2_ws/src"
          spellCheck={false}
          aria-describedby={error ? errorId : undefined}
          aria-invalid={error ? true : undefined}
        />
        <button type="submit" className="nb-btn nb-btn-accent" disabled={busy}>
          {busy ? 'Loading…' : 'Load definitions'}
        </button>
        <button type="button" className="nb-btn" onClick={onHealthOnly} disabled={busy}>
          Health only
        </button>
      </form>
      {error && <div id={errorId} className="nb-decoder-error">{error}</div>}
      {note && !error && <div className="nb-decoder-note">{note}</div>}
    </section>
  )
}
