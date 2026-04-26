// TransformEditor — apply derived signals to a topic.
//
// Two tabs:
//   1. Common: dropdown menu (derivative / integral / MA / low_pass /
//      scale / abs / shift) with appropriate parameter inputs
//   2. Expression: free-form Polars expression with a server-sandboxed
//      eval (sandbox lives in core/transforms.py:apply_polars_expression)
//
// Live preview hits POST /api/transforms/preview and shows a small
// thumbnail chart of the transformed signal. Save adds a derived series
// to the parent Explorer's plot.

import React, { useEffect, useMemo, useState } from 'react'
import Plot from 'react-plotly.js'
import {
  api,
  TransformOp,
  TransformPreviewResponse,
} from '../api'
import { useErrorToast } from '../ErrorToast'

interface Props {
  bagId: number
  topic: string
  // The numeric columns available on this topic (timestamp_ns excluded).
  numericColumns: string[]
  onSave: (label: string, points: Array<{ t_ns: number; v: number }>) => void
  onClose: () => void
}

type Tab = 'common' | 'expression'

const OP_LABELS: Record<TransformOp, string> = {
  derivative: 'Derivative (d/dt)',
  integral: 'Integral (∫ dt)',
  moving_average: 'Moving average',
  low_pass: 'Low-pass filter',
  scale: 'Scale (multiply)',
  abs: 'Absolute value',
  shift: 'Shift (lag/lead)',
}

export default function TransformEditor({
  bagId,
  topic,
  numericColumns,
  onSave,
  onClose,
}: Props) {
  const toast = useErrorToast()
  const [tab, setTab] = useState<Tab>('common')

  // Common-mode state
  const [op, setOp] = useState<TransformOp>('derivative')
  const [column, setColumn] = useState<string>(numericColumns[0] ?? '')
  const [scaleFactor, setScaleFactor] = useState(1.0)
  const [maWindow, setMaWindow] = useState(5)
  const [lpAlpha, setLpAlpha] = useState(0.1)
  const [shiftPeriods, setShiftPeriods] = useState(1)

  // Expression-mode state
  const [expression, setExpression] = useState(
    numericColumns.length > 0
      ? `pl.col("${numericColumns[0]}") * 2`
      : 'pl.col("timestamp_ns")',
  )

  const [preview, setPreview] = useState<TransformPreviewResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const buildBody = useMemo(
    () => () => {
      if (tab === 'common') {
        const params: Record<string, number> = {}
        if (op === 'scale') params.factor = scaleFactor
        if (op === 'moving_average') params.window = maWindow
        if (op === 'low_pass') params.alpha = lpAlpha
        if (op === 'shift') params.periods = shiftPeriods
        return { bag_id: bagId, topic, op, column, params, max_points: 800 }
      }
      return { bag_id: bagId, topic, expression, max_points: 800 }
    },
    [tab, op, column, scaleFactor, maWindow, lpAlpha, shiftPeriods, expression, bagId, topic],
  )

  async function runPreview() {
    setLoading(true)
    setError(null)
    try {
      const r = await api.previewTransform(buildBody())
      setPreview(r)
    } catch (e: any) {
      const msg = e?.message ?? String(e)
      setError(msg)
      setPreview(null)
    }
    setLoading(false)
  }

  // Auto-preview when inputs change; debounce 350ms so typing in
  // expression mode doesn't fire a request per keystroke.
  useEffect(() => {
    const timer = setTimeout(runPreview, 350)
    return () => clearTimeout(timer)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, op, column, scaleFactor, maWindow, lpAlpha, shiftPeriods, expression])

  function handleSave() {
    if (!preview || !preview.data.length) {
      toast.push('warn', 'Run a preview first, then save.')
      return
    }
    const valueKey = preview.label
    const points = preview.data.map(row => ({
      t_ns: Number(row.timestamp_ns),
      v: Number(row[valueKey]),
    }))
    onSave(preview.label, points)
    toast.push('info', `Added "${preview.label}" to the plot`)
    onClose()
  }

  const overlayStyle: React.CSSProperties = {
    position: 'fixed',
    inset: 0,
    background: 'rgba(0, 0, 0, 0.7)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 100,
  }
  const dialogStyle: React.CSSProperties = {
    background: '#161b22',
    border: '1px solid #30363d',
    borderRadius: 12,
    padding: 20,
    width: 640,
    maxHeight: '85vh',
    overflow: 'auto',
  }
  const tabStyle = (active: boolean): React.CSSProperties => ({
    background: active ? '#1f6feb22' : '#21262d',
    border: active ? '1px solid #1f6feb' : '1px solid #30363d',
    borderRadius: 6,
    padding: '6px 14px',
    color: '#e1e4e8',
    cursor: 'pointer',
    fontSize: 13,
  })
  const inputStyle: React.CSSProperties = {
    background: '#0d1117',
    border: '1px solid #30363d',
    borderRadius: 6,
    padding: '6px 10px',
    color: '#e1e4e8',
    fontSize: 13,
  }

  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={dialogStyle} onClick={e => e.stopPropagation()}>
        <h2 style={{ fontSize: 18, fontWeight: 600, marginBottom: 4 }}>
          Transform editor
        </h2>
        <p style={{ color: '#8b949e', fontSize: 13, marginBottom: 16 }}>
          Apply a math op to <code>{topic}</code> and add the result as a
          new derived series.
        </p>

        <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
          <button onClick={() => setTab('common')} style={tabStyle(tab === 'common')}>
            Common
          </button>
          <button onClick={() => setTab('expression')} style={tabStyle(tab === 'expression')}>
            Expression
          </button>
        </div>

        {tab === 'common' && (
          <div style={{ marginBottom: 16 }}>
            <label style={{ display: 'block', marginBottom: 12 }}>
              <span style={{ fontSize: 13, color: '#8b949e' }}>Operation</span>
              <select
                value={op}
                onChange={e => setOp(e.target.value as TransformOp)}
                style={{ ...inputStyle, width: '100%', marginTop: 4 }}
              >
                {(Object.keys(OP_LABELS) as TransformOp[]).map(o => (
                  <option key={o} value={o}>
                    {OP_LABELS[o]}
                  </option>
                ))}
              </select>
            </label>
            <label style={{ display: 'block', marginBottom: 12 }}>
              <span style={{ fontSize: 13, color: '#8b949e' }}>Column</span>
              <select
                value={column}
                onChange={e => setColumn(e.target.value)}
                style={{ ...inputStyle, width: '100%', marginTop: 4 }}
              >
                {numericColumns.map(c => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>
            </label>
            {op === 'scale' && (
              <label style={{ display: 'block', marginBottom: 12 }}>
                <span style={{ fontSize: 13, color: '#8b949e' }}>Factor</span>
                <input
                  type="number"
                  value={scaleFactor}
                  step={0.1}
                  onChange={e => setScaleFactor(Number(e.target.value))}
                  style={{ ...inputStyle, width: 120, marginTop: 4 }}
                />
              </label>
            )}
            {op === 'moving_average' && (
              <label style={{ display: 'block', marginBottom: 12 }}>
                <span style={{ fontSize: 13, color: '#8b949e' }}>Window (samples)</span>
                <input
                  type="number"
                  value={maWindow}
                  min={1}
                  onChange={e => setMaWindow(Math.max(1, Number(e.target.value)))}
                  style={{ ...inputStyle, width: 120, marginTop: 4 }}
                />
              </label>
            )}
            {op === 'low_pass' && (
              <label style={{ display: 'block', marginBottom: 12 }}>
                <span style={{ fontSize: 13, color: '#8b949e' }}>
                  Alpha (smaller = more smoothing)
                </span>
                <input
                  type="number"
                  value={lpAlpha}
                  step={0.05}
                  min={0.01}
                  max={1.0}
                  onChange={e => setLpAlpha(Number(e.target.value))}
                  style={{ ...inputStyle, width: 120, marginTop: 4 }}
                />
              </label>
            )}
            {op === 'shift' && (
              <label style={{ display: 'block', marginBottom: 12 }}>
                <span style={{ fontSize: 13, color: '#8b949e' }}>Periods</span>
                <input
                  type="number"
                  value={shiftPeriods}
                  step={1}
                  onChange={e => setShiftPeriods(Number(e.target.value))}
                  style={{ ...inputStyle, width: 120, marginTop: 4 }}
                />
              </label>
            )}
          </div>
        )}

        {tab === 'expression' && (
          <div style={{ marginBottom: 16 }}>
            <label style={{ display: 'block', marginBottom: 12 }}>
              <span style={{ fontSize: 13, color: '#8b949e' }}>Polars expression</span>
              <textarea
                value={expression}
                onChange={e => setExpression(e.target.value)}
                style={{
                  ...inputStyle,
                  width: '100%',
                  marginTop: 4,
                  minHeight: 90,
                  fontFamily: 'monospace',
                  resize: 'vertical',
                }}
              />
            </label>
            <p style={{ fontSize: 11, color: '#8b949e' }}>
              Allowed: <code>pl.col(...)</code>, <code>pl.lit(...)</code>, arithmetic,
              chained methods (<code>.abs()</code>, <code>.pow(2)</code>,
              <code>.sqrt()</code>, <code>.rolling_mean(N)</code>, etc.). Forbidden:
              imports, <code>__</code> attributes, <code>pl.read_csv</code> and other
              IO functions.
            </p>
          </div>
        )}

        <div
          style={{
            background: '#0d1117',
            border: '1px solid #30363d',
            borderRadius: 6,
            padding: 8,
            marginBottom: 16,
          }}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
            <span style={{ fontSize: 12, color: '#8b949e' }}>Preview</span>
            {loading && <span style={{ fontSize: 11, color: '#8b949e' }}>updating…</span>}
          </div>
          {error ? (
            <div style={{ color: '#f85149', fontSize: 12, padding: 8 }}>
              {error}
            </div>
          ) : preview && preview.data.length > 0 ? (
            <Plot
              data={[
                {
                  type: 'scattergl',
                  mode: 'lines',
                  x: preview.data.map(r => Number(r.timestamp_ns) / 1e9),
                  y: preview.data.map(r => Number(r[preview.label])),
                  line: { color: '#58a6ff', width: 1.2 },
                  hoverinfo: 'skip',
                },
              ]}
              layout={{
                autosize: true,
                height: 180,
                margin: { l: 40, r: 8, t: 8, b: 28 },
                paper_bgcolor: '#0d1117',
                plot_bgcolor: '#0d1117',
                font: { color: '#8b949e', size: 10 },
                xaxis: { gridcolor: '#21262d', color: '#8b949e' },
                yaxis: { gridcolor: '#21262d', color: '#8b949e' },
                showlegend: false,
              }}
              style={{ width: '100%' }}
              useResizeHandler
              config={{ displayModeBar: false, responsive: true }}
            />
          ) : (
            <div style={{ color: '#8b949e', fontSize: 12, padding: 16, textAlign: 'center' }}>
              {loading ? '' : 'Adjust inputs above to preview the transform.'}
            </div>
          )}
        </div>

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
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={!preview || !!error}
            style={{
              background: !preview || !!error ? '#21262d' : '#238636',
              color: '#fff',
              border: 'none',
              borderRadius: 6,
              padding: '8px 16px',
              cursor: !preview || !!error ? 'not-allowed' : 'pointer',
              fontWeight: 600,
            }}
          >
            Add to plot
          </button>
        </div>
      </div>
    </div>
  )
}
