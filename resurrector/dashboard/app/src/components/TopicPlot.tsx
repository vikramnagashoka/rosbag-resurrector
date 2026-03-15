import React, { useMemo } from 'react'

interface TopicData {
  topic: string
  total: number
  columns: string[]
  data: Record<string, any>[]
}

interface Props {
  data: TopicData
}

export default function TopicPlot({ data }: Props) {
  // Identify numeric columns for plotting
  const numericColumns = useMemo(() => {
    if (data.data.length === 0) return []
    return data.columns.filter(col => {
      if (col === 'timestamp_ns') return false
      const sample = data.data[0][col]
      return typeof sample === 'number'
    })
  }, [data])

  const timestamps = useMemo(() => {
    if (data.data.length === 0) return []
    const firstTs = data.data[0].timestamp_ns
    return data.data.map(d => ((d.timestamp_ns - firstTs) / 1e9).toFixed(3))
  }, [data])

  // Quick stats
  const stats = useMemo(() => {
    return numericColumns.slice(0, 8).map(col => {
      const values = data.data.map(d => d[col]).filter(v => typeof v === 'number' && isFinite(v))
      if (values.length === 0) return { col, min: 0, max: 0, mean: 0, std: 0 }
      const min = Math.min(...values)
      const max = Math.max(...values)
      const mean = values.reduce((a, b) => a + b, 0) / values.length
      const std = Math.sqrt(values.reduce((a, b) => a + (b - mean) ** 2, 0) / values.length)
      return { col, min, max, mean, std }
    })
  }, [data, numericColumns])

  // SVG-based mini charts (no Plotly dependency needed for basic display)
  function MiniChart({ col, values }: { col: string; values: number[] }) {
    if (values.length < 2) return null
    const min = Math.min(...values)
    const max = Math.max(...values)
    const range = max - min || 1
    const w = 600
    const h = 100
    const points = values.map((v, i) => {
      const x = (i / (values.length - 1)) * w
      const y = h - ((v - min) / range) * h
      return `${x},${y}`
    }).join(' ')

    return (
      <div style={{ marginBottom: '16px' }}>
        <div style={{ fontSize: '13px', fontWeight: 500, color: '#58a6ff', marginBottom: '4px' }}>{col}</div>
        <svg width={w} height={h} style={{ background: '#0d1117', borderRadius: '4px' }}>
          <polyline points={points} fill="none" stroke="#58a6ff" strokeWidth="1.5" />
        </svg>
      </div>
    )
  }

  return (
    <div style={{
      background: '#161b22',
      border: '1px solid #30363d',
      borderRadius: '8px',
      padding: '16px',
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
        <h3 style={{ fontSize: '16px', fontWeight: 600 }}>{data.topic}</h3>
        <span style={{ color: '#8b949e', fontSize: '13px' }}>
          {data.total.toLocaleString()} messages | {data.columns.length} columns
        </span>
      </div>

      {/* Quick stats */}
      {stats.length > 0 && (
        <div style={{ marginBottom: '16px' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #21262d' }}>
                <th style={{ padding: '4px 8px', textAlign: 'left', color: '#8b949e' }}>Column</th>
                <th style={{ padding: '4px 8px', textAlign: 'right', color: '#8b949e' }}>Min</th>
                <th style={{ padding: '4px 8px', textAlign: 'right', color: '#8b949e' }}>Max</th>
                <th style={{ padding: '4px 8px', textAlign: 'right', color: '#8b949e' }}>Mean</th>
                <th style={{ padding: '4px 8px', textAlign: 'right', color: '#8b949e' }}>Std</th>
              </tr>
            </thead>
            <tbody>
              {stats.map(s => (
                <tr key={s.col} style={{ borderBottom: '1px solid #21262d' }}>
                  <td style={{ padding: '4px 8px', color: '#58a6ff' }}>{s.col}</td>
                  <td style={{ padding: '4px 8px', textAlign: 'right' }}>{s.min.toFixed(4)}</td>
                  <td style={{ padding: '4px 8px', textAlign: 'right' }}>{s.max.toFixed(4)}</td>
                  <td style={{ padding: '4px 8px', textAlign: 'right' }}>{s.mean.toFixed(4)}</td>
                  <td style={{ padding: '4px 8px', textAlign: 'right' }}>{s.std.toFixed(4)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Charts */}
      {numericColumns.slice(0, 6).map(col => (
        <MiniChart
          key={col}
          col={col}
          values={data.data.map(d => d[col]).filter(v => typeof v === 'number' && isFinite(v))}
        />
      ))}

      {numericColumns.length === 0 && (
        <div style={{ color: '#8b949e', textAlign: 'center', padding: '24px' }}>
          No numeric columns to plot. Columns: {data.columns.join(', ')}
        </div>
      )}
    </div>
  )
}
