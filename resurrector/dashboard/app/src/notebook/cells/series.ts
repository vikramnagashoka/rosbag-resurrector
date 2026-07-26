import { TopicDataResponse } from '../../api'

export interface Series { label: string; ts: number[]; values: number[] }

export const SERIES_COLORS = ['#5a57d6', '#bf8a2c', '#2f8f5f', '#3f6fb0', '#c75c4b', '#7c4dd6']

// Pull numeric columns out of a topic-data response as plottable series.
// Shared by the plot + stats cells.
export function extractSeries(resp: TopicDataResponse): Series[] {
  if (!resp.data.length) return []
  const ts = resp.data.map(r => Number(r.timestamp_ns))
  const out: Series[] = []
  for (const col of resp.columns) {
    if (col === 'timestamp_ns') continue
    if (typeof resp.data[0][col] !== 'number') continue
    out.push({
      label: col,
      ts,
      values: resp.data.map(r => (typeof r[col] === 'number' ? (r[col] as number) : NaN)),
    })
  }
  return out
}

export function seriesStats(values: number[]): { min: number; mean: number; max: number; std: number } {
  const v = values.filter(Number.isFinite)
  if (!v.length) return { min: NaN, mean: NaN, max: NaN, std: NaN }
  let min = Infinity, max = -Infinity, sum = 0
  for (const x of v) { min = Math.min(min, x); max = Math.max(max, x); sum += x }
  const mean = sum / v.length
  let sq = 0
  for (const x of v) sq += (x - mean) ** 2
  return { min, mean, max, std: Math.sqrt(sq / v.length) }
}
