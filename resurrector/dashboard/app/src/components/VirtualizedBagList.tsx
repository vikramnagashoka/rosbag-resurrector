// Virtualized bag list — v0.6.0 (Sub-feature C.7)
//
// Library scrolls smoothly past thousands of indexed bags by rendering
// only the rows currently in view. Uses react-window v2's List
// component with a fixed row height.
//
// Activation threshold: when bags.length > VIRTUALIZE_AT, switches to
// virtualized rendering. Below that, the original flat map is fine —
// virtualization adds DOM-recycling overhead that's only worth it on
// large lists.

import { CSSProperties, memo } from 'react'
import { Link } from 'react-router-dom'
import { List, type RowComponentProps } from 'react-window'

import { Bag } from '../api'
import HealthBadge from './HealthBadge'

const VIRTUALIZE_AT = 100  // < 100 bags: render flat. >= 100: virtualize.
const ROW_HEIGHT = 92      // matches the bag-card height in the inline path

function formatSize(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let size = bytes
  for (const unit of units) {
    if (size < 1024) return `${size.toFixed(1)} ${unit}`
    size /= 1024
  }
  return `${size.toFixed(1)} PB`
}

function basename(path: string): string {
  return path.split(/[/\\]/).pop() || path
}

interface BagRowProps {
  bag: Bag
}

/** A single bag's clickable card. Memoized so virtualized re-renders
 *  during scroll don't repaint every visible row. */
const BagRow = memo(function BagRow({ bag }: BagRowProps) {
  return (
    <Link
      to={`/classic/bag/${bag.id}`}
      style={{ textDecoration: 'none', color: 'inherit', display: 'block' }}
    >
      <div
        style={{
          background: 'var(--color-bg-card)',
          border: '1px solid var(--color-border)',
          borderRadius: 'var(--radius-lg)',
          padding: 'var(--space-4)',
          margin: '0 0 var(--space-3) 0',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          transition: 'border-color var(--duration-fast) var(--easing)',
          height: ROW_HEIGHT - 12,
          boxSizing: 'border-box',
        }}
        onMouseEnter={e => (e.currentTarget.style.borderColor = 'var(--color-accent)')}
        onMouseLeave={e => (e.currentTarget.style.borderColor = 'var(--color-border)')}
      >
        <div style={{ flex: 1, overflow: 'hidden' }}>
          <div
            style={{
              fontSize: 'var(--text-lg)',
              fontWeight: 600,
              color: 'var(--color-text)',
              marginBottom: 'var(--space-1)',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {basename(bag.path)}
          </div>
          <div
            style={{
              display: 'flex', gap: 'var(--space-4)',
              color: 'var(--color-text-secondary)', fontSize: 'var(--text-base)',
            }}
          >
            <span>{bag.duration_sec?.toFixed(1)}s</span>
            <span>{formatSize(bag.size_bytes)}</span>
            <span>{bag.topics.length} topics</span>
            <span>{bag.message_count?.toLocaleString()} msgs</span>
          </div>
        </div>
        <HealthBadge score={bag.health_score} />
      </div>
    </Link>
  )
})

interface Props {
  bags: Bag[]
  /** Pixel height of the scrollable viewport when virtualized. */
  height?: number
}

// react-window v2 types `rowComponent` props as: caller-supplied
// rowProps merged with the runtime `index` + `style` props the
// list provides per row.
type Row = RowComponentProps<{ bags: Bag[] }>

function VirtualizedRow({ index, style, bags }: Row) {
  return (
    <div style={style as CSSProperties}>
      <BagRow bag={bags[index]} />
    </div>
  )
}

export default function VirtualizedBagList({ bags, height = 600 }: Props) {
  // Below the threshold, flat-map (lighter, no DOM recycling) — react-window
  // adds overhead that's not worth it for short lists.
  if (bags.length < VIRTUALIZE_AT) {
    return (
      <div>
        {bags.map(b => (
          <BagRow key={b.id} bag={b} />
        ))}
      </div>
    )
  }
  // Virtualized path — only renders rows in the visible window.
  return (
    <List
      rowComponent={VirtualizedRow}
      rowCount={bags.length}
      rowHeight={ROW_HEIGHT}
      rowProps={{ bags }}
      defaultHeight={height}
      overscanCount={5}
      style={{ overflow: 'auto' }}
    />
  )
}
