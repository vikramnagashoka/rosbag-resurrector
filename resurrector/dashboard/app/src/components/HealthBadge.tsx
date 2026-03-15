import React from 'react'

interface Props {
  score: number | null
  size?: 'small' | 'medium' | 'large'
}

export default function HealthBadge({ score, size = 'medium' }: Props) {
  if (score === null || score === undefined) {
    return <span style={{ color: '#8b949e', fontSize: '13px' }}>?</span>
  }

  let bg: string
  let color: string
  if (score >= 90) {
    bg = '#238636'
    color = '#fff'
  } else if (score >= 70) {
    bg = '#9e6a03'
    color = '#fff'
  } else if (score >= 50) {
    bg = '#bd561d'
    color = '#fff'
  } else {
    bg = '#da3633'
    color = '#fff'
  }

  const sizes = {
    small: { fontSize: '11px', padding: '2px 6px' },
    medium: { fontSize: '13px', padding: '4px 10px' },
    large: { fontSize: '18px', padding: '6px 16px' },
  }

  return (
    <span style={{
      background: bg,
      color,
      borderRadius: '12px',
      fontWeight: 700,
      display: 'inline-block',
      ...sizes[size],
    }}>
      {score}/100
    </span>
  )
}
