// <Badge> — v0.6.0 (Sub-feature C.3). Lightweight pill for status,
// counts, type labels, etc.

import { HTMLAttributes } from 'react'
import styles from './Badge.module.css'

type Variant = 'neutral' | 'success' | 'warning' | 'danger' | 'info' | 'accent'

interface Props extends HTMLAttributes<HTMLSpanElement> {
  variant?: Variant
}

export default function Badge({
  variant = 'neutral', className, children, ...rest
}: Props) {
  return (
    <span
      className={`${styles.badge} ${styles[variant]} ${className ?? ''}`}
      {...rest}
    >
      {children}
    </span>
  )
}
