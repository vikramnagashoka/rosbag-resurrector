// <Card> + <CardHeader> + <CardBody> + <CardFooter> — v0.6.0
// (Sub-feature C.3). Uses CSS module styling from Card.module.css.

import { HTMLAttributes } from 'react'
import styles from './Card.module.css'

interface CardProps extends HTMLAttributes<HTMLDivElement> {
  hoverable?: boolean
}

export function Card({ hoverable, className, children, ...rest }: CardProps) {
  const cls = [
    styles.card,
    hoverable ? styles.cardHoverable : '',
    className ?? '',
  ].filter(Boolean).join(' ')
  return <div className={cls} {...rest}>{children}</div>
}

export function CardHeader({ className, children, ...rest }: HTMLAttributes<HTMLDivElement>) {
  return <div className={`${styles.header} ${className ?? ''}`} {...rest}>{children}</div>
}

export function CardBody({ className, children, ...rest }: HTMLAttributes<HTMLDivElement>) {
  return <div className={`${styles.body} ${className ?? ''}`} {...rest}>{children}</div>
}

export function CardFooter({ className, children, ...rest }: HTMLAttributes<HTMLDivElement>) {
  return <div className={`${styles.footer} ${className ?? ''}`} {...rest}>{children}</div>
}

export default Card
