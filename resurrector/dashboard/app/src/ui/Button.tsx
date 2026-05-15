// <Button> — v0.6.0 (Sub-feature C.3)
//
// Reusable button with token-based styling. Variant + size are
// taste-driven; the underlying element is always <button>.

import { forwardRef, ButtonHTMLAttributes } from 'react'
import styles from './Button.module.css'

type Variant = 'primary' | 'secondary' | 'ghost' | 'danger'
type Size = 'sm' | 'md' | 'lg'

interface Props extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant
  size?: Size
}

const Button = forwardRef<HTMLButtonElement, Props>(
  ({ variant = 'secondary', size = 'md', className, children, ...rest }, ref) => {
    const cls = [
      styles.button,
      styles[variant],
      styles[size],
      className ?? '',
    ].filter(Boolean).join(' ')
    return (
      <button ref={ref} className={cls} {...rest}>
        {children}
      </button>
    )
  },
)

Button.displayName = 'Button'
export default Button
