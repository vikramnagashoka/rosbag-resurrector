// A failed export's error, kept next to the control that started it.
//
// Export, trim and dataset-version export answer a failed-columns 422
// with one line per column plus what to do: more than an 8-second toast
// can show. Callers pass the same message to runWithToast's `onError`,
// which makes the toast silent, so this block is the copy screen readers
// announce (once) and the one that stays.

import React, { useEffect, useRef } from 'react'

// Red-on-dark block for the classic (dark) pages. The notebook pages use
// the .nb-export-error class instead.
export const CLASSIC_EXPORT_FAILURE_STYLE: React.CSSProperties = {
  background: 'rgba(248,81,73,0.1)',
  border: '1px solid #f85149',
  borderRadius: 6,
  padding: '8px 12px',
  color: '#ffa198',
  fontSize: 13,
  marginBottom: 16,
}

export default function ExportFailure({ message, className, style }: {
  message: string
  className?: string
  style?: React.CSSProperties
}) {
  const ref = useRef<HTMLDivElement>(null)
  // It renders under the form, often below a scrolling dialog's fold.
  useEffect(() => {
    ref.current?.scrollIntoView?.({ block: 'nearest' })
  }, [message])
  return (
    <div
      ref={ref}
      role="alert"
      className={className}
      style={{
        // One failed column per line; long paths wrap instead of overflowing.
        whiteSpace: 'pre-wrap',
        overflowWrap: 'anywhere',
        // Scrolling it into view also shows the dialog's buttons under it.
        scrollMarginBottom: 64,
        ...style,
      }}
    >
      {message}
    </div>
  )
}
