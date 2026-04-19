// Root-level error surface for the dashboard.
//
// Any page that catches an `ApiError` can call `useErrorToast()` and
// push a message; it renders as a dismissable banner in the top-right.
//
// We render at most 3 stacked toasts at a time; older ones age out
// automatically after 8 seconds so the screen doesn't fill with
// rate-limit warnings during bursty operations.

import React, { createContext, useCallback, useContext, useEffect, useState } from 'react'

type Level = 'error' | 'warn' | 'info'

interface Toast {
  id: number
  level: Level
  message: string
}

interface Ctx {
  push: (level: Level, message: string) => void
}

const ErrorToastContext = createContext<Ctx | null>(null)
let idCounter = 0

export function ErrorToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([])

  const push = useCallback((level: Level, message: string) => {
    const id = ++idCounter
    setToasts(prev => {
      const next = [...prev, { id, level, message }]
      return next.slice(-3)
    })
    setTimeout(() => {
      setToasts(prev => prev.filter(t => t.id !== id))
    }, 8000)
  }, [])

  return (
    <ErrorToastContext.Provider value={{ push }}>
      {children}
      <div
        style={{
          position: 'fixed',
          top: 16,
          right: 16,
          display: 'flex',
          flexDirection: 'column',
          gap: 8,
          zIndex: 9999,
          pointerEvents: 'none',
        }}
      >
        {toasts.map(t => (
          <div
            key={t.id}
            role="alert"
            style={{
              background: t.level === 'error' ? '#f85149' : t.level === 'warn' ? '#d29922' : '#388bfd',
              color: '#fff',
              padding: '10px 14px',
              borderRadius: 6,
              boxShadow: '0 4px 12px rgba(0,0,0,0.4)',
              minWidth: 240,
              maxWidth: 420,
              fontSize: 13,
              lineHeight: 1.4,
              pointerEvents: 'auto',
            }}
          >
            {t.message}
          </div>
        ))}
      </div>
    </ErrorToastContext.Provider>
  )
}

export function useErrorToast() {
  const ctx = useContext(ErrorToastContext)
  if (!ctx) throw new Error('useErrorToast must be used within <ErrorToastProvider>')
  return ctx
}

// Convenience wrapper: runs an async thunk and pushes any ApiError to
// the toast. Returns the resolved value or null on failure, so callers
// can do `const data = await runWithToast(toast, () => api.listBags())`.
import { ApiError } from './api'

export async function runWithToast<T>(
  toast: Ctx,
  fn: () => Promise<T>,
  opts?: { successMessage?: string; errorPrefix?: string },
): Promise<T | null> {
  try {
    const result = await fn()
    if (opts?.successMessage) toast.push('info', opts.successMessage)
    return result
  } catch (e) {
    if (e instanceof ApiError) {
      const prefix = opts?.errorPrefix ? `${opts.errorPrefix}: ` : ''
      toast.push('error', `${prefix}${e.message}`)
    } else {
      const prefix = opts?.errorPrefix ? `${opts.errorPrefix}: ` : ''
      toast.push('error', `${prefix}${String(e)}`)
    }
    return null
  }
}
