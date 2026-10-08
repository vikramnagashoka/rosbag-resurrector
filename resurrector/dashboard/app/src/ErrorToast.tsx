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
  announce: boolean
}

interface PushOptions {
  // false: the caller shows the same message on the page with its own
  // role="alert", so a screen reader would read it twice.
  announce?: boolean
}

interface Ctx {
  push: (level: Level, message: string, opts?: PushOptions) => void
}

const ErrorToastContext = createContext<Ctx | null>(null)
let idCounter = 0

export function ErrorToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([])

  const push = useCallback((level: Level, message: string, opts?: PushOptions) => {
    const id = ++idCounter
    const announce = opts?.announce ?? true
    setToasts(prev => {
      const next = [...prev, { id, level, message, announce }]
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
            role={t.announce ? 'alert' : undefined}
            data-testid="toast"
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
              // Keep a multi-line message's line breaks; wrap long paths.
              whiteSpace: 'pre-wrap',
              overflowWrap: 'anywhere',
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
// `onError` gets the same message (without the prefix) for callers that
// also keep it on screen, since a toast is gone after 8 seconds. Those
// callers announce it themselves, so the toast is then silent.
import { ApiError } from './api'

export async function runWithToast<T>(
  toast: Ctx,
  fn: () => Promise<T>,
  opts?: { successMessage?: string; errorPrefix?: string; onError?: (message: string) => void },
): Promise<T | null> {
  try {
    const result = await fn()
    if (opts?.successMessage) toast.push('info', opts.successMessage)
    return result
  } catch (e) {
    const message = e instanceof ApiError ? e.message : String(e)
    const prefix = opts?.errorPrefix ? `${opts.errorPrefix}: ` : ''
    toast.push('error', `${prefix}${message}`, { announce: !opts?.onError })
    opts?.onError?.(message)
    return null
  }
}
