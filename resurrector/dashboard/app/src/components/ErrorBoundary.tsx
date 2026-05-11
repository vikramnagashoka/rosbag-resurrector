// ErrorBoundary — catches JS exceptions from descendant components and
// renders a fallback instead of unmounting the entire React tree.
//
// Motivation: Three.js (Bundle A.1 SceneViewer) throws an uncaught
// exception when WebGL context creation fails. Without this boundary,
// the exception bubbled up to React's root and blanked the entire
// Explorer page — users with WebGL disabled (privacy extensions,
// corporate-locked browsers, GPU driver issues) lost the whole app.
// Now they see a contained "Scene viewer unavailable" message and
// the rest of Explorer still works.
//
// Class component (not a hook) because React's error catching is
// only available via componentDidCatch / getDerivedStateFromError —
// no hook equivalent.

import { Component, ErrorInfo, ReactNode } from 'react'

interface Props {
  children: ReactNode
  fallback?: (error: Error, reset: () => void) => ReactNode
  /** Optional callback invoked when an error is caught — useful for
   *  client-side logging or analytics. */
  onError?: (error: Error, info: ErrorInfo) => void
}

interface State {
  error: Error | null
}

export default class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null }

  static getDerivedStateFromError(error: Error): State {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // Always log to console so devs see the stack in DevTools.
    console.error('ErrorBoundary caught:', error, info)
    this.props.onError?.(error, info)
  }

  reset = () => {
    this.setState({ error: null })
  }

  render() {
    if (this.state.error) {
      if (this.props.fallback) {
        return this.props.fallback(this.state.error, this.reset)
      }
      // Default fallback — generic, non-cute, with the actual error
      // message so devs can diagnose without DevTools open.
      return (
        <div
          style={{
            padding: 'var(--space-5)',
            background: 'var(--color-bg-card)',
            border: '1px solid var(--color-border)',
            borderRadius: 'var(--radius-lg)',
            color: 'var(--color-text)',
            fontFamily: 'var(--font-sans)',
          }}
          role="alert"
        >
          <div style={{ fontSize: 'var(--text-lg)', fontWeight: 600, marginBottom: 'var(--space-2)' }}>
            Something broke in this view
          </div>
          <div style={{ color: 'var(--color-text-secondary)', fontSize: 'var(--text-base)', marginBottom: 'var(--space-3)' }}>
            {this.state.error.message || String(this.state.error)}
          </div>
          <button
            onClick={this.reset}
            style={{
              padding: 'var(--space-2) var(--space-3)',
              background: 'var(--color-bg-elevated)',
              color: 'var(--color-text)',
              border: '1px solid var(--color-border)',
              borderRadius: 'var(--radius-md)',
              cursor: 'pointer',
              fontSize: 'var(--text-base)',
            }}
          >
            Try again
          </button>
        </div>
      )
    }
    return this.props.children
  }
}
