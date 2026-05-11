// Tests for ErrorBoundary — verifies it catches thrown errors from
// descendant components and renders the fallback instead of letting
// the exception propagate.
//
// React logs caught errors to console.error during these tests; we
// suppress that output to keep test logs clean.

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import ErrorBoundary from './ErrorBoundary'


function ThrowingChild({ message = 'kaboom' }: { message?: string }): JSX.Element {
  throw new Error(message)
}

function HealthyChild() {
  return <div>healthy content</div>
}


describe('ErrorBoundary', () => {
  beforeEach(() => {
    // React calls console.error twice when catching: once for the
    // error itself and once with a recovery hint. Suppress to keep
    // test output clean. We still assert the boundary caught it via
    // the rendered fallback.
    vi.spyOn(console, 'error').mockImplementation(() => {})
  })

  it('renders children when nothing throws', () => {
    render(
      <ErrorBoundary>
        <HealthyChild />
      </ErrorBoundary>,
    )
    expect(screen.getByText('healthy content')).toBeInTheDocument()
  })

  it('renders default fallback when child throws', () => {
    render(
      <ErrorBoundary>
        <ThrowingChild />
      </ErrorBoundary>,
    )
    expect(screen.getByRole('alert')).toBeInTheDocument()
    expect(screen.getByText(/Something broke/i)).toBeInTheDocument()
    expect(screen.getByText(/kaboom/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Try again/i })).toBeInTheDocument()
  })

  it('renders custom fallback when child throws', () => {
    render(
      <ErrorBoundary
        fallback={(error) => <div>custom: {error.message}</div>}
      >
        <ThrowingChild message="WebGL not available" />
      </ErrorBoundary>,
    )
    expect(screen.getByText('custom: WebGL not available')).toBeInTheDocument()
  })

  it('calls onError when an error is caught', () => {
    const onError = vi.fn()
    render(
      <ErrorBoundary onError={onError}>
        <ThrowingChild message="caught me" />
      </ErrorBoundary>,
    )
    expect(onError).toHaveBeenCalledOnce()
    expect(onError.mock.calls[0][0].message).toBe('caught me')
  })

  it('reset() restores normal rendering when the failing child is replaced', () => {
    // ToggleChild simulates a child that toggles between throwing and
    // healthy — the real-world analog is a Three.js component whose
    // failure was env-dependent (WebGL availability) and the user
    // wants to retry after fixing it.
    let shouldThrow = true
    function ToggleChild(): JSX.Element {
      if (shouldThrow) throw new Error('temporary')
      return <div>healthy after toggle</div>
    }

    const { rerender } = render(
      <ErrorBoundary>
        <ToggleChild />
      </ErrorBoundary>,
    )
    expect(screen.getByText(/Something broke/i)).toBeInTheDocument()

    // Flip the flag and click "Try again" to clear the error state,
    // then re-render so the boundary's child function runs again.
    shouldThrow = false
    fireEvent.click(screen.getByRole('button', { name: /Try again/i }))
    rerender(
      <ErrorBoundary>
        <ToggleChild />
      </ErrorBoundary>,
    )
    expect(screen.getByText('healthy after toggle')).toBeInTheDocument()
  })

  it('does not blank the surrounding tree when child throws', () => {
    // The whole point of this component — caller's other UI must
    // remain in the DOM
    render(
      <div>
        <div>sibling-before</div>
        <ErrorBoundary>
          <ThrowingChild />
        </ErrorBoundary>
        <div>sibling-after</div>
      </div>,
    )
    expect(screen.getByText('sibling-before')).toBeInTheDocument()
    expect(screen.getByText('sibling-after')).toBeInTheDocument()
    expect(screen.getByRole('alert')).toBeInTheDocument()
  })
})
