// Vitest setup — extends expect with @testing-library/jest-dom matchers
// (toBeInTheDocument, toHaveAttribute, toHaveTextContent, etc.).
import '@testing-library/jest-dom'

// jsdom doesn't ship ResizeObserver. react-window v2 uses it for
// auto-sizing. Provide a no-op polyfill so virtualization tests
// don't crash; the layout doesn't need to actually measure under jsdom.
if (typeof globalThis.ResizeObserver === 'undefined') {
  globalThis.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver
}
