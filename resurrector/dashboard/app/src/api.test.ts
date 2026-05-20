import { describe, it, expect } from 'vitest'
import { ApiError } from './api'

// ApiError used to render "[object Object]" whenever the server returned
// a nested-detail body, e.g. our structured 503/409 responses or
// FastAPI's 422 array-detail validation errors. This test pins the
// extraction behaviour.

describe('ApiError message extraction', () => {
  it('extracts a plain string detail (FastAPI default 4xx shape)', () => {
    const e = new ApiError(404, { detail: 'Bag not found' }, 'fallback')
    expect(e.message).toBe('Bag not found')
  })

  it('extracts nested detail.message (our structured 503 shape)', () => {
    const e = new ApiError(503, {
      detail: {
        kind: 'capability_unavailable',
        capability: 'bridge_live',
        message: 'Live mode requires rclpy.',
        install_command: 'Install ROS 2',
      },
    }, 'fallback')
    expect(e.message).toBe('Live mode requires rclpy.')
  })

  it('extracts top-level error (bridge subprocess shape)', () => {
    const e = new ApiError(400, { error: 'Not in playback mode' }, 'fallback')
    expect(e.message).toBe('Not in playback mode')
  })

  it('JSON-stringifies array details instead of producing [object Object]', () => {
    // This is the 422 validation-error shape that triggered the
    // original "play: [object Object]" toast bug.
    const e = new ApiError(422, {
      detail: [
        { type: 'missing', loc: ['query', 'request'], msg: 'Field required' },
      ],
    }, 'fallback')
    expect(e.message).not.toBe('[object Object]')
    expect(e.message).toContain('missing')
  })

  it('returns the raw string when detail is itself a string', () => {
    const e = new ApiError(500, 'Internal Server Error', 'fallback')
    expect(e.message).toBe('Internal Server Error')
  })

  it('falls back to the provided fallback for unknown shapes', () => {
    const e = new ApiError(500, null, 'POST /x failed (500)')
    expect(e.message).toBe('POST /x failed (500)')
  })
})
