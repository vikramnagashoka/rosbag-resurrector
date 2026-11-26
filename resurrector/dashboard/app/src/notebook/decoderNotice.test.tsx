// DecoderNotice (topics with no decoder) and the topic lists it keeps them
// out of. api.loadDecoders is stubbed at the module boundary; the rest is
// the real component.

import { afterEach, describe, expect, it, vi } from 'vitest'
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { api, ApiError } from '../api'
import DecoderNotice from './DecoderNotice'
import { plottableTopics } from './types'

// GET /api/bags/{id}/decoders `undecodable`, as resurrector/dashboard/api.py
// bag_decoders returns it.
const GIZMO = {
  topic: '/gizmo',
  message_type: 'acme_msgs/msg/Gizmo',
  reason: 'the bag has only a ros2idl definition of it',
}

function renderNotice(over: Partial<React.ComponentProps<typeof DecoderNotice>> = {}) {
  const props = {
    topics: [GIZMO],
    healthOnly: false,
    success: null,
    onHealthOnly: vi.fn(),
    onReopen: vi.fn(),
    onDismissSuccess: vi.fn(),
    onLoaded: vi.fn(async () => {}),
    ...over,
  }
  render(<DecoderNotice {...props} />)
  return props
}

afterEach(() => {
  cleanup()
  vi.restoreAllMocks()
})

describe('DecoderNotice', () => {
  it('names each topic, its type and why', () => {
    renderNotice()
    expect(screen.getByText("1 topic can't be decoded")).toBeTruthy()
    expect(screen.getByText('/gizmo')).toBeTruthy()
    expect(screen.getByText('acme_msgs/msg/Gizmo')).toBeTruthy()
    expect(screen.getByText(GIZMO.reason)).toBeTruthy()
  })

  it('Health only hands the choice to the parent', () => {
    const p = renderNotice()
    fireEvent.click(screen.getByRole('button', { name: 'Health only' }))
    expect(p.onHealthOnly).toHaveBeenCalledOnce()
  })

  it('folds to one line after Health only and reopens', () => {
    const p = renderNotice({ healthOnly: true })
    expect(screen.getByText(/health only, no decoder/)).toBeTruthy()
    expect(screen.queryByLabelText('Folder of .msg files')).toBeNull()
    fireEvent.click(screen.getByRole('button', { name: 'Load definitions…' }))
    expect(p.onReopen).toHaveBeenCalledOnce()
  })

  it('asks for a path before calling the server', async () => {
    const load = vi.spyOn(api, 'loadDecoders')
    renderNotice()
    fireEvent.click(screen.getByRole('button', { name: 'Load definitions' }))
    expect(await screen.findByText(/Enter the path of a folder of .msg files/)).toBeTruthy()
    expect(load).not.toHaveBeenCalled()
  })

  it('loads the folder and reports it to the parent', async () => {
    const load = vi.spyOn(api, 'loadDecoders').mockResolvedValue({
      path: '/home/me/ws/src', message_types: ['acme_msgs/msg/Gizmo'], loaded_paths: ['/home/me/ws/src'],
    })
    const p = renderNotice()
    fireEvent.change(screen.getByLabelText('Folder of .msg files'), { target: { value: ' ~/ws/src ' } })
    fireEvent.click(screen.getByRole('button', { name: 'Load definitions' }))
    await waitFor(() => expect(p.onLoaded).toHaveBeenCalledWith('/home/me/ws/src', ['acme_msgs/msg/Gizmo']))
    expect(load).toHaveBeenCalledWith('~/ws/src')
  })

  it("shows the server's reason when the folder can't be loaded", async () => {
    vi.spyOn(api, 'loadDecoders').mockRejectedValue(
      new ApiError(400, { detail: 'No .msg files under /tmp/x (looked for <package>/msg/<Type>.msg)' }, 'x'),
    )
    const p = renderNotice()
    fireEvent.change(screen.getByLabelText('Folder of .msg files'), { target: { value: '/tmp/x' } })
    fireEvent.click(screen.getByRole('button', { name: 'Load definitions' }))
    expect(await screen.findByText(/No .msg files under \/tmp\/x/)).toBeTruthy()
    expect(p.onLoaded).not.toHaveBeenCalled()
    expect(screen.getByLabelText('Folder of .msg files').getAttribute('aria-invalid')).toBe('true')
  })

  it('renders the success line once nothing is left, and nothing without one', () => {
    renderNotice({ topics: [], success: 'Loaded 1 message type from /ws. /gizmo decodes now.' })
    expect(screen.getByText(/gizmo decodes now/)).toBeTruthy()
    cleanup()
    const { container } = render(
      <DecoderNotice topics={[]} healthOnly={false} success={null} onHealthOnly={() => {}}
        onReopen={() => {}} onDismissSuccess={() => {}} onLoaded={async () => {}} />,
    )
    expect(container.innerHTML).toBe('')
  })
})

describe('plottableTopics', () => {
  it('leaves out undecodable and image topics', () => {
    const nb = {
      bagTopics: [
        { name: '/gizmo', messageType: 'acme_msgs/msg/Gizmo', messageCount: 5 },
        { name: '/widget', messageType: 'acme_msgs/msg/Widget', messageCount: 5 },
        { name: '/cam', messageType: 'sensor_msgs/msg/Image', messageCount: 5 },
      ],
    }
    expect(plottableTopics(nb)).toEqual(['/gizmo', '/widget'])
    expect(plottableTopics({ ...nb, undecodableTopics: ['/gizmo'] })).toEqual(['/widget'])
  })
})
