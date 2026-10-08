// A failed export (the failed-columns 422) on the surfaces outside the two
// export dialogs: the Explorer's trim popover and both Datasets pages, plus
// the toast they share. The API is stubbed at the module boundary; the
// components and the toast provider are real.

import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from 'vitest'
import { cleanup, fireEvent, render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { api, ApiError, type Dataset } from './api'
import { ErrorToastProvider, runWithToast, useErrorToast } from './ErrorToast'
import TrimExportPopover from './components/TrimExportPopover'
import ClassicDatasets from './pages/Datasets'
import NotebookDatasetsPage from './notebook/pages/DatasetsPage'
import { EXPORT_COLUMN_FAILURES_BODY, EXPORT_COLUMN_FAILURES_MESSAGE } from './exportPresetFixtures'

const failure = (url: string) => new ApiError(422, EXPORT_COLUMN_FAILURES_BODY, `POST ${url} failed (422)`)

let scrollIntoView: Mock<(arg?: boolean | ScrollIntoViewOptions) => void>

beforeEach(() => {
  scrollIntoView = vi.fn<(arg?: boolean | ScrollIntoViewOptions) => void>()
  Element.prototype.scrollIntoView = scrollIntoView  // jsdom has none
})

afterEach(() => {
  cleanup()
  vi.restoreAllMocks()
  delete (Element.prototype as Partial<Element>).scrollIntoView
})

// The error is on screen once, announced once, with its line breaks; the
// toast that goes with it is silent.
function expectShownOnce(inline: HTMLElement, text: string, toastText: string) {
  expect(inline.textContent).toBe(text)
  expect(inline.style.whiteSpace).toBe('pre-wrap')
  const alerts = screen.getAllByRole('alert').map(a => a.textContent)
  expect(alerts.filter(t => t === text || t === toastText)).toEqual([text])
  expect(screen.getAllByTestId('toast').map(t => t.textContent)).toContain(toastText)
  expect(document.body.textContent).not.toContain('[object Object]')
  expect(scrollIntoView.mock.contexts).toContain(inline)
}

describe('trim popover', () => {
  const TRIM_RESPONSE = {
    bag_id: 7, format: 'csv', start_sec: 0, end_sec: 1, output: '/data/exports/trim_1',
  }

  function popover(): HTMLElement {
    return screen.getByRole('heading', { name: 'Trim & export' }).parentElement!
  }

  it('keeps a failed-columns error in the popover until an export works', async () => {
    // Would catch: the trim 422 reaching the user only as an 8-second toast
    // with its lines run together (v0.8.5's popover had no inline error),
    // the last success's "✓ path" line left next to the error, and the
    // error outliving a retry that works.
    const trim = vi.spyOn(api, 'trimRange')
      .mockResolvedValueOnce(TRIM_RESPONSE)
      .mockRejectedValueOnce(failure('/api/bags/7/trim'))
      .mockResolvedValueOnce(TRIM_RESPONSE)
    render(
      <ErrorToastProvider>
        <TrimExportPopover
          bagId={7} startSec={0} endSec={1} availableTopics={['/joint_states']} onClose={() => {}}
        />
      </ErrorToastProvider>,
    )
    const exportButton = () => within(popover()).getByRole('button', { name: 'Export' })

    fireEvent.click(exportButton())
    expect(await within(popover()).findByText('✓ /data/exports/trim_1')).toBeInTheDocument()

    fireEvent.click(exportButton())
    const inline = await within(popover()).findByRole('alert')
    expectShownOnce(
      inline,
      `Export failed: ${EXPORT_COLUMN_FAILURES_MESSAGE}`,
      `Trim export: ${EXPORT_COLUMN_FAILURES_MESSAGE}`,
    )
    expect(within(popover()).queryByText('✓ /data/exports/trim_1')).toBeNull()

    fireEvent.click(exportButton())
    expect(await within(popover()).findByText('✓ /data/exports/trim_1')).toBeInTheDocument()
    expect(within(popover()).queryByRole('alert')).toBeNull()
    expect(trim).toHaveBeenCalledTimes(3)
  })
})

const DATASET: Dataset = {
  id: 1, name: 'pick-place', description: '', created_at: '', updated_at: '',
  versions: [{ version: '1.0', created_at: '2026-10-08', export_format: 'csv' }],
}

describe.each([
  ['classic', ClassicDatasets],
  ['notebook', NotebookDatasetsPage],
])('%s Datasets page', (_name, Page) => {
  it('keeps a failed-columns error on the page until an export works', async () => {
    // Would catch: the dataset-version export 422 reaching the user only as
    // an 8-second toast with its lines run together, and the error outliving
    // a retry that works.
    vi.spyOn(api, 'listDatasets').mockResolvedValue({ datasets: [DATASET] })
    const exportVersion = vi.spyOn(api, 'exportDatasetVersion')
      .mockRejectedValueOnce(failure('/api/datasets/pick-place/versions/1.0/export'))
      .mockResolvedValueOnce({ output: '/data/datasets/pick-place/1.0' })
    render(
      <MemoryRouter>
        <ErrorToastProvider>
          <Page />
        </ErrorToastProvider>
      </MemoryRouter>,
    )
    fireEvent.click(await screen.findByText('pick-place'))
    fireEvent.click(screen.getByRole('button', { name: 'Export' }))

    const inline = await screen.findByRole('alert')
    expectShownOnce(
      inline,
      `Export of pick-place@1.0 failed: ${EXPORT_COLUMN_FAILURES_MESSAGE}`,
      `Export: ${EXPORT_COLUMN_FAILURES_MESSAGE}`,
    )

    fireEvent.click(screen.getByRole('button', { name: 'Export' }))
    expect(await screen.findByText('Exported to /data/datasets/pick-place/1.0')).toBeInTheDocument()
    expect(screen.queryByText(/^Export of pick-place@1.0 failed/)).toBeNull()
    expect(exportVersion).toHaveBeenCalledTimes(2)
  })
})

describe('error toast', () => {
  function Pusher({ onError }: { onError?: (m: string) => void }) {
    const toast = useErrorToast()
    return (
      <button onClick={() => runWithToast(toast, () => Promise.reject(failure('/x')), {
        errorPrefix: 'Export', onError,
      })}>go</button>
    )
  }

  it('keeps a multi-line message on separate lines and announces it', async () => {
    // Would catch: the toast running a failed-columns message's lines
    // together (v0.8.5's toast had no white-space rule).
    render(<ErrorToastProvider><Pusher /></ErrorToastProvider>)
    fireEvent.click(screen.getByRole('button', { name: 'go' }))
    const toast = await screen.findByRole('alert')
    expect(toast).toHaveAttribute('data-testid', 'toast')
    expect(toast.textContent).toBe(`Export: ${EXPORT_COLUMN_FAILURES_MESSAGE}`)
    expect(toast.style.whiteSpace).toBe('pre-wrap')
  })

  it('is silent when the caller shows the error itself', async () => {
    // Would catch: a screen reader reading the whole message twice, once
    // from the toast and once from the caller's inline error.
    const onError = vi.fn()
    render(<ErrorToastProvider><Pusher onError={onError} /></ErrorToastProvider>)
    fireEvent.click(screen.getByRole('button', { name: 'go' }))
    const toast = await screen.findByTestId('toast')
    expect(toast.textContent).toBe(`Export: ${EXPORT_COLUMN_FAILURES_MESSAGE}`)
    expect(toast).not.toHaveAttribute('role')
    expect(onError).toHaveBeenCalledWith(EXPORT_COLUMN_FAILURES_MESSAGE)
  })
})
