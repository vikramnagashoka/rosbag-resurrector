// A failed export (the failed-columns 422) on the surfaces outside the two
// export dialogs: the Explorer's trim popover and both Datasets pages, plus
// the toast they share. The API is stubbed at the module boundary; the
// components and the toast provider are real.

import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from 'vitest'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
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

// The error is on screen once, with its line breaks, and announced once:
// by its toast, since the on-screen copy has no live role.
function expectShownOnce(inline: HTMLElement, text: string, toastText: string) {
  expect(inline.textContent).toBe(text)
  expect(inline.style.whiteSpace).toBe('pre-wrap')
  expect(inline).not.toHaveAttribute('role')
  expect(inline).not.toHaveAttribute('aria-live')
  const announced = screen.getAllByRole('alert').filter(a => a.textContent === text || a.textContent === toastText)
  expect(announced).toHaveLength(1)
  expect(announced[0]).toHaveAttribute('data-testid', 'toast')
  expect(announced[0].textContent).toBe(toastText)
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

  function renderPopover() {
    return render(
      <ErrorToastProvider>
        <TrimExportPopover
          bagId={7} startSec={0} endSec={1} availableTopics={['/joint_states']} onClose={() => {}}
        />
      </ErrorToastProvider>,
    )
  }

  const exportButton = () => within(popover()).getByRole('button', { name: 'Export' })

  it('keeps a failed-columns error in the popover until an export works', async () => {
    // Would catch: the trim 422 reaching the user only as an 8-second toast
    // with its lines run together (v0.8.5's popover had no inline error),
    // the last success's "✓ path" line left next to the error, and the
    // error outliving a retry that works.
    const trim = vi.spyOn(api, 'trimRange')
      .mockResolvedValueOnce(TRIM_RESPONSE)
      .mockRejectedValueOnce(failure('/api/bags/7/trim'))
      .mockResolvedValueOnce(TRIM_RESPONSE)
    renderPopover()

    fireEvent.click(exportButton())
    expect(await within(popover()).findByText('✓ /data/exports/trim_1')).toBeInTheDocument()

    fireEvent.click(exportButton())
    const inline = await within(popover()).findByTestId('export-failure')
    expectShownOnce(
      inline,
      `Export failed: ${EXPORT_COLUMN_FAILURES_MESSAGE}`,
      `Trim export: ${EXPORT_COLUMN_FAILURES_MESSAGE}`,
    )
    expect(within(popover()).queryByText('✓ /data/exports/trim_1')).toBeNull()

    fireEvent.click(exportButton())
    expect(await within(popover()).findByText('✓ /data/exports/trim_1')).toBeInTheDocument()
    expect(within(popover()).queryByTestId('export-failure')).toBeNull()
    expect(trim).toHaveBeenCalledTimes(3)
  })

  it('announces a failure once when the popover was closed before it', async () => {
    // Would catch: a trim that fails after its popover was closed (an
    // overlay click closes it mid-export) never being announced, because
    // the toast was silent on the assumption the popover shows the error.
    let fail!: () => void
    const trim = vi.spyOn(api, 'trimRange').mockImplementationOnce(
      () => new Promise((_resolve, reject) => { fail = () => reject(failure('/api/bags/7/trim')) }),
    )
    const { rerender } = renderPopover()
    fireEvent.click(exportButton())
    await waitFor(() => expect(trim).toHaveBeenCalledTimes(1))
    rerender(<ErrorToastProvider>{null}</ErrorToastProvider>)
    expect(screen.queryByRole('heading', { name: 'Trim & export' })).toBeNull()

    fail()
    const toast = await screen.findByRole('alert')
    expect(toast).toHaveAttribute('data-testid', 'toast')
    expect(toast.textContent).toBe(`Trim export: ${EXPORT_COLUMN_FAILURES_MESSAGE}`)
    expect(screen.getAllByRole('alert')).toHaveLength(1)
  })
})

const DATASET: Dataset = {
  id: 1, name: 'pick-place', description: '', created_at: '', updated_at: '',
  versions: [
    { version: '1.0', created_at: '2026-10-08', export_format: 'csv' },
    { version: '2.0', created_at: '2026-10-08', export_format: 'csv' },
  ],
}
const OTHER: Dataset = {
  id: 2, name: 'stack-cups', description: '', created_at: '', updated_at: '',
  versions: [{ version: '1.0', created_at: '2026-10-08', export_format: 'csv' }],
}
const ERROR_TEXT = `Export of pick-place@1.0 failed: ${EXPORT_COLUMN_FAILURES_MESSAGE}`

describe.each([
  ['classic', ClassicDatasets],
  ['notebook', NotebookDatasetsPage],
])('%s Datasets page', (_name, Page) => {
  function renderPage() {
    return render(
      <MemoryRouter>
        <ErrorToastProvider>
          <Page />
        </ErrorToastProvider>
      </MemoryRouter>,
    )
  }

  function versionRow(version: string): HTMLElement {
    return screen.getAllByRole('row').find(r => within(r).queryByText(version, { exact: true }))!
  }

  // Selects pick-place, has its 1.0 export fail, and returns the error block.
  async function failExport(): Promise<HTMLElement> {
    vi.spyOn(api, 'listDatasets').mockResolvedValue({ datasets: [DATASET, OTHER] })
    vi.spyOn(api, 'exportDatasetVersion')
      .mockRejectedValueOnce(failure('/api/datasets/pick-place/versions/1.0/export'))
      .mockResolvedValueOnce({ output: '/data/datasets/pick-place/1.0' })
    renderPage()
    fireEvent.click(await screen.findByText('pick-place'))
    fireEvent.click(within(versionRow('1.0')).getByRole('button', { name: 'Export' }))
    return screen.findByTestId('export-failure')
  }

  it('keeps a failed-columns error on the page until an export works', async () => {
    // Would catch: the dataset-version export 422 reaching the user only as
    // an 8-second toast with its lines run together, a toast that doesn't
    // say which dataset and version failed, and the error outliving a retry
    // that works.
    const inline = await failExport()
    expectShownOnce(inline, ERROR_TEXT, ERROR_TEXT)

    fireEvent.click(within(versionRow('1.0')).getByRole('button', { name: 'Export' }))
    expect(await screen.findByText('Exported to /data/datasets/pick-place/1.0')).toBeInTheDocument()
    expect(screen.queryByTestId('export-failure')).toBeNull()
    expect(api.exportDatasetVersion).toHaveBeenCalledTimes(2)
  })

  it('keeps the error on screen when another dataset is selected', async () => {
    // Would catch: the error vanishing (and, with a silent toast, never
    // being announced) because the page only showed it under the dataset
    // that failed, while the user had moved on to another one.
    await failExport()
    fireEvent.click(screen.getByText('stack-cups'))
    expect(await screen.findByRole('heading', { name: 'stack-cups' })).toBeInTheDocument()
    expect(screen.getByTestId('export-failure').textContent).toBe(ERROR_TEXT)
  })

  it('drops the error when its version is deleted, not another one', async () => {
    // Would catch: "Export of pick-place@1.0 failed" staying on screen after
    // the user deleted 1.0 (the hint's next step is a new Parquet version,
    // then deleting the failed one), or a delete of 2.0 clearing it.
    vi.spyOn(window, 'confirm').mockReturnValue(true)
    const deleteVersion = vi.spyOn(api, 'deleteDatasetVersion').mockImplementation(
      async (name, version) => ({ deleted: { name, version } }),
    )
    vi.spyOn(api, 'getDataset').mockResolvedValue(DATASET)
    await failExport()

    fireEvent.click(within(versionRow('2.0')).getByRole('button', { name: 'Delete' }))
    expect(await screen.findByText('Deleted pick-place@2.0')).toBeInTheDocument()
    expect(screen.getByTestId('export-failure').textContent).toBe(ERROR_TEXT)

    fireEvent.click(within(versionRow('1.0')).getByRole('button', { name: 'Delete' }))
    expect(await screen.findByText('Deleted pick-place@1.0')).toBeInTheDocument()
    expect(screen.queryByTestId('export-failure')).toBeNull()
    expect(deleteVersion).toHaveBeenCalledTimes(2)
  })

  it('drops the error when its dataset is deleted, not another one', async () => {
    // Would catch: the failed export of a dataset the user just deleted
    // staying on the page, or deleting an unrelated dataset clearing it.
    vi.spyOn(window, 'confirm').mockReturnValue(true)
    const deleteDataset = vi.spyOn(api, 'deleteDataset').mockImplementation(
      async name => ({ deleted: name }),
    )
    await failExport()
    // The ✕ next to the dataset's name in the list.
    const deleteButton = (name: string) =>
      screen.getAllByText('✕').find(x => x.parentElement!.textContent === `${name}✕`)!

    fireEvent.click(deleteButton('stack-cups'))
    expect(await screen.findByText('Deleted "stack-cups"')).toBeInTheDocument()
    expect(screen.getByTestId('export-failure').textContent).toBe(ERROR_TEXT)

    fireEvent.click(deleteButton('pick-place'))
    expect(await screen.findByText('Deleted "pick-place"')).toBeInTheDocument()
    expect(screen.queryByTestId('export-failure')).toBeNull()
    expect(deleteDataset).toHaveBeenCalledTimes(2)
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

  it('still announces when the caller keeps its own copy', async () => {
    // Would catch: the toast going silent whenever the caller passes
    // onError. The caller's copy may never render (it unmounted, or shows
    // something else), so the toast is the one copy that's announced.
    const onError = vi.fn()
    render(<ErrorToastProvider><Pusher onError={onError} /></ErrorToastProvider>)
    fireEvent.click(screen.getByRole('button', { name: 'go' }))
    const toast = await screen.findByRole('alert')
    expect(toast).toHaveAttribute('data-testid', 'toast')
    expect(toast.textContent).toBe(`Export: ${EXPORT_COLUMN_FAILURES_MESSAGE}`)
    expect(onError).toHaveBeenCalledWith(EXPORT_COLUMN_FAILURES_MESSAGE)
  })
})
