// Both export dialogs (classic components/ExportDialog and notebook/
// ExportDialog) against the real backend response shape. api.exportBag is
// stubbed at the module boundary; everything else (toasts, form state,
// accessibility wiring) is the real component.

import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from 'vitest'
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { api } from './api'
import { ErrorToastProvider } from './ErrorToast'
import ClassicExportDialog from './components/ExportDialog'
import NotebookExportDialog from './notebook/ExportDialog'

// POST /api/bags/{id}/export body, as resurrector/dashboard/api.py
// export_bag returns it.
const BACKEND_EXPORT_RESPONSE = { status: 'completed', output_path: '/data/exports/run_7' }

type Dialog = typeof ClassicExportDialog
const dialogs: Array<[string, Dialog]> = [
  ['classic', ClassicExportDialog],
  ['notebook', NotebookExportDialog],
]

function renderDialog(Dialog: Dialog) {
  return render(
    <ErrorToastProvider>
      <Dialog bagId={7} availableTopics={['/imu/data', '/joint_states']} onClose={() => {}} />
    </ErrorToastProvider>,
  )
}

function formatSelect(): HTMLSelectElement {
  const select = screen
    .getAllByRole('combobox')
    .find(s => s.querySelector('option[value="hdf5"]'))
  if (!select) throw new Error('format select not found')
  return select as HTMLSelectElement
}

function rateInput(): HTMLInputElement {
  return screen.getByRole('textbox', { name: /^(Downsample|Frame rate)/ }) as HTMLInputElement
}

function exportButton(): HTMLButtonElement {
  return screen.getByRole('button', { name: 'Export' }) as HTMLButtonElement
}

describe.each(dialogs)('%s export dialog', (_name, Dialog) => {
  let exportBag: MockInstance<typeof api.exportBag>

  beforeEach(() => {
    vi.spyOn(api, 'listExportPresets').mockResolvedValue([])
    vi.spyOn(api, 'getCapabilities').mockRejectedValue(new Error('not needed here'))
    exportBag = vi.spyOn(api, 'exportBag').mockResolvedValue(BACKEND_EXPORT_RESPONSE)
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
  })

  it('reports the output_path the backend returned, not "undefined"', async () => {
    renderDialog(Dialog)
    fireEvent.click(exportButton())

    expect(await screen.findByRole('status')).toHaveTextContent('Exported to /data/exports/run_7')
    const toasts = screen.getAllByRole('alert').map(a => a.textContent)
    expect(toasts).toContain('Exported to /data/exports/run_7')
    expect(toasts.join('\n')).not.toContain('undefined')
  })

  it('blocks a LeRobot export whose fps rounds below 1, then sends 1 for 0.5', async () => {
    renderDialog(Dialog)
    fireEvent.change(formatSelect(), { target: { value: 'lerobot' } })
    fireEvent.change(rateInput(), { target: { value: '0.4' } })

    expect(exportButton()).toBeDisabled()
    expect(rateInput()).toHaveAttribute('aria-invalid', 'true')
    expect(rateInput()).toHaveAccessibleDescription('LeRobot needs at least 1 fps.')
    expect(screen.queryByText(/Rounded to 0 fps/)).toBeNull()
    fireEvent.click(exportButton())
    expect(exportBag).not.toHaveBeenCalled()

    fireEvent.change(rateInput(), { target: { value: '0.5' } })
    expect(exportButton()).toBeEnabled()
    expect(rateInput()).not.toHaveAttribute('aria-invalid')
    fireEvent.click(exportButton())
    await waitFor(() => expect(exportBag).toHaveBeenCalledTimes(1))
    expect(exportBag.mock.calls[0][1].downsample_hz).toBe(1)
  })

  it('keeps the rounding hint out of the fps field accessible name', () => {
    renderDialog(Dialog)
    fireEvent.change(formatSelect(), { target: { value: 'lerobot' } })
    fireEvent.change(rateInput(), { target: { value: '14.6' } })

    expect(rateInput()).toHaveAccessibleName(/^Frame rate \(fps(, default 30)?\)$/)
    expect(rateInput()).toHaveAccessibleDescription('Rounded to 15 fps.')
  })
})
