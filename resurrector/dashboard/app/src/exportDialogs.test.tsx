// Both export dialogs (classic components/ExportDialog and notebook/
// ExportDialog) against the real backend response shape. api.exportBag is
// stubbed at the module boundary; everything else (toasts, form state,
// accessibility wiring) is the real component.

import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from 'vitest'
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { api, type CapabilityMap } from './api'
import { ErrorToastProvider } from './ErrorToast'
import ClassicExportDialog from './components/ExportDialog'
import NotebookExportDialog from './notebook/ExportDialog'
import {
  ALL_EXPORTS_CMD,
  ALL_EXPORTS_NO_WHEEL_DESCRIPTION,
  RLDS_NOT_INSTALLED_REASON,
  RLDS_NO_WHEEL_REASON,
  capabilitiesFor,
  exportPresetsFor,
  type ExportEnv,
} from './exportPresetFixtures'

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

  it('gives the fps field its normal border back after an error clears', () => {
    // Would catch: the classic dialog adding `borderColor` on top of the
    // `border` shorthand for the error state. Clearing the error makes React
    // remove `borderColor`, which also wipes the colour out of `border`, so
    // the field kept a browser-default border until it remounted.
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    renderDialog(Dialog)
    fireEvent.change(formatSelect(), { target: { value: 'lerobot' } })
    const normal = rateInput().style.borderTopColor

    fireEvent.change(rateInput(), { target: { value: '0.4' } })
    fireEvent.change(rateInput(), { target: { value: '14.6' } })

    expect(rateInput()).not.toHaveAttribute('aria-invalid')
    expect(rateInput().style.borderTopColor).toBe(normal)
    const styleWarnings = consoleError.mock.calls.filter(args =>
      String(args[0]).includes('a style property during rerender'))
    expect(styleWarnings).toEqual([])
  })
})

// /api/export-presets reports `unavailable_reason` per preset. Where the
// [all-exports] pip command can't install tensorflow (Python 3.14, Intel
// macOS on 3.13, Windows ARM64), the dialogs must say why rlds is off
// instead of offering that command as the fix.
describe.each(dialogs)('%s export dialog preset availability', (_name, Dialog) => {
  function mockEnv(env: ExportEnv) {
    vi.spyOn(api, 'listExportPresets').mockResolvedValue(exportPresetsFor(env))
    vi.spyOn(api, 'getCapabilities').mockResolvedValue(capabilitiesFor(env) as CapabilityMap)
  }

  async function presetOption(name: string): Promise<HTMLOptionElement> {
    const option = await screen.findByRole('option', { name: new RegExp(`^${name}\\b`) })
    return option as HTMLOptionElement
  }

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
  })

  it('explains an rlds preset pip cannot fix and offers no install command', async () => {
    // Would catch: "1 preset(s) unavailable — Zarr / RLDS extras not
    // installed." plus a copy block for the [all-exports] command, and an
    // "rlds (extras not installed)" option, on an interpreter where running
    // that command can't make RLDS work.
    mockEnv('no-wheel')
    renderDialog(Dialog)

    await waitFor(() => expect(document.body).toHaveTextContent(ALL_EXPORTS_NO_WHEEL_DESCRIPTION))
    expect(document.body).toHaveTextContent(RLDS_NO_WHEEL_REASON)
    const rlds = await presetOption('rlds')
    expect(rlds.disabled).toBe(true)
    expect(rlds).toHaveAttribute('title', RLDS_NO_WHEEL_REASON)
    expect(rlds.closest('select')).toHaveAccessibleDescription(`rlds: ${RLDS_NO_WHEEL_REASON}`)
    expect(document.body).not.toHaveTextContent('extras not installed')
    expect(document.body).not.toHaveTextContent('need the Zarr / RLDS extras')
    // Neither the copy block nor the <code> line holding the bare command.
    expect(screen.queryByText(ALL_EXPORTS_CMD)).toBeNull()
    expect(screen.queryByRole('button', { name: 'Copy' })).toBeNull()
  })

  it('keeps the pip command when it still unlocks Zarr, and says it will not fix rlds', async () => {
    // Would catch: hiding the command that does install Zarr, or letting
    // the banner promise it fixes rlds too.
    mockEnv('no-wheel-no-zarr')
    renderDialog(Dialog)

    expect(await screen.findByText(ALL_EXPORTS_CMD)).toBeInTheDocument()
    await waitFor(() => expect(document.body).toHaveTextContent(ALL_EXPORTS_NO_WHEEL_DESCRIPTION))
    expect(document.body).toHaveTextContent(RLDS_NO_WHEEL_REASON)
    expect((await presetOption('multimodal')).textContent).toBe('multimodal (extras not installed)')
    expect((await presetOption('rlds')).textContent).not.toContain('extras not installed')
  })

  it('leaves the plain missing-extra case alone: banner and pip command', async () => {
    // Would catch: the platform wording leaking into the case where the
    // extra just isn't installed yet and the pip command is the whole fix.
    mockEnv('tensorflow-missing')
    renderDialog(Dialog)

    expect(await screen.findByText(ALL_EXPORTS_CMD)).toBeInTheDocument()
    expect(document.body).toHaveTextContent(/1 preset\(s\) (unavailable — Zarr \/ RLDS extras not installed|need the Zarr \/ RLDS extras)\./)
    const rlds = await presetOption('rlds')
    expect(rlds.disabled).toBe(true)
    expect(rlds.textContent).toBe('rlds (extras not installed)')
    // The banner already covers it; no second copy of the reason.
    expect(document.body).not.toHaveTextContent(RLDS_NOT_INSTALLED_REASON)
    expect(document.body).not.toHaveTextContent('on this interpreter')
  })
})
