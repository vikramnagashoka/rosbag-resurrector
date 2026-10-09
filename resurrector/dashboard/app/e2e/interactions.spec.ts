import { existsSync } from 'node:fs'
import { join } from 'node:path'
import { test, expect, type APIRequestContext, type Locator, type Page, type Route } from '@playwright/test'
import {
  ALL_EXPORTS_CMD,
  ALL_EXPORTS_NO_WHEEL_DESCRIPTION,
  EXPORT_COLUMN_FAILURES_BODY,
  EXPORT_COLUMN_FAILURES_MESSAGE,
  EXPORT_FAILED_COLUMN_LINES,
  RLDS_NO_WHEEL_REASON,
  capabilitiesFor,
  exportPresetsFor,
  type ExportEnv,
} from '../src/exportPresetFixtures'

// Behavioural tests for interactions that screenshot diffs can't
// reliably capture (clicks, state changes, WebGL canvas content,
// dropdown reactivity).
//
// Pair these with visual.spec.ts: visual tests catch *what it looks
// like*, interactions catch *what it does*. Both layers need to exist
// for features that ship with new UI affordances.

test.describe('Notebook workspace (v0.8 overhaul)', () => {
  test('rail is backed by real indexed bags + switching swaps the header', async ({ page }) => {
    // Would catch: notebooks not loading from /api/bags, or rail clicks
    // not swapping the active notebook into the header.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })

    // At least one notebook from the hermetic env's indexed bag(s).
    const items = page.locator('.nb-list-item')
    await expect(items.first()).toBeVisible()

    // Add a blank investigation via the + menu → it becomes active.
    await page.locator('.nb-new-btn').click()
    await page.getByRole('menuitem', { name: /New notebook/ }).click()
    await expect(page.locator('.nb-title')).toHaveText('Untitled investigation')

    // Click the first (real-bag) notebook → header swaps away from Untitled.
    await items.first().click()
    await expect(page.locator('.nb-title')).not.toHaveText('Untitled investigation')
  })

  test('rail + menu creates folders and notebooks can be organized into them', async ({ page }) => {
    // Would catch: the + menu not offering folder creation, folders not
    // rendering as collapsible groups, or the move-to-folder control not
    // re-parenting a notebook.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })

    // Open the + menu → New folder. A folder group appears with an inline
    // rename input focused; commit a name with Enter.
    await page.locator('.nb-new-btn').click()
    await page.getByRole('menuitem', { name: /New folder/ }).click()
    const rename = page.locator('.nb-folder-rename')
    await expect(rename).toBeVisible()
    await rename.fill('Sensors')
    await rename.press('Enter')
    await expect(page.locator('.nb-folder-name', { hasText: 'Sensors' })).toBeVisible()

    // The folder starts empty.
    await expect(page.getByText('Empty — use + to add a notebook')).toBeVisible()

    // Add a notebook directly into the folder via the folder's own +.
    await page.locator('.nb-folder .nb-folder-btn[title="New notebook in folder"]').click()
    await expect(page.locator('.nb-title')).toHaveText('Untitled investigation')
    // Folder now shows a child item and its count badge reads 1.
    await expect(page.locator('.nb-folder-kids .nb-list-item')).toHaveCount(1)
    await expect(page.locator('.nb-folder-count')).toHaveText('1')

    // Move a top-level (real-bag) notebook into the folder via its select.
    const topLevelItem = page.locator('.nb-list > .nb-list-item').first()
    await topLevelItem.locator('.nb-move-select').selectOption({ label: 'Sensors' })
    await expect(page.locator('.nb-folder-kids .nb-list-item')).toHaveCount(2)

    // Collapsing the folder hides its children.
    await page.locator('.nb-folder-toggle').click()
    await expect(page.locator('.nb-folder-kids')).toHaveCount(0)
  })

  test('a blank notebook can be pointed at an indexed bag before analysis', async ({ page }) => {
    // Would catch: blank investigations being a dead end — no way to attach
    // a bag, so the command bar/chips have no data to act on.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('.nb-list-item').first()).toBeVisible()

    // Create a blank notebook via the + menu → it has no bag.
    await page.locator('.nb-new-btn').click()
    await page.getByRole('menuitem', { name: /New notebook/ }).click()
    await expect(page.getByText('no bag attached')).toBeVisible()
    // The bag picker is shown and the command input is disabled until attach.
    await expect(page.getByText('Point this notebook at a bag')).toBeVisible()
    // Both paths are presented: upload a new bag, and pick an indexed one.
    await expect(page.locator('.nb-bagpick-import-card')).toBeVisible()
    await expect(page.locator('.nb-bagpick-import-card')).toContainText('Upload a new bag')
    await expect(page.locator('.nb-bagpick input[type="file"]')).toHaveCount(1)
    await expect(page.locator('.nb-bagpick-item').first()).toBeVisible()
    await expect(page.locator('.nb-cmd-input')).toBeDisabled()

    // Attach the first indexed bag → header stats populate, picker disappears,
    // command bar enables, and a suggestion chip now works.
    await page.locator('.nb-bagpick-item').first().click()
    await expect(page.getByText('Point this notebook at a bag')).toHaveCount(0)
    await expect(page.locator('.nb-cmd-input')).toBeEnabled()
    await expect(page.locator('.nb-header-meta')).toContainText('topics')

    await page.getByRole('button', { name: /Health report/ }).click()
    await expect(page.getByText('bf.health().report()')).toBeVisible()
  })

  test('rail reaches all workflow pages, warm-themed under /n', async ({ page }) => {
    // Would catch: any workflow page dropping back to the old dark UI, or a
    // rail link / back-to-notebook path regressing.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })

    const nav = page.locator('.nb-railnav')
    await expect(nav.getByText('MORE TOOLS')).toBeVisible()

    // Every rail link opens a NATIVE warm page (.nb-page shell, no classic
    // navbar) with the expected title, and the back link returns to /n.
    const pages: [string, RegExp, string][] = [
      ['Library', /\/n\/library$/, 'Library'],
      ['Datasets', /\/n\/datasets$/, 'Datasets'],
      ['Bridge', /\/n\/bridge$/, 'Bridge control'],
      ['Help & Docs', /\/n\/help$/, 'Help & Docs'],
    ]
    for (const [label, url, title] of pages) {
      await nav.getByRole('link', { name: label }).click()
      await page.waitForURL(url)
      await expect(page.locator('.nb-page')).toBeVisible()
      await expect(page.locator('.nb-page-title')).toHaveText(title)
      await page.locator('.nb-page-back').click()
      await page.waitForURL(/\/n$/)
      await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    }
  })

  test('Library card opens the bag in the notebook workspace', async ({ page }) => {
    // Would catch: the warm Library not deep-linking a bag into /n/<id>.
    await page.goto('/n/library')
    await expect(page.locator('.nb-page-title')).toHaveText('Library')
    const card = page.locator('.nb-lib-card').first()
    await expect(card).toBeVisible({ timeout: 10_000 })
    await card.click()
    await page.waitForURL(/\/n\/nb-bag-\d+$/)
    // Lands in the notebook with that bag active (header shows real stats).
    await expect(page.locator('.nb-header-meta')).toContainText('topics')
  })

  test('Export button opens the warm export dialog for the active bag', async ({ page }) => {
    // Would catch: the notebook Export (the gap the classic Explorer had but
    // /n lacked) not opening or not listing formats/topics.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('.nb-list-item').first()).toBeVisible()
    await page.getByRole('button', { name: 'Export', exact: true }).click()
    const modal = page.locator('.nb-export')
    await expect(modal).toBeVisible()
    await expect(modal.getByText('Export data')).toBeVisible()
    // Format dropdown + at least one topic checkbox present.
    await expect(modal.locator('select').first()).toBeVisible()
    await expect(modal.locator('.nb-export-topic').first()).toBeVisible()
  })

  test('Export dialog names the right extra for an unavailable LeRobot preset', async ({ page, request }) => {
    // Would catch: the LeRobot preset gated behind the Zarr/RLDS banner and
    // its [all-exports] command (the pre-v0.8.4 wiring, which installs the
    // wrong package), or the preset staying selectable without LeRobot.
    const caps = await request.get('/api/system/capabilities').then(r => r.json())
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('.nb-list-item').first()).toBeVisible()
    await page.getByRole('button', { name: 'Export', exact: true }).click()
    const modal = page.locator('.nb-export')
    // Scope to the preset select (the format select also has a lerobot option).
    const lerobotOption = modal.locator('select:has(option[value="training-tabular"]) option[value="lerobot"]')
    await expect(lerobotOption).toBeAttached()

    if (!caps.lerobot.available) {
      const banner = modal.locator('.nb-bridge-banner', { hasText: 'LeRobot extra' })
      await expect(banner).toBeVisible()
      await expect(banner.locator('code')).toContainText("rosbag-resurrector[lerobot]")
      // toBeDisabled() doesn't honor `disabled` on <option>; assert the attribute.
      await expect(lerobotOption).toHaveAttribute('disabled', '')
    } else {
      await expect(modal.getByText('LeRobot extra')).toHaveCount(0)
      await expect(lerobotOption).not.toHaveAttribute('disabled')
    }
    // The Zarr/RLDS banner must no longer advertise LeRobot output.
    await expect(modal.getByText('Install for Zarr or LeRobot/RLDS output.')).toHaveCount(0)
  })

  test('Datasets warm page creates a dataset (native, not the dark UI)', async ({ page }) => {
    // Would catch: the Datasets port not rendering in the warm theme, or the
    // create flow (modal → /api/datasets) breaking.
    await page.goto('/n/datasets')
    await expect(page.locator('.nb-page-title')).toHaveText('Datasets')

    await page.getByRole('button', { name: 'New dataset' }).click()
    const modal = page.locator('.nb-modal')
    await expect(modal).toBeVisible()
    const name = `nb-e2e-${Date.now()}`
    await modal.locator('input').first().fill(name)
    await modal.getByRole('button', { name: 'Create' }).click()

    // The new dataset appears in the warm list.
    await expect(page.locator('.nb-ds-item', { hasText: name })).toBeVisible({ timeout: 10_000 })
  })

  test('Bridge warm page loads with status + live-mode install banner', async ({ page }) => {
    // Would catch: the Bridge port not rendering in the warm theme, or the
    // live-mode rclpy gate regressing.
    await page.goto('/n/bridge')
    await expect(page.locator('.nb-page-title')).toHaveText('Bridge control')
    // Status panel + start form present in the warm shell.
    await expect(page.locator('.nb-bridge-status')).toBeVisible()
    await expect(page.getByRole('button', { name: /Start bridge/ })).toBeVisible()

    // Switching to live mode surfaces the rclpy install banner (extras absent
    // in the hermetic env) and disables Start.
    await page.getByRole('button', { name: /^live$/ }).click()
    await expect(page.getByText('Bridge live mode needs rclpy (ROS 2).')).toBeVisible()
    await expect(page.getByRole('button', { name: /Start bridge/ })).toBeDisabled()
  })

  test('uploading a bag file in the picker indexes + attaches it', async ({ page, request }) => {
    // Would catch: the upload endpoint or the picker's file-input wiring
    // breaking — a blank notebook must be attachable by uploading a file,
    // not only by picking an already-indexed bag.
    const bags = await request.get('/api/bags').then(r => r.json())
    const bagPath = bags[0].path as string   // a real bag file on this machine

    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('.nb-list-item').first()).toBeVisible()
    await page.locator('.nb-new-btn').click()
    await page.getByRole('menuitem', { name: /New notebook/ }).click()
    await expect(page.getByText('Point this notebook at a bag')).toBeVisible()

    // Set the file on the hidden input (bypasses the OS file dialog).
    await page.locator('.nb-bagpick input[type="file"]').setInputFiles(bagPath)

    // Upload → index → attach: picker disappears, header stats populate.
    await expect(page.getByText('Point this notebook at a bag')).toHaveCount(0, { timeout: 20_000 })
    await expect(page.locator('.nb-header-meta')).toContainText('topics')
    await expect(page.locator('.nb-cmd-input')).toBeEnabled()
  })

  test('rail + → Scan folder imports bags from a directory', async ({ page, request }) => {
    // Would catch: the rail Scan-folder form not wiring to /api/scan or not
    // merging newly-indexed bags into the rail.
    const bags = await request.get('/api/bags').then(r => r.json())
    const bagPath = bags[0].path as string
    const dir = bagPath.replace(/[/\\][^/\\]+$/, '')   // parent directory

    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await page.locator('.nb-new-btn').click()
    await page.getByRole('menuitem', { name: /Scan folder/ }).click()

    const form = page.locator('.nb-scan-form')
    await expect(form).toBeVisible()
    await form.locator('.nb-scan-input').fill(dir)
    await form.locator('.nb-scan-go').click()
    // The scan reports how many bags it indexed from the directory.
    await expect(page.locator('.nb-scan-msg')).toContainText(/Indexed \d+ of \d+/, { timeout: 20_000 })
  })

  test('rail footer shows real capability status, not fabricated bars', async ({ page, request }) => {
    // Would catch: the footer regressing to hardcoded fake "4 ready · 2
    // partial" data (a credibility smell), or the capabilities fetch failing.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })

    // The backend reports N capabilities; the footer must render exactly one
    // segment per capability and an "M of N ready" meta line that agrees.
    const caps = await request.get('/api/system/capabilities').then(r => r.json())
    const total = Object.keys(caps).length
    const ready = Object.values(caps).filter((c: any) => c.available).length
    await expect(page.locator('.nb-status-seg')).toHaveCount(total)
    await expect(page.locator('.nb-status-meta')).toContainText(`${ready} of ${total} ready`)
  })

  test('Share copies a link and ⌘K focuses the command bar', async ({ page }) => {
    // Would catch: Share regressing to a dead stub, or the ⌘K focus
    // shortcut not being wired.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })

    const share = page.getByRole('button', { name: 'Share' })
    await share.click()
    await expect(page.getByRole('button', { name: 'Copied ✓' })).toBeVisible()

    // Ctrl+K (matches metaKey||ctrlKey handler) focuses the command input.
    await page.keyboard.press('Control+k')
    await expect(page.locator('.nb-cmd-input')).toBeFocused()
  })

  test('Health report chip adds a health cell that renders a real score ring', async ({ page }) => {
    // Would catch: the cell framework not appending cells, or the health
    // cell not wiring to /api/bags/:id/health.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    // Active notebook is the first real bag; add a health cell.
    await page.getByRole('button', { name: /Health report/ }).click()
    // The shared cell shell shows the command string…
    await expect(page.getByText('bf.health().report()')).toBeVisible()
    // …and the body renders the conic score ring from real health data.
    await expect(page.getByRole('img', { name: /Health score/ })).toBeVisible({ timeout: 10_000 })
    // …plus the v0.8 enriched sections: per-check breakdown + summary.
    await expect(page.getByText('CHECKS')).toBeVisible()
    await expect(page.getByText(/topics checked/)).toBeVisible()
  })

  test('Plot signal chip adds a plot cell with a real SVG chart + topic dropdown', async ({ page }) => {
    // Would catch: plot cell not rendering, the downsampled-series fetch
    // failing, or the header topic dropdown not re-driving the cell.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await page.getByRole('button', { name: /Plot signal/ }).click()

    // The chart renders with at least one series polyline carrying points.
    // (Don't assert toBeVisible on the polyline — constant-signal topics
    // draw a flat zero-height line that Playwright reports as hidden.)
    await expect(page.locator('.nb-chart')).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('.nb-chart polyline')).not.toHaveCount(0)
    const points = await page.locator('.nb-chart polyline').first().getAttribute('points')
    expect(points && points.length).toBeTruthy()

    // The topic dropdown re-drives the command string.
    const select = page.locator('.nb-cell-select')
    const options = await select.locator('option').allTextContents()
    expect(options.length).toBeGreaterThan(1)
    await select.selectOption(options[1])
    await expect(page.getByText(`bf["${options[1]}"].plot()`)).toBeVisible()
  })

  test('stats / sync / scene cells render real data from live endpoints', async ({ page }) => {
    // Would catch: any of the PR 4 cell renderers regressing or their
    // endpoint wiring breaking (stats compute, /sync, /scene/topics).
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })

    // Stats — table with the sampled-points footer.
    await page.getByRole('button', { name: /Statistics/ }).click()
    await expect(page.locator('.nb-table').first()).toBeVisible({ timeout: 10_000 })
    await expect(page.getByText(/sampled points/)).toBeVisible()

    // Sync — aligned head() via /api/bags/:id/sync.
    await page.getByRole('button', { name: /Synchronize/ }).click()
    await expect(page.getByText(/^bf\.sync\(\[/)).toBeVisible({ timeout: 10_000 })

    // Scene — live 3D render (react-three-fiber canvas) + metadata caption.
    await page.getByRole('button', { name: /3D scene/ }).click()
    await expect(page.locator('.nb-scene-live canvas')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('.nb-scene-caption')).toContainText('drag to orbit')
  })

  test('command palette filters the catalog + Enter runs the top match', async ({ page }) => {
    // Would catch: palette not filtering, Enter-to-run regressing, or the
    // catalog not being topic-aware.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })

    const input = page.locator('.nb-cmd-input')
    await input.click()
    await input.fill('health')
    await expect(page.locator('.nb-palette')).toBeVisible()
    await expect(page.locator('.nb-palette-row').first()).toContainText('bf.health().report()')

    // Enter runs the top match → a health cell appears; palette closes.
    await input.press('Enter')
    await expect(page.getByRole('img', { name: /Health score/ })).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('.nb-palette')).toHaveCount(0)

    // Filtering by topic name narrows to that topic's commands. The catalog
    // match stays FIRST (Enter runs it); the free-form "run as query cell"
    // fallback is always appended LAST for non-empty input.
    await input.fill('lidar plot')
    await expect(page.locator('.nb-palette-row')).toHaveCount(2)
    await expect(page.locator('.nb-palette-row').first()).toContainText('bf["/lidar/points"].plot()')
    await expect(page.locator('.nb-palette-row').last()).toContainText('Run as a free Polars expression')
  })

  test('linked time-cursor spans plots + the time toggle flips a cell to Own time', async ({ page }) => {
    // Would catch: hover not setting the shared cursor, consumers not
    // drawing it, or the per-cell time toggle regressing.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await page.getByRole('button', { name: /Plot signal/ }).click()
    await page.getByRole('button', { name: /Plot signal/ }).click()
    await expect(page.locator('.nb-chart').first()).toBeVisible({ timeout: 10_000 })

    // Hovering plot [1] draws the dashed cursor on BOTH linked plots.
    const box = await page.locator('.nb-chart-wrap').first().boundingBox()
    await page.mouse.move(box!.x + box!.width * 0.6, box!.y + box!.height * 0.5)
    await expect(page.locator('.nb-chart line[stroke-dasharray="4 3"]')).toHaveCount(2)

    // The per-cell time toggle flips Shared time → Own time.
    const toggle = page.locator('.nb-time-toggle').first()
    await expect(toggle).toContainText('Shared time')
    await toggle.click()
    await expect(toggle).toContainText('Own time')
  })

  test('brushing a plot → Explain renders a grounded card from the copilot', async ({ page }) => {
    // Would catch: drag-select not producing a toolbar, the header command
    // not updating to .select(), or the Explain endpoint wiring breaking.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await page.getByRole('button', { name: /Plot signal/ }).click()
    await expect(page.locator('.nb-chart').first()).toBeVisible({ timeout: 10_000 })

    const box = await page.locator('.nb-chart-wrap').first().boundingBox()
    await page.mouse.move(box!.x + box!.width * 0.3, box!.y + box!.height * 0.5)
    await page.mouse.down()
    await page.mouse.move(box!.x + box!.width * 0.65, box!.y + box!.height * 0.5, { steps: 8 })
    await page.mouse.up()

    // Toolbar appears; the header command reflects the selection.
    await expect(page.locator('.nb-sel-toolbar')).toBeVisible({ timeout: 5_000 })
    await expect(page.getByText(/\.select\(/)).toBeVisible()

    // Explain calls the real /explain endpoint → grounded narrative card.
    await page.getByRole('button', { name: /Explain/ }).click()
    await expect(page.locator('.nb-explain-body')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('.nb-explain-body')).toContainText('window')
  })

  test('Transform chip adds a transform cell that previews a derived series', async ({ page }) => {
    // Would catch: the classic Transform editor's op/column/expression flow
    // not being ported into the notebook — the capability the user flagged
    // as missing from /n.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await page.locator('.nb-chip', { hasText: 'Transform' }).click()

    // Cell shell shows the derived-signal command, defaulting to derivative.
    await expect(page.getByText(/\.derivative\(/)).toBeVisible()

    // Common-mode controls exist: operation + column selects.
    const opSelect = page.locator('.nb-tf-field', { hasText: 'Operation' }).locator('select')
    await expect(opSelect).toBeVisible()

    // The live preview renders the derived series (real /transforms/preview).
    await expect(page.locator('.nb-transform .nb-chart')).toBeVisible({ timeout: 15_000 })

    // Switching the op re-drives the header command string.
    await opSelect.selectOption('integral')
    await expect(page.getByText(/\.integral\(/)).toBeVisible()

    // Expression mode swaps in the Polars expression input.
    await page.getByRole('button', { name: 'Expression' }).click()
    await expect(page.locator('.nb-tf-expr-input')).toBeVisible()
  })

  test('cell ? toggle opens an accurate inline guide', async ({ page }) => {
    // Would catch: the per-cell guide regressing — the ? button missing,
    // the panel not opening/closing, or a cell type shipping without its
    // guide content.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('.nb-list-item').first()).toBeVisible()

    await page.getByRole('button', { name: /Plot signal/ }).click()
    const help = page.locator('.nb-cell-help').first()
    await expect(help).toBeVisible()
    await help.click()
    // The guide documents the brush → Explain interaction (plot's key affordance).
    await expect(page.locator('.nb-guide')).toBeVisible()
    await expect(page.locator('.nb-guide')).toContainText('Brushes a time window')
    // Toggle closes it.
    await help.click()
    await expect(page.locator('.nb-guide')).toHaveCount(0)

    // A second cell type gets its own content (query documents the sandbox).
    await page.getByRole('button', { name: /^\+ Query$/ }).click()
    await page.locator('.nb-cell-help').last().click()
    await expect(page.locator('.nb-guide')).toContainText('rejected server-side')
  })

  test('query cell: write a free Polars expression, run it, see chart + table', async ({ page }) => {
    // Would catch: the free-form exploration path breaking — the Query chip
    // not adding a cell, column chips not inserting, the sandboxed
    // /api/transforms/preview run failing, or results not rendering.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('.nb-list-item').first()).toBeVisible()

    await page.getByRole('button', { name: /^\+ Query$/ }).click()
    const editor = page.locator('.nb-query-editor')
    await expect(editor).toBeVisible()

    // Build an expression from a real column via the clickable chip, so the
    // test doesn't hardcode this bag's field names.
    const firstCol = page.locator('.nb-query-col').first()
    await expect(firstCol).toBeVisible({ timeout: 10_000 })
    await firstCol.click()
    await editor.click()
    await editor.press('End')
    await editor.pressSequentially(' * 2')
    await page.locator('.nb-query .nb-search-go').click()

    // Result: legend + the head-of-data table with real rows.
    await expect(page.locator('.nb-query .nb-tf-legend')).toBeVisible({ timeout: 15_000 })
    const rows = page.locator('.nb-query-table tbody tr')
    await expect(rows.first()).toBeVisible()

    // A bad expression surfaces the sandbox error instead of dying silently.
    await editor.fill('import os')
    await page.locator('.nb-query .nb-search-go').click()
    await expect(page.locator('.nb-query-error')).toBeVisible({ timeout: 10_000 })
  })

  test('command bar: unrecognized input offers "run as query cell"', async ({ page }) => {
    // Would catch: the palette fallback regressing — typing a free
    // expression must offer a query-cell entry, and Enter must create the
    // cell carrying that expression.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('.nb-list-item').first()).toBeVisible()

    const input = page.locator('.nb-cmd-input')
    await input.click()
    await input.fill('pl.col("nope").abs()')
    // No catalog entry matches → the query fallback is the top entry.
    await expect(page.getByText('Run as a free Polars expression')).toBeVisible()
    await input.press('Enter')

    // The query cell exists, carries the typed expression, and honestly
    // surfaces the backend's unknown-column error from its auto-run.
    await expect(page.locator('.nb-query-editor')).toHaveValue('pl.col("nope").abs()')
    await expect(page.locator('.nb-query-error')).toBeVisible({ timeout: 15_000 })
  })

  test('adding a cell scrolls it into view (no invisible appends)', async ({ page }) => {
    // Would catch: cells appending below the fold with no scroll — clicking
    // "+ Transform" while scrolled up looked like a no-op (spotted in the
    // v0.8 demo recording: the tour clicked chips but the UI showed nothing).
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('.nb-list-item').first()).toBeVisible()

    // Health (tall) then two more cells — the feed must overflow, and each
    // newly added cell must end up visible in the viewport.
    await page.getByRole('button', { name: /Health report/ }).click()
    await expect(page.getByRole('img', { name: /Health score/ })).toBeVisible({ timeout: 10_000 })
    await page.getByRole('button', { name: /Plot signal/ }).click()
    await expect(page.locator('.nb-cell').nth(1)).toBeInViewport({ timeout: 10_000 })
    await page.getByRole('button', { name: /Statistics/ }).click()
    await expect(page.locator('.nb-cell').nth(2)).toBeInViewport({ timeout: 10_000 })
  })

  test('Compare bags chip overlays a topic across bags (native, not the old UI)', async ({ page }) => {
    // Would catch: the classic Compare-runs page not being ported into the
    // notebook — cross-bag overlay must be a native cell, and it must render
    // one series per selected bag from /api/compare/topics.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await page.locator('.nb-chip', { hasText: 'Compare bags' }).click()

    // Bag chips appear; the cell auto-seeds two bags selected.
    const onChips = page.locator('.nb-cmp-bagchip.on')
    await expect(onChips).toHaveCount(2, { timeout: 10_000 })

    // The overlay renders one polyline per selected bag (two series).
    await expect(page.locator('.nb-compare .nb-chart')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('.nb-compare .nb-chart polyline')).toHaveCount(2)
    // The auto-picked Value column must be a real signal, never a wall-clock
    // field — stamp_sec as the default draws meaningless monotonic staircases.
    const valueSel = page.locator('.nb-cmp-controls .nb-tf-field:nth-child(2) select')
    await expect(valueSel).toBeVisible()
    expect(await valueSel.inputValue()).not.toMatch(/stamp|_ns$|timestamp/)
    // Legend carries a chip per bag.
    await expect(page.locator('.nb-compare .nb-legend-chip')).toHaveCount(2)

    // Deselecting a bag drops it below the 2-bag minimum → prompt returns.
    await page.locator('.nb-cmp-bagchip.on').first().click()
    await expect(page.getByText('Select at least two bags to overlay.')).toBeVisible()
  })

  test('search cell pre-checks its prerequisites and shows the install banner on add', async ({ page, request }) => {
    // Would catch: the proactive banner regressing to the old behavior where
    // a missing [vision] extra (or an unindexed bag) only surfaced AFTER the
    // user typed a query and searched. The banner must appear the moment the
    // cell is added, with a copyable command, and the input must be disabled.
    //
    // Which banner depends on the backend: CI has no vision extras → the
    // pip-install banner; a dev box with vision → this env's bags are never
    // frame-indexed → the index-frames banner. Both are pre-check states.
    const caps = await request.get('/api/system/capabilities').then(r => r.ok() ? r.json() : null)
    const visionInstalled = caps?.vision?.available === true

    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('.nb-list-item').first()).toBeVisible()
    await page.getByRole('button', { name: /Semantic search/ }).click()

    // Banner appears with NO typing, carrying the right command + copy button.
    const banner = page.locator('.nb-cell-banner')
    await expect(banner).toBeVisible({ timeout: 10_000 })
    if (visionInstalled) {
      await expect(banner).toContainText('aren’t indexed yet')
      await expect(banner.locator('code')).toContainText('resurrector index-frames')
    } else {
      await expect(banner).toContainText('needs the vision extras')
      await expect(banner.locator('code')).toContainText('pip install')
    }
    await expect(banner.locator('.nb-cell-banner-copy')).toBeVisible()
    // Searching is blocked while prerequisites are missing.
    await expect(page.locator('.nb-search-input')).toBeDisabled()
    await expect(page.locator('.nb-search-go')).toBeDisabled()
  })

  test('new-notebook button adds + activates a blank investigation', async ({ page }) => {
    // Would catch: the "+" menu's New notebook item regressing to a no-op.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    // Wait for the real-bag notebooks to finish streaming in before acting,
    // so we're not racing the async /api/bags load.
    await expect(page.locator('.nb-list-item').first()).toBeVisible()

    // No "Untitled investigation" item exists until we add one.
    const untitled = page.locator('.nb-list-item', { hasText: 'Untitled investigation' })
    await expect(untitled).toHaveCount(0)
    await page.locator('.nb-new-btn').click()
    await page.getByRole('menuitem', { name: /New notebook/ }).click()
    // The new blank notebook appears in the rail and becomes active.
    await expect(untitled).toHaveCount(1)
    await expect(page.locator('.nb-title')).toHaveText('Untitled investigation')
  })
})

test.describe('Library → Explorer navigation', () => {
  test('clicking a bag card lands on its Explorer view with topics listed', async ({ page }) => {
    // Would catch: SPA-route regressions (e.g. v0.5.x SPA fallback
    // broke direct nav to /bag/N before commit 40bfb9e fixed it).
    await page.goto('/classic')
    const card = page.getByText(/scene_demo\.mcap/).first()
    await expect(card).toBeVisible({ timeout: 10_000 })
    await card.click()

    await page.waitForURL(/\/classic\/bag\/\d+/)
    // Topics-panel rows have a unique "<msg-type> | N msgs" subtitle that
    // doesn't appear in the Topic Timeline strip up top — anchor on it
    // to dodge the strict-mode violation `/tf` would cause otherwise.
    await expect(page.getByText('sensor_msgs/msg/PointCloud2 | 80 msgs')).toBeVisible({ timeout: 10_000 })
    await expect(page.getByText('tf2_msgs/msg/TFMessage | 240 msgs')).toBeVisible()
  })
})

test.describe('Ask your bag — Explain', () => {
  test('Explain button is present and disabled until a range is brushed', async ({ page }) => {
    // Would catch: the v0.7 Explain button regressing — missing entirely,
    // or wired so it's always enabled/disabled regardless of selection.
    // (Brushing Plotly's canvas is flaky, so we assert the disabled-until-
    // selection wiring; the panel content is API-driven + backend-tested.)
    await page.goto('/classic')
    await page.getByText(/scene_demo\.mcap/).first().click()
    await page.waitForURL(/\/classic\/bag\/\d+/)

    const explain = page.getByRole('button', { name: /^Explain/ })
    await expect(explain).toBeVisible({ timeout: 10_000 })
    // No range brushed yet → disabled.
    await expect(explain).toBeDisabled()
  })

  test('explain API returns a grounded narrative for a window', async ({ request }) => {
    // Backs the panel: the endpoint the Explain button calls must return a
    // narrative + evidence grounded in real per-topic activity.
    const bags = await request.get('/api/bags').then(r => r.json())
    expect(bags.length).toBeGreaterThan(0)
    const id = bags[0].id
    const r = await request.get(`/api/bags/${id}/explain`, {
      params: { start_sec: 0, end_sec: 4, use_llm: false },
    })
    expect(r.ok()).toBeTruthy()
    const data = await r.json()
    expect(data.source).toBe('rule_based')
    expect(data.narrative.length).toBeGreaterThan(0)
    expect(data.evidence.totals.messages_in_window).toBeGreaterThan(0)
  })

  test('incident report endpoint returns a self-contained HTML attachment', async ({ request }) => {
    // Backs the panel's "Download report" button (v0.7.1). The report must be
    // a downloadable, self-contained HTML — no external asset refs.
    const bags = await request.get('/api/bags').then(r => r.json())
    const id = bags[0].id
    const r = await request.get(`/api/bags/${id}/report`, {
      params: { start_sec: 0, end_sec: 4, fmt: 'html', use_llm: false },
    })
    expect(r.ok()).toBeTruthy()
    expect(r.headers()['content-disposition']).toContain('attachment')
    const html = await r.text()
    expect(html).toContain('Incident Report')
    expect(html).toContain('</svg>')          // inline activity chart
    expect(html).not.toContain('src="http')   // no external assets
  })
})

test.describe('Explorer Scene tab', () => {
  test('Hide button clears the active Cloud topic', async ({ page }) => {
    // Would catch: the Hide button (added in v0.6.1) regressing to no-op,
    // or the dropdown failing to reflect the cleared state.
    await page.goto('/classic')
    const card = page.getByText(/scene_demo\.mcap/).first()
    await expect(card).toBeVisible({ timeout: 10_000 })
    await card.click()
    await page.waitForURL(/\/classic\/bag\/\d+/)

    // Open the Scene tab. Need to pick the /lidar/points topic in the
    // left Topics panel first because the tab controls are gated on a
    // selected topic in Explorer.
    // Click the Topics-panel row for /lidar/points — anchor on the
    // unique subtitle to avoid matching the Topic Timeline label.
    await page.getByText('sensor_msgs/msg/PointCloud2 | 80 msgs').click()
    await page.getByRole('button', { name: /^scene$/i }).click()

    // The Cloud dropdown should default to /lidar/points and the Hide
    // button should be visible.
    const cloudLabel = page.locator('label', { hasText: 'Cloud:' })
    await expect(cloudLabel).toBeVisible({ timeout: 10_000 })
    const cloudSelect = cloudLabel.locator('select')
    await expect(cloudSelect).toHaveValue('/lidar/points')

    // Click Hide; the select should snap to the empty "(none)" option.
    await cloudLabel.getByRole('button', { name: /^hide$/i }).click()
    await expect(cloudSelect).toHaveValue('')
    // Hide button disappears once nothing is selected.
    await expect(cloudLabel.getByRole('button', { name: /^hide$/i })).toHaveCount(0)
  })

  test('Max points dropdown changes the rendered cap', async ({ page }) => {
    // Would catch: the Max points control losing its onChange wiring.
    await page.goto('/classic')
    await page.getByText(/scene_demo\.mcap/).first().click()
    await page.waitForURL(/\/classic\/bag\/\d+/)
    // Click the Topics-panel row for /lidar/points — anchor on the
    // unique subtitle to avoid matching the Topic Timeline label.
    await page.getByText('sensor_msgs/msg/PointCloud2 | 80 msgs').click()
    await page.getByRole('button', { name: /^scene$/i }).click()

    const maxLabel = page.locator('label', { hasText: 'Max points:' })
    const maxSelect = maxLabel.locator('select')
    // Default lowered to 5k in v0.6.1 to avoid burying labels at first load.
    await expect(maxSelect).toHaveValue('5000')
    await maxSelect.selectOption('1000')
    await expect(maxSelect).toHaveValue('1000')
  })
})

test.describe('Library scan with .bag file', () => {
  test('ros1 install banner appears when scan hits a .bag without mcap CLI', async ({ page, request }) => {
    // Would catch: scan error classification regressing — the kind
    // field on per-file errors is what routes the banner.

    // Hit the API directly to trigger a scan over the test root which
    // includes a stub .bag (placed by run-dashboard.sh).
    const root = await request.get('/api/system/paths')
      .then(r => r.json())
      .then(d => d.cache_dir as string)
      .catch(() => null)

    // Find a known directory that the dashboard can scan. Use the
    // bag's parent directory by reading an indexed bag's path.
    const bags = await request.get('/api/bags').then(r => r.json())
    expect(bags.length).toBeGreaterThan(0)
    const scanDir = bags[0].path.replace(/\/[^/]+$/, '')

    await page.goto('/classic')
    // Use the scan form. Library has a collapsed scan header — toggle
    // it open if needed.
    const headerToggle = page.getByTitle(/scan a folder for bag files/i)
    if (await headerToggle.isVisible().catch(() => false)) {
      await headerToggle.click()
    }
    const scanInput = page.locator('input').filter({ hasText: '' }).first()
    // The scan input is the first text input on Library — set it to
    // a folder containing a .bag stub and trigger the scan.
    const inputs = page.locator('input[type="text"]')
    const scanPathInput = inputs.first()
    await scanPathInput.fill(scanDir)
    await page.keyboard.press('Enter')

    // Either the ros1 banner appears (if a .bag is present) OR no banner
    // (if the test root has only MCAPs). Both are valid; assert ONLY
    // that the page didn't crash and no "[object Object]" toast appears.
    await page.waitForTimeout(1500)
    const alerts = await page.getByRole('alert').allTextContents()
    expect(alerts.join('\n')).not.toContain('[object Object]')
  })
})

// Shared by the classic and notebook export dialogs: both must tell the truth
// about LeRobot, which ignores `sync` (it always resamples every topic onto
// its own fps grid) and reads the rate field as the dataset fps.
async function expectLerobotExportControlsHonest(page: Page, modal: Locator) {
  const format = modal.locator('select:has(option[value="hdf5"])')
  const syncBox = modal.getByRole('checkbox', { name: /Synchronize topics/ })
  const rate = modal.getByRole('textbox', { name: /^(Downsample|Frame rate)/ })
  const exportButton = modal.getByRole('button', { name: 'Export', exact: true })

  // Sync OFF under Parquet, then LeRobot: the box must still read checked,
  // because LeRobot always aligns. Rendering the user's own `sync` here
  // (checked={sync}) would show an unchecked, disabled box.
  await expect(format).toHaveValue('parquet')
  await expect(syncBox).toBeEnabled()
  await syncBox.uncheck()
  await expect(syncBox).not.toBeChecked()
  await format.selectOption('lerobot')
  await expect(syncBox).toBeChecked()
  await expect(syncBox).toBeDisabled()
  await format.selectOption('parquet')
  await expect(syncBox).toBeEnabled()
  await expect(syncBox).not.toBeChecked()

  // Sync ON under Parquet so the switch to LeRobot has stale state to leak.
  await syncBox.check()

  await format.selectOption('lerobot')
  await expect(syncBox).toBeDisabled()
  await expect(syncBox).toBeChecked()
  await expect(modal.getByText(/Always on for LeRobot/)).toBeVisible()
  await expect(rate).toHaveAccessibleName(/^Frame rate \(fps[^)]*\)$/)
  await expect(rate).toHaveAttribute('placeholder', /30/)

  // The field's border before any error, taken with the field focused like
  // it is after each fill below.
  const borderColor = () => rate.evaluate(el => getComputedStyle(el).borderTopColor)
  await rate.fill('')
  const normalBorder = await borderColor()

  // An fps that rounds to 0 blocks Export: the backend would read 0 as
  // "unset" and silently write 30 fps.
  await rate.fill('0.4')
  await expect(rate).toHaveAccessibleDescription('LeRobot needs at least 1 fps.')
  await expect(modal.getByText(/Rounded to 0 fps/)).toHaveCount(0)
  await expect(exportButton).toBeDisabled()
  await expect.poll(borderColor).not.toBe(normalBorder)

  // A fractional fps is rounded, and the UI says so before export. The note
  // describes the field; it is not part of the field's name.
  await rate.fill('14.6')
  await expect(modal.getByText('Rounded to 15 fps.')).toBeVisible()
  await expect(rate).toHaveAccessibleName(/^Frame rate \(fps[^)]*\)$/)
  await expect(rate).toHaveAccessibleDescription('Rounded to 15 fps.')
  await expect(exportButton).toBeEnabled()
  // The error border goes away with the error. The classic dialog once
  // left a browser-default grey border here until it remounted.
  await expect.poll(borderColor).toBe(normalBorder)

  // The request carries exactly what the dialog shows: no sync flag, integer
  // fps. Fulfilled with export_bag's real response shape.
  const sent: URL[] = []
  await page.route(/\/api\/bags\/\d+\/export\?/, async route => {
    sent.push(new URL(route.request().url()))
    await route.fulfill({ json: { status: 'completed', output_path: '/tmp/e2e-lerobot' } })
  })
  await exportButton.click()
  await expect.poll(() => sent.length).toBe(1)
  const q = sent[0].searchParams
  expect(q.get('format')).toBe('lerobot')
  expect(q.get('downsample_hz')).toBe('15')
  expect(q.has('sync')).toBe(false)
  await expect(modal.getByTestId('export-result')).toHaveText('Exported to /tmp/e2e-lerobot')

  // Back to Parquet: sync is a real choice again and the user's pick survived.
  await format.selectOption('parquet')
  await expect(syncBox).toBeEnabled()
  await expect(syncBox).toBeChecked()
  await expect(modal.getByText(/Always on for LeRobot/)).toHaveCount(0)
  await expect(rate).toHaveAccessibleName(/^Downsample \(Hz/)
}

// A real Parquet export through the hermetic dashboard (no route mocking):
// the dialog must name the directory the backend actually wrote.
async function expectRealExportReportsPath(
  page: Page,
  request: APIRequestContext,
  modal: Locator,
  label: string,
) {
  const paths = await (await request.get('/api/system/paths')).json()
  const root = paths.allowed_roots[0] as string
  const outDir = join(root, `e2e-export-${label}-${Date.now()}`)

  await modal.getByRole('textbox', { name: 'Output directory' }).fill(outDir)
  await modal.getByRole('button', { name: 'Export', exact: true }).click()

  await expect(modal.getByTestId('export-result')).toHaveText(`Exported to ${outDir}`, { timeout: 30_000 })
  // Announced once, by the toast: the dialog's line is not a live region.
  await expect(page.getByRole('alert').filter({ hasText: `Exported to ${outDir}` })).toHaveCount(1)
  await expect(page.getByRole('alert').filter({ hasText: `Exported to ${outDir}` })).toHaveAttribute('data-testid', 'toast')
  await expect(modal.getByRole('status')).toHaveCount(0)
  expect((await page.getByRole('alert').allTextContents()).join('\n')).not.toContain('undefined')
  // The path shown is where the files are.
  expect(existsSync(join(outDir, 'lidar_points.parquet'))).toBe(true)
}

async function openNotebookExportDialog(page: Page): Promise<Locator> {
  await page.goto('/n')
  await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
  await expect(page.locator('.nb-list-item').first()).toBeVisible()
  await page.getByRole('button', { name: 'Export', exact: true }).click()
  const modal = page.locator('.nb-export')
  await expect(modal).toBeVisible()
  return modal
}

async function openClassicExportDialog(page: Page): Promise<Locator> {
  await page.goto('/classic')
  await page.getByText(/scene_demo\.mcap/).first().click()
  await page.waitForURL(/\/classic\/bag\/\d+/)
  await page.getByRole('button', { name: 'Export', exact: true }).click()
  const modal = page.locator('div:has(> h2:text-is("Export Data"))')
  await expect(modal).toBeVisible()
  return modal
}

test.describe('Export dialog with LeRobot format', () => {
  test('notebook dialog disables sync and relabels the rate as fps', async ({ page }) => {
    // Would catch: the notebook export dialog offering "Synchronize topics"
    // and "Downsample (Hz)" for LeRobot, where the backend ignores sync and
    // uses the rate as the integer dataset fps (the v0.8.4 audit finding);
    // a leftover sync=true riding along on a LeRobot export request; the
    // checkbox showing the user's own unchecked sync under LeRobot; an fps
    // of 0.4 being sent as 0 (silently 30 fps); the rounding note leaking
    // into the field's accessible name.
    const modal = await openNotebookExportDialog(page)
    await expectLerobotExportControlsHonest(page, modal)
  })

  test('classic dialog disables sync and relabels the rate as fps', async ({ page }) => {
    // Would catch: the same LeRobot sync/fps problems in the classic
    // Explorer export dialog.
    const modal = await openClassicExportDialog(page)
    await expectLerobotExportControlsHonest(page, modal)
  })
})

// Serve /api/export-presets and /api/system/capabilities as the backend
// reports them in `env` (see src/exportPresetFixtures.ts). Call before the
// page loads.
async function mockExportEnv(page: Page, env: ExportEnv) {
  await page.route(/\/api\/export-presets(\?|$)/, route =>
    route.fulfill({ json: exportPresetsFor(env) }))
  await page.route(/\/api\/system\/capabilities(\?|$)/, route =>
    route.fulfill({ json: capabilitiesFor(env) }))
}

// On an interpreter where [all-exports] can't install tensorflow, with zarr
// already installed: rlds is the only preset off and pip can't change that.
async function expectRldsExplainedWithoutPip(modal: Locator) {
  const rlds = modal.locator('select:has(option[value="training-tabular"]) option[value="rlds"]')
  // Banner text comes from the capabilities response; once it is up, both
  // payloads have rendered and the absence checks below mean something.
  await expect(modal.getByText(ALL_EXPORTS_NO_WHEEL_DESCRIPTION)).toBeVisible()
  await expect(modal.getByText(RLDS_NO_WHEEL_REASON)).toBeVisible()
  await expect(rlds).toHaveAttribute('disabled', '')
  await expect(rlds).toHaveAttribute('title', RLDS_NO_WHEEL_REASON)
  await expect(rlds).not.toContainText('extras not installed')
  await expect(modal).not.toContainText('extras not installed')
  await expect(modal).not.toContainText('need the Zarr / RLDS extras')
  // No copy block or <code> line holding the bare command.
  await expect(modal.getByText(ALL_EXPORTS_CMD, { exact: true })).toHaveCount(0)
  await expect(modal.getByRole('button', { name: 'Copy' })).toHaveCount(0)
  await expect(modal.getByText('after installing')).toHaveCount(0)
}

// A supported interpreter where the extra just isn't installed: the
// banner and its pip command are the whole fix, as before.
async function expectPipOfferedForRlds(modal: Locator) {
  const rlds = modal.locator('select:has(option[value="training-tabular"]) option[value="rlds"]')
  await expect(modal.getByText(ALL_EXPORTS_CMD, { exact: true })).toBeVisible()
  await expect(modal.getByText(
    /^1 preset\(s\) (unavailable — Zarr \/ RLDS extras not installed|need the Zarr \/ RLDS extras)\.$/,
  )).toBeVisible()
  await expect(rlds).toHaveAttribute('disabled', '')
  await expect(rlds).toHaveText('rlds (extras not installed)')
  await expect(modal).not.toContainText('on this interpreter')
}

test.describe('Export dialog when pip cannot install tensorflow', () => {
  test('notebook dialog says why rlds is off instead of offering pip', async ({ page }) => {
    // Would catch: the notebook dialog showing "1 preset(s) need the Zarr /
    // RLDS extras." with the [all-exports] command, and "rlds (extras not
    // installed)", on Python 3.14 where that command can't install
    // tensorflow; the backend's unavailable_reason never reaching the UI.
    await mockExportEnv(page, 'no-wheel')
    await expectRldsExplainedWithoutPip(await openNotebookExportDialog(page))
  })

  test('classic dialog says why rlds is off instead of offering pip', async ({ page }) => {
    // Would catch: the classic dialog's InstallBanner title hiding the
    // capability description, plus a copy block for a command that can't
    // help on this interpreter.
    await mockExportEnv(page, 'no-wheel')
    await expectRldsExplainedWithoutPip(await openClassicExportDialog(page))
  })

  test('notebook dialog still offers pip where it installs tensorflow', async ({ page }) => {
    // Would catch: the platform handling swallowing the ordinary
    // missing-extra banner and its pip command.
    await mockExportEnv(page, 'tensorflow-missing')
    await expectPipOfferedForRlds(await openNotebookExportDialog(page))
  })

  test('classic dialog still offers pip where it installs tensorflow', async ({ page }) => {
    // Would catch: the same regression in the classic dialog.
    await mockExportEnv(page, 'tensorflow-missing')
    await expectPipOfferedForRlds(await openClassicExportDialog(page))
  })
})

test.describe('Export dialog success message', () => {
  test('notebook dialog shows the path a real export wrote', async ({ page, request }) => {
    // Would catch: api.exportBag typed as { output } while export_bag
    // returns { status, output_path }, which made every successful export
    // report "Exported to undefined".
    const modal = await openNotebookExportDialog(page)
    await expectRealExportReportsPath(page, request, modal, 'notebook')
  })

  test('classic dialog shows the path a real export wrote', async ({ page, request }) => {
    // Would catch: the same { output } / { output_path } mismatch in the
    // classic Explorer export dialog.
    const modal = await openClassicExportDialog(page)
    await expectRealExportReportsPath(page, request, modal, 'classic')
  })
})

// The 422 that export, trim and dataset-version export send when the chosen
// format can't store some columns, as _export_error_handler sends it
// (src/exportPresetFixtures.ts). Mocked: which columns a format can't store
// changes as the writers improve, and what's under test is how the page
// shows the explanation. Requests are answered in turn: success, the 422,
// success, so a stale success line and a stale error both show up.
function successThenFailureThenSuccess(first: object, last: object = first) {
  let calls = 0
  return (route: Route) => {
    calls += 1
    if (calls === 2) return route.fulfill({ status: 422, json: EXPORT_COLUMN_FAILURES_BODY })
    return route.fulfill({ json: calls === 1 ? first : last })
  }
}

// The failed-columns error is on screen in full, one column per line,
// wrapped, and announced once: by its toast, since the on-screen copy has
// no live role.
async function expectFailureShownOnce(page: Page, inline: Locator, text: string, toastText: string) {
  await expect(inline).toBeVisible()
  expect(await inline.textContent()).toBe(text)
  // The whole block, not just its first lines above a scrolling box's fold.
  await expect(inline).toBeInViewport({ ratio: 1 })
  const toast = page.getByTestId('toast').filter({ hasText: toastText.slice(0, 40) })
  await expect(toast).toHaveCount(1)
  expect(await toast.textContent()).toBe(toastText)
  for (const box of [inline, toast]) {
    const lines = (await box.innerText()).split('\n').map(l => l.trim())
    for (const line of EXPORT_FAILED_COLUMN_LINES) expect(lines).toContain(line.trim())
    // The long path wraps instead of running out of the box.
    expect(await box.evaluate(el => el.scrollWidth <= el.clientWidth)).toBe(true)
  }
  expect(await inline.getAttribute('role')).toBeNull()
  expect(await inline.getAttribute('aria-live')).toBeNull()
  await expect(toast).toHaveAttribute('role', 'alert')
  const alerts = await page.getByRole('alert').allTextContents()
  expect(alerts.filter(a => a === text || a === toastText)).toEqual([toastText])
  expect(alerts.join('\n')).not.toContain('[object Object]')
  expect(alerts.join('\n')).not.toMatch(/Internal Server Error/i)
}

async function expectFailedColumnsExplained(page: Page, modal: Locator) {
  await page.route(
    /\/api\/bags\/\d+\/export\?/,
    successThenFailureThenSuccess({ status: 'completed', output_path: '/tmp/e2e-retry' }),
  )
  const exportButton = modal.getByRole('button', { name: 'Export', exact: true })
  await modal.locator('select:has(option[value="csv"])').selectOption('csv')

  await exportButton.click()
  await expect(modal.getByTestId('export-result')).toHaveText('Exported to /tmp/e2e-retry')

  await exportButton.click()
  const expected = `Export failed: ${EXPORT_COLUMN_FAILURES_MESSAGE}`
  await expectFailureShownOnce(page, modal.getByTestId('export-failure'), expected, expected)
  // So are the buttons under it, to retry or close.
  await expect(exportButton).toBeInViewport({ ratio: 1 })
  // The last export's "Exported to" line went when this one started.
  await expect(modal.getByTestId('export-result')).toHaveCount(0)

  await exportButton.click()
  await expect(modal.getByTestId('export-result')).toHaveText('Exported to /tmp/e2e-retry')
  await expect(modal.getByTestId('export-failure')).toHaveCount(0)
}

// Closes the dialog while its export is still running, then fails the
// export: the toast is the only place left to say so.
async function expectFailureAnnouncedAfterClose(page: Page, modal: Locator) {
  let release!: () => void
  const released = new Promise<void>(r => { release = r })
  await page.route(/\/api\/bags\/\d+\/export\?/, async route => {
    await released
    await route.fulfill({ status: 422, json: EXPORT_COLUMN_FAILURES_BODY })
  })
  await modal.locator('select:has(option[value="csv"])').selectOption('csv')
  const request = page.waitForRequest(/\/api\/bags\/\d+\/export\?/)
  await modal.getByRole('button', { name: 'Export', exact: true }).click()
  await request
  await modal.getByRole('button', { name: 'Close', exact: true }).click()
  await expect(modal).toHaveCount(0)

  release()
  const expected = `Export failed: ${EXPORT_COLUMN_FAILURES_MESSAGE}`
  const announced = page.getByRole('alert').filter({ hasText: expected.slice(0, 40) })
  await expect(announced).toHaveCount(1)
  expect(await announced.textContent()).toBe(expected)
  await expect(announced).toHaveAttribute('data-testid', 'toast')
  await expect(page.getByTestId('export-failure')).toHaveCount(0)
}

test.describe('Export dialog when columns fail to serialize', () => {
  test('notebook dialog keeps each failed column and its reason on screen', async ({ page }) => {
    // Would catch: export_bag's failed-columns error reaching the user only
    // as an 8-second toast with the column lines run together (in
    // v0.8.5 it was a bare 500, "Export failed: Internal Server Error"), a
    // structured detail rendered as "[object Object]", the error opening
    // below the dialog's scroll fold, a screen reader reading it twice
    // (dialog + toast), the previous export's "Exported to" line staying
    // next to it, and the error sticking around after a retry that works.
    await expectFailedColumnsExplained(page, await openNotebookExportDialog(page))
  })

  test('classic dialog keeps each failed column and its reason on screen', async ({ page }) => {
    // Would catch: the same in the classic Explorer export dialog.
    await expectFailedColumnsExplained(page, await openClassicExportDialog(page))
  })

  test('notebook dialog closed mid-export still has its failure announced', async ({ page }) => {
    // Would catch: a failure nobody hears because the dialog was closed
    // while the export ran (Close stays enabled), the toast having been
    // made silent on the assumption that the dialog shows the error.
    await expectFailureAnnouncedAfterClose(page, await openNotebookExportDialog(page))
  })

  test('classic dialog closed mid-export still has its failure announced', async ({ page }) => {
    // Would catch: the same in the classic Explorer export dialog.
    await expectFailureAnnouncedAfterClose(page, await openClassicExportDialog(page))
  })
})

test.describe('Trim popover when columns fail to serialize', () => {
  test('keeps each failed column on screen until an export works', async ({ page }) => {
    // Would catch: /trim's failed-columns 422 reaching the user only as an
    // 8-second toast with its lines run together (v0.8.5's popover had no
    // inline error), the error opening below the popover's fold, the last
    // export's "✓ path" line staying next to it, and the error outliving a
    // retry that works.
    await page.setViewportSize({ width: 1280, height: 600 })
    await page.goto('/classic')
    await page.getByText(/scene_demo\.mcap/).first().click()
    await page.waitForURL(/\/classic\/bag\/\d+/)
    await page.getByText('sensor_msgs/msg/PointCloud2 | 80 msgs').click()
    await page.getByRole('button', { name: /^Trim (manually|current zoom)…$/ }).click()
    const popover = page.locator('div:has(> h2:text-is("Trim & export"))')
    await expect(popover).toBeVisible()
    await page.route(/\/api\/bags\/\d+\/trim$/, successThenFailureThenSuccess({
      bag_id: 1, format: 'csv', start_sec: 0, end_sec: 1, output: '/tmp/e2e-trim',
    }))
    await popover.locator('select:has(option[value="csv"])').selectOption('csv')
    const exportButton = popover.getByRole('button', { name: 'Export', exact: true })
    const done = popover.getByText('✓ /tmp/e2e-trim')

    await exportButton.click()
    await expect(done).toBeVisible()

    await exportButton.click()
    await expectFailureShownOnce(
      page, popover.getByTestId('export-failure'),
      `Export failed: ${EXPORT_COLUMN_FAILURES_MESSAGE}`,
      `Trim export: ${EXPORT_COLUMN_FAILURES_MESSAGE}`,
    )
    await expect(exportButton).toBeInViewport({ ratio: 1 })
    await expect(done).toHaveCount(0)

    await exportButton.click()
    await expect(done).toBeVisible()
    await expect(popover.getByTestId('export-failure')).toHaveCount(0)
  })
})

test.describe('Datasets page when columns fail to serialize', () => {
  test('notebook page keeps each failed column on screen until an export works', async ({ page, request }) => {
    // Would catch: a dataset-version export's failed-columns 422 reaching
    // the user only as an 8-second toast with its lines run together (the
    // v0.8.5 page had no inline error), the error rendering below the fold
    // under a long version list, and the error outliving a retry that works.
    await page.setViewportSize({ width: 1280, height: 600 })
    const bags = await (await request.get('/api/bags')).json()
    const name = `e2e-fail-${Date.now()}`
    expect((await request.post('/api/datasets', { data: { name } })).ok()).toBe(true)
    for (let i = 1; i <= 8; i++) {
      const created = await request.post(`/api/datasets/${name}/versions`, {
        data: { version: `v${i}`, bag_refs: [{ path: bags[0].path }], export_format: 'csv' },
      })
      expect(created.ok()).toBe(true)
    }
    await page.route(
      new RegExp(`/api/datasets/${name}/versions/v1/export$`),
      successThenFailureThenSuccess(
        { name, version: 'v1', output: '/tmp/e2e-ds-first' },
        { name, version: 'v1', output: '/tmp/e2e-ds-retry' },
      ),
    )
    await page.goto('/n/datasets')
    await page.locator('.nb-ds-item', { hasText: name }).click()
    const exportButton = page
      .getByRole('row').filter({ has: page.getByRole('cell', { name: 'v1', exact: true }) })
      .getByRole('button', { name: 'Export', exact: true })
    const inline = page.getByTestId('export-failure')

    await exportButton.click()
    await expect(page.getByTestId('toast').filter({ hasText: 'Exported to /tmp/e2e-ds-first' })).toHaveCount(1)

    await exportButton.click()
    // The toast says which version failed, as the page's copy does.
    const text = `Export of ${name}@v1 failed: ${EXPORT_COLUMN_FAILURES_MESSAGE}`
    await expectFailureShownOnce(page, inline, text, text)

    await exportButton.click()
    await expect(page.getByTestId('toast').filter({ hasText: 'Exported to /tmp/e2e-ds-retry' })).toHaveCount(1)
    await expect(inline).toHaveCount(0)
  })

  // Two datasets, the first with versions v1 and v2 and the second with a
  // v1, and the first's v1 export answered with the failed-columns 422.
  async function datasetsWithFailingV1(page: Page, request: APIRequestContext) {
    const bags = await (await request.get('/api/bags')).json()
    // Neither name contains the other, so a hasText match is unambiguous.
    const name = `e2e-stale-${Date.now()}`
    const other = `e2e-other-${Date.now()}`
    for (const ds of [name, other]) {
      expect((await request.post('/api/datasets', { data: { name: ds } })).ok()).toBe(true)
    }
    // The other dataset has a v1 too: version strings repeat across datasets.
    for (const [ds, version] of [[name, 'v1'], [name, 'v2'], [other, 'v1']]) {
      const created = await request.post(`/api/datasets/${ds}/versions`, {
        data: { version, bag_refs: [{ path: bags[0].path }], export_format: 'csv' },
      })
      expect(created.ok()).toBe(true)
    }
    await page.route(
      new RegExp(`/api/datasets/${name}/versions/v1/export$`),
      route => route.fulfill({ status: 422, json: EXPORT_COLUMN_FAILURES_BODY }),
    )
    // Deletes ask confirm() first; Playwright would dismiss it.
    page.on('dialog', dialog => dialog.accept())
    return { name, other, text: `Export of ${name}@v1 failed: ${EXPORT_COLUMN_FAILURES_MESSAGE}` }
  }

  function versionRow(page: Page, version: string): Locator {
    return page.getByRole('row').filter({ has: page.getByRole('cell', { name: version, exact: true }) })
  }

  test('notebook page keeps the error across datasets until its version is deleted', async ({ page, request }) => {
    // Would catch: the error vanishing when the user selects another
    // dataset (it used to live in the failed dataset's panel), deleting
    // the other dataset's v1 clearing it, and "Export of X@v1 failed"
    // staying on screen after v1 was deleted, the natural next step once
    // the hint has them add a version in another format.
    const { name, other, text } = await datasetsWithFailingV1(page, request)
    await page.goto('/n/datasets')
    await page.locator('.nb-ds-item', { hasText: name }).click()
    await versionRow(page, 'v1').getByRole('button', { name: 'Export', exact: true }).click()
    const inline = page.getByTestId('export-failure')
    await expect(inline).toHaveText(text)

    await page.locator('.nb-ds-item', { hasText: other }).click()
    await expect(page.locator('.nb-panel-title')).toHaveText(other)
    await expect(inline).toHaveText(text)
    await versionRow(page, 'v1').getByRole('button', { name: 'Delete', exact: true }).click()
    await expect(versionRow(page, 'v1')).toHaveCount(0)
    await expect(inline).toHaveText(text)

    await page.locator('.nb-ds-item', { hasText: name }).click()
    await versionRow(page, 'v2').getByRole('button', { name: 'Delete', exact: true }).click()
    await expect(versionRow(page, 'v2')).toHaveCount(0)
    await expect(inline).toHaveText(text)

    await versionRow(page, 'v1').getByRole('button', { name: 'Delete', exact: true }).click()
    await expect(versionRow(page, 'v1')).toHaveCount(0)
    await expect(inline).toHaveCount(0)
  })

  test('classic page drops the error when its dataset is deleted, not another', async ({ page, request }) => {
    // Would catch: the failed export of a dataset the user just deleted
    // staying on the classic page, the error living only under the
    // selected dataset (so it vanished on selecting another one), or
    // deleting another dataset clearing it.
    const { name, other, text } = await datasetsWithFailingV1(page, request)
    await page.goto('/classic/datasets')
    // A list item's name; the ✕ that deletes the dataset sits next to it.
    const listName = (ds: string) => page.locator('strong', { hasText: new RegExp(`^${ds}$`) })
    await listName(name).click()
    await versionRow(page, 'v1').getByRole('button', { name: 'Export', exact: true }).click()
    const inline = page.getByTestId('export-failure')
    await expect(inline).toHaveText(text)

    await listName(other).click()
    await expect(page.getByRole('heading', { name: other })).toBeVisible()
    await expect(inline).toHaveText(text)
    await listName(other).locator('xpath=following-sibling::button').click()
    await expect(page.getByTestId('toast').filter({ hasText: `Deleted "${other}"` })).toHaveCount(1)
    await expect(inline).toHaveText(text)

    await listName(name).locator('xpath=following-sibling::button').click()
    await expect(page.getByTestId('toast').filter({ hasText: `Deleted "${name}"` })).toHaveCount(1)
    await expect(inline).toHaveCount(0)
  })
})
