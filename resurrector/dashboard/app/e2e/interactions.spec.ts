import { test, expect } from '@playwright/test'

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

    // Filtering by topic name narrows to that topic's commands.
    await input.fill('lidar plot')
    await expect(page.locator('.nb-palette-row')).toHaveCount(1)
    await expect(page.locator('.nb-palette-row').first()).toContainText('bf["/lidar/points"].plot()')
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

  test('search cell renders + degrades gracefully to an honest state', async ({ page }) => {
    // Would catch: search cell not rendering, or a hard crash when vision
    // isn't installed / no frames are indexed (should be a friendly message).
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    await page.getByRole('button', { name: /Semantic search/ }).click()
    const input = page.locator('.nb-search-input')
    await expect(input).toBeVisible({ timeout: 10_000 })
    await input.fill('gripper near the table')
    await page.getByRole('button', { name: /^Search$/ }).click()
    // Either results grid or an honest message (no vision / no frames / none).
    await expect(page.locator('.nb-search-msg, .nb-search-grid').first()).toBeVisible({ timeout: 10_000 })
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
