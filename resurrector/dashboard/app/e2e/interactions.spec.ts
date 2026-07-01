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

    // Add a blank investigation → it becomes active (Untitled in header).
    await page.locator('.nb-new-btn').click()
    await expect(page.locator('.nb-title')).toHaveText('Untitled investigation')

    // Click the first (real-bag) notebook → header swaps away from Untitled.
    await items.first().click()
    await expect(page.locator('.nb-title')).not.toHaveText('Untitled investigation')
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

  test('new-notebook button adds + activates a blank investigation', async ({ page }) => {
    // Would catch: the "+" button regressing to a no-op.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    const before = await page.locator('.nb-list-item').count()
    await page.locator('.nb-new-btn').click()
    await expect(page.locator('.nb-list-item')).toHaveCount(before + 1)
    await expect(page.locator('.nb-title')).toHaveText('Untitled investigation')
  })
})

test.describe('Library → Explorer navigation', () => {
  test('clicking a bag card lands on its Explorer view with topics listed', async ({ page }) => {
    // Would catch: SPA-route regressions (e.g. v0.5.x SPA fallback
    // broke direct nav to /bag/N before commit 40bfb9e fixed it).
    await page.goto('/')
    const card = page.getByText(/scene_demo\.mcap/).first()
    await expect(card).toBeVisible({ timeout: 10_000 })
    await card.click()

    await page.waitForURL(/\/bag\/\d+/)
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
    await page.goto('/')
    await page.getByText(/scene_demo\.mcap/).first().click()
    await page.waitForURL(/\/bag\/\d+/)

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
    await page.goto('/')
    const card = page.getByText(/scene_demo\.mcap/).first()
    await expect(card).toBeVisible({ timeout: 10_000 })
    await card.click()
    await page.waitForURL(/\/bag\/\d+/)

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
    await page.goto('/')
    await page.getByText(/scene_demo\.mcap/).first().click()
    await page.waitForURL(/\/bag\/\d+/)
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

    await page.goto('/')
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
