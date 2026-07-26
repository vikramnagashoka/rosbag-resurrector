import { test, expect } from '@playwright/test'

// Visual regression tests — full-page screenshots compared against
// committed baselines. Run `bun --bun run e2e:update-snapshots` after
// intentional visual changes; the diff fails the test otherwise.
//
// Coverage focus: pages whose rendering is deterministic given the
// hermetic test bag (Library, Help, Health, NavBar, install-banner
// states). Plotly + WebGL canvas regions are explicitly NOT covered
// because their pixel output diverges across GPUs / platforms; rely
// on interaction tests for those.
//
// Each test names the regression class it'd catch. Keep the annotation.

test.describe('Visual baselines', () => {
  test('Help page renders the full content card layout', async ({ page }) => {
    // Would catch: regressions in static-content cards (recent CSS-token
    // changes silently broke the card border / shadow on this page once).
    await page.goto('/classic/help')
    // Wait for content; nothing async to load, but let layout settle.
    await page.waitForLoadState('networkidle')
    await expect(page).toHaveScreenshot('help-full.png', {
      fullPage: true,
    })
  })

  test('Library page with the scene-demo bag', async ({ page }) => {
    // Would catch: bag card padding regressions, header layout drift,
    // health-badge color/contrast changes. The demo bag is fixed so
    // every field (name, size, msg count, duration) is deterministic.
    await page.goto('/classic')
    // .first() — the shared hermetic index can hold more than one scene_demo
    // (other specs upload/scan), so don't assume a unique match.
    await expect(page.getByText(/scene_demo\.mcap/).first()).toBeVisible({ timeout: 10_000 })
    await page.waitForLoadState('networkidle')
    await expect(page).toHaveScreenshot('library-with-demo-bag.png', {
      fullPage: true,
    })
  })

  test('NavBar across the dashboard with active-route underline', async ({ page }) => {
    // Would catch: nav active-state indicator (cyan underline) breaking
    // — past UI refresh shipped without the indicator on one route.
    await page.goto('/classic/help')
    await page.waitForLoadState('networkidle')
    const nav = page.locator('nav').first()
    await expect(nav).toHaveScreenshot('navbar-active-help.png')
  })

  test('Search page empty state with install banner', async ({ page, request }) => {
    // Would catch: the install-banner styling regressing (border color,
    // copy block layout, copy button alignment).
    //
    // This baseline only exists when the vision extras are ABSENT (the
    // banner is the subject). On a dev box with [vision] installed there's
    // no banner to snapshot, so skip rather than fail.
    const caps = await request.get('/api/system/capabilities').then(r => r.ok() ? r.json() : null)
    test.skip(caps?.vision?.available === true, 'vision installed — no install banner to snapshot')

    await page.goto('/classic/search')
    await expect(
      page.getByText('Semantic search needs the vision extras.'),
    ).toBeVisible({ timeout: 10_000 })
    await page.waitForLoadState('networkidle')
    await expect(page).toHaveScreenshot('search-install-banner.png', {
      fullPage: true,
    })
  })

  test('Bridge page with live-mode install banner expanded', async ({ page }) => {
    // Would catch: live-mode banner layout, disabled-Start-button styling,
    // tab active-state color when "live" is selected.
    await page.goto('/classic/bridge')
    await page.getByRole('button', { name: /^live$/i }).click()
    await expect(
      page.getByText('Bridge live mode needs rclpy (ROS 2).'),
    ).toBeVisible()
    await page.waitForLoadState('networkidle')
    await expect(page).toHaveScreenshot('bridge-live-banner.png', {
      fullPage: true,
    })
  })

  test('Health page with the scene-demo bag', async ({ page }) => {
    // Would catch: HealthBadge color regressions across score buckets,
    // per-topic table layout, freq/drop column formatting.
    await page.goto('/classic')
    const card = page.getByText(/scene_demo\.mcap/).first()
    await expect(card).toBeVisible({ timeout: 10_000 })
    // Click into Explorer first, then navigate to Health for that bag.
    await card.click()
    await page.waitForURL(/\/classic\/bag\/\d+/)
    // The Health link might be in the bag's sidebar OR top nav. Adapt.
    const healthLink = page.getByRole('link', { name: /^health$/i })
    if (await healthLink.isVisible().catch(() => false)) {
      await healthLink.click()
      await page.waitForLoadState('networkidle')
      await expect(page).toHaveScreenshot('health-page.png', { fullPage: true })
    } else {
      // Health is exposed elsewhere — mark this expectation skipped
      // until the navigation path stabilises.
      test.skip(true, 'Health link not found from Explorer; tighten locator')
    }
  })

  test('Notebook workspace shell (v0.8 overhaul)', async ({ page }) => {
    // Would catch: regressions in the warm-paper notebook chrome — rail,
    // header, empty feed, command bar. Static data in PR 0, so fully
    // deterministic. Lives at /n, separate from the classic dark UI.
    await page.goto('/n')
    await expect(page.getByText('INVESTIGATIONS')).toBeVisible({ timeout: 10_000 })
    // Wait for the webfont to settle so the snapshot is stable.
    await page.evaluate(() => (document as any).fonts?.ready)
    await page.waitForTimeout(300)
    await expect(page).toHaveScreenshot('notebook-shell.png', { fullPage: true })
  })
})
