import { test, expect } from '@playwright/test'

// Smoke + behavioural coverage for bugs that historically slipped past
// the unit + API suites. Each test is annotated with the bug it would
// have caught — keep that annotation when adding new cases so the
// rationale stays attached to the test.

test.describe('Search page', () => {
  test('search degrades cleanly by vision state (banner or working search, never an opaque toast)', async ({ page, request }) => {
    // Would have caught: opaque 500 toast when /api/search/frames
    // failed because sentence-transformers wasn't installed.
    //
    // The bug class is "opaque error toast", which must never appear in
    // EITHER vision state. Whether the friendly install banner shows depends
    // on whether the backend actually has the vision extras — so we detect
    // that first (via /api/capabilities) and assert the correct branch
    // strictly. In CI the extras are absent → the banner must show. On a dev
    // box with [vision] installed → no banner, search works. Both are healthy.
    // /api/system/capabilities returns a dict keyed by capability name.
    const caps = await request.get('/api/system/capabilities').then(r => r.ok() ? r.json() : null)
    const visionInstalled = caps?.vision?.available === true

    await page.goto('/search')

    const banner = page.getByText('Semantic search needs the vision extras.')
    if (visionInstalled) {
      // Healthy state: no install banner; search UI is live.
      await page.waitForTimeout(800)
      await expect(banner).toHaveCount(0)
    } else {
      // Missing extras: the friendly banner is what the user must see.
      await expect(banner).toBeVisible({ timeout: 10_000 })
    }

    // The invariant that holds in BOTH states: no raw error toast.
    await expect(page.getByText(/Internal Server Error/i)).toHaveCount(0)
    await expect(page.getByText(/\[object Object\]/)).toHaveCount(0)
  })
})

test.describe('Bridge page', () => {
  test('live mode shows install banner when rclpy missing', async ({ page }) => {
    // Would have caught: live mode used to spawn a subprocess that
    // died silently. Now we pre-check + render an install banner.
    await page.goto('/bridge')

    await page.getByRole('button', { name: /^live$/i }).click()

    await expect(
      page.getByText('Bridge live mode needs rclpy (ROS 2).'),
    ).toBeVisible({ timeout: 5_000 })

    // Start button must be disabled in live mode without the cap.
    const startBtn = page.getByRole('button', { name: /start bridge/i })
    await expect(startBtn).toBeDisabled()
  })

  test('playback bridge: starting + clicking play never shows "[object Object]"', async ({ page, request }) => {
    // Would have caught: bridge_proxy had `request: Any` so FastAPI
    // returned 422 with array-detail, which the dashboard's toast
    // rendered as "play: [object Object]".

    // Make sure no leftover bridge is running on the chosen test port.
    const TEST_BRIDGE_PORT = 9991
    await request.post('/api/bridge/stop').catch(() => {})

    await page.goto('/bridge')

    // Stop any pre-existing bridge from a previous test run.
    const stopButton = page.getByRole('button', { name: /^stop$/i })
    if (await stopButton.isVisible().catch(() => false)) {
      await stopButton.click()
      await expect(stopButton).toBeHidden({ timeout: 10_000 })
    }

    // Fetch an indexed bag to play back.
    const bagsResp = await request.get('/api/bags')
    const bags = await bagsResp.json()
    expect(bags.length).toBeGreaterThan(0)
    const bagPath = bags[0].path

    // Fill in bag path + port (avoid colliding with a real bridge on 9090).
    await page.locator('input[placeholder*="recording.mcap"]').fill(bagPath)
    const portInput = page.locator('input[type="number"]').last()
    await portInput.fill(String(TEST_BRIDGE_PORT))

    await page.getByRole('button', { name: /start bridge/i }).click()

    // Wait until the status banner shows the new running state.
    await expect(
      page.getByText(/playback mode · port \d+ · pid \d+/i),
    ).toBeVisible({ timeout: 15_000 })

    // Click play in the playback control panel.
    await page.getByRole('button', { name: /^play$/i }).click()

    // Wait briefly for the toast (success or error) to actually render,
    // then snapshot every visible alert. Asserting the text content
    // never contains "[object Object]" — that's the regression. Using
    // toHaveCount(0) here is wrong because it polls FOR count=0 and
    // would happily wait out the toast's 8s auto-dismiss.
    await page.waitForTimeout(800)
    const alerts = await page.getByRole('alert').allTextContents()
    expect(alerts.join('\n'), `toast contents: ${JSON.stringify(alerts)}`)
      .not.toContain('[object Object]')

    // Stop the test bridge so the next test starts clean.
    await page.getByRole('button', { name: /^stop$/i }).click().catch(() => {})
  })
})

test.describe('Library page', () => {
  test('loads and lists indexed bags', async ({ page }) => {
    await page.goto('/')
    await expect(page.getByText(/scene_demo\.mcap/i)).toBeVisible({ timeout: 10_000 })
  })
})
