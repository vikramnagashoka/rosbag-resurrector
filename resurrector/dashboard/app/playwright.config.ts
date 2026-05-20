import { defineConfig, devices } from '@playwright/test'

// E2E + visual regression tests for the dashboard.
//
// Boots `resurrector dashboard` against a temp index pointed at a
// known synthetic bag (so the test is hermetic — doesn't depend on
// whatever's in the developer's ~/.resurrector). Each spec drives
// the real React app in a real browser and asserts on user-visible
// behaviour, not API contracts.
//
// Run:  bun --bun run e2e
// Update screenshot baselines:  bun --bun run e2e -- --update-snapshots
export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: 1,
  reporter: process.env.CI ? 'github' : 'list',
  use: {
    baseURL: 'http://127.0.0.1:8967',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    actionTimeout: 5000,
  },
  webServer: {
    // Boots the dashboard against a hermetic fixture index. The
    // global setup script populates it before the server starts.
    command: 'bash e2e/run-dashboard.sh',
    url: 'http://127.0.0.1:8967/api/system/capabilities',
    reuseExistingServer: false,
    timeout: 60_000,
    stdout: 'pipe',
    stderr: 'pipe',
  },
  expect: {
    toHaveScreenshot: {
      // Slightly relaxed pixel threshold — anti-aliasing across
      // macOS / Linux can cause sub-pixel drift on font rendering.
      maxDiffPixelRatio: 0.02,
    },
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'], viewport: { width: 1280, height: 800 } },
    },
  ],
})
