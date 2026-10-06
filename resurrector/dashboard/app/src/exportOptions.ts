// Form rules shared by both export dialogs (classic components/ExportDialog
// and notebook/ExportDialog). LeRobot export ignores `sync`: it always
// resamples every topic causally onto its own fps grid. It also reads
// `downsample_hz` as the dataset fps, rounded to an integer (default 30).
// Mirrors Exporter.export in resurrector/core/export.py.

export const LEROBOT_DEFAULT_FPS = 30

export const LEROBOT_SYNC_NOTE =
  'Always on for LeRobot: every topic is resampled onto the frame-rate grid ' +
  '(latest sample at or before each frame).'

export function isLerobot(format: string): boolean {
  return format === 'lerobot'
}

function parseRate(rate: string): number | undefined {
  return rate.trim() ? parseFloat(rate) : undefined
}

/**
 * The `sync` / `downsample_hz` part of an export request, matching what the
 * dialog shows. For LeRobot, no `sync` (the backend would ignore whatever
 * stale value the hidden state holds) and the fps already rounded, so the
 * request names the frame rate the dataset will actually get.
 */
export function syncAndRateParams(
  format: string,
  sync: boolean,
  rate: string,
): { sync?: boolean; downsample_hz?: number } {
  const n = parseRate(rate)
  if (!isLerobot(format)) return { sync, downsample_hz: n }
  return { downsample_hz: n === undefined ? undefined : Math.round(n) }
}

/** Shown under the fps field when LeRobot will round a fractional value. */
export function fpsRoundingHint(format: string, rate: string): string | null {
  const n = parseRate(rate)
  if (!isLerobot(format) || n === undefined) return null
  if (!Number.isFinite(n) || Number.isInteger(n)) return null
  return `Rounded to ${Math.round(n)} fps.`
}
