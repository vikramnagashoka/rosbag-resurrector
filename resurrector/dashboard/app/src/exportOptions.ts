// Form rules shared by both export dialogs (classic components/ExportDialog
// and notebook/ExportDialog). LeRobot export ignores `sync`: it always
// resamples every topic causally onto its own fps grid. It also reads
// `downsample_hz` as the dataset fps, rounded to an integer (default 30).
// Mirrors Exporter.export in resurrector/core/export.py.

export const LEROBOT_DEFAULT_FPS = 30

export const LEROBOT_SYNC_NOTE =
  'Always on for LeRobot: every topic is resampled onto the frame-rate grid ' +
  '(latest sample at or before each frame).'

export const LEROBOT_MIN_FPS_ERROR = 'LeRobot needs at least 1 fps.'
export const LEROBOT_FPS_NOT_A_NUMBER = 'Frame rate must be a number.'

export function isLerobot(format: string): boolean {
  return format === 'lerobot'
}

function parseRate(rate: string): number | undefined {
  return rate.trim() ? parseFloat(rate) : undefined
}

/**
 * Why the rate field must block Export, or null when it can be sent.
 *
 * Only LeRobot has a floor: the backend reads the fps as
 * `downsample_hz or 30`, so a value that rounds to 0 would silently become
 * a 30 fps dataset. Blank is fine (the backend default applies).
 */
export function rateError(format: string, rate: string): string | null {
  const n = parseRate(rate)
  if (!isLerobot(format) || n === undefined) return null
  if (!Number.isFinite(n)) return LEROBOT_FPS_NOT_A_NUMBER
  return Math.round(n) >= 1 ? null : LEROBOT_MIN_FPS_ERROR
}

/**
 * The `sync` / `downsample_hz` part of an export request, matching what the
 * dialog shows. For LeRobot, no `sync` (the backend would ignore whatever
 * stale value the hidden state holds) and the fps already rounded, so the
 * request names the frame rate the dataset will actually get.
 *
 * Throws when `rateError` would block the form, so no LeRobot request ever
 * carries an fps of 0 even if a caller skips the disabled-button gate.
 */
export function syncAndRateParams(
  format: string,
  sync: boolean,
  rate: string,
): { sync?: boolean; downsample_hz?: number } {
  const n = parseRate(rate)
  if (!isLerobot(format)) return { sync, downsample_hz: n }
  const error = rateError(format, rate)
  if (error) throw new Error(error)
  return { downsample_hz: n === undefined ? undefined : Math.round(n) }
}

/** Shown under the fps field when LeRobot will round a fractional value. */
export function fpsRoundingHint(format: string, rate: string): string | null {
  const n = parseRate(rate)
  if (!isLerobot(format) || n === undefined || rateError(format, rate)) return null
  if (Number.isInteger(n)) return null
  return `Rounded to ${Math.round(n)} fps.`
}
