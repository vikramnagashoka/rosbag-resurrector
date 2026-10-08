// Form rules shared by both export dialogs (classic components/ExportDialog
// and notebook/ExportDialog). LeRobot export ignores `sync`: it always
// resamples every topic causally onto its own fps grid. It also reads
// `downsample_hz` as the dataset fps, rounded to an integer (default 30).
// Mirrors Exporter.export in resurrector/core/export.py.

import type { Capability, CapabilityMap, ExportPreset } from './api'

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

// ---- Preset availability --------------------------------------------------
// /api/export-presets says why each unavailable preset can't run, and the
// reason names the fix. Usually that is the extra's pip command ("Install
// with: pip install 'rosbag-resurrector[all-exports]'"). Where pip can't
// deliver the dependency on this interpreter (tensorflow has no wheel for
// Python 3.14, Intel macOS on 3.13 or Windows ARM64), the reason puts a step
// before it: "Use Python 3.10-3.13 on ..., then: pip install ..."
// (tensorflow_install_hint in resurrector/core/export.py). The dialogs must
// not offer the command as the fix there.

function installCommandFor(preset: ExportPreset, caps: CapabilityMap | null): string | undefined {
  const byName: Record<string, Capability | undefined> = caps ?? {}
  for (const extra of preset.extras_required) {
    // The [all-exports] extra is the `all_exports` capability.
    const cap = byName[extra.replace(/-/g, '_')]
    if (cap) return cap.install_command
  }
  return undefined
}

/**
 * True when `preset` is unavailable and installing its extra on this
 * interpreter would not change that: the reason asks for another step
 * first, or doesn't name the extra's install command at all. A preset
 * without a reason reads the old way, with the missing extra as the only
 * gate.
 */
export function installWontFix(preset: ExportPreset, caps: CapabilityMap | null): boolean {
  const reason = preset.unavailable_reason
  if (preset.available || !reason) return false
  if (/\bthen: /.test(reason)) return true
  const command = installCommandFor(preset, caps)
  return command !== undefined && !reason.includes(command)
}

export interface ExtraGap {
  /** Unavailable presets that installing the extra unlocks. */
  fixable: ExportPreset[]
  /** Unavailable presets the extra can't unlock on this interpreter. */
  stuck: ExportPreset[]
}

/** The unavailable presets `extra` gates, split by whether installing it fixes them. */
export function extraGap(presets: ExportPreset[], extra: string, caps: CapabilityMap | null): ExtraGap {
  const blocked = presets.filter(p => !p.available && p.extras_required.includes(extra))
  return {
    fixable: blocked.filter(p => !installWontFix(p, caps)),
    stuck: blocked.filter(p => installWontFix(p, caps)),
  }
}

export function hasGap(gap: ExtraGap): boolean {
  return gap.fixable.length + gap.stuck.length > 0
}

/** Preset dropdown label: "(extras not installed)" only where installing them helps. */
export function presetOptionLabel(preset: ExportPreset, caps: CapabilityMap | null): string {
  if (preset.available) return preset.name
  return installWontFix(preset, caps)
    ? `${preset.name} (unavailable here, see below)`
    : `${preset.name} (extras not installed)`
}
