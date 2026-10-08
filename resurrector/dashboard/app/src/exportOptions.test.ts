import { describe, expect, it } from 'vitest'
import type { CapabilityMap, ExportPreset } from './api'
import {
  LEROBOT_FPS_NOT_A_NUMBER,
  LEROBOT_MIN_FPS_ERROR,
  extraGap,
  fpsRoundingHint,
  installWontFix,
  presetOptionLabel,
  rateError,
  syncAndRateParams,
} from './exportOptions'
import {
  RLDS_NOT_INSTALLED_REASON,
  RLDS_NO_WHEEL_REASON,
  ZARR_NOT_INSTALLED_REASON,
  capabilitiesFor,
  exportPresetsFor,
} from './exportPresetFixtures'

describe('installWontFix', () => {
  const caps = capabilitiesFor('no-wheel') as CapabilityMap
  const presets = exportPresetsFor('no-wheel-no-zarr')
  const byName = (name: string) => presets.find(p => p.name === name) as ExportPreset
  const withReason = (name: string, reason: string | null): ExportPreset =>
    ({ ...byName(name), available: false, unavailable_reason: reason })

  it('is false where the reason names the extra as the whole fix', () => {
    expect(installWontFix(withReason('multimodal', ZARR_NOT_INSTALLED_REASON), caps)).toBe(false)
    expect(installWontFix(withReason('rlds', RLDS_NOT_INSTALLED_REASON), caps)).toBe(false)
    // lerobot_export.INSTALL_HINT: no "Install with:", still just the command.
    const lerobotHint =
      "LeRobot export needs the [lerobot] extra (Python 3.12+): pip install 'rosbag-resurrector[lerobot]'"
    expect(installWontFix(withReason('lerobot', lerobotHint), caps)).toBe(false)
  })

  it('is true where the reason puts another step before the command', () => {
    expect(installWontFix(byName('rlds'), caps)).toBe(true)
    // Holds before capabilities load, too.
    expect(installWontFix(byName('rlds'), null)).toBe(true)
  })

  it('is true where the reason does not name the extra install command', () => {
    expect(installWontFix(withReason('rlds', 'RLDS export is not supported on Windows ARM64.'), caps)).toBe(true)
  })

  it('reads a preset without a reason the old way: the extra is the fix', () => {
    expect(installWontFix(withReason('rlds', null), caps)).toBe(false)
    expect(installWontFix(byName('camera-only'), caps)).toBe(false)
  })

  it('splits an extra\'s blocked presets and labels them to match', () => {
    const gap = extraGap(presets, 'all-exports', caps)
    expect(gap.fixable.map(p => p.name)).toEqual(['multimodal'])
    expect(gap.stuck.map(p => p.name)).toEqual(['rlds'])
    expect(extraGap(presets, 'lerobot', caps)).toEqual({ fixable: [], stuck: [] })
    expect(presetOptionLabel(byName('multimodal'), caps)).toBe('multimodal (extras not installed)')
    expect(presetOptionLabel(byName('rlds'), caps)).toBe('rlds (unavailable here, see below)')
    expect(presetOptionLabel(byName('camera-only'), caps)).toBe('camera-only')
    expect(byName('rlds').unavailable_reason).toBe(RLDS_NO_WHEEL_REASON)
  })
})

describe('syncAndRateParams', () => {
  it('passes sync and the raw rate through for chunk-streaming formats', () => {
    expect(syncAndRateParams('parquet', true, '12.5')).toEqual({ sync: true, downsample_hz: 12.5 })
    expect(syncAndRateParams('hdf5', false, '  ')).toEqual({ sync: false, downsample_hz: undefined })
  })

  it('drops a leftover sync flag for lerobot, which ignores it', () => {
    expect(syncAndRateParams('lerobot', true, '')).toEqual({ downsample_hz: undefined })
    expect(syncAndRateParams('lerobot', false, '')).not.toHaveProperty('sync')
  })

  it('sends the whole-number fps lerobot will actually use', () => {
    expect(syncAndRateParams('lerobot', false, '30')).toEqual({ downsample_hz: 30 })
    expect(syncAndRateParams('lerobot', false, '14.6')).toEqual({ downsample_hz: 15 })
  })
})

describe('fpsRoundingHint', () => {
  it('names the rounded fps only for a fractional lerobot rate', () => {
    expect(fpsRoundingHint('lerobot', '14.6')).toBe('Rounded to 15 fps.')
    expect(fpsRoundingHint('lerobot', '15')).toBeNull()
    expect(fpsRoundingHint('lerobot', '')).toBeNull()
    expect(fpsRoundingHint('lerobot', 'abc')).toBeNull()
    expect(fpsRoundingHint('parquet', '14.6')).toBeNull()
  })
})

// The backend reads downsample_hz with `downsample_hz or 30`, so a 0 that
// the dialog rounded down to would silently become a 30 fps dataset. A rate
// that rounds below 1 must block Export instead of being sent.
describe('LeRobot fps that rounds below 1', () => {
  const cases: Array<{
    rate: string
    error: string | null
    hint: string | null
    sent?: number
  }> = [
    { rate: '0.4', error: LEROBOT_MIN_FPS_ERROR, hint: null },
    { rate: '0', error: LEROBOT_MIN_FPS_ERROR, hint: null },
    { rate: '-2', error: LEROBOT_MIN_FPS_ERROR, hint: null },
    { rate: '0.5', error: null, hint: 'Rounded to 1 fps.', sent: 1 },
    { rate: '', error: null, hint: null, sent: undefined },
    { rate: '29.97', error: null, hint: 'Rounded to 30 fps.', sent: 30 },
  ]

  it.each(cases)('rate $rate', ({ rate, error, hint, sent }) => {
    expect(rateError('lerobot', rate)).toBe(error)
    expect(fpsRoundingHint('lerobot', rate)).toBe(hint)
    if (error) {
      expect(() => syncAndRateParams('lerobot', false, rate)).toThrow(error)
    } else {
      expect(syncAndRateParams('lerobot', false, rate)).toEqual({ downsample_hz: sent })
    }
  })

  it('never builds a lerobot request carrying fps 0', () => {
    for (const rate of ['0', '0.0', '0.49', '-0.4']) {
      expect(() => syncAndRateParams('lerobot', false, rate)).toThrow(LEROBOT_MIN_FPS_ERROR)
    }
  })

  it('blocks a rate that is not a number instead of sending NaN', () => {
    expect(rateError('lerobot', 'abc')).toBe(LEROBOT_FPS_NOT_A_NUMBER)
    expect(() => syncAndRateParams('lerobot', false, 'abc')).toThrow(LEROBOT_FPS_NOT_A_NUMBER)
  })

  it('leaves other formats alone: a sub-1 Hz downsample is a real choice there', () => {
    expect(rateError('parquet', '0.4')).toBeNull()
    expect(syncAndRateParams('parquet', false, '0.4')).toEqual({ sync: false, downsample_hz: 0.4 })
  })
})
