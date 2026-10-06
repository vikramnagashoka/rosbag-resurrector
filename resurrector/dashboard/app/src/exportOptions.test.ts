import { describe, expect, it } from 'vitest'
import { fpsRoundingHint, syncAndRateParams } from './exportOptions'

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
