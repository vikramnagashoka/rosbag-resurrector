// Static sample notebooks for PR 0 (shell only). Replaced by real
// backend-backed notebooks in later PRs — this just lets the chrome
// render against representative data while the wiring lands.

import { Notebook } from './types'

export const SAMPLE_NOTEBOOKS: Notebook[] = [
  {
    id: 'nb-gripper',
    title: 'Gripper slip — root cause',
    bag: 'dropbox_pick_fail_03.mcap',
    health: 71,
    tier: 'bad',
    durationLabel: '42.8s',
    durationSec: 42.8,
    topicCount: 7,
    messageCount: 9910,
    cells: [],
  },
  {
    id: 'nb-pickfail',
    title: 'Pick-failure investigation',
    bag: 'dropbox_pick_fail_03.mcap',
    health: 71,
    tier: 'bad',
    durationLabel: '42.8s',
    durationSec: 42.8,
    topicCount: 7,
    messageCount: 9910,
    cells: [],
  },
  {
    id: 'nb-warehouse',
    title: 'Warehouse loop QC',
    bag: 'warehouse_loop_2026-05-11.mcap',
    health: 93,
    tier: 'good',
    durationLabel: '5m 02s',
    durationSec: 302,
    topicCount: 11,
    messageCount: 184000,
    cells: [],
  },
  {
    id: 'nb-imu',
    title: 'IMU rate anomaly',
    bag: 'v06_qc_slow_imu.mcap',
    health: 84,
    tier: 'warn',
    durationLabel: '18.2s',
    durationSec: 18.2,
    topicCount: 5,
    messageCount: 4200,
    cells: [],
  },
]
