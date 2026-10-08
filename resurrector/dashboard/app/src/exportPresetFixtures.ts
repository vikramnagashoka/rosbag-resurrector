// Test payloads for the export dialogs, shaped like GET /api/export-presets
// and GET /api/system/capabilities (list_export_presets and
// get_system_capabilities in resurrector/dashboard/api.py; the failed
// export body at the end has its own note), with the exact
// strings resurrector/core/export.py and core/capabilities.py produce.
// tests/test_rlds_capability.py pins those strings on the Python side.
// Shared by exportDialogs.test.tsx and e2e/interactions.spec.ts.

import type { Capability, ExportPreset } from './api'

export const ALL_EXPORTS_CMD = "pip install 'rosbag-resurrector[all-exports]'"
const LEROBOT_CMD = "pip install 'rosbag-resurrector[lerobot]'"
const TF_WHERE =
  'Python 3.10-3.13 on x86_64/aarch64 Linux, Apple-silicon macOS or x64 ' +
  'Windows, or Python 3.10-3.12 on Intel macOS'

// export_dependency_problem('rlds') where [all-exports] can't install
// tensorflow (Python 3.14 here), and where it can but hasn't.
export const RLDS_NO_WHEEL_REASON =
  'RLDS export needs tensorflow, which publishes no stable wheel for Python ' +
  `3.14 on Apple-silicon macOS. Use ${TF_WHERE}, then: ${ALL_EXPORTS_CMD}`
export const RLDS_NOT_INSTALLED_REASON =
  `RLDS export needs tensorflow, which isn't installed. Install with: ${ALL_EXPORTS_CMD}`
export const ZARR_NOT_INSTALLED_REASON =
  `Zarr export requires the zarr package. Install with: ${ALL_EXPORTS_CMD}`

// _all_exports_description() with and without the platform explanation.
const ALL_EXPORTS_DESCRIPTION = 'Zarr and RLDS (TFRecord) export formats'
export const ALL_EXPORTS_NO_WHEEL_DESCRIPTION =
  `${ALL_EXPORTS_DESCRIPTION}. On this interpreter the extra installs Zarr only: ` +
  'tensorflow publishes no stable wheel for Python 3.14 on Apple-silicon macOS. ' +
  `For RLDS, use ${TF_WHERE}.`

/**
 * - `no-wheel`: zarr installed, no tensorflow wheel for this interpreter.
 *   Only rlds is off and the extra's pip command can't change that.
 * - `no-wheel-no-zarr`: as above with zarr missing too. The pip command
 *   still unlocks the Zarr preset, not rlds.
 * - `tensorflow-missing`: a supported interpreter where the extra simply
 *   isn't installed yet; the pip command fixes rlds.
 */
export type ExportEnv = 'no-wheel' | 'no-wheel-no-zarr' | 'tensorflow-missing'

function preset(
  fields: Pick<ExportPreset, 'name' | 'format' | 'description'> & Partial<ExportPreset>,
): ExportPreset {
  return {
    sync: true,
    sync_method: 'nearest',
    downsample_hz: null,
    topic_filter: null,
    extras_required: [],
    available: true,
    unavailable_reason: null,
    ...fields,
  }
}

function unavailable(reason: string | null): Pick<ExportPreset, 'available' | 'unavailable_reason'> {
  return { available: reason === null, unavailable_reason: reason }
}

export function exportPresetsFor(env: ExportEnv): ExportPreset[] {
  const rldsReason = env === 'tensorflow-missing' ? RLDS_NOT_INSTALLED_REASON : RLDS_NO_WHEEL_REASON
  const zarrReason = env === 'no-wheel-no-zarr' ? ZARR_NOT_INSTALLED_REASON : null
  return [
    preset({
      name: 'lerobot', format: 'lerobot', downsample_hz: 30,
      description: 'LeRobot-format dataset for robot-learning training. Resampled onto a uniform 30 fps grid, camera topics as video.',
      extras_required: ['lerobot'],
    }),
    preset({
      name: 'rlds', format: 'rlds', downsample_hz: 10,
      description: 'RLDS / TFRecord for RT-2 / OpenX-style training pipelines. Time-synced, 10 Hz.',
      extras_required: ['all-exports'], ...unavailable(rldsReason),
    }),
    preset({
      name: 'training-tabular', format: 'parquet', downsample_hz: 50, topic_filter: 'non-images',
      description: 'Numerical sensor data for classical ML. Parquet, time-synced at 50 Hz, image topics excluded.',
    }),
    preset({
      name: 'camera-only', format: 'hdf5', sync: false, topic_filter: 'images',
      description: 'Image topics only, native rates. HDF5 for CV training data prep.',
    }),
    preset({
      name: 'multimodal', format: 'zarr',
      description: 'All topics, time-synced, Zarr for chunked multimodal datasets.',
      extras_required: ['all-exports'], ...unavailable(zarrReason),
    }),
  ]
}

export function capabilitiesFor(env: ExportEnv): Record<string, Capability> {
  const caps: Capability[] = [
    {
      name: 'vision', available: true,
      install_command: "pip install 'rosbag-resurrector[vision]'",
      description: 'Semantic frame search via CLIP embeddings',
    },
    {
      name: 'bridge_live', available: false,
      install_command: 'Install ROS 2 (which provides rclpy). See https://docs.ros.org/en/jazzy/Installation.html',
      description: 'Record / relay topics from a running ROS 2 system in real time',
    },
    {
      name: 'ros1_convert', available: true,
      install_command: 'brew install mcap   # macOS',
      description: 'Auto-convert ROS 1 .bag files to MCAP during scan',
    },
    {
      name: 'all_exports', available: false,
      install_command: ALL_EXPORTS_CMD,
      description: env === 'tensorflow-missing' ? ALL_EXPORTS_DESCRIPTION : ALL_EXPORTS_NO_WHEEL_DESCRIPTION,
    },
    {
      name: 'lerobot', available: true,
      install_command: LEROBOT_CMD,
      description: 'LeRobot v3 dataset export (state, actions, camera video). Needs Python 3.12+',
    },
    {
      name: 'publish', available: true,
      install_command: "pip install 'rosbag-resurrector[publish]'",
      description: 'Publish datasets to the HuggingFace Hub with an auto card',
    },
    {
      name: 'copilot', available: true,
      install_command: "pip install 'rosbag-resurrector[copilot]'",
      description: "'Ask your bag' — grounded natural-language analysis",
    },
  ]
  return Object.fromEntries(caps.map(c => [c.name, c]))
}

// POST /api/bags/{id}/export when a writer can't store some columns: the
// 422 body _export_error_handler (resurrector/dashboard/api.py) returns,
// carrying the message ExportError (resurrector/core/export.py) formats,
// here for HDF5 and two list columns. tests/test_export_error_surfacing.py
// pins the body's shape and this exact message on the Python side.
export const EXPORT_FAILED_FILE = '/data/exports/run_7/lidar_scan.h5'
const LIST_REASON =
  'HDF5 does not support dtype object containing sequences (e.g. variable-length lists)'
export const EXPORT_FAILED_COLUMN_LINES = [
  `  - ranges: TypeError: ${LIST_REASON}`,
  `  - intensities: TypeError: ${LIST_REASON}`,
]
export const EXPORT_COLUMN_FAILURES_MESSAGE =
  `2 column(s) could not be written to ${EXPORT_FAILED_FILE}:\n` +
  `${EXPORT_FAILED_COLUMN_LINES.join('\n')}\n` +
  'That file is partial: the columns above are missing or incomplete; every ' +
  'other column is complete. The export stopped there, so any later topics, ' +
  'splits or bags were not exported. To keep these columns, export to ' +
  'Parquet, which stores every column type.'
export const EXPORT_COLUMN_FAILURES_BODY = {
  detail: {
    kind: 'export_column_failures',
    message: EXPORT_COLUMN_FAILURES_MESSAGE,
    output: EXPORT_FAILED_FILE,
    failures: ['ranges', 'intensities'].map(column => ({
      column, error_type: 'TypeError', message: LIST_REASON,
    })),
  },
}
