import { CellType } from './types'

// Per-cell-type inline guides, shown by the ? toggle in the cell header.
// CONTRACT: every line here describes what the cell actually does today —
// when a cell's behavior changes, its guide changes in the same PR. No
// aspirational docs.

export interface CellGuide {
  what: string
  rows: { label: string; text: string }[]
}

export const CELL_GUIDES: Partial<Record<CellType, CellGuide>> = {
  plot: {
    what: 'Multi-series line chart of the topic’s numeric fields, downsampled server-side (legend shows total vs plotted).',
    rows: [
      { label: 'Topic', text: 'Header dropdown — any non-image topic. Numeric fields become series automatically.' },
      { label: 'Hover', text: 'Moves the time cursor. On Shared time it drives every linked cell (plots, images, sync).' },
      { label: 'Shared / Own time', text: 'Header pill. Own time detaches this cell from the shared cursor.' },
      { label: 'Drag', text: 'Brushes a time window → toolbar with ✦ Explain (grounded explanation of the window: rates, gaps, health findings — AI narrative when the copilot extra + key are installed, rule-based otherwise), Export range (self-contained incident-report HTML), and Clear.' },
    ],
  },
  transform: {
    what: 'Derives a new signal from one numeric column and plots it live (preview over ~400 sampled points).',
    rows: [
      { label: 'Common tab', text: 'Derivative (d/dt) · Integral · Moving average (window N samples) · Low-pass (alpha 0.01–1; smaller = smoother) · Scale (factor) · Absolute value · Shift (±periods).' },
      { label: 'Expression tab', text: 'Free-form Polars — pl.col(), pl.lit(), arithmetic, chained methods (.abs(), .diff(), .rolling_mean(N), .pow(2), .sqrt()…). Imports, dunders, and IO are rejected server-side.' },
      { label: 'Column', text: 'Any numeric field of the selected topic.' },
      { label: 'vs Query cell', text: 'Transform live-previews while you tweak; the Query cell runs on demand and adds a data table.' },
    ],
  },
  stats: {
    what: 'min / mean / max / σ for every numeric field of the topic.',
    rows: [
      { label: 'Sampling', text: 'Computed client-side over ≤500 sampled points, not the full topic — the footer states the sample size. For exact aggregates over everything, export and run offline.' },
    ],
  },
  health: {
    what: 'The bag’s health report: 0–100 score, per-check breakdown, and the top issues.',
    rows: [
      { label: 'Score ring', text: 'Overall score. ≥90 good · 80–89 warn · <80 bad (same buckets as the rail dots).' },
      { label: 'Checks', text: 'One row per check dimension (rate stability, gaps, ordering…). A dimension’s score is its WORST topic — a single bad topic can’t hide in an average.' },
      { label: 'Issues', text: 'Individual findings with severity and the affected topic.' },
    ],
  },
  sync: {
    what: 'Time-aligns 2+ topics with nearest-neighbor matching and shows the first aligned rows.',
    rows: [
      { label: 'Method', text: 'nearest, tolerance 50 ms — each output row pairs samples within the tolerance; ties prefer the later sample.' },
      { label: 'Rows', text: 'First 8 aligned rows. The full engine (methods, tolerances, buffering policies) is bf.sync() in the Python API.' },
      { label: 'Cursor', text: 'On Shared time, the row nearest the linked cursor highlights.' },
    ],
  },
  image: {
    what: 'Frame viewer for a camera topic (Image / CompressedImage), decoded server-side.',
    rows: [
      { label: 'Scrubber', text: 'On Own time, the slider steps through frames 0…N−1 (message index).' },
      { label: 'Shared time', text: 'The frame follows the linked cursor from other cells — brush a plot, watch the camera track it. The slider disables while following.' },
      { label: 'Topic', text: 'Header dropdown appears when the bag has multiple camera topics.' },
    ],
  },
  scene: {
    what: 'Live 3D render of a point-cloud topic plus TF frame triads, at the bag’s mid-time snapshot.',
    rows: [
      { label: 'Navigate', text: 'Drag to orbit · scroll to zoom · right-drag to pan.' },
      { label: 'Points', text: 'Sampled to ≤8,000 points for the cell (the caption shows the count actually drawn).' },
      { label: 'Time', text: 'Renders one snapshot at the bag’s midpoint — it does not yet follow the linked cursor. The classic Explorer’s Scene tab (/classic) has playback and per-topic controls.' },
      { label: 'TF', text: 'Frame axes come from /tf + /tf_static resolved at the snapshot time.' },
    ],
  },
  search: {
    what: 'Natural-language search over camera frames using local CLIP (ViT-B/32) embeddings.',
    rows: [
      { label: 'Prerequisites', text: 'The [vision] extra installed, then index frames once per bag: resurrector index-frames <bag>. The cell tells you which step is missing.' },
      { label: 'Query', text: 'Describe the moment (“gripper near the table”, “bright outdoor scene”). Top 24 frames return with similarity scores.' },
      { label: 'Min similarity', text: 'Slider 0–0.50 filters the grid. CLIP scores are relative — 0.25+ is usually a real match; tune per bag.' },
      { label: 'Open frame →', text: 'Adds an image cell pinned to that frame.' },
    ],
  },
  compare: {
    what: 'Overlays one topic across several bags on a shared relative-time axis — one series per bag.',
    rows: [
      { label: 'Bags', text: 'Click chips to include 2+ bags. Topics offered are the union across the selected bags.' },
      { label: 'Value', text: 'Which numeric column to overlay. Defaults to the first column whose values actually vary (never a timestamp).' },
      { label: 'Time', text: 'Each bag’s clock is shifted to start at 0 s so runs align at their beginnings.' },
    ],
  },
  query: {
    what: 'Free-form exploration: write any Polars expression over the topic and run it on demand.',
    rows: [
      { label: 'Columns', text: 'The topic’s numeric fields as chips — click to insert pl.col("…") at the cursor.' },
      { label: 'Run', text: 'Button or ⌘⏎ / Ctrl+⏎. Result renders as a chart plus the first 8 rows as a table (≤400 sampled points).' },
      { label: 'Allowed', text: 'pl.col(), pl.lit(), arithmetic, chained methods (.abs(), .diff(), .rolling_mean(N), .pow(2)…). Imports, dunders, and IO are rejected server-side — errors surface right in the cell.' },
      { label: 'Scope', text: 'One topic per query today. Cross-topic joins: use the sync cell or bf.sync() in Python.' },
    ],
  },
}
