// Typed client for the dashboard REST API.
//
// Centralizes:
//  - URL construction
//  - JSON parsing
//  - error handling — 4xx/5xx throw `ApiError` instead of being silently
//    swallowed as arbitrary data
//
// Every page in the dashboard imports from this module. When we add a
// new endpoint, it goes here first; pages only deal with typed helpers.

export class ApiError extends Error {
  readonly status: number
  readonly detail: unknown
  constructor(status: number, detail: unknown, fallback: string) {
    const message =
      typeof detail === 'object' && detail !== null && 'detail' in (detail as any)
        ? String((detail as any).detail)
        : typeof detail === 'string'
        ? detail
        : fallback
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.detail = detail
  }
}

async function request<T>(
  method: string,
  path: string,
  opts?: { body?: unknown; query?: Record<string, string | number | boolean | undefined | null>; expectBlob?: boolean },
): Promise<T> {
  const q = opts?.query
    ? '?' +
      Object.entries(opts.query)
        .filter(([, v]) => v !== undefined && v !== null && v !== '')
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
        .join('&')
    : ''
  const init: RequestInit = { method, headers: {} }
  if (opts?.body !== undefined) {
    ;(init.headers as Record<string, string>)['Content-Type'] = 'application/json'
    init.body = JSON.stringify(opts.body)
  }
  const res = await fetch(path + q, init)
  if (!res.ok) {
    let detail: unknown = res.statusText
    try {
      detail = await res.json()
    } catch {
      // not JSON; keep statusText
    }
    throw new ApiError(res.status, detail, `${method} ${path} failed (${res.status})`)
  }
  if (opts?.expectBlob) {
    return (await res.blob()) as unknown as T
  }
  return (await res.json()) as T
}

// ---------- Types ----------

export interface Bag {
  id: number
  path: string
  duration_sec: number
  size_bytes: number
  message_count: number
  health_score: number | null
  topics: { name: string; message_type: string; message_count: number }[]
  tags: { key: string; value: string }[]
}

export interface TopicRow {
  [column: string]: unknown
}

export interface TopicDataResponse {
  topic: string
  total: number
  columns: string[]
  data: TopicRow[]
  downsampled: boolean
  max_points?: number
  offset?: number
  limit?: number
}

export interface HealthReport {
  score: number
  issues: Array<{
    check: string
    severity: string
    message: string
    topic?: string
    start_time?: number
    end_time?: number
    details?: unknown
  }>
  recommendations: string[]
  topic_scores: Record<string, { score: number; issue_count: number }>
}

export interface Annotation {
  id: number
  bag_id: number
  topic?: string | null
  timestamp_ns: number
  text: string
  created_at?: string
  updated_at?: string
}

export interface Dataset {
  id: number
  name: string
  description: string
  created_at: string
  updated_at: string
  versions?: Array<{ version: string; created_at: string; export_format: string }>
}

export interface BridgeStatus {
  running: boolean
  mode?: string
  port?: number
  pid?: number
  exited?: boolean
  return_code?: number
}

export interface FrameSearchResult {
  query: string
  mode: 'frames' | 'clips'
  results: Array<Record<string, unknown>>
}

export interface FrameIndexStatus {
  bag_id: number
  indexed: boolean
  frame_count: number
  topics_indexed: string[]
}

// ---------- Endpoints ----------

export const api = {
  // Bags
  listBags: (query?: { search?: string; after?: string; before?: string; has_topic?: string; min_health?: number }) =>
    request<Bag[]>('GET', '/api/bags', { query }),
  getBag: (id: number) => request<Bag>('GET', `/api/bags/${id}`),
  getBagHealth: (id: number) => request<HealthReport>('GET', `/api/bags/${id}/health`),
  getTopicData: (
    bagId: number,
    topic: string,
    opts?: { startSec?: number; endSec?: number; maxPoints?: number; limit?: number; offset?: number },
  ) => {
    const t = topic.startsWith('/') ? topic.slice(1) : topic
    return request<TopicDataResponse>('GET', `/api/bags/${bagId}/topics/${t}`, {
      query: {
        start_sec: opts?.startSec,
        end_sec: opts?.endSec,
        max_points: opts?.maxPoints,
        limit: opts?.limit,
        offset: opts?.offset,
      },
    })
  },
  getSyncedData: (bagId: number, topics: string[], method = 'nearest', toleranceMs = 50, limit = 5000) =>
    request<TopicDataResponse>('GET', `/api/bags/${bagId}/sync`, {
      query: { topics: topics.join(','), method, tolerance_ms: toleranceMs, limit },
    }),
  exportBag: (
    bagId: number,
    body: { topics: string[]; format: string; output_dir: string; sync?: boolean; sync_method?: string; downsample_hz?: number },
  ) => request<{ output: string }>('POST', `/api/bags/${bagId}/export`, { body, query: {
    topics: body.topics.join(','),
    format: body.format,
    output_dir: body.output_dir,
    sync: body.sync,
    sync_method: body.sync_method,
    downsample_hz: body.downsample_hz,
  } }),
  frameUrl: (bagId: number, topic: string, frameIndex: number, width?: number) => {
    const t = topic.startsWith('/') ? topic.slice(1) : topic
    const w = width ? `?width=${width}` : ''
    return `/api/bags/${bagId}/topics/${t}/frame/${frameIndex}${w}`
  },

  // Scan
  triggerScan: (path: string) => request<{ scanned: number; indexed: number; errors: unknown[] }>(
    'POST', `/api/scan`, { query: { path } },
  ),

  // Search
  searchFrames: (
    q: string,
    opts?: { topK?: number; bagId?: number; minSimilarity?: number; clips?: boolean; clipDuration?: number },
  ) =>
    request<FrameSearchResult>('GET', `/api/search/frames`, {
      query: {
        q,
        top_k: opts?.topK,
        bag_id: opts?.bagId,
        min_similarity: opts?.minSimilarity,
        clips: opts?.clips,
        clip_duration: opts?.clipDuration,
      },
    }),
  getFrameIndexStatus: (bagId: number) =>
    request<FrameIndexStatus>('GET', `/api/bags/${bagId}/frame-index-status`),

  // Annotations
  listAnnotations: (bagId: number, topic?: string) =>
    request<{ annotations: Annotation[] }>('GET', `/api/bags/${bagId}/annotations`, {
      query: { topic },
    }),
  createAnnotation: (bagId: number, body: { timestamp_ns: number; text: string; topic?: string }) =>
    request<Annotation>('POST', `/api/bags/${bagId}/annotations`, { body }),
  updateAnnotation: (id: number, text: string) =>
    request<Annotation>('PATCH', `/api/annotations/${id}`, { body: { text } }),
  deleteAnnotation: (id: number) => request<{ deleted: number }>('DELETE', `/api/annotations/${id}`),

  // Datasets
  listDatasets: () => request<{ datasets: Dataset[] }>('GET', `/api/datasets`),
  getDataset: (name: string) => request<Dataset>('GET', `/api/datasets/${encodeURIComponent(name)}`),
  createDataset: (body: { name: string; description?: string }) =>
    request<Dataset>('POST', `/api/datasets`, { body }),
  deleteDataset: (name: string) =>
    request<{ deleted: string }>('DELETE', `/api/datasets/${encodeURIComponent(name)}`),
  createDatasetVersion: (name: string, body: Record<string, unknown>) =>
    request<{ name: string; version: string }>(
      'POST', `/api/datasets/${encodeURIComponent(name)}/versions`, { body },
    ),
  deleteDatasetVersion: (name: string, version: string) =>
    request<{ deleted: { name: string; version: string } }>(
      'DELETE', `/api/datasets/${encodeURIComponent(name)}/versions/${encodeURIComponent(version)}`,
    ),
  exportDatasetVersion: (name: string, version: string, outputDir: string) =>
    request<{ output: string }>(
      'POST',
      `/api/datasets/${encodeURIComponent(name)}/versions/${encodeURIComponent(version)}/export`,
      { body: { output_dir: outputDir } },
    ),

  // Bridge
  startBridge: (body: {
    mode: 'playback' | 'live'
    bag_path?: string
    topics?: string[]
    speed?: number
    port?: number
  }) => request<{ mode: string; port: number; pid: number }>('POST', `/api/bridge/start`, { body }),
  stopBridge: () => request<{ stopped: boolean }>('POST', `/api/bridge/stop`),
  bridgeStatus: () => request<BridgeStatus>('GET', `/api/bridge/status`),
  bridgeProxy: <T>(method: string, path: string, body?: unknown) =>
    request<T>(method, `/api/bridge/proxy/${path.startsWith('/') ? path.slice(1) : path}`, { body }),

  // ---------- v0.3.1: power features ----------

  // Per-topic message density histograms for the timeline ribbon.
  getDensity: (bagId: number, opts?: { bins?: number; topic?: string }) =>
    request<DensityResponse>('GET', `/api/bags/${bagId}/density`, {
      query: { bins: opts?.bins, topic: opts?.topic },
    }),

  // Trim a time-range to a chosen export format.
  trimRange: (
    bagId: number,
    body: {
      start_sec: number
      end_sec: number
      topics: string[]
      format: 'mcap' | 'parquet' | 'csv' | 'hdf5' | 'numpy' | 'zarr' | 'mp4'
      output_path: string
    },
  ) => request<TrimResponse>('POST', `/api/bags/${bagId}/trim`, { body }),

  // Apply a transform (menu op or expression) and get downsampled preview.
  previewTransform: (body: TransformPreviewRequest) =>
    request<TransformPreviewResponse>('POST', `/api/transforms/preview`, { body }),

  // Cross-bag overlay of one topic.
  compareTopics: (body: CompareTopicsRequest) =>
    request<CompareTopicsResponse>('POST', `/api/compare/topics`, { body }),

  // Server-side absolute paths the browser can safely include in
  // output_path values (avoids the "browser can't expand ~" trap).
  getSystemPaths: () => request<SystemPaths>('GET', `/api/system/paths`),
}

// ---------- v0.3.1 types ----------

export interface DensityBucket {
  bins: number[]
  start_time_ns: number
  end_time_ns: number
  total: number
  bin_width_ns: number
}

export interface DensityResponse {
  bag_id: number
  bins: number
  density: Record<string, DensityBucket>
}

export interface TrimResponse {
  bag_id: number
  format: string
  start_sec: number
  end_sec: number
  output: string
}

export type TransformOp =
  | 'derivative'
  | 'integral'
  | 'moving_average'
  | 'low_pass'
  | 'scale'
  | 'abs'
  | 'shift'

export interface TransformPreviewRequest {
  bag_id: number
  topic: string
  max_points?: number
  // Menu mode:
  op?: TransformOp
  column?: string
  params?: Record<string, number | string | boolean>
  // Expression mode:
  expression?: string
}

export interface TransformPreviewResponse {
  topic: string
  label: string
  total: number
  downsampled: boolean
  data: Array<Record<string, unknown>>
}

export interface CompareTopicsRequest {
  bag_ids: number[]
  topic: string
  offsets_sec?: number[]
  labels?: string[]
  max_points_per_bag?: number
}

export interface CompareTopicsResponse {
  topic: string
  bag_ids: number[]
  labels: string[]
  columns: string[]
  data: Array<Record<string, unknown>>
}

export interface SystemPaths {
  home: string
  tmp: string
  cwd: string
  resurrector_cache: string
  allowed_roots: string[]
}
