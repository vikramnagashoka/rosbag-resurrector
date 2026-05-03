// 3D scene viewer (v0.5.0)
//
// Renders the bag's TF tree (frame axes) and an optional PointCloud2
// snapshot at a chosen timestamp. Backed by Plotly 3D scatter+line
// traces — chosen over a full Three.js wrapper because:
//   - already a dashboard dependency (no bundle bloat)
//   - free pan/zoom/rotate camera
//   - 5-10k points renders smoothly enough for a "pose at time T" view
//
// URDF + animated playback are deferred to v0.6+. Camera image overlays
// would require a separate canvas composited under the 3D — also v0.6.

import { useEffect, useMemo, useRef, useState } from 'react'
import Plot from 'react-plotly.js'

import {
  api,
  ApiError,
  ScenePointCloud,
  SceneTfTree,
  SceneTopics,
} from '../api'

interface Props {
  bagId: number
  bagDurationSec: number
  bagStartNs: number
}

const AXIS_LENGTH = 0.3 // meters — drawn for each frame's local x/y/z axes

function quatToMatrix(q: [number, number, number, number]): number[][] {
  const [qx, qy, qz, qw] = q
  const norm = Math.sqrt(qx * qx + qy * qy + qz * qz + qw * qw) || 1
  const x = qx / norm, y = qy / norm, z = qz / norm, w = qw / norm
  return [
    [1 - 2 * (y * y + z * z), 2 * (x * y - w * z), 2 * (x * z + w * y)],
    [2 * (x * y + w * z), 1 - 2 * (x * x + z * z), 2 * (y * z - w * x)],
    [2 * (x * z - w * y), 2 * (y * z + w * x), 1 - 2 * (x * x + y * y)],
  ]
}

function applyTransform(
  M: number[][],
  t: [number, number, number],
  p: [number, number, number],
): [number, number, number] {
  return [
    M[0][0] * p[0] + M[0][1] * p[1] + M[0][2] * p[2] + t[0],
    M[1][0] * p[0] + M[1][1] * p[1] + M[1][2] * p[2] + t[1],
    M[2][0] * p[0] + M[2][1] * p[1] + M[2][2] * p[2] + t[2],
  ]
}

// Walk the TF graph and return each frame's origin + axis endpoints in
// the implicit world frame. Returns null if the graph has no root.
function resolveFramePoses(tree: SceneTfTree): Map<string, {
  origin: [number, number, number]
  xAxis: [number, number, number]
  yAxis: [number, number, number]
  zAxis: [number, number, number]
  parent: string | null
}> {
  const out = new Map<string, {
    origin: [number, number, number]
    xAxis: [number, number, number]
    yAxis: [number, number, number]
    zAxis: [number, number, number]
    parent: string | null
  }>()
  // Pick the first root; if there are no edges yet, return empty map.
  const root = tree.roots[0] || tree.frames[0]
  if (!root) return out
  out.set(root, {
    origin: [0, 0, 0],
    xAxis: [AXIS_LENGTH, 0, 0],
    yAxis: [0, AXIS_LENGTH, 0],
    zAxis: [0, 0, AXIS_LENGTH],
    parent: null,
  })
  // Build child→edge map for an iterative walk
  const childToEdge = new Map(tree.edges.map(e => [e.child_frame, e]))
  let progress = true
  let safety = tree.frames.length + 4
  while (progress && safety-- > 0) {
    progress = false
    for (const e of tree.edges) {
      if (out.has(e.child_frame) || !out.has(e.parent_frame)) continue
      const parentPose = out.get(e.parent_frame)!
      const M = quatToMatrix(e.rotation)
      const trans = e.translation
      // Compute the child's origin and the three axis endpoints in parent
      // coords, then in WORLD coords by recursive composition. Since we
      // only stored the parent's origin in world coords, we need to walk
      // up; instead, accumulate from the parent's basis.
      const parentBasis = {
        x: [
          parentPose.xAxis[0] - parentPose.origin[0],
          parentPose.xAxis[1] - parentPose.origin[1],
          parentPose.xAxis[2] - parentPose.origin[2],
        ] as [number, number, number],
        y: [
          parentPose.yAxis[0] - parentPose.origin[0],
          parentPose.yAxis[1] - parentPose.origin[1],
          parentPose.yAxis[2] - parentPose.origin[2],
        ] as [number, number, number],
        z: [
          parentPose.zAxis[0] - parentPose.origin[0],
          parentPose.zAxis[1] - parentPose.origin[1],
          parentPose.zAxis[2] - parentPose.origin[2],
        ] as [number, number, number],
      }
      // Normalize parent basis vectors (length AXIS_LENGTH) for clean rotation
      const norm = (v: [number, number, number]) => {
        const n = Math.hypot(v[0], v[1], v[2]) || 1
        return [v[0] / n, v[1] / n, v[2] / n] as [number, number, number]
      }
      const px = norm(parentBasis.x)
      const py = norm(parentBasis.y)
      const pz = norm(parentBasis.z)
      // Construct parent→world rotation matrix from its basis vectors
      const parentToWorld = [
        [px[0], py[0], pz[0]],
        [px[1], py[1], pz[1]],
        [px[2], py[2], pz[2]],
      ]
      // Translation in parent's frame, expressed in world
      const transWorld = applyTransform(parentToWorld, [0, 0, 0], trans)
      const childOrigin: [number, number, number] = [
        parentPose.origin[0] + transWorld[0],
        parentPose.origin[1] + transWorld[1],
        parentPose.origin[2] + transWorld[2],
      ]
      // Compose rotations: childAxisInParent = M @ unit; childAxisInWorld = parentToWorld @ that
      const childInParent = (axis: [number, number, number]) =>
        applyTransform(M, [0, 0, 0], axis)
      const childInWorld = (axis: [number, number, number]) =>
        applyTransform(parentToWorld, [0, 0, 0], axis)
      const cx = childInWorld(childInParent([AXIS_LENGTH, 0, 0]))
      const cy = childInWorld(childInParent([0, AXIS_LENGTH, 0]))
      const cz = childInWorld(childInParent([0, 0, AXIS_LENGTH]))
      out.set(e.child_frame, {
        origin: childOrigin,
        xAxis: [childOrigin[0] + cx[0], childOrigin[1] + cx[1], childOrigin[2] + cx[2]],
        yAxis: [childOrigin[0] + cy[0], childOrigin[1] + cy[1], childOrigin[2] + cy[2]],
        zAxis: [childOrigin[0] + cz[0], childOrigin[1] + cz[1], childOrigin[2] + cz[2]],
        parent: e.parent_frame,
      })
      progress = true
    }
  }
  // Suppress unused-var warning for childToEdge — kept for future per-edge tooltips
  void childToEdge
  return out
}

export default function SceneViewer({ bagId, bagDurationSec, bagStartNs }: Props) {
  const [topics, setTopics] = useState<SceneTopics | null>(null)
  const [tree, setTree] = useState<SceneTfTree | null>(null)
  const [pointCloud, setPointCloud] = useState<ScenePointCloud | null>(null)
  const [selectedPointCloudTopic, setSelectedPointCloudTopic] = useState<string | null>(null)
  const [timeOffsetSec, setTimeOffsetSec] = useState<number>(bagDurationSec)
  const [maxPoints, setMaxPoints] = useState<number>(5000)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const inflightRef = useRef<number>(0)

  const timeNs = useMemo(
    () => bagStartNs + Math.max(0, timeOffsetSec) * 1e9,
    [bagStartNs, timeOffsetSec],
  )

  // Fetch the topic list once
  useEffect(() => {
    api.listSceneTopics(bagId).then(t => {
      setTopics(t)
      if (t.pointclouds.length > 0) {
        setSelectedPointCloudTopic(t.pointclouds[0])
      }
    }).catch(e => {
      setError(e instanceof ApiError ? e.message : String(e))
    })
  }, [bagId])

  // Fetch the TF tree at the chosen timestamp
  useEffect(() => {
    setLoading(true)
    const reqId = ++inflightRef.current
    api.getSceneTfTree(bagId, Math.round(timeNs)).then(t => {
      if (reqId !== inflightRef.current) return
      setTree(t)
      setLoading(false)
    }).catch(e => {
      if (reqId !== inflightRef.current) return
      setError(e instanceof ApiError ? e.message : String(e))
      setLoading(false)
    })
  }, [bagId, timeNs])

  // Fetch the point cloud snapshot
  useEffect(() => {
    if (!selectedPointCloudTopic) {
      setPointCloud(null)
      return
    }
    api.getScenePointCloud(bagId, selectedPointCloudTopic, {
      timeNs: Math.round(timeNs),
      maxPoints,
    }).then(setPointCloud).catch(e => {
      setError(e instanceof ApiError ? e.message : String(e))
    })
  }, [bagId, selectedPointCloudTopic, timeNs, maxPoints])

  // Build Plotly traces
  const traces = useMemo(() => {
    if (!tree) return []
    const poses = resolveFramePoses(tree)
    const out: any[] = []
    // Frame axes — one trace per axis color so the legend is readable
    const xLines: number[][] = [[], [], []]
    const yLines: number[][] = [[], [], []]
    const zLines: number[][] = [[], [], []]
    poses.forEach((pose) => {
      // X axis (red): origin → xAxis
      xLines[0].push(pose.origin[0], pose.xAxis[0], NaN)
      xLines[1].push(pose.origin[1], pose.xAxis[1], NaN)
      xLines[2].push(pose.origin[2], pose.xAxis[2], NaN)
      yLines[0].push(pose.origin[0], pose.yAxis[0], NaN)
      yLines[1].push(pose.origin[1], pose.yAxis[1], NaN)
      yLines[2].push(pose.origin[2], pose.yAxis[2], NaN)
      zLines[0].push(pose.origin[0], pose.zAxis[0], NaN)
      zLines[1].push(pose.origin[1], pose.zAxis[1], NaN)
      zLines[2].push(pose.origin[2], pose.zAxis[2], NaN)
    })
    out.push({
      type: 'scatter3d', mode: 'lines', name: 'X axis',
      x: xLines[0], y: xLines[1], z: xLines[2],
      line: { color: '#ff5454', width: 4 },
    })
    out.push({
      type: 'scatter3d', mode: 'lines', name: 'Y axis',
      x: yLines[0], y: yLines[1], z: yLines[2],
      line: { color: '#54ff54', width: 4 },
    })
    out.push({
      type: 'scatter3d', mode: 'lines', name: 'Z axis',
      x: zLines[0], y: zLines[1], z: zLines[2],
      line: { color: '#5454ff', width: 4 },
    })
    // Frame origin labels
    const labelX: number[] = []
    const labelY: number[] = []
    const labelZ: number[] = []
    const labels: string[] = []
    poses.forEach((pose, frame) => {
      labelX.push(pose.origin[0])
      labelY.push(pose.origin[1])
      labelZ.push(pose.origin[2])
      labels.push(frame)
    })
    out.push({
      type: 'scatter3d', mode: 'markers+text', name: 'Frames',
      x: labelX, y: labelY, z: labelZ,
      text: labels,
      textposition: 'top center',
      marker: { size: 4, color: '#e1e4e8' },
      textfont: { size: 10, color: '#e1e4e8' },
    })
    if (pointCloud && pointCloud.points.length > 0) {
      const px = pointCloud.points.map(p => p[0])
      const py = pointCloud.points.map(p => p[1])
      const pz = pointCloud.points.map(p => p[2])
      out.push({
        type: 'scatter3d', mode: 'markers',
        name: `${selectedPointCloudTopic} (${pointCloud.n_points})`,
        x: px, y: py, z: pz,
        marker: {
          size: 1.5,
          color: pz,
          colorscale: 'Viridis',
          opacity: 0.7,
        },
      })
    }
    return out
  }, [tree, pointCloud, selectedPointCloudTopic])

  if (error) {
    return (
      <div style={{ color: '#f85149', padding: 24, fontFamily: 'monospace' }}>
        Scene viewer error: {error}
      </div>
    )
  }
  if (!topics) {
    return <div style={{ color: '#8b949e', padding: 24 }}>Loading scene topics…</div>
  }
  if (
    topics.tf.length === 0 &&
    topics.tf_static.length === 0 &&
    topics.pointclouds.length === 0
  ) {
    return (
      <div style={{ color: '#8b949e', padding: 24, fontFamily: 'monospace' }}>
        No /tf, /tf_static, or PointCloud2 topics in this bag.
        <br />
        The 3D scene viewer needs at least one of these to render anything useful.
      </div>
    )
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <div
        style={{
          display: 'flex', alignItems: 'center', gap: 16,
          fontSize: 12, color: '#e1e4e8',
        }}
      >
        <label style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          Time:
          <input
            type="range"
            min={0}
            max={bagDurationSec}
            step={0.05}
            value={timeOffsetSec}
            onChange={e => setTimeOffsetSec(Number(e.target.value))}
            style={{ width: 240 }}
          />
          <span style={{ fontFamily: 'monospace', minWidth: 60 }}>
            {timeOffsetSec.toFixed(2)}s
          </span>
        </label>
        {topics.pointclouds.length > 0 && (
          <label style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            Cloud:
            <select
              value={selectedPointCloudTopic ?? ''}
              onChange={e =>
                setSelectedPointCloudTopic(e.target.value || null)
              }
              style={{ padding: 4, background: '#0d1117', color: '#e1e4e8', border: '1px solid #30363d' }}
            >
              <option value="">(none)</option>
              {topics.pointclouds.map(t => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>
          </label>
        )}
        <label style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          Max points:
          <select
            value={maxPoints}
            onChange={e => setMaxPoints(Number(e.target.value))}
            style={{ padding: 4, background: '#0d1117', color: '#e1e4e8', border: '1px solid #30363d' }}
          >
            <option value={1000}>1k</option>
            <option value={5000}>5k</option>
            <option value={10000}>10k</option>
            <option value={25000}>25k</option>
          </select>
        </label>
        {tree && (
          <span style={{ color: '#8b949e' }}>
            {tree.frames.length} frames · {tree.dynamic_count} dynamic + {tree.static_count} static
          </span>
        )}
        {loading && <span style={{ color: '#8b949e' }}>(loading…)</span>}
      </div>
      <Plot
        data={traces}
        layout={{
          autosize: true,
          margin: { l: 0, r: 0, t: 0, b: 0 },
          paper_bgcolor: '#0d1117',
          plot_bgcolor: '#0d1117',
          scene: {
            aspectmode: 'data',
            bgcolor: '#0d1117',
            xaxis: {
              gridcolor: '#30363d',
              zerolinecolor: '#30363d',
              color: '#8b949e',
              title: { text: 'X (m)' },
            },
            yaxis: {
              gridcolor: '#30363d',
              zerolinecolor: '#30363d',
              color: '#8b949e',
              title: { text: 'Y (m)' },
            },
            zaxis: {
              gridcolor: '#30363d',
              zerolinecolor: '#30363d',
              color: '#8b949e',
              title: { text: 'Z (m)' },
            },
          },
          legend: { font: { color: '#e1e4e8' }, bgcolor: 'rgba(0,0,0,0)' },
          height: 600,
        }}
        config={{ displayModeBar: true, responsive: true }}
        style={{ width: '100%' }}
      />
    </div>
  )
}
