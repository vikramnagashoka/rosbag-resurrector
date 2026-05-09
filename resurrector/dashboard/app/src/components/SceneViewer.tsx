// 3D scene viewer (v0.6.0)
//
// Three.js / react-three-fiber renderer for the bag's TF tree and an
// optional PointCloud2 snapshot at a chosen timestamp. Replaces the
// Plotly 3D viewer that shipped in v0.5.0. Wires to the same backend
// scene endpoints — no API changes.
//
// Why Three.js: Plotly 3D worked for v0.5.0 but tops out around 10-25k
// points and can't load mesh / URDF assets. Three.js + react-three-fiber
// gives us actual scene-graph composition and is the foundation for
// URDF (A.2), markers (A.3), and camera-overlay (A.4) sub-features.

import { useEffect, useMemo, useRef, useState } from 'react'
import { Canvas, useFrame } from '@react-three/fiber'
import { OrbitControls, Grid, Html } from '@react-three/drei'
import * as THREE from 'three'

import {
  api,
  ApiError,
  ScenePointCloud,
  SceneTfTree,
  SceneTopics,
} from '../api'
import { resolveFramePoses, posesCentroid } from './sceneMath'

interface Props {
  bagId: number
  bagDurationSec: number
  bagStartNs: number
}

const AXIS_LENGTH = 0.3 // meters — drawn for each frame's local x/y/z axes

// One frame's coordinate triad (red X / green Y / blue Z) + label.
function FrameTriad({
  pose, name,
}: { pose: THREE.Matrix4; name: string }) {
  const origin = useMemo(() => {
    const v = new THREE.Vector3()
    v.setFromMatrixPosition(pose)
    return v
  }, [pose])

  const axisEnds = useMemo(() => {
    const xLocal = new THREE.Vector3(AXIS_LENGTH, 0, 0).applyMatrix4(pose)
    const yLocal = new THREE.Vector3(0, AXIS_LENGTH, 0).applyMatrix4(pose)
    const zLocal = new THREE.Vector3(0, 0, AXIS_LENGTH).applyMatrix4(pose)
    return { x: xLocal, y: yLocal, z: zLocal }
  }, [pose])

  return (
    <group>
      <Line start={origin} end={axisEnds.x} color="#ff5454" />
      <Line start={origin} end={axisEnds.y} color="#54ff54" />
      <Line start={origin} end={axisEnds.z} color="#5454ff" />
      <mesh position={origin}>
        <sphereGeometry args={[0.015, 12, 12]} />
        <meshBasicMaterial color="#e1e4e8" />
      </mesh>
      <Html position={[origin.x, origin.y + 0.05, origin.z]} center>
        <div style={{
          color: '#e1e4e8',
          fontSize: 11,
          fontFamily: 'monospace',
          background: 'rgba(13,17,23,0.7)',
          padding: '1px 4px',
          borderRadius: 2,
          whiteSpace: 'nowrap',
          pointerEvents: 'none',
        }}>{name}</div>
      </Html>
    </group>
  )
}

// Drei doesn't expose a simple <Line> for 3D segments out of the box in
// the 9.x line, so we render a thin BufferGeometry segment manually.
function Line({
  start, end, color,
}: { start: THREE.Vector3; end: THREE.Vector3; color: string }) {
  const ref = useRef<THREE.BufferGeometry>(null)
  useEffect(() => {
    if (!ref.current) return
    const positions = new Float32Array([
      start.x, start.y, start.z,
      end.x, end.y, end.z,
    ])
    ref.current.setAttribute(
      'position',
      new THREE.BufferAttribute(positions, 3),
    )
    ref.current.computeBoundingSphere()
  }, [start, end])

  return (
    <line>
      <bufferGeometry ref={ref} />
      <lineBasicMaterial color={color} linewidth={2} />
    </line>
  )
}

// Point cloud rendered as THREE.Points with z-color gradient.
function PointCloud({ points }: { points: [number, number, number][] }) {
  const geometry = useMemo(() => {
    const geom = new THREE.BufferGeometry()
    const positions = new Float32Array(points.length * 3)
    const colors = new Float32Array(points.length * 3)

    let zMin = Infinity, zMax = -Infinity
    for (const [, , z] of points) {
      if (z < zMin) zMin = z
      if (z > zMax) zMax = z
    }
    const zRange = zMax - zMin || 1

    for (let i = 0; i < points.length; i++) {
      const [x, y, z] = points[i]
      positions[i * 3] = x
      positions[i * 3 + 1] = y
      positions[i * 3 + 2] = z
      // Viridis-ish: blue (low Z) → cyan → green → yellow (high Z)
      const t = (z - zMin) / zRange
      colors[i * 3] = Math.max(0, Math.min(1, t * 1.2))            // R rises with t
      colors[i * 3 + 1] = Math.max(0, Math.min(1, 0.5 + t * 0.5))  // G high
      colors[i * 3 + 2] = Math.max(0, Math.min(1, 1.0 - t))         // B falls with t
    }

    geom.setAttribute('position', new THREE.BufferAttribute(positions, 3))
    geom.setAttribute('color', new THREE.BufferAttribute(colors, 3))
    return geom
  }, [points])

  return (
    <points geometry={geometry}>
      <pointsMaterial size={0.025} vertexColors sizeAttenuation />
    </points>
  )
}

// Tiny widget that auto-fits the OrbitControls target to the scene's
// bounding sphere on every TF tree change so the user doesn't have to
// hunt for the scene with the mouse.
function CameraAutoFit({ poses }: { poses: Map<string, THREE.Matrix4> }) {
  useFrame((state) => {
    const c = posesCentroid(poses)
    if (!c) return
    state.camera.lookAt(c.x, c.y, c.z)
  })
  return null
}

export default function SceneViewer({ bagId, bagDurationSec, bagStartNs }: Props) {
  const [topics, setTopics] = useState<SceneTopics | null>(null)
  const [tree, setTree] = useState<SceneTfTree | null>(null)
  const [pointCloud, setPointCloud] = useState<ScenePointCloud | null>(null)
  const [selectedPointCloudTopic, setSelectedPointCloudTopic] = useState<string | null>(null)
  const [timeOffsetSec, setTimeOffsetSec] = useState<number>(bagDurationSec)
  const [maxPoints, setMaxPoints] = useState<number>(10000)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const inflightRef = useRef<number>(0)

  const timeNs = useMemo(
    () => bagStartNs + Math.max(0, timeOffsetSec) * 1e9,
    [bagStartNs, timeOffsetSec],
  )

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

  const poses = useMemo(() => tree ? resolveFramePoses(tree) : new Map(), [tree])

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
          fontSize: 12, color: '#e1e4e8', flexWrap: 'wrap',
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
            <option value={50000}>50k</option>
            <option value={100000}>100k</option>
          </select>
        </label>
        {tree && (
          <span style={{ color: '#8b949e' }}>
            {tree.frames.length} frames · {tree.dynamic_count} dynamic + {tree.static_count} static
          </span>
        )}
        {loading && <span style={{ color: '#8b949e' }}>(loading…)</span>}
      </div>
      <div style={{ width: '100%', height: 600, background: '#0d1117', borderRadius: 6 }}>
        <Canvas
          camera={{ position: [3, 3, 3], fov: 50, near: 0.01, far: 1000 }}
          gl={{ antialias: true }}
        >
          <color attach="background" args={['#0d1117']} />
          <ambientLight intensity={0.5} />
          <directionalLight position={[5, 10, 5]} intensity={0.8} />
          <Grid
            args={[20, 20]}
            cellColor="#30363d"
            sectionColor="#484f58"
            sectionSize={5}
            fadeDistance={30}
            infiniteGrid
          />
          {tree && Array.from(poses.entries()).map(([frame, pose]) => (
            <FrameTriad key={frame} pose={pose} name={frame} />
          ))}
          {pointCloud && pointCloud.points.length > 0 && (
            <PointCloud points={pointCloud.points} />
          )}
          <OrbitControls makeDefault />
          <CameraAutoFit poses={poses} />
        </Canvas>
      </div>
    </div>
  )
}
