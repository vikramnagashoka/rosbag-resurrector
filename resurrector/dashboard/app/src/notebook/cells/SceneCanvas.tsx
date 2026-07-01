import React, { useEffect, useMemo, useRef, useState } from 'react'
import { Canvas, useThree } from '@react-three/fiber'
import { OrbitControls, Grid, Html } from '@react-three/drei'
import * as THREE from 'three'
import { api, SceneTfTree, ScenePointCloud } from '../../api'
import { resolveFramePoses, posesCentroid } from '../../components/sceneMath'

// Slim live 3D render for the notebook scene cell — point cloud + TF triads
// at a chosen bag time, sized to fit the cell. Reuses the tested pose math
// (sceneMath) and the same rendering approach as the full SceneViewer, but
// without its sidebar/controls chrome. One WebGL canvas per scene cell.

// Synchronous WebGL probe — @react-three/fiber's <Canvas> throws async and
// bypasses error boundaries, so we gate mounting up front (same rationale
// as SceneViewer.detectWebGL).
function detectWebGL(): boolean {
  if (typeof document === 'undefined') return false
  try {
    const c = document.createElement('canvas')
    if (!(c.getContext('webgl2') || c.getContext('webgl'))) return false
    const r = new THREE.WebGLRenderer({ canvas: c, antialias: false })
    r.setSize(1, 1, false); r.clear(); r.dispose()
    return true
  } catch { return false }
}
const WEBGL_AVAILABLE = detectWebGL()

const AXIS = 0.3

function Segment({ a, b, color }: { a: THREE.Vector3; b: THREE.Vector3; color: string }) {
  const ref = useRef<THREE.BufferGeometry>(null)
  useEffect(() => {
    if (!ref.current) return
    ref.current.setAttribute('position',
      new THREE.BufferAttribute(new Float32Array([a.x, a.y, a.z, b.x, b.y, b.z]), 3))
    ref.current.computeBoundingSphere()
  }, [a, b])
  return <line><bufferGeometry ref={ref} /><lineBasicMaterial color={color} /></line>
}

function FrameTriad({ pose, name }: { pose: THREE.Matrix4; name: string }) {
  const origin = useMemo(() => new THREE.Vector3().setFromMatrixPosition(pose), [pose])
  const ends = useMemo(() => ({
    x: new THREE.Vector3(AXIS, 0, 0).applyMatrix4(pose),
    y: new THREE.Vector3(0, AXIS, 0).applyMatrix4(pose),
    z: new THREE.Vector3(0, 0, AXIS).applyMatrix4(pose),
  }), [pose])
  return (
    <group>
      <Segment a={origin} b={ends.x} color="#ff5454" />
      <Segment a={origin} b={ends.y} color="#54ff54" />
      <Segment a={origin} b={ends.z} color="#5454ff" />
      <mesh position={origin}>
        <sphereGeometry args={[0.015, 12, 12]} />
        <meshBasicMaterial color="#e1e4e8" />
      </mesh>
      <Html position={[origin.x, origin.y + 0.05, origin.z]} center>
        <div style={{
          color: '#67e8f9', fontSize: 10, fontWeight: 600, fontFamily: 'monospace',
          background: 'rgba(0,0,0,0.85)', padding: '1px 5px', borderRadius: 3,
          border: '1px solid rgba(103,232,249,0.5)', whiteSpace: 'nowrap', pointerEvents: 'none',
        }}>{name}</div>
      </Html>
    </group>
  )
}

function PointCloud({ points }: { points: [number, number, number][] }) {
  const geometry = useMemo(() => {
    const geom = new THREE.BufferGeometry()
    const pos = new Float32Array(points.length * 3)
    const col = new Float32Array(points.length * 3)
    let zMin = Infinity, zMax = -Infinity
    for (const [, , z] of points) { if (z < zMin) zMin = z; if (z > zMax) zMax = z }
    const zRange = zMax - zMin || 1
    for (let i = 0; i < points.length; i++) {
      const [x, y, z] = points[i]
      pos[i * 3] = x; pos[i * 3 + 1] = y; pos[i * 3 + 2] = z
      const t = (z - zMin) / zRange
      col[i * 3] = Math.min(1, t * 1.2)
      col[i * 3 + 1] = Math.min(1, 0.5 + t * 0.5)
      col[i * 3 + 2] = Math.max(0, 1 - t)
    }
    geom.setAttribute('position', new THREE.BufferAttribute(pos, 3))
    geom.setAttribute('color', new THREE.BufferAttribute(col, 3))
    return geom
  }, [points])
  useEffect(() => () => geometry.dispose(), [geometry])
  return (
    <points geometry={geometry}>
      <pointsMaterial size={0.025} vertexColors sizeAttenuation />
    </points>
  )
}

function AutoFit({ poses }: { poses: Map<string, THREE.Matrix4> }) {
  const { camera } = useThree()
  useEffect(() => {
    const c = posesCentroid(poses)
    if (c) camera.lookAt(c.x, c.y, c.z)
  }, [poses, camera])
  return null
}

export default function SceneCanvas({
  bagId, topic, timeNs, maxPoints = 8000, onRuntime, onStats,
}: {
  bagId: number
  topic: string
  timeNs: number
  maxPoints?: number
  onRuntime?: (ms: number) => void
  onStats?: (s: { nPoints: number; nFrames: number }) => void
}) {
  const [tree, setTree] = useState<SceneTfTree | null>(null)
  const [cloud, setCloud] = useState<ScenePointCloud | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    const t0 = performance.now()
    Promise.all([
      api.getSceneTfTree(bagId, Math.round(timeNs)).catch(() => null),
      api.getScenePointCloud(bagId, topic, { timeNs: Math.round(timeNs), maxPoints }).catch(() => null),
    ]).then(([t, c]) => {
      if (cancelled) return
      setTree(t); setCloud(c)
      onRuntime?.(performance.now() - t0)
      onStats?.({ nPoints: c?.n_points ?? 0, nFrames: t?.frames.length ?? 0 })
      if (!t && !c) setError('No scene data at this time.')
    })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bagId, topic, timeNs, maxPoints])

  const poses = useMemo(() => (tree ? resolveFramePoses(tree) : new Map()), [tree])

  if (!WEBGL_AVAILABLE) {
    return <div className="nb-scene-label" style={{ margin: 'auto' }}>WebGL unavailable — 3D render disabled</div>
  }
  if (error) return <div className="nb-cell-loading" style={{ margin: 'auto' }}>{error}</div>

  return (
    <Canvas
      camera={{ position: [3, 3, 3], fov: 50, near: 0.01, far: 1000 }}
      gl={{ antialias: true }}
      style={{ width: '100%', height: '100%' }}
    >
      <color attach="background" args={['#141821']} />
      <ambientLight intensity={0.5} />
      <directionalLight position={[5, 10, 5]} intensity={0.8} />
      <Grid args={[20, 20]} cellColor="#30363d" sectionColor="#484f58" sectionSize={5} fadeDistance={30} infiniteGrid />
      {Array.from(poses.entries()).map(([frame, pose]) => (
        <FrameTriad key={frame} pose={pose} name={frame} />
      ))}
      {cloud && cloud.points.length > 0 && <PointCloud points={cloud.points} />}
      <OrbitControls makeDefault />
      <AutoFit poses={poses} />
    </Canvas>
  )
}
