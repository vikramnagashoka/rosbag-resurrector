// Unit tests for sceneMath.ts — the pure math + tree-walk helpers
// powering the 3D SceneViewer.
//
// These tests deliberately exercise rainy-day cases (cycles, dis-
// connected sub-trees, malformed inputs) because if the JS port of
// the TF-walk math diverges from the Python TFTree.chain() logic,
// the dashboard will silently render wrong poses.

import { describe, it, expect } from 'vitest'
import * as THREE from 'three'
import { resolveFramePoses, posesCentroid } from './sceneMath'
import { SceneTfTree, SceneTfEdge } from '../api'

function edge(
  parent: string,
  child: string,
  t: [number, number, number] = [0, 0, 0],
  q: [number, number, number, number] = [0, 0, 0, 1],
  isStatic = false,
): SceneTfEdge {
  return {
    parent_frame: parent,
    child_frame: child,
    timestamp_ns: 0,
    translation: t,
    rotation: q,
    is_static: isStatic,
  }
}

function tree(roots: string[], frames: string[], edges: SceneTfEdge[]): SceneTfTree {
  return {
    time_ns: 0,
    edges,
    frames,
    roots,
    static_count: edges.filter(e => e.is_static).length,
    dynamic_count: edges.filter(e => !e.is_static).length,
  }
}

function approx(a: number, b: number, eps = 1e-9): boolean {
  return Math.abs(a - b) < eps
}

describe('resolveFramePoses — happy path', () => {
  it('returns identity for the root', () => {
    const t = tree(['world'], ['world'], [])
    const poses = resolveFramePoses(t)
    expect(poses.size).toBe(1)
    const p = poses.get('world')!
    expect(p.equals(new THREE.Matrix4().identity())).toBe(true)
  })

  it('resolves a single child translation', () => {
    const t = tree(['world'], ['world', 'base'], [edge('world', 'base', [1, 2, 3])])
    const poses = resolveFramePoses(t)
    expect(poses.size).toBe(2)
    const p = poses.get('base')!
    expect(approx(p.elements[12], 1)).toBe(true)
    expect(approx(p.elements[13], 2)).toBe(true)
    expect(approx(p.elements[14], 3)).toBe(true)
  })

  it('composes a chain world→base→imu', () => {
    const t = tree(['world'], ['world', 'base', 'imu'], [
      edge('world', 'base', [1, 0, 0]),
      edge('base', 'imu', [0, 0.5, 0.1]),
    ])
    const poses = resolveFramePoses(t)
    const imu = poses.get('imu')!
    // Translation in world: (1, 0.5, 0.1)
    expect(approx(imu.elements[12], 1)).toBe(true)
    expect(approx(imu.elements[13], 0.5)).toBe(true)
    expect(approx(imu.elements[14], 0.1)).toBe(true)
  })

  it('handles sibling chains (one parent, two children)', () => {
    const t = tree(['world'], ['world', 'base', 'imu', 'cam'], [
      edge('world', 'base', [1, 0, 0]),
      edge('base', 'imu', [0, 0.5, 0]),
      edge('base', 'cam', [0.2, 0, 0.3]),
    ])
    const poses = resolveFramePoses(t)
    expect(poses.size).toBe(4)
    const cam = poses.get('cam')!
    // (1+0.2, 0+0, 0+0.3) = (1.2, 0, 0.3)
    expect(approx(cam.elements[12], 1.2)).toBe(true)
    expect(approx(cam.elements[14], 0.3)).toBe(true)
  })

  it('applies a 90° rotation about Z correctly', () => {
    const s = Math.sin(Math.PI / 4)
    const c = Math.cos(Math.PI / 4)
    const t = tree(['world'], ['world', 'rotated'], [
      edge('world', 'rotated', [0, 0, 0], [0, 0, s, c]),
    ])
    const poses = resolveFramePoses(t)
    const r = poses.get('rotated')!
    // Apply r to local x-axis (1, 0, 0). Should land at (0, 1, 0).
    const v = new THREE.Vector3(1, 0, 0).applyMatrix4(r)
    expect(approx(v.x, 0)).toBe(true)
    expect(approx(v.y, 1)).toBe(true)
    expect(approx(v.z, 0)).toBe(true)
  })
})

describe('resolveFramePoses — rainy day', () => {
  it('returns empty map for an empty tree', () => {
    const t = tree([], [], [])
    expect(resolveFramePoses(t).size).toBe(0)
  })

  it('returns empty map when frames list is undefined', () => {
    // Defensive — the API contract is non-null but if it somehow sneaks
    // through, we should not crash.
    const t = { ...tree(['x'], [], []), frames: undefined as unknown as string[] }
    expect(resolveFramePoses(t).size).toBe(0)
  })

  it('drops disconnected sub-trees (parent never reached from root)', () => {
    // a → b is connected; c → d is its own island (parent c has no edge in)
    const t = tree(['a'], ['a', 'b', 'c', 'd'], [
      edge('a', 'b', [1, 0, 0]),
      edge('c', 'd', [5, 0, 0]),
    ])
    const poses = resolveFramePoses(t)
    expect(poses.has('a')).toBe(true)
    expect(poses.has('b')).toBe(true)
    expect(poses.has('c')).toBe(false) // unreachable from root
    expect(poses.has('d')).toBe(false)
  })

  it('falls back to first frame when roots[] is empty', () => {
    const t = tree([], ['x', 'y'], [edge('x', 'y', [1, 0, 0])])
    const poses = resolveFramePoses(t)
    expect(poses.has('x')).toBe(true)
    expect(poses.has('y')).toBe(true)
  })

  it('does not infinite-loop on a cycle a→b→a', () => {
    // Cycle: a→b, b→a. The walk seeds "a" as identity, places "b" as
    // child of a, but then sees "a" already in out — never re-enters.
    // The safety counter caps total iterations; this should terminate.
    const t = tree(['a'], ['a', 'b'], [edge('a', 'b'), edge('b', 'a')])
    const start = Date.now()
    const poses = resolveFramePoses(t)
    const elapsed = Date.now() - start
    expect(elapsed).toBeLessThan(50) // should be near-instant
    expect(poses.has('a')).toBe(true)
    expect(poses.has('b')).toBe(true)
  })

  it('handles a deep linear chain (10 frames)', () => {
    const frames = Array.from({ length: 10 }, (_, i) => `f${i}`)
    const edges = frames.slice(1).map((child, i) => edge(frames[i], child, [1, 0, 0]))
    const t = tree(['f0'], frames, edges)
    const poses = resolveFramePoses(t)
    expect(poses.size).toBe(10)
    const last = poses.get('f9')!
    // 9 translation units of x = 9
    expect(approx(last.elements[12], 9)).toBe(true)
  })

  it('does not pollute output when the same edge appears twice', () => {
    // Duplicates can sneak in if the backend emits both /tf and /tf_static
    // for the same child. We pick the first that wins; subsequent ones
    // are no-ops (skipped because child already in out).
    const t = tree(['world'], ['world', 'base'], [
      edge('world', 'base', [1, 0, 0]),
      edge('world', 'base', [99, 0, 0]),
    ])
    const poses = resolveFramePoses(t)
    expect(approx(poses.get('base')!.elements[12], 1)).toBe(true)
  })

  it('normalizes a slightly drifted quaternion without producing NaN', () => {
    const t = tree(['world'], ['world', 'base'], [
      edge('world', 'base', [0, 0, 0], [0.001, 0, 0, 1.0]), // norm > 1
    ])
    const poses = resolveFramePoses(t)
    const m = poses.get('base')!
    for (const v of m.elements) {
      expect(Number.isFinite(v)).toBe(true)
    }
  })
})

describe('posesCentroid', () => {
  it('returns null for empty', () => {
    expect(posesCentroid(new Map())).toBeNull()
  })

  it('computes centroid of a few translations', () => {
    const m1 = new THREE.Matrix4().setPosition(1, 0, 0)
    const m2 = new THREE.Matrix4().setPosition(0, 2, 0)
    const m3 = new THREE.Matrix4().setPosition(0, 0, 3)
    const c = posesCentroid(new Map([['a', m1], ['b', m2], ['c', m3]]))!
    expect(approx(c.x, 1 / 3)).toBe(true)
    expect(approx(c.y, 2 / 3)).toBe(true)
    expect(approx(c.z, 1)).toBe(true)
  })

  it('handles a single pose', () => {
    const m = new THREE.Matrix4().setPosition(5, 6, 7)
    const c = posesCentroid(new Map([['only', m]]))!
    expect(approx(c.x, 5)).toBe(true)
    expect(approx(c.y, 6)).toBe(true)
    expect(approx(c.z, 7)).toBe(true)
  })
})
