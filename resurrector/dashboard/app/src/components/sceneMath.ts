// Pure math + tree-walk helpers for the 3D scene viewer.
//
// Lives in its own module so it can be unit-tested without spinning up
// React or Three.js, and so future scene sub-features (URDF, markers,
// camera overlay) can reuse the TF-pose composition without duplicating
// the walk.

import * as THREE from 'three'
import { SceneTfTree } from '../api'

/**
 * Compute the 4x4 world-pose of every frame in the tree by walking
 * child→parent edges, starting from the root frame as the identity.
 *
 * Returns a Map<frame_name, THREE.Matrix4>. Disconnected sub-trees
 * (frames whose parent isn't reachable from a root) are silently
 * dropped — they have no meaningful world pose.
 *
 * Cycle-safe: bounds the walk to ``frames.length + 4`` iterations.
 */
export function resolveFramePoses(tree: SceneTfTree): Map<string, THREE.Matrix4> {
  const out = new Map<string, THREE.Matrix4>()
  if (!tree.frames || tree.frames.length === 0) return out

  // Pick a root: prefer the first declared root; fall back to the first
  // frame name (e.g. when roots[] is empty due to disconnected sub-trees).
  const root = tree.roots?.[0] || tree.frames[0]
  out.set(root, new THREE.Matrix4().identity())

  const safetyMax = tree.frames.length + 4
  let progress = true
  let iterations = 0
  while (progress && iterations < safetyMax) {
    progress = false
    iterations++
    for (const e of tree.edges) {
      if (out.has(e.child_frame)) continue
      const parentPose = out.get(e.parent_frame)
      if (!parentPose) continue

      // Edge transform: translation + quaternion (x, y, z, w)
      const edge = new THREE.Matrix4()
      const q = new THREE.Quaternion(
        e.rotation[0], e.rotation[1], e.rotation[2], e.rotation[3],
      ).normalize()
      edge.makeRotationFromQuaternion(q)
      edge.setPosition(e.translation[0], e.translation[1], e.translation[2])

      // Compose with parent's world pose
      const childPose = new THREE.Matrix4().multiplyMatrices(parentPose, edge)
      out.set(e.child_frame, childPose)
      progress = true
    }
  }
  return out
}

/**
 * Compute the centroid of a set of frame poses' translations. Used by
 * the SceneViewer's auto-fit camera so the view focuses on something
 * meaningful when the bag's TF tree spans a wide spatial range.
 *
 * Returns ``null`` if the input is empty.
 */
export function posesCentroid(
  poses: Map<string, THREE.Matrix4>,
): THREE.Vector3 | null {
  if (poses.size === 0) return null
  const sum = new THREE.Vector3(0, 0, 0)
  poses.forEach((m) => {
    sum.x += m.elements[12]
    sum.y += m.elements[13]
    sum.z += m.elements[14]
  })
  const n = poses.size
  return new THREE.Vector3(sum.x / n, sum.y / n, sum.z / n)
}
