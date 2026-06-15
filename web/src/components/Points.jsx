import { useMemo, useEffect } from 'react'
import * as THREE from 'three'

// Soft circular sprite shared by all point clouds.
const sprite = (() => {
  const c = document.createElement('canvas')
  c.width = c.height = 64
  const ctx = c.getContext('2d')
  const g = ctx.createRadialGradient(32, 32, 0, 32, 32, 32)
  g.addColorStop(0.0, 'rgba(255,255,255,1)')
  g.addColorStop(0.25, 'rgba(255,255,255,0.85)')
  g.addColorStop(1.0, 'rgba(255,255,255,0)')
  ctx.fillStyle = g
  ctx.fillRect(0, 0, 64, 64)
  return new THREE.CanvasTexture(c)
})()

const vertexShader = `
  attribute vec3 color;
  attribute float psize;
  varying vec3 vColor;
  uniform float uScale;
  void main() {
    vColor = color;
    vec4 mv = modelViewMatrix * vec4(position, 1.0);
    gl_PointSize = psize * uScale / max(-mv.z, 1.0);
    gl_Position = projectionMatrix * mv;
  }`

const fragmentShader = `
  uniform sampler2D uTex;
  uniform float uOpacity;
  varying vec3 vColor;
  void main() {
    vec4 t = texture2D(uTex, gl_PointCoord);
    if (t.a < 0.05) discard;
    gl_FragColor = vec4(vColor, 1.0) * t * uOpacity;
  }`

/**
 * positions/colors: Float32Array length 3N ; sizes: Float32Array length N.
 * Additive-blended sprites with true per-point size + perspective attenuation.
 */
export default function Points({ positions, colors, sizes, scale = 420, opacity = 0.95, additive = true }) {
  const points = useMemo(() => {
    const geo = new THREE.BufferGeometry()
    geo.setAttribute('position', new THREE.BufferAttribute(positions, 3))
    geo.setAttribute('color', new THREE.BufferAttribute(colors, 3))
    geo.setAttribute('psize', new THREE.BufferAttribute(sizes, 1))
    const mat = new THREE.ShaderMaterial({
      uniforms: {
        uTex: { value: sprite },
        uScale: { value: scale },
        uOpacity: { value: opacity },
      },
      vertexShader,
      fragmentShader,
      transparent: true,
      depthWrite: false,
      blending: additive ? THREE.AdditiveBlending : THREE.NormalBlending,
    })
    return new THREE.Points(geo, mat)
  }, [positions, colors, sizes, scale, opacity, additive])

  useEffect(() => () => {
    points.geometry.dispose()
    points.material.dispose()
  }, [points])

  return <primitive object={points} />
}
