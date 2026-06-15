import { useRef, useEffect } from 'react'
import { Canvas, useFrame } from '@react-three/fiber'
import { OrbitControls, Stars as BackgroundStars } from '@react-three/drei'
import * as THREE from 'three'

// Smoothly eases the orbit target toward `focus` when it changes.
function FocusController({ controls, focus }) {
  const goal = useRef(new THREE.Vector3())
  useEffect(() => {
    if (focus) goal.current.set(focus.x, focus.y, focus.z)
  }, [focus])
  useFrame(() => {
    if (focus && controls.current) {
      controls.current.target.lerp(goal.current, 0.08)
      controls.current.update()
    }
  })
  return null
}

function EarthMarker() {
  return (
    <mesh position={[0, 0, 0]}>
      <sphereGeometry args={[6, 24, 24]} />
      <meshStandardMaterial color="#2dd4bf" emissive="#0b3b36" roughness={0.5} />
    </mesh>
  )
}

export default function Scene({ children, cameraPos = [0, 0, 1600], far = 80000, focus = null }) {
  const controls = useRef()
  return (
    <Canvas camera={{ position: cameraPos, fov: 60, far }} dpr={[1, 2]}>
      <ambientLight intensity={0.25} />
      <pointLight position={[100, 100, 100]} intensity={0.8} />
      <OrbitControls ref={controls} enablePan enableZoom enableRotate
                     zoomSpeed={1.2} rotateSpeed={0.6} />
      <FocusController controls={controls} focus={focus} />
      <BackgroundStars radius={40000} depth={60} count={4000} factor={5}
                       saturation={0} fade speed={0.5} />
      <EarthMarker />
      {children}
    </Canvas>
  )
}
