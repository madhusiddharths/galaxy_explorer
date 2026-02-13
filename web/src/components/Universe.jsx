import React, { useRef, useMemo, useState } from 'react'
import { Canvas, useFrame } from '@react-three/fiber'
import { OrbitControls, Stars as BackgroundStars } from '@react-three/drei'
import * as THREE from 'three'



const StarPoints = ({ stars, onHover }) => {
    const meshRef = useRef()

    // Create a circular texture
    const texture = useMemo(() => {
        const canvas = document.createElement('canvas');
        canvas.width = 32;
        canvas.height = 32;
        const context = canvas.getContext('2d');
        context.beginPath();
        context.arc(16, 16, 14, 0, 2 * Math.PI);
        context.fillStyle = 'white';
        context.fill();
        const t = new THREE.CanvasTexture(canvas);
        return t;
    }, []);

    // Convert stars data to buffer attributes
    const { positions, colors, sizes } = useMemo(() => {
        const count = stars.length
        const positions = new Float32Array(count * 3)
        const colors = new Float32Array(count * 3)
        const sizes = new Float32Array(count)

        const purple = new THREE.Color(0x8A2BE2) // BlueViolet
        const white = new THREE.Color(0xFFFFFF)
        const tempColor = new THREE.Color()

        for (let i = 0; i < count; i++) {
            const star = stars[i]
            positions[i * 3] = star.x
            positions[i * 3 + 1] = star.y
            positions[i * 3 + 2] = star.z

            // Color based on mag
            const mag = star.mag || 15
            const t = Math.max(0, Math.min(1, (20 - mag) / 15))

            tempColor.lerpColors(purple, white, t)

            colors[i * 3] = tempColor.r
            colors[i * 3 + 1] = tempColor.g
            colors[i * 3 + 2] = tempColor.b

            sizes[i] = (t * 3.0) + 1.5
        }

        return { positions, colors, sizes }
    }, [stars])

    return (
        <points
            ref={meshRef}
            onPointerMove={(e) => {
                e.stopPropagation()
                onHover(stars[e.index])
            }}
            onPointerOut={() => onHover(null)}
        >
            <bufferGeometry>
                <bufferAttribute
                    attach="attributes-position"
                    count={positions.length / 3}
                    array={positions}
                    itemSize={3}
                />
                <bufferAttribute
                    attach="attributes-color"
                    count={colors.length / 3}
                    array={colors}
                    itemSize={3}
                />
                <bufferAttribute
                    attach="attributes-size"
                    count={sizes.length}
                    array={sizes}
                    itemSize={1}
                />
            </bufferGeometry>
            <pointsMaterial
                map={texture}
                size={5}
                sizeAttenuation={true}
                vertexColors
                transparent
                opacity={0.9}
                depthWrite={false}
                blending={THREE.AdditiveBlending}
                alphaTest={0.01}
            />
        </points>
    )
}

const Universe = ({ stars }) => {
    const [hoveredStar, setHoveredStar] = useState(null)

    // Removed early return to ensure background and Earth are always visible
    // if (!stars || stars.length === 0) return null

    return (
        <div style={{ width: '100vw', height: '100vh', background: '#000', position: 'relative' }}>
            {hoveredStar && (
                <div style={{
                    position: 'absolute',
                    top: 80,
                    right: 20,
                    background: 'rgba(0,0,0,0.8)',
                    color: 'white',
                    padding: '10px',
                    borderRadius: '8px',
                    pointerEvents: 'none',
                    zIndex: 100,
                    border: '1px solid #444'
                }}>
                    <h3>Star Details</h3>
                    <p><b>ID:</b> {hoveredStar.source_id}</p>
                    <p><b>Dist:</b> {hoveredStar.dist?.toFixed(2)} ly</p>
                    <p><b>Mag:</b> {hoveredStar.mag?.toFixed(2)}</p>
                </div>
            )}

            <Canvas camera={{ position: [0, 0, 4000], fov: 60, far: 50000 }}>
                <ambientLight intensity={0.1} />
                <pointLight position={[10, 10, 10]} />
                <OrbitControls enablePan={true} enableZoom={true} enableRotate={true} />

                <BackgroundStars radius={30000} depth={50} count={5000} factor={4} saturation={0} fade speed={1} />

                <StarPoints key={stars.length} stars={stars} onHover={setHoveredStar} />

                {/* Earth Reference */}
                <mesh position={[0, 0, 0]}>
                    <sphereGeometry args={[10, 32, 32]} />
                    <meshStandardMaterial color="#20B2AA" emissive="#004040" roughness={0.5} />
                </mesh>

            </Canvas>
        </div>
    )
}

export default Universe
