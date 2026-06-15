import { useState, useEffect, useMemo } from 'react'
import axios from 'axios'
import Scene from '../components/Scene'
import Points from '../components/Points'
import HRDiagram from '../components/HRDiagram'

// density heat ramp (matches the HR palette feel)
function heat(t) {
  const s = [[8, 10, 30], [28, 55, 150], [40, 160, 200], [230, 215, 120], [255, 255, 255]]
  const x = Math.min(0.999, Math.max(0, t)) * (s.length - 1)
  const i = Math.floor(x), f = x - i
  return s[i].map((c, k) => (c + (s[i + 1][k] - c) * f) / 255)
}
// approximate true star colour from BP-RP
function bpColor(bp) {
  const t = Math.min(1, Math.max(0, (bp + 0.4) / 3.2))
  return [0.6 + 0.4 * t, 0.7 + 0.1 * t - 0.2 * t * t, 1.0 - 0.7 * t]
}

export default function Structure() {
  const [minD, setMinD] = useState(0)
  const [maxD, setMaxD] = useState(2000)
  const [minN, setMinN] = useState(0)
  const [mode, setMode] = useState('density')
  const [voxels, setVoxels] = useState([])
  const [hr, setHr] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => { axios.get('/hr').then((r) => setHr(r.data.bins)).catch(() => {}) }, [])

  useEffect(() => {
    const t = setTimeout(() => {
      setLoading(true)
      axios.post('/density', { min_dist: minD, max_dist: maxD })
        .then((r) => setVoxels(r.data.voxels))
        .catch(() => setVoxels([]))
        .finally(() => setLoading(false))
    }, 250)
    return () => clearTimeout(t)
  }, [minD, maxD])

  const { positions, colors, sizes, totalStars, filteredCount } = useMemo(() => {
    const filteredVoxels = voxels.filter(v => v.n >= minN)
    const n = filteredVoxels.length
    const positions = new Float32Array(n * 3)
    const colors = new Float32Array(n * 3)
    const sizes = new Float32Array(n)
    let total = 0
    const maxLog = Math.log1p(Math.max(1, ...voxels.map((v) => v.n)))
    filteredVoxels.forEach((v, i) => {
      positions[i * 3] = v.x; positions[i * 3 + 1] = v.y; positions[i * 3 + 2] = v.z
      const c = mode === 'density' ? heat(Math.log1p(v.n) / maxLog) : bpColor(v.bp_rp ?? 1)
      colors[i * 3] = c[0]; colors[i * 3 + 1] = c[1]; colors[i * 3 + 2] = c[2]
      sizes[i] = 3.5 + 22 * Math.sqrt(v.n) / Math.sqrt(Math.exp(maxLog))
      total += v.n
    })
    return { positions, colors, sizes, totalStars: total, filteredCount: n }
  }, [voxels, mode, minN])

  const cam = Math.max(400, maxD * 0.9)

  return (
    <>
      <Scene cameraPos={[0, cam * 0.4, cam]} far={Math.max(60000, maxD * 4)}>
        <Points positions={positions} colors={colors} sizes={sizes} scale={1700} opacity={0.9} />
      </Scene>

      {loading && <div className="loading">scanning the catalogue…</div>}

      <div className="panel card controls">
        <h3>Structure of the Solar Neighbourhood</h3>
        <div className="muted" style={{ fontSize: '0.78rem' }}>
          Stellar density aggregated over the whole Gaia catalogue, per HEALPix sector × distance shell.
        </div>
        <label>Min distance: {minD} ly</label>
        <input type="range" min={0} max={16000} step={100} value={minD}
               onChange={(e) => setMinD(Math.min(+e.target.value, maxD - 100))} />
        <label>Max distance: {maxD} ly</label>
        <input type="range" min={200} max={17000} step={100} value={maxD}
               onChange={(e) => setMaxD(Math.max(+e.target.value, minD + 100))} />
        <label>Min stars/cluster: {minN.toLocaleString()}</label>
        <input type="range" min={0} max={30000} step={50} value={minN}
               onChange={(e) => setMinN(+e.target.value)} />
        <label>Colour by</label>
        <div style={{ display: 'flex', gap: 8 }}>
          <button onClick={() => setMode('density')}
                  style={{ borderColor: mode === 'density' ? 'var(--accent)' : undefined }}>Density</button>
          <button onClick={() => setMode('colour')}
                  style={{ borderColor: mode === 'colour' ? 'var(--accent)' : undefined }}>Star colour</button>
        </div>
      </div>

      <div className="panel card readout">
        <div className="row">
          <span className="muted">Voxels</span>
          <span>{filteredCount.toLocaleString()} <span className="muted" style={{fontSize: '0.8em'}}>({voxels.length.toLocaleString()})</span></span>
        </div>
        <div className="row"><span className="muted">Stars</span><span>{totalStars.toLocaleString()}</span></div>
        <div className="row muted" style={{ marginTop: 6, fontSize: '0.72rem' }}>teal dot = Sun</div>
      </div>

      <div className="panel card legend" style={{ padding: 8 }}>
        <HRDiagram bins={hr} />
      </div>
    </>
  )
}
