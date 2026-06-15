import { useState, useEffect, useMemo } from 'react'
import axios from 'axios'
import Scene from '../components/Scene'
import Points from '../components/Points'

function hsl(h, s, l) {
  const a = s * Math.min(l, 1 - l)
  const f = (n) => {
    const k = (n + h / 30) % 12
    return l - a * Math.max(-1, Math.min(k - 3, 9 - k, 1))
  }
  return [f(0), f(8), f(4)]
}
const clusterColor = (id) => (id < 0 ? [0.32, 0.35, 0.45] : hsl((id * 137.5) % 360, 0.65, 0.62))

export default function Families() {
  const [data, setData] = useState({ clusters: [], stars: [] })
  const [loading, setLoading] = useState(true)
  const [focus, setFocus] = useState(null)
  const [active, setActive] = useState(null)

  useEffect(() => {
    setLoading(true)
    axios.get('/clusters')
      .then((r) => setData(r.data))
      .catch(() => setData({ clusters: [], stars: [] }))
      .finally(() => setLoading(false))
  }, [])

  const { positions, colors, sizes } = useMemo(() => {
    const s = data.stars
    const n = s.length
    const positions = new Float32Array(n * 3)
    const colors = new Float32Array(n * 3)
    const sizes = new Float32Array(n)
    s.forEach((st, i) => {
      positions[i * 3] = st.x; positions[i * 3 + 1] = st.y; positions[i * 3 + 2] = st.z
      const dim = active != null && st.cid !== active
      const c = clusterColor(st.cid)
      const k = dim ? 0.25 : 1
      colors[i * 3] = c[0] * k; colors[i * 3 + 1] = c[1] * k; colors[i * 3 + 2] = c[2] * k
      sizes[i] = (2.5 + Math.max(0, 12 - st.mag) * 0.5) * (dim ? 0.6 : 1)
    })
    return { positions, colors, sizes }
  }, [data, active])

  const pick = (c) => {
    setActive(c.id)
    setFocus({ x: c.x, y: c.y, z: c.z })
  }

  return (
    <>
      <Scene cameraPos={[0, 250, 950]} far={20000} focus={focus}>
        <Points positions={positions} colors={colors} sizes={sizes} scale={380} opacity={0.95} />
      </Scene>

      {loading && <div className="loading">clustering stars in 6D…</div>}

      <div className="panel card controls">
        <h3>Stellar Families</h3>
        <div className="muted" style={{ fontSize: '0.78rem' }}>
          Open clusters &amp; co-moving groups found by HDBSCAN over 6D phase space
          (position + velocity) within the solar neighbourhood. Click a group to fly to it.
        </div>
        <div className="row" style={{ marginTop: 10 }}>
          <span className="muted">Groups</span><span>{data.clusters.length}</span>
        </div>
        <div className="row"><span className="muted">Member stars</span>
          <span>{data.stars.length.toLocaleString()}</span></div>
        {active != null && (
          <button style={{ marginTop: 10 }} onClick={() => { setActive(null); setFocus({ x: 0, y: 0, z: 0 }) }}>
            ← Show all groups
          </button>
        )}
      </div>

      <div className="panel card clist">
        <h3 style={{ margin: '2px 0 8px' }}>Catalogue</h3>
        {data.clusters.map((c) => (
          <div className="item" key={c.id} onClick={() => pick(c)}
               style={{ background: active === c.id ? 'rgba(122,162,255,0.14)' : undefined }}>
            <span className="swatch" style={{ background: `rgb(${clusterColor(c.id).map((v) => Math.round(v * 255)).join(',')})` }} />
            <span style={{ flex: 1 }}>{c.name || `Group ${c.id}`}</span>
            <span className="muted">{c.n}★ · {Math.round(c.dist)} ly</span>
          </div>
        ))}
        {!data.clusters.length && !loading && <div className="muted" style={{ fontSize: '0.8rem' }}>No clusters yet — run bq_jobs/clustering.py.</div>}
      </div>
    </>
  )
}
