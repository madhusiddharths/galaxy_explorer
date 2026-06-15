import { useRef, useEffect } from 'react'

const BP_MIN = -0.5, BP_MAX = 4.0      // colour (BP-RP): blue/hot -> red/cool
const M_MIN = -6, M_MAX = 18           // absolute G mag: luminous(top) -> faint(bottom)

// dark -> blue -> cyan -> gold -> white
const STOPS = [
  [0.0, [8, 10, 30]], [0.2, [28, 55, 150]], [0.45, [40, 160, 200]],
  [0.7, [230, 215, 120]], [1.0, [255, 255, 255]],
]
function ramp(t) {
  for (let i = 1; i < STOPS.length; i++) {
    if (t <= STOPS[i][0]) {
      const [t0, c0] = STOPS[i - 1], [t1, c1] = STOPS[i]
      const f = (t - t0) / (t1 - t0)
      return c0.map((c, k) => Math.round(c + (c1[k] - c) * f))
    }
  }
  return STOPS[STOPS.length - 1][1]
}

export default function HRDiagram({ bins, width = 340, height = 330 }) {
  const ref = useRef()
  useEffect(() => {
    const cv = ref.current
    if (!cv) return
    const ctx = cv.getContext('2d')
    const padL = 40, padB = 30, padT = 26, padR = 12
    const pw = width - padL - padR, ph = height - padT - padB

    ctx.clearRect(0, 0, width, height)
    ctx.fillStyle = '#070912'
    ctx.fillRect(0, 0, width, height)

    if (!bins || !bins.length) {
      ctx.fillStyle = '#8a93ac'; ctx.font = '12px system-ui'
      ctx.fillText('Loading HR diagram…', padL, height / 2)
      return
    }

    const maxLog = Math.log1p(Math.max(...bins.map((b) => b.n)))
    const xOf = (bp) => padL + ((bp - BP_MIN) / (BP_MAX - BP_MIN)) * pw
    const yOf = (m) => padT + ((m - M_MIN) / (M_MAX - M_MIN)) * ph
    const dw = (0.05 / (BP_MAX - BP_MIN)) * pw + 1
    const dh = (0.2 / (M_MAX - M_MIN)) * ph + 1

    for (const b of bins) {
      const t = Math.log1p(b.n) / maxLog
      const [r, g, bl] = ramp(t)
      ctx.fillStyle = `rgb(${r},${g},${bl})`
      ctx.fillRect(xOf(b.bp_rp), yOf(b.absmag) - dh, dw, dh)
    }

    // axes
    ctx.strokeStyle = '#2a3350'; ctx.lineWidth = 1
    ctx.strokeRect(padL, padT, pw, ph)
    ctx.fillStyle = '#cfd6e6'; ctx.font = '12px system-ui'
    ctx.fillText('HR diagram — colour vs luminosity', padL, 16)
    ctx.fillStyle = '#8a93ac'; ctx.font = '10px system-ui'
    ctx.fillText('blue / hot', padL, height - 8)
    ctx.fillText('red / cool', width - 60, height - 8)
    ctx.save(); ctx.translate(11, padT + ph / 2); ctx.rotate(-Math.PI / 2)
    ctx.fillText('← brighter', -28, 0); ctx.restore()
  }, [bins, width, height])

  return <canvas ref={ref} width={width} height={height} className="hr" />
}
