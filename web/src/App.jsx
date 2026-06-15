import { useState } from 'react'
import Structure from './pages/Structure'
import Families from './pages/Families'
import './App.css'

const TABS = [
  { id: 'structure', label: 'Structure' },
  { id: 'families', label: 'Families' },
]

export default function App() {
  const [page, setPage] = useState('structure')
  return (
    <div className="app">
      <div className="nav">
        <div className="brand">
          Galaxy Explorer <span className="sub">· Gaia DR3 · 557M stars</span>
        </div>
        <div className="tabs">
          {TABS.map((t) => (
            <button key={t.id}
                    className={`tab ${page === t.id ? 'active' : ''}`}
                    onClick={() => setPage(t.id)}>
              {t.label}
            </button>
          ))}
        </div>
      </div>

      {page === 'structure' ? <Structure /> : <Families />}
    </div>
  )
}
