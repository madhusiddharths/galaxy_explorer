import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import Universe from './components/Universe'
import Controls from './components/Controls'
import './App.css'

function App() {
  const [stars, setStars] = useState([])
  const [loading, setLoading] = useState(false)
  const [count, setCount] = useState(0)


  const fetchStars = useCallback(async (params) => {
    setLoading(true)
    try {
      const response = await axios.post('http://localhost:8000/stars', params)
      setStars(response.data.stars)
      setCount(response.data.count)
    } catch (error) {
      console.error("Failed to fetch stars:", error)
    } finally {
      setLoading(false)
    }
  }, [])

  return (
    <>
      <div style={{ position: 'absolute', top: 20, left: 20, color: 'white', zIndex: 10, pointerEvents: 'none' }}>
        <h1>Galaxy Explorer</h1>
        <p>{loading ? "Loading..." : `${count} Stars Loaded`}</p>
      </div>

      <Controls onUpdate={fetchStars} />
      <Universe stars={stars} />
    </>
  )
}

export default App
