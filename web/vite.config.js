import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// In dev, proxy API calls to the FastAPI backend (uvicorn on :8000).
// In production the same FastAPI process serves these routes + the built SPA.
const api = { target: 'http://localhost:8000', changeOrigin: true }

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/stars': api,
      '/density': api,
      '/hr': api,
      '/clusters': api,
      '/health': api,
    },
  },
})
