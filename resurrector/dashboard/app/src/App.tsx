import React, { Suspense } from 'react'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Library from './pages/Library'
import Health from './pages/Health'
import Compare from './pages/Compare'
import Search from './pages/Search'
import Datasets from './pages/Datasets'
import Bridge from './pages/Bridge'
import Help from './pages/Help'
import NavBar from './components/NavBar'
import { ErrorToastProvider } from './ErrorToast'

// Lazy-load Plotly-heavy pages — they pull in plotly.js-cartesian
// (~1MB gz) which we don't want charging every Library/Health visit.
const Explorer = React.lazy(() => import('./pages/Explorer'))
const CompareRuns = React.lazy(() => import('./pages/CompareRuns'))

const loadingStyle: React.CSSProperties = {
  color: 'var(--color-text-secondary)',
  padding: 'var(--space-5)',
}

export default function App() {
  return (
    <ErrorToastProvider>
      <BrowserRouter>
        <NavBar />
        <main style={{ padding: 'var(--space-5)', maxWidth: '1400px', margin: '0 auto' }}>
          <Suspense fallback={<div style={loadingStyle}>Loading…</div>}>
            <Routes>
              <Route path="/" element={<Library />} />
              <Route path="/bag/:id" element={<Explorer />} />
              <Route path="/bag/:id/health" element={<Health />} />
              <Route path="/compare" element={<Compare />} />
              <Route path="/compare-runs" element={<CompareRuns />} />
              <Route path="/search" element={<Search />} />
              <Route path="/datasets" element={<Datasets />} />
              <Route path="/bridge" element={<Bridge />} />
              <Route path="/help" element={<Help />} />
            </Routes>
          </Suspense>
        </main>
      </BrowserRouter>
    </ErrorToastProvider>
  )
}
