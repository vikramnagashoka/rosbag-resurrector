import React, { Suspense } from 'react'
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom'
import Library from './pages/Library'
import Health from './pages/Health'
import Compare from './pages/Compare'
import Search from './pages/Search'
import Datasets from './pages/Datasets'
import Bridge from './pages/Bridge'
import { ErrorToastProvider } from './ErrorToast'

// Lazy-load Plotly-heavy pages — they pull in plotly.js-cartesian
// (~1MB gz) which we don't want charging every Library/Health visit.
const Explorer = React.lazy(() => import('./pages/Explorer'))
const CompareRuns = React.lazy(() => import('./pages/CompareRuns'))

const navStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: '18px',
  padding: '12px 24px',
  background: '#161b22',
  borderBottom: '1px solid #30363d',
}

const logoStyle: React.CSSProperties = {
  fontSize: '18px',
  fontWeight: 700,
  color: '#58a6ff',
  marginRight: '12px',
}

const linkStyle: React.CSSProperties = {
  color: '#8b949e',
  fontSize: '14px',
  textDecoration: 'none',
}

const loadingStyle: React.CSSProperties = {
  color: '#8b949e',
  padding: '24px',
}

export default function App() {
  return (
    <ErrorToastProvider>
      <BrowserRouter>
        <nav style={navStyle}>
          <Link to="/" style={logoStyle}>
            RosBag Resurrector
          </Link>
          <Link to="/" style={linkStyle}>
            Library
          </Link>
          <Link to="/search" style={linkStyle}>
            Search
          </Link>
          <Link to="/datasets" style={linkStyle}>
            Datasets
          </Link>
          <Link to="/compare" style={linkStyle}>
            Compare
          </Link>
          <Link to="/compare-runs" style={linkStyle}>
            Compare runs
          </Link>
          <Link to="/bridge" style={linkStyle}>
            Bridge
          </Link>
        </nav>
        <main style={{ padding: '24px', maxWidth: '1400px', margin: '0 auto' }}>
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
            </Routes>
          </Suspense>
        </main>
      </BrowserRouter>
    </ErrorToastProvider>
  )
}
