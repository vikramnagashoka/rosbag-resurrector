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
// Notebook UI overhaul (v0.8) — lives at /n, built alongside the existing
// pages. Lazy so its scoped theme + future Plotly cells don't load until
// visited. Becomes the default surface only once it reaches parity.
const NotebookWorkspace = React.lazy(() => import('./notebook/NotebookWorkspace'))

const loadingStyle: React.CSSProperties = {
  color: 'var(--color-text-secondary)',
  padding: 'var(--space-5)',
}

// The classic page-per-feature layout (dark theme). The notebook workspace
// renders full-screen outside this chrome.
function ClassicLayout() {
  return (
    <>
      <NavBar />
      <main style={{ padding: 'var(--space-5)', maxWidth: '1400px', margin: '0 auto' }}>
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
      </main>
    </>
  )
}

export default function App() {
  return (
    <ErrorToastProvider>
      <BrowserRouter>
        <Suspense fallback={<div style={loadingStyle}>Loading…</div>}>
          <Routes>
            {/* Notebook workspace — full-screen, no NavBar/main chrome. */}
            <Route path="/n" element={<NotebookWorkspace />} />
            <Route path="/n/:notebookId" element={<NotebookWorkspace />} />
            {/* Everything else keeps the classic NavBar + main layout. */}
            <Route path="/*" element={<ClassicLayout />} />
          </Routes>
        </Suspense>
      </BrowserRouter>
    </ErrorToastProvider>
  )
}
