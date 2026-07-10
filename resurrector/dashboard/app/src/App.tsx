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
// Notebook-native warm-themed workflow pages (ports of the classic
// Datasets + Bridge). Full-screen like the workspace, scoped `.nb` theme.
const DatasetsPage = React.lazy(() => import('./notebook/pages/DatasetsPage'))
const BridgePage = React.lazy(() => import('./notebook/pages/BridgePage'))
const LibraryPage = React.lazy(() => import('./notebook/pages/LibraryPage'))
const HelpPage = React.lazy(() => import('./notebook/pages/HelpPage'))

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
            {/* Notebook workspace + its warm-themed workflow pages —
                full-screen, no NavBar/main chrome. Static paths rank above
                the :notebookId param, so /n/datasets wins over /n/:id. */}
            <Route path="/n" element={<NotebookWorkspace />} />
            <Route path="/n/datasets" element={<DatasetsPage />} />
            <Route path="/n/bridge" element={<BridgePage />} />
            <Route path="/n/library" element={<LibraryPage />} />
            <Route path="/n/help" element={<HelpPage />} />
            <Route path="/n/:notebookId" element={<NotebookWorkspace />} />
            {/* Everything else keeps the classic NavBar + main layout. */}
            <Route path="/*" element={<ClassicLayout />} />
          </Routes>
        </Suspense>
      </BrowserRouter>
    </ErrorToastProvider>
  )
}
