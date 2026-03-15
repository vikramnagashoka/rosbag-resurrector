import React from 'react'
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom'
import Library from './pages/Library'
import Explorer from './pages/Explorer'
import Health from './pages/Health'
import Compare from './pages/Compare'

const navStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: '24px',
  padding: '12px 24px',
  background: '#161b22',
  borderBottom: '1px solid #30363d',
}

const logoStyle: React.CSSProperties = {
  fontSize: '18px',
  fontWeight: 700,
  color: '#58a6ff',
}

const linkStyle: React.CSSProperties = {
  color: '#8b949e',
  fontSize: '14px',
}

export default function App() {
  return (
    <BrowserRouter>
      <nav style={navStyle}>
        <Link to="/" style={logoStyle}>RosBag Resurrector</Link>
        <Link to="/" style={linkStyle}>Library</Link>
        <Link to="/compare" style={linkStyle}>Compare</Link>
      </nav>
      <main style={{ padding: '24px', maxWidth: '1400px', margin: '0 auto' }}>
        <Routes>
          <Route path="/" element={<Library />} />
          <Route path="/bag/:id" element={<Explorer />} />
          <Route path="/bag/:id/health" element={<Health />} />
          <Route path="/compare" element={<Compare />} />
        </Routes>
      </main>
    </BrowserRouter>
  )
}
