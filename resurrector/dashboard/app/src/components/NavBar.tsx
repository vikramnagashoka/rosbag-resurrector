// Top navigation — v0.6.0 refresh (Sub-feature C.2)
//
// Extracted from App.tsx so the styling lives in a CSS module
// (NavBar.module.css) and active-route highlighting is centralized.
// Pages and routing logic stay in App.tsx.

import { Link, useLocation } from 'react-router-dom'
import styles from './NavBar.module.css'

// The classic UI lives under /classic/*. These are its absolute links.
const PRIMARY_LINKS = [
  { to: '/classic', label: 'Library', match: (p: string) => p === '/classic' || p.startsWith('/classic/bag/') },
  { to: '/classic/search', label: 'Search', match: (p: string) => p === '/classic/search' },
  { to: '/classic/datasets', label: 'Datasets', match: (p: string) => p.startsWith('/classic/datasets') },
  { to: '/classic/compare', label: 'Compare', match: (p: string) => p === '/classic/compare' },
  { to: '/classic/compare-runs', label: 'Compare runs', match: (p: string) => p === '/classic/compare-runs' },
  { to: '/classic/bridge', label: 'Bridge', match: (p: string) => p === '/classic/bridge' },
] as const

export default function NavBar() {
  const { pathname } = useLocation()

  return (
    <nav className={styles.nav}>
      <Link to="/classic" className={styles.logo}>
        <span className={styles.logoMark} />
        RosBag Resurrector
      </Link>
      {PRIMARY_LINKS.map(l => (
        <Link
          key={l.to}
          to={l.to}
          className={`${styles.link} ${l.match(pathname) ? styles.linkActive : ''}`}
        >
          {l.label}
        </Link>
      ))}
      <div className={styles.spacer} />
      <Link
        to="/"
        className={styles.link}
        title="The notebook workspace (now the default UI)"
      >
        ✦ Notebook
      </Link>
      <Link
        to="/classic/help"
        className={`${styles.link} ${pathname === '/classic/help' ? styles.linkActive : ''}`}
      >
        Help & Docs
      </Link>
    </nav>
  )
}
