// Top navigation — v0.6.0 refresh (Sub-feature C.2)
//
// Extracted from App.tsx so the styling lives in a CSS module
// (NavBar.module.css) and active-route highlighting is centralized.
// Pages and routing logic stay in App.tsx.

import { Link, useLocation } from 'react-router-dom'
import styles from './NavBar.module.css'

const PRIMARY_LINKS = [
  { to: '/', label: 'Library', match: (p: string) => p === '/' || p.startsWith('/bag/') },
  { to: '/search', label: 'Search', match: (p: string) => p === '/search' },
  { to: '/datasets', label: 'Datasets', match: (p: string) => p.startsWith('/datasets') },
  { to: '/compare', label: 'Compare', match: (p: string) => p === '/compare' },
  { to: '/compare-runs', label: 'Compare runs', match: (p: string) => p === '/compare-runs' },
  { to: '/bridge', label: 'Bridge', match: (p: string) => p === '/bridge' },
] as const

export default function NavBar() {
  const { pathname } = useLocation()

  return (
    <nav className={styles.nav}>
      <Link to="/" className={styles.logo}>
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
        to="/n"
        className={`${styles.link} ${pathname.startsWith('/n') ? styles.linkActive : ''}`}
        title="The new notebook workspace (experimental)"
      >
        ✦ Notebook
      </Link>
      <Link
        to="/help"
        className={`${styles.link} ${pathname === '/help' ? styles.linkActive : ''}`}
      >
        Help & Docs
      </Link>
    </nav>
  )
}
