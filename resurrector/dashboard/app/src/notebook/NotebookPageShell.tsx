import React from 'react'
import { Link } from 'react-router-dom'
import '../styles/notebook.css'

// Warm-themed chrome for the notebook-native workflow pages (Datasets,
// Bridge). Keeps the paper palette + a back-to-notebook link so leaving a
// notebook for one of these tools no longer drops the user into the old
// dark UI. Scoped under `.nb` like the rest of the notebook surface.

export default function NotebookPageShell({
  title, subtitle, actions, children,
}: {
  title: string
  subtitle?: React.ReactNode
  actions?: React.ReactNode
  children: React.ReactNode
}) {
  return (
    <div className="nb nb-page">
      <header className="nb-page-header">
        <div className="nb-page-head-left">
          <Link to="/n" className="nb-page-back" title="Back to the notebook workspace">← Notebook</Link>
          <div className="nb-page-titles">
            <div className="nb-page-title">{title}</div>
            {subtitle && <div className="nb-page-sub">{subtitle}</div>}
          </div>
        </div>
        {actions && <div className="nb-page-actions">{actions}</div>}
      </header>
      <main className="nb-page-main">{children}</main>
    </div>
  )
}
