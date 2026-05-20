import React, { useEffect, useState } from 'react'
import { api, Capability, CapabilityName, CapabilityMap } from '../api'

interface InstallBannerProps {
  capability: Capability
  title?: string
  helperText?: React.ReactNode
}

// Reusable warning banner for optional capabilities. Each surface that
// gates on `vision`, `bridge_live`, `ros1_convert`, or `all_exports`
// renders this when the capability is missing — uniform copy, single
// place to maintain the look.
export function InstallBanner({ capability, title, helperText }: InstallBannerProps) {
  return (
    <div style={{
      background: '#1c1c0e',
      border: '1px solid #d29922',
      borderRadius: 8,
      padding: 20,
      marginBottom: 16,
      color: '#d29922',
    }}>
      <strong style={{ fontSize: 14 }}>
        {title ?? `${capability.description} isn't available yet.`}
      </strong>
      {helperText && (
        <div style={{ marginTop: 8, color: '#e1e4e8', fontSize: 13 }}>
          {helperText}
        </div>
      )}
      <div style={{ marginTop: 12 }}>
        <CopyBlock text={capability.install_command} />
      </div>
      <div style={{ marginTop: 8, fontSize: 12, color: '#8b949e' }}>
        Restart <code>resurrector dashboard</code> after installing.
      </div>
    </div>
  )
}

// Tiny hook for pages that need the full capability map.
export function useCapabilities(): CapabilityMap | null {
  const [caps, setCaps] = useState<CapabilityMap | null>(null)
  useEffect(() => {
    let cancelled = false
    api.getCapabilities()
      .then(c => { if (!cancelled) setCaps(c) })
      .catch(() => { /* failure is fine; assume caps unknown */ })
    return () => { cancelled = true }
  }, [])
  return caps
}

// Single-capability variant — convenient when a page only gates on one.
export function useCapability(name: CapabilityName): Capability | null {
  const caps = useCapabilities()
  return caps ? caps[name] : null
}

function CopyBlock({ text }: { text: string }) {
  const isMultiline = text.includes('\n')
  return (
    <div style={{ display: 'flex', gap: 8, alignItems: 'flex-start' }}>
      <pre style={{
        flex: 1, background: '#0d1117', border: '1px solid #30363d',
        borderRadius: 4, padding: '8px 10px', fontSize: 12, color: '#e1e4e8',
        fontFamily: 'monospace', margin: 0, whiteSpace: 'pre-wrap',
        wordBreak: 'break-word',
      }}>{text}</pre>
      <CopyButton text={text} multiline={isMultiline} />
    </div>
  )
}

function CopyButton({ text, multiline }: { text: string; multiline?: boolean }) {
  const [copied, setCopied] = useState(false)
  return (
    <button
      type="button"
      onClick={() => {
        navigator.clipboard.writeText(text)
        setCopied(true)
        setTimeout(() => setCopied(false), 1500)
      }}
      style={{
        background: '#21262d', color: '#e1e4e8', border: '1px solid #30363d',
        borderRadius: 4, padding: '6px 10px', fontSize: 11, cursor: 'pointer',
        fontFamily: 'monospace', whiteSpace: 'nowrap',
        alignSelf: multiline ? 'flex-start' : 'center',
      }}
    >
      {copied ? 'Copied' : 'Copy'}
    </button>
  )
}
