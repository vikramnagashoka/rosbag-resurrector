import React, { useEffect, useRef, useState } from 'react'
import { api, BridgeStatus } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'

// Polled every 3s when we have a bridge running so the UI notices
// unexpected subprocess death (E2 in the plan review).
const STATUS_POLL_MS = 3000

export default function Bridge() {
  const [status, setStatus] = useState<BridgeStatus>({ running: false })
  const [mode, setMode] = useState<'playback' | 'live'>('playback')
  const [bagPath, setBagPath] = useState('')
  const [topics, setTopics] = useState('')
  const [speed, setSpeed] = useState(1.0)
  const [port, setPort] = useState(9090)
  const [starting, setStarting] = useState(false)
  const toast = useErrorToast()
  const prevRunning = useRef(false)

  async function refreshStatus(showToast = false) {
    const s = await runWithToast(toast, () => api.bridgeStatus())
    if (s) {
      // E2: notice when bridge dies between polls
      if (prevRunning.current && !s.running) {
        const reason = s.exited
          ? `Bridge exited with code ${s.return_code ?? '?'}`
          : 'Bridge stopped unexpectedly'
        toast.push('warn', reason)
      }
      prevRunning.current = s.running
      setStatus(s)
    }
  }

  useEffect(() => {
    refreshStatus()
    const timer = setInterval(() => {
      refreshStatus()
    }, STATUS_POLL_MS)
    return () => clearInterval(timer)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  async function startBridge() {
    setStarting(true)
    const body: Parameters<typeof api.startBridge>[0] = { mode, port }
    if (mode === 'playback') {
      if (!bagPath.trim()) {
        toast.push('error', 'Bag path is required for playback')
        setStarting(false)
        return
      }
      body.bag_path = bagPath.trim()
      body.speed = speed
    } else {
      const topicList = topics.split(',').map(t => t.trim()).filter(Boolean)
      if (!topicList.length) {
        toast.push('error', 'At least one topic is required for live mode')
        setStarting(false)
        return
      }
      body.topics = topicList
    }
    const r = await runWithToast(toast, () => api.startBridge(body), {
      errorPrefix: 'Start bridge',
    })
    if (r) toast.push('info', `Bridge started on port ${r.port}`)
    await refreshStatus()
    setStarting(false)
  }

  async function stopBridge() {
    const r = await runWithToast(toast, () => api.stopBridge())
    if (r?.stopped) toast.push('info', 'Bridge stopped')
    await refreshStatus()
  }

  async function sendControl(cmd: string) {
    const r = await runWithToast(
      toast,
      () => api.bridgeProxy('POST', `api/playback/${cmd}`),
      { errorPrefix: cmd },
    )
    if (r) toast.push('info', `Sent ${cmd}`)
  }

  const panelStyle: React.CSSProperties = {
    background: '#161b22',
    border: '1px solid #30363d',
    borderRadius: 8,
    padding: 16,
  }

  return (
    <div>
      <h1 style={{ fontSize: 24, fontWeight: 600, marginBottom: 4 }}>Bridge control</h1>
      <p style={{ color: '#8b949e', fontSize: 14, marginBottom: 20 }}>
        Start a PlotJuggler-compatible WebSocket bridge from any bag file,
        or relay live ROS 2 topics. Connect PlotJuggler to{' '}
        <code>ws://localhost:{status.port ?? port}/ws</code> after starting.
      </p>

      <div
        style={{
          ...panelStyle,
          marginBottom: 24,
          borderLeft:
            status.running ? '3px solid #3fb950' : '3px solid #8b949e',
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <strong style={{ color: status.running ? '#3fb950' : '#8b949e' }}>
              {status.running ? 'Running' : 'Not running'}
            </strong>
            {status.running && (
              <span style={{ color: '#8b949e', fontSize: 13, marginLeft: 12 }}>
                {status.mode} mode · port {status.port} · pid {status.pid}
              </span>
            )}
          </div>
          {status.running && (
            <button
              onClick={stopBridge}
              style={{
                background: '#da3633',
                color: '#fff',
                border: 'none',
                borderRadius: 6,
                padding: '6px 14px',
                cursor: 'pointer',
              }}
            >
              Stop
            </button>
          )}
        </div>
      </div>

      {!status.running ? (
        <div style={panelStyle}>
          <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 12 }}>Start bridge</h3>
          <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
            {(['playback', 'live'] as const).map(m => (
              <button
                key={m}
                onClick={() => setMode(m)}
                style={{
                  background: mode === m ? '#1f6feb22' : '#21262d',
                  border: mode === m ? '1px solid #1f6feb' : '1px solid #30363d',
                  borderRadius: 6,
                  padding: '6px 14px',
                  color: '#e1e4e8',
                  cursor: 'pointer',
                  fontSize: 13,
                }}
              >
                {m}
              </button>
            ))}
          </div>

          {mode === 'playback' ? (
            <>
              <label style={{ display: 'block', marginBottom: 12 }}>
                <span style={{ fontSize: 13, color: '#8b949e' }}>Bag path</span>
                <input
                  value={bagPath}
                  onChange={e => setBagPath(e.target.value)}
                  placeholder="/path/to/recording.mcap"
                  style={{
                    width: '100%',
                    background: '#0d1117',
                    border: '1px solid #30363d',
                    borderRadius: 6,
                    padding: '8px 12px',
                    color: '#e1e4e8',
                    marginTop: 4,
                  }}
                />
              </label>
              <label style={{ display: 'inline-block', marginBottom: 12, marginRight: 16 }}>
                <span style={{ fontSize: 13, color: '#8b949e' }}>Speed</span>
                <input
                  type="number"
                  value={speed}
                  onChange={e => setSpeed(Number(e.target.value))}
                  min={0.1}
                  max={20}
                  step={0.1}
                  style={{
                    display: 'block',
                    background: '#0d1117',
                    border: '1px solid #30363d',
                    borderRadius: 6,
                    padding: '6px 10px',
                    color: '#e1e4e8',
                    marginTop: 4,
                    width: 100,
                  }}
                />
              </label>
            </>
          ) : (
            <label style={{ display: 'block', marginBottom: 12 }}>
              <span style={{ fontSize: 13, color: '#8b949e' }}>Topics (comma-separated)</span>
              <input
                value={topics}
                onChange={e => setTopics(e.target.value)}
                placeholder="/imu/data, /joint_states, /camera/rgb"
                style={{
                  width: '100%',
                  background: '#0d1117',
                  border: '1px solid #30363d',
                  borderRadius: 6,
                  padding: '8px 12px',
                  color: '#e1e4e8',
                  marginTop: 4,
                }}
              />
            </label>
          )}
          <label style={{ display: 'inline-block', marginBottom: 12, marginRight: 16 }}>
            <span style={{ fontSize: 13, color: '#8b949e' }}>Port</span>
            <input
              type="number"
              value={port}
              onChange={e => setPort(Number(e.target.value))}
              min={1024}
              max={65535}
              style={{
                display: 'block',
                background: '#0d1117',
                border: '1px solid #30363d',
                borderRadius: 6,
                padding: '6px 10px',
                color: '#e1e4e8',
                marginTop: 4,
                width: 100,
              }}
            />
          </label>

          <div style={{ marginTop: 8 }}>
            <button
              onClick={startBridge}
              disabled={starting}
              style={{
                background: starting ? '#21262d' : '#238636',
                color: '#fff',
                border: 'none',
                borderRadius: 6,
                padding: '8px 20px',
                cursor: starting ? 'not-allowed' : 'pointer',
                fontWeight: 600,
              }}
            >
              {starting ? 'Starting…' : 'Start bridge'}
            </button>
          </div>
        </div>
      ) : (
        <div style={panelStyle}>
          <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 12 }}>Playback control</h3>
          <div style={{ display: 'flex', gap: 8 }}>
            {['play', 'pause'].map(cmd => (
              <button
                key={cmd}
                onClick={() => sendControl(cmd)}
                style={{
                  background: '#238636',
                  color: '#fff',
                  border: 'none',
                  borderRadius: 6,
                  padding: '8px 20px',
                  cursor: 'pointer',
                }}
              >
                {cmd}
              </button>
            ))}
          </div>
          <p style={{ color: '#8b949e', fontSize: 13, marginTop: 16 }}>
            For seek / speed changes in detail, open the standalone bridge viewer at{' '}
            <code>http://localhost:{status.port}/</code>.
          </p>
        </div>
      )}
    </div>
  )
}
