import React, { useEffect, useRef, useState } from 'react'
import { api, BridgeStatus } from '../../api'
import { runWithToast, useErrorToast } from '../../ErrorToast'
import { useCapability } from '../../components/InstallBanner'
import NotebookPageShell from '../NotebookPageShell'

// Warm-themed port of the classic Bridge control page. Same workflow
// (status + playback/live start + controls), notebook paper palette.

const STATUS_POLL_MS = 3000

export default function BridgePage() {
  const [status, setStatus] = useState<BridgeStatus>({ running: false })
  const [mode, setMode] = useState<'playback' | 'live'>('playback')
  const [bagPath, setBagPath] = useState('')
  const [topics, setTopics] = useState('')
  const [speed, setSpeed] = useState(1.0)
  const [port, setPort] = useState(9090)
  const [starting, setStarting] = useState(false)
  const liveCap = useCapability('bridge_live')
  const toast = useErrorToast()
  const prevRunning = useRef(false)

  async function refreshStatus() {
    const s = await runWithToast(toast, () => api.bridgeStatus())
    if (s) {
      if (prevRunning.current && !s.running) {
        toast.push('warn', s.exited ? `Bridge exited with code ${s.return_code ?? '?'}` : 'Bridge stopped unexpectedly')
      }
      prevRunning.current = s.running
      setStatus(s)
    }
  }

  useEffect(() => {
    refreshStatus()
    const timer = setInterval(refreshStatus, STATUS_POLL_MS)
    return () => clearInterval(timer)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  async function startBridge() {
    setStarting(true)
    const body: Parameters<typeof api.startBridge>[0] = { mode, port }
    if (mode === 'playback') {
      if (!bagPath.trim()) { toast.push('error', 'Bag path is required for playback'); setStarting(false); return }
      body.bag_path = bagPath.trim(); body.speed = speed
    } else {
      const topicList = topics.split(',').map(t => t.trim()).filter(Boolean)
      if (!topicList.length) { toast.push('error', 'At least one topic is required for live mode'); setStarting(false); return }
      body.topics = topicList
    }
    const r = await runWithToast(toast, () => api.startBridge(body), { errorPrefix: 'Start bridge' })
    if (r) toast.push('info', `Bridge started on port ${r.port}`)
    await refreshStatus(); setStarting(false)
  }

  async function stopBridge() {
    const r = await runWithToast(toast, () => api.stopBridge())
    if (r?.stopped) toast.push('info', 'Bridge stopped')
    await refreshStatus()
  }

  async function sendControl(cmd: string) {
    const r = await runWithToast(toast, () => api.bridgeProxy('POST', `api/playback/${cmd}`), { errorPrefix: cmd })
    if (r) toast.push('info', `Sent ${cmd}`)
  }

  const liveBlocked = liveCap !== null && !liveCap.available

  return (
    <NotebookPageShell
      title="Bridge control"
      subtitle={<>PlotJuggler-compatible WebSocket bridge. Connect to <code>ws://localhost:{status.port ?? port}/ws</code> after starting.</>}
    >
      <div className={`nb-panel nb-bridge-status${status.running ? ' running' : ''}`}>
        <div className="nb-bridge-status-row">
          <div>
            <strong className={status.running ? 'nb-run-on' : 'nb-run-off'}>
              {status.running ? 'Running' : 'Not running'}
            </strong>
            {status.running && (
              <span className="nb-panel-sub" style={{ marginLeft: 12 }}>
                {status.mode} mode · port {status.port} · pid {status.pid}
              </span>
            )}
          </div>
          {status.running && <button className="nb-btn nb-btn-danger" onClick={stopBridge}>Stop</button>}
        </div>
      </div>

      {!status.running ? (
        <div className="nb-panel">
          <h3 className="nb-panel-h3">Start bridge</h3>
          <div className="nb-bridge-modes">
            {(['playback', 'live'] as const).map(m => (
              <button
                key={m}
                className={`nb-tf-tab${mode === m ? ' active' : ''}`}
                onClick={() => setMode(m)}
                title={m === 'live' && liveBlocked ? 'Live mode needs rclpy (ROS 2)' : undefined}
              >{m}</button>
            ))}
          </div>

          {mode === 'live' && liveBlocked && liveCap && (
            <div className="nb-bridge-banner">
              <div className="nb-bridge-banner-title">Bridge live mode needs rclpy (ROS 2).</div>
              <div className="nb-bridge-banner-body">
                Install ROS 2 to subscribe to running topics. Playback mode works without it — just
                hand it an existing recording.
              </div>
              <code>{liveCap.install_command}</code>
            </div>
          )}

          {mode === 'playback' ? (
            <>
              <label className="nb-modal-field">
                <span>Bag path</span>
                <input value={bagPath} placeholder="/path/to/recording.mcap" onChange={e => setBagPath(e.target.value)} />
              </label>
              <label className="nb-tf-field nb-tf-param" style={{ marginRight: 16 }}>
                <span>Speed</span>
                <input type="number" value={speed} min={0.1} max={20} step={0.1} onChange={e => setSpeed(Number(e.target.value))} />
              </label>
            </>
          ) : (
            <label className="nb-modal-field">
              <span>Topics (comma-separated)</span>
              <input value={topics} placeholder="/imu/data, /joint_states, /camera/rgb" onChange={e => setTopics(e.target.value)} />
            </label>
          )}

          <label className="nb-tf-field nb-tf-param">
            <span>Port</span>
            <input type="number" value={port} min={1024} max={65535} onChange={e => setPort(Number(e.target.value))} />
          </label>

          <div style={{ marginTop: 14 }}>
            <button
              className="nb-btn nb-btn-accent"
              onClick={startBridge}
              disabled={starting || (mode === 'live' && liveBlocked)}
              title={mode === 'live' && liveBlocked ? 'Install ROS 2 (rclpy) to use live mode' : undefined}
            >{starting ? 'Starting…' : 'Start bridge'}</button>
          </div>
        </div>
      ) : (
        <div className="nb-panel">
          <h3 className="nb-panel-h3">Playback control</h3>
          <div className="nb-bridge-modes">
            {['play', 'pause'].map(cmd => (
              <button key={cmd} className="nb-btn nb-btn-accent" onClick={() => sendControl(cmd)}>{cmd}</button>
            ))}
          </div>
          <p className="nb-panel-sub" style={{ marginTop: 14 }}>
            For seek / speed changes, open the standalone bridge viewer at{' '}
            <code>http://localhost:{status.port}/</code>.
          </p>
        </div>
      )}
    </NotebookPageShell>
  )
}
