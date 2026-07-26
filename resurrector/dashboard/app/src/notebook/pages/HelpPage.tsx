import React from 'react'
import { Link } from 'react-router-dom'
import NotebookPageShell from '../NotebookPageShell'

// Warm-themed port of the classic Help & Docs page. Same content, notebook
// paper palette. Intra-app links point at the notebook surfaces (/n/...)
// where those workflows now live.

function C({ children }: { children: React.ReactNode }) {
  return <code className="nb-doc-code">{children}</code>
}
function Block({ children }: { children: string }) {
  return <pre className="nb-doc-block">{children}</pre>
}

const CLI: [string, string][] = [
  ['resurrector doctor', 'Verify install — pass/warn/fail grid for every dep'],
  ['resurrector demo --full', 'Generate a synthetic bag and walk the pipeline'],
  ['resurrector scan ~/recordings', 'Index a folder of bags'],
  ['resurrector list --has-topic /imu/data --min-health 80', 'Filter the index'],
  ['resurrector info bag.mcap', 'Detailed summary of one bag'],
  ['resurrector health bag.mcap', '0–100 quality score + per-topic breakdown'],
  ['resurrector export bag.mcap -t /imu/data -f parquet -o ./out', 'Export topics'],
  ['resurrector index-frames bag.mcap', 'Build CLIP embeddings (one-time)'],
  ['resurrector dashboard', 'Launch this dashboard'],
  ['resurrector bridge playback bag.mcap', 'PlotJuggler-compatible WebSocket bridge'],
]

export default function HelpPage() {
  return (
    <NotebookPageShell
      title="Help & Docs"
      subtitle="Everything you need — from this dashboard, the CLI, the Python API, and the REST backend."
    >
      <div className="nb-doc">
        <section className="nb-panel nb-doc-card" id="quickstart">
          <h2 className="nb-doc-h2">Quick start</h2>
          <p>
            In the notebook, use <b>+ → Scan folder</b> in the rail to index a directory of bags, or
            open the <Link className="nb-doc-link" to="/n/library">Library</Link> to browse + scan.
            A blank notebook can also <b>upload a bag</b> directly.
          </p>
          <p>
            No bag yet? <C>pip install rosbag-resurrector</C> then <C>resurrector demo --full</C>{' '}
            writes a synthetic sample to <C>~/.resurrector/demo_sample.mcap</C> — scan{' '}
            <C>~/.resurrector/</C> and you're set.
          </p>
        </section>

        <section className="nb-panel nb-doc-card" id="tour">
          <h2 className="nb-doc-h2">Notebook tour</h2>
          <p>Every capability is a cell you add from the command bar or the suggestion chips:</p>
          <table className="nb-dtable nb-doc-table">
            <thead><tr><th>Cell</th><th>What it does</th></tr></thead>
            <tbody>
              {[
                ['Plot signal', 'Multi-series line chart with linked time-cursor + brush-to-Explain / Export range'],
                ['Transform', 'Derived signals — derivative, integral, moving average, low-pass, or a Polars expression'],
                ['Statistics', 'min / mean / max / σ per numeric column'],
                ['Health report', 'Score ring + per-check breakdown + summary'],
                ['Synchronize', 'Time-align two topics'],
                ['Compare bags', 'Overlay one topic across multiple bags on a shared time axis'],
                ['Camera frames', 'Scrub image / compressed-image topics'],
                ['3D scene', 'Live point cloud + TF triads'],
                ['Semantic search', 'CLIP natural-language frame search (needs the [vision] extra)'],
              ].map(([c, d]) => (
                <tr key={c}><td className="nb-td-accent">{c}</td><td>{d}</td></tr>
              ))}
            </tbody>
          </table>
          <p className="nb-doc-muted">
            Management surfaces live as pages: <Link className="nb-doc-link" to="/n/datasets">Datasets</Link>{' '}
            (versioned ML dataset collections) and <Link className="nb-doc-link" to="/n/bridge">Bridge</Link>{' '}
            (PlotJuggler WebSocket relay). Header <b>Export</b> writes the active bag to Parquet / HDF5 /
            CSV / LeRobot / RLDS.
          </p>
        </section>

        <section className="nb-panel nb-doc-card" id="cli">
          <h2 className="nb-doc-h2">CLI reference</h2>
          <p>Run <C>resurrector --help</C> for the full list. Most-used:</p>
          <table className="nb-dtable nb-doc-table">
            <thead><tr><th>Command</th><th>What it does</th></tr></thead>
            <tbody>
              {CLI.map(([cmd, desc]) => (
                <tr key={cmd}><td className="nb-td-accent nb-doc-cmd">{cmd}</td><td>{desc}</td></tr>
              ))}
            </tbody>
          </table>
          <p className="nb-doc-muted">
            macOS / zsh: quote pip extras — <C>pip install 'rosbag-resurrector[vision]'</C>.
          </p>
        </section>

        <section className="nb-panel nb-doc-card" id="python">
          <h2 className="nb-doc-h2">Python API</h2>
          <p>The library is also a Python package — useful in Jupyter, scripts, and ML pipelines.</p>
          <Block>{`from resurrector import BagFrame

bf = BagFrame("experiment.mcap")
bf.info()                                      # rich summary
df = bf["/imu/data"].to_polars()               # any topic → Polars

# Stream a large topic without OOM
for chunk in bf["/camera/rgb"].iter_chunks(chunk_size=10_000):
    process(chunk)

# Multi-stream sync
synced = bf.sync(["/imu/data", "/joint_states"], method="nearest", tolerance_ms=50)

# Health + export
report = bf.health_report()
bf.export(topics=["/imu/data"], format="parquet", output="./out", sync=True)`}</Block>
        </section>

        <section className="nb-panel nb-doc-card" id="rest">
          <h2 className="nb-doc-h2">REST API</h2>
          <p>Everything this dashboard does goes through a FastAPI backend:</p>
          <p>
            <a className="nb-doc-link" href="/docs" target="_blank" rel="noreferrer">→ /docs (Swagger)</a>
            {'   '}
            <a className="nb-doc-link" href="/redoc" target="_blank" rel="noreferrer">→ /redoc</a>
            {'   '}
            <a className="nb-doc-link" href="/openapi.json" target="_blank" rel="noreferrer">→ /openapi.json</a>
          </p>
          <p className="nb-doc-muted">
            All endpoints under <C>/api/...</C>. Filesystem routes honor <C>RESURRECTOR_ALLOWED_ROOTS</C>.
          </p>
        </section>

        <section className="nb-panel nb-doc-card" id="troubleshooting">
          <h2 className="nb-doc-h2">Troubleshooting</h2>
          <h3 className="nb-doc-h3">“zsh: no matches found: rosbag-resurrector[vision]”</h3>
          <p>zsh treats <C>[vision]</C> as a glob. Quote it: <C>pip install 'rosbag-resurrector[vision]'</C>.</p>
          <h3 className="nb-doc-h3">“403 from the scan input”</h3>
          <p>The dashboard only scans paths under <C>RESURRECTOR_ALLOWED_ROOTS</C> (defaults to home). Broaden it:</p>
          <Block>{`export RESURRECTOR_ALLOWED_ROOTS=/data/bags:/mnt/recordings
resurrector dashboard`}</Block>
          <h3 className="nb-doc-h3">“Search returns blank frames”</h3>
          <p>CLIP needs real camera footage, not the synthetic demo's noise frames. Index a real bag and re-run.</p>
        </section>

        <section className="nb-panel nb-doc-card" id="links">
          <h2 className="nb-doc-h2">Links</h2>
          <ul className="nb-doc-links">
            <li><a className="nb-doc-link" href="https://github.com/vikramnagashoka/rosbag-resurrector" target="_blank" rel="noreferrer">GitHub</a> — source, issues, releases</li>
            <li><a className="nb-doc-link" href="https://github.com/vikramnagashoka/rosbag-resurrector#readme" target="_blank" rel="noreferrer">README</a> — install, performance contract, formats</li>
            <li><a className="nb-doc-link" href="https://github.com/vikramnagashoka/rosbag-resurrector/blob/main/CHANGELOG.md" target="_blank" rel="noreferrer">CHANGELOG</a></li>
            <li><a className="nb-doc-link" href="https://pypi.org/project/rosbag-resurrector/" target="_blank" rel="noreferrer">PyPI</a></li>
          </ul>
        </section>
      </div>
    </NotebookPageShell>
  )
}
