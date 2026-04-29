import React from 'react'
import { Link } from 'react-router-dom'

const cardStyle: React.CSSProperties = {
  background: '#161b22',
  border: '1px solid #30363d',
  borderRadius: 8,
  padding: '20px 24px',
  marginBottom: 20,
}

const h2Style: React.CSSProperties = {
  fontSize: 18,
  fontWeight: 600,
  marginTop: 0,
  marginBottom: 12,
  color: '#e1e4e8',
}

const h3Style: React.CSSProperties = {
  fontSize: 14,
  fontWeight: 600,
  marginTop: 16,
  marginBottom: 6,
  color: '#e1e4e8',
}

const pStyle: React.CSSProperties = {
  fontSize: 14,
  lineHeight: 1.55,
  color: '#c9d1d9',
  marginBottom: 12,
}

const mutedStyle: React.CSSProperties = {
  fontSize: 13,
  color: '#8b949e',
  lineHeight: 1.5,
}

const codeBlockStyle: React.CSSProperties = {
  background: '#0d1117',
  border: '1px solid #30363d',
  borderRadius: 6,
  padding: '10px 14px',
  fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
  fontSize: 13,
  color: '#c9d1d9',
  whiteSpace: 'pre-wrap',
  overflow: 'auto',
  marginBottom: 12,
}

const inlineCodeStyle: React.CSSProperties = {
  background: '#0d1117',
  border: '1px solid #30363d',
  borderRadius: 4,
  padding: '1px 6px',
  fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
  fontSize: 12,
  color: '#79c0ff',
}

const linkStyle: React.CSSProperties = {
  color: '#58a6ff',
  textDecoration: 'none',
}

const tableStyle: React.CSSProperties = {
  width: '100%',
  borderCollapse: 'collapse',
  fontSize: 13,
  marginBottom: 12,
}

const thStyle: React.CSSProperties = {
  textAlign: 'left',
  padding: '8px 10px',
  borderBottom: '2px solid #30363d',
  color: '#8b949e',
  fontWeight: 600,
}

const tdStyle: React.CSSProperties = {
  padding: '8px 10px',
  borderBottom: '1px solid #21262d',
  color: '#c9d1d9',
  verticalAlign: 'top',
}

const tocStyle: React.CSSProperties = {
  ...cardStyle,
  position: 'sticky',
  top: 12,
}

function Code({ children }: { children: React.ReactNode }) {
  return <code style={inlineCodeStyle}>{children}</code>
}

function CodeBlock({ children }: { children: string }) {
  return <pre style={codeBlockStyle}>{children}</pre>
}

export default function Help() {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: '220px 1fr', gap: 24 }}>
      {/* Sidebar TOC */}
      <aside style={tocStyle}>
        <div
          style={{ fontSize: 12, color: '#8b949e', textTransform: 'uppercase', marginBottom: 8 }}
        >
          On this page
        </div>
        <ul style={{ listStyle: 'none', padding: 0, margin: 0, fontSize: 13 }}>
          {[
            ['#quickstart', 'Quick start'],
            ['#dashboard-tour', 'Dashboard tour'],
            ['#cli', 'CLI reference'],
            ['#python-api', 'Python API'],
            ['#rest-api', 'REST API'],
            ['#troubleshooting', 'Troubleshooting'],
            ['#links', 'Links'],
          ].map(([href, label]) => (
            <li key={href} style={{ marginBottom: 6 }}>
              <a href={href} style={{ ...linkStyle, color: '#8b949e' }}>
                {label}
              </a>
            </li>
          ))}
        </ul>
      </aside>

      <div>
        <h1 style={{ fontSize: 24, fontWeight: 600, marginBottom: 4 }}>Help & Docs</h1>
        <p style={{ color: '#8b949e', fontSize: 14, marginBottom: 20 }}>
          Everything you need to use RosBag Resurrector — from this dashboard, the CLI, the
          Python API, and the REST backend.
        </p>

        {/* Quick start */}
        <section id="quickstart" style={cardStyle}>
          <h2 style={h2Style}>Quick start</h2>
          <p style={pStyle}>
            Open{' '}
            <Link to="/" style={linkStyle}>
              Library
            </Link>
            , paste a folder path into the <b>Scan folder</b> input, and click{' '}
            <b>Scan folder</b>. Bags appear with health badges. Click into one to open the
            Explorer.
          </p>
          <p style={pStyle}>
            Don't have a bag yet?{' '}
            <Code>pip install rosbag-resurrector</Code> then{' '}
            <Code>resurrector demo --full</Code> generates a synthetic sample at{' '}
            <Code>~/.resurrector/demo_sample.mcap</Code> — paste{' '}
            <Code>~/.resurrector/</Code> into the scan input and you're set.
          </p>
        </section>

        {/* Dashboard tour */}
        <section id="dashboard-tour" style={cardStyle}>
          <h2 style={h2Style}>Dashboard tour</h2>
          <p style={pStyle}>
            What each top-nav page does. Most pages are read-only views over the same
            indexed-bag data; the Library page is the only place that triggers index changes.
          </p>

          <table style={tableStyle}>
            <thead>
              <tr>
                <th style={thStyle}>Page</th>
                <th style={thStyle}>What it does</th>
                <th style={thStyle}>When to use it</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td style={tdStyle}>
                  <Link to="/" style={linkStyle}>
                    Library
                  </Link>
                </td>
                <td style={tdStyle}>
                  Browse + scan bags. Filter by date, topic, health score, or tag. Click a
                  bag to open the Explorer.
                </td>
                <td style={tdStyle}>
                  Starting point for every session. Trigger <Code>scan</Code> here.
                </td>
              </tr>
              <tr>
                <td style={tdStyle}>Explorer (per-bag)</td>
                <td style={tdStyle}>
                  Plotly chart with brush-zoom + click-to-annotate, multi-stream Sync tab,
                  Images tab for video topics, math/transform editor, brush-to-trim export.
                </td>
                <td style={tdStyle}>
                  Inspect a single bag in detail. Most analysis happens here.
                </td>
              </tr>
              <tr>
                <td style={tdStyle}>
                  <Link to="/search" style={linkStyle}>
                    Search
                  </Link>
                </td>
                <td style={tdStyle}>
                  Natural-language search across video frames using CLIP embeddings. Type
                  what you're looking for; results return ranked thumbnails.
                </td>
                <td style={tdStyle}>
                  "Find that one moment in the recording I half-remember." Requires{' '}
                  <Code>[vision]</Code> extra and an <Code>index-frames</Code> run first.
                </td>
              </tr>
              <tr>
                <td style={tdStyle}>
                  <Link to="/datasets" style={linkStyle}>
                    Datasets
                  </Link>
                </td>
                <td style={tdStyle}>
                  Create and version reproducible bag-derived datasets. Each version pins
                  bags + sync/export config; export materializes data + manifest + auto-README.
                </td>
                <td style={tdStyle}>
                  Building ML training datasets you'll re-export later (LeRobot, RLDS, etc.).
                </td>
              </tr>
              <tr>
                <td style={tdStyle}>
                  <Link to="/compare" style={linkStyle}>
                    Compare
                  </Link>
                </td>
                <td style={tdStyle}>
                  Side-by-side diff of two bags' topic lists, message counts, durations.
                </td>
                <td style={tdStyle}>
                  "Did I record what I think I recorded?" sanity checks.
                </td>
              </tr>
              <tr>
                <td style={tdStyle}>
                  <Link to="/compare-runs" style={linkStyle}>
                    Compare runs
                  </Link>
                </td>
                <td style={tdStyle}>
                  Overlay the same topic across N bags on one Plotly chart, with per-bag
                  offset sliders for sub-second alignment fine-tuning.
                </td>
                <td style={tdStyle}>
                  Run-to-run comparison of the same experiment after a controller / policy
                  / hardware change.
                </td>
              </tr>
              <tr>
                <td style={tdStyle}>
                  <Link to="/bridge" style={linkStyle}>
                    Bridge
                  </Link>
                </td>
                <td style={tdStyle}>
                  Start a PlotJuggler-compatible WebSocket bridge from any bag (playback) or
                  live ROS 2 topics. Connect PlotJuggler → WebSocket Client →{' '}
                  <Code>ws://host:9090/ws</Code>.
                </td>
                <td style={tdStyle}>
                  Stream data into PlotJuggler for fast OpenGL plotting.
                </td>
              </tr>
            </tbody>
          </table>
        </section>

        {/* CLI reference */}
        <section id="cli" style={cardStyle}>
          <h2 style={h2Style}>CLI reference</h2>
          <p style={pStyle}>
            Every command has full <Code>--help</Code> output with per-flag examples. Run{' '}
            <Code>resurrector --help</Code> for the full list. Most-used commands:
          </p>

          <table style={tableStyle}>
            <thead>
              <tr>
                <th style={thStyle}>Command</th>
                <th style={thStyle}>What it does</th>
              </tr>
            </thead>
            <tbody>
              {[
                ['resurrector doctor', 'Verify install — pass/warn/fail grid for every dep'],
                ['resurrector demo --full', 'Generate a synthetic bag and walk the pipeline'],
                ['resurrector scan ~/recordings', 'Index a folder of bags'],
                ['resurrector list --has-topic /imu/data --min-health 80', 'Filter the index'],
                ['resurrector info bag.mcap', 'Detailed summary of one bag'],
                ['resurrector quicklook bag.mcap', 'Compact terminal overview with sparklines'],
                ['resurrector health bag.mcap', '0–100 quality score + per-topic breakdown'],
                ['resurrector export bag.mcap -t /imu/data -f parquet -o ./out', 'Export topics'],
                ['resurrector index-frames bag.mcap', 'Build CLIP embeddings (one-time)'],
                ['resurrector search-frames "robot arm" --save ./hits', 'Natural-language frame search'],
                ['resurrector dashboard', 'Launch this dashboard'],
                ['resurrector bridge playback bag.mcap', 'PlotJuggler-compatible WebSocket bridge'],
              ].map(([cmd, desc]) => (
                <tr key={cmd}>
                  <td style={{ ...tdStyle, fontFamily: 'ui-monospace, monospace', color: '#79c0ff' }}>
                    {cmd}
                  </td>
                  <td style={tdStyle}>{desc}</td>
                </tr>
              ))}
            </tbody>
          </table>

          <p style={mutedStyle}>
            Tip on macOS / zsh: square brackets in pip extras need quotes —{' '}
            <Code>pip install 'rosbag-resurrector[vision]'</Code>.
          </p>
        </section>

        {/* Python API */}
        <section id="python-api" style={cardStyle}>
          <h2 style={h2Style}>Python API</h2>
          <p style={pStyle}>
            The library is also a Python package — useful in Jupyter, scripts, and ML
            training pipelines. Public types: <Code>BagFrame</Code>, <Code>scan</Code>,{' '}
            <Code>search</Code>, <Code>DatasetManager</Code>, <Code>BagRef</Code>,{' '}
            <Code>SyncConfig</Code>, <Code>DatasetMetadata</Code>.
          </p>

          <h3 style={h3Style}>Open and inspect a bag</h3>
          <CodeBlock>{`from resurrector import BagFrame

bf = BagFrame("experiment.mcap")
bf.info()                                      # rich summary
df = bf["/imu/data"].to_polars()               # any topic → Polars
df_pd = bf["/imu/data"].to_pandas()            # or Pandas`}</CodeBlock>

          <h3 style={h3Style}>Stream a large topic without OOM</h3>
          <CodeBlock>{`for chunk in bf["/camera/rgb"].iter_chunks(chunk_size=10_000):
    process(chunk)

# Or with explicit-lifecycle Arrow IPC for filter/projection pushdown:
with bf["/imu/data"].materialize_ipc_cache() as cache:
    df = cache.scan().filter(pl.col("x") > 0).collect()`}</CodeBlock>

          <h3 style={h3Style}>Multi-stream sync</h3>
          <CodeBlock>{`synced = bf.sync(
    ["/imu/data", "/joint_states"],
    method="nearest",      # or "interpolate" / "sample_and_hold"
    tolerance_ms=50,
)`}</CodeBlock>

          <h3 style={h3Style}>Health, export, search</h3>
          <CodeBlock>{`# Health
report = bf.health_report()
print(f"{report.score}/100, {len(report.issues)} issues")

# Export
bf.export(topics=["/imu/data"], format="parquet", output="./out",
          sync=True, sync_method="nearest", downsample_hz=50)

# Search across the index
from resurrector import search
hits = search("topic:/imu/data health:>=80 after:2026-04-01")`}</CodeBlock>

          <p style={mutedStyle}>
            Every public method has full <Code>help(method)</Code> docs in a Python REPL —
            args, returns, raises, runnable example.
          </p>
        </section>

        {/* REST API */}
        <section id="rest-api" style={cardStyle}>
          <h2 style={h2Style}>REST API</h2>
          <p style={pStyle}>
            Everything this dashboard does goes through a FastAPI backend. The full
            interactive OpenAPI reference (try-it-out, schemas, response examples) is
            auto-generated by FastAPI:
          </p>
          <p style={pStyle}>
            <a href="/docs" target="_blank" rel="noreferrer" style={linkStyle}>
              → /docs (Swagger UI)
            </a>
            &nbsp;&nbsp;
            <a href="/redoc" target="_blank" rel="noreferrer" style={linkStyle}>
              → /redoc (ReDoc)
            </a>
            &nbsp;&nbsp;
            <a href="/openapi.json" target="_blank" rel="noreferrer" style={linkStyle}>
              → /openapi.json (raw spec)
            </a>
          </p>
          <p style={mutedStyle}>
            All endpoints live under <Code>/api/...</Code>. Path validation via{' '}
            <Code>RESURRECTOR_ALLOWED_ROOTS</Code> applies to every route that touches the
            filesystem.
          </p>
        </section>

        {/* Troubleshooting */}
        <section id="troubleshooting" style={cardStyle}>
          <h2 style={h2Style}>Troubleshooting</h2>

          <h3 style={h3Style}>"resurrector: command not found"</h3>
          <p style={pStyle}>
            Your virtualenv isn't active. Run <Code>source .venv/bin/activate</Code> (or the
            equivalent for your env manager) and try again.
          </p>

          <h3 style={h3Style}>"zsh: no matches found: rosbag-resurrector[vision]"</h3>
          <p style={pStyle}>
            zsh treats <Code>[vision]</Code> as a glob. Quote the package spec:{' '}
            <Code>pip install 'rosbag-resurrector[vision]'</Code>.
          </p>

          <h3 style={h3Style}>"Search returns blank frames"</h3>
          <p style={pStyle}>
            CLIP works on real images, not the synthetic demo bag's noise frames. Index a
            real bag with actual camera footage — see the launch docs for a public dataset
            recipe — and re-run the search.
          </p>

          <h3 style={h3Style}>"403 from the dashboard scan input"</h3>
          <p style={pStyle}>
            By default the dashboard only scans paths under{' '}
            <Code>RESURRECTOR_ALLOWED_ROOTS</Code> (defaults to your home directory). Set
            the env var to broaden:
          </p>
          <CodeBlock>{`export RESURRECTOR_ALLOWED_ROOTS=/data/bags:/mnt/recordings
resurrector dashboard`}</CodeBlock>

          <h3 style={h3Style}>"resurrector doctor" fails on a check</h3>
          <p style={pStyle}>
            Each row prints the exact <Code>pip install</Code> command needed. Re-run{' '}
            <Code>resurrector doctor</Code> after installing.
          </p>
        </section>

        {/* Links */}
        <section id="links" style={cardStyle}>
          <h2 style={h2Style}>Links</h2>
          <ul style={{ ...pStyle, paddingLeft: 20 }}>
            <li>
              <a
                href="https://github.com/vikramnagashoka/rosbag-resurrector"
                target="_blank"
                rel="noreferrer"
                style={linkStyle}
              >
                GitHub repo
              </a>{' '}
              — source, issues, releases
            </li>
            <li>
              <a
                href="https://github.com/vikramnagashoka/rosbag-resurrector#readme"
                target="_blank"
                rel="noreferrer"
                style={linkStyle}
              >
                README
              </a>{' '}
              — full installation, performance contract, format support details
            </li>
            <li>
              <a
                href="https://github.com/vikramnagashoka/rosbag-resurrector/blob/main/CHANGELOG.md"
                target="_blank"
                rel="noreferrer"
                style={linkStyle}
              >
                CHANGELOG
              </a>{' '}
              — what changed in each release
            </li>
            <li>
              <a
                href="https://pypi.org/project/rosbag-resurrector/"
                target="_blank"
                rel="noreferrer"
                style={linkStyle}
              >
                PyPI
              </a>{' '}
              — <Code>pip install rosbag-resurrector</Code>
            </li>
            <li>
              <a
                href="https://github.com/vikramnagashoka/rosbag-resurrector/issues/new"
                target="_blank"
                rel="noreferrer"
                style={linkStyle}
              >
                Report an issue / request a feature
              </a>
            </li>
          </ul>
        </section>
      </div>
    </div>
  )
}
