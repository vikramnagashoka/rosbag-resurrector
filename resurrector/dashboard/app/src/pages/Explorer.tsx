import React, { useEffect, useMemo, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import TopicPlot, { PlotSeries } from '../components/TopicPlot'
import Timeline from '../components/Timeline'
import HealthBadge from '../components/HealthBadge'
import ExportDialog from '../components/ExportDialog'
import SyncView from '../components/SyncView'
import ImageViewer from '../components/ImageViewer'
import BookmarksPanel from '../components/BookmarksPanel'
import DensityRibbon from '../components/DensityRibbon'
import TransformEditor from '../components/TransformEditor'
import TrimExportPopover from '../components/TrimExportPopover'
import JupyterButton from '../components/JupyterButton'
import { api, Annotation, Bag, TopicDataResponse } from '../api'
import { runWithToast, useErrorToast } from '../ErrorToast'

const IMAGE_TYPES = new Set([
  'sensor_msgs/msg/Image',
  'sensor_msgs/msg/CompressedImage',
])

function formatSize(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let size = bytes
  for (const unit of units) {
    if (size < 1024) return `${size.toFixed(1)} ${unit}`
    size /= 1024
  }
  return `${size.toFixed(1)} PB`
}

function basename(path: string): string {
  return path.split(/[/\\]/).pop() || path
}

function timestampsAndSeries(topicData: TopicDataResponse): {
  ts_ns: number[]
  series: PlotSeries[]
  numericColumns: string[]
} {
  if (!topicData.data.length)
    return { ts_ns: [], series: [], numericColumns: [] }
  const ts_ns: number[] = topicData.data.map(r => Number(r.timestamp_ns))
  const series: PlotSeries[] = []
  const numericColumns: string[] = []
  for (const col of topicData.columns) {
    if (col === 'timestamp_ns') continue
    const sampleValue = topicData.data[0][col]
    if (typeof sampleValue !== 'number') continue
    numericColumns.push(col)
    const values = topicData.data.map(r => {
      const v = r[col]
      return typeof v === 'number' ? v : NaN
    })
    series.push({ label: col, timestamps_ns: ts_ns, values })
  }
  return { ts_ns, series, numericColumns }
}

type Tab = 'plot' | 'sync' | 'images'

export default function Explorer() {
  const { id } = useParams<{ id: string }>()
  const bagId = Number(id)
  const toast = useErrorToast()

  const [bag, setBag] = useState<Bag | null>(null)
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null)
  const [topicData, setTopicData] = useState<TopicDataResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [showExport, setShowExport] = useState(false)
  const [activeTab, setActiveTab] = useState<Tab>('plot')
  const [xRangeSec, setXRangeSec] = useState<{ start: number; end: number } | null>(null)

  // v0.3.1 state
  const [showTransformEditor, setShowTransformEditor] = useState(false)
  const [derivedSeries, setDerivedSeries] = useState<PlotSeries[]>([])
  const [trimRange, setTrimRange] = useState<{ start: number; end: number } | null>(null)

  // Lifted annotations state — owned here so TopicPlot and BookmarksPanel
  // share one source of truth. Without this, the panel's internal cache
  // diverged from TopicPlot's after a click-to-add and the panel kept
  // showing "0 bookmarks" while the chart drew them just fine.
  const [annotations, setAnnotations] = useState<Annotation[]>([])
  const [annotationsLoading, setAnnotationsLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    runWithToast(toast, () => api.getBag(bagId)).then(b => {
      if (b) setBag(b)
      setLoading(false)
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bagId])

  // Fetch topic data on topic change OR zoom.
  useEffect(() => {
    if (!selectedTopic) {
      setTopicData(null)
      return
    }
    runWithToast(
      toast,
      () =>
        api.getTopicData(bagId, selectedTopic, {
          maxPoints: 2000,
          startSec: xRangeSec?.start,
          endSec: xRangeSec?.end,
        }),
      { errorPrefix: 'Load topic' },
    ).then(d => {
      if (d) setTopicData(d)
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bagId, selectedTopic, xRangeSec])

  // When the user picks a different topic, drop derived series — they're
  // computed against the previous topic's columns and would be confusing
  // to keep.
  useEffect(() => {
    setDerivedSeries([])
  }, [selectedTopic])

  // Load annotations once per bag. Both TopicPlot and BookmarksPanel
  // consume this state via props.
  useEffect(() => {
    setAnnotationsLoading(true)
    runWithToast(toast, () => api.listAnnotations(bagId)).then(r => {
      if (r) setAnnotations(r.annotations)
      setAnnotationsLoading(false)
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bagId])

  const selectedTopicInfo = useMemo(
    () => bag?.topics.find(t => t.name === selectedTopic) || null,
    [bag, selectedTopic],
  )
  const isImageTopic =
    selectedTopicInfo && IMAGE_TYPES.has(selectedTopicInfo.message_type)

  const { ts_ns, series, numericColumns } = useMemo(
    () =>
      topicData
        ? timestampsAndSeries(topicData)
        : { ts_ns: [], series: [], numericColumns: [] },
    [topicData],
  )
  const firstTs = ts_ns[0] ?? 0

  // First-message timestamp from the bag (used by BookmarksPanel +
  // DensityRibbon time alignment). Falls back to firstTs from current
  // topic data when bag-wide info isn't available.
  const bagFirstTs = useMemo(() => {
    // The /api/bags response doesn't currently include start_time_ns;
    // density and bookmark APIs return absolute timestamps that we
    // align against firstTs from the current topic. Good enough for
    // ribbon shading; bookmarks display relative t which is what users
    // care about visually.
    return firstTs
  }, [firstTs])

  // When the user picks an image topic, switch to the images tab.
  useEffect(() => {
    if (isImageTopic) setActiveTab('images')
    else if (activeTab === 'images') setActiveTab('plot')
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isImageTopic])

  function handleJumpToTimestamp(relativeSec: number) {
    // Set a small window centered on the bookmark / ribbon click so the
    // user sees what's around the point. ~1 second window by default.
    const half = 0.5
    setXRangeSec({
      start: Math.max(0, relativeSec - half),
      end: relativeSec + half,
    })
    if (!selectedTopic && bag?.topics.length) {
      // Need a topic selected to render anything; pick the first
      // numeric-looking topic.
      const first = bag.topics.find(t => !IMAGE_TYPES.has(t.message_type))
      if (first) setSelectedTopic(first.name)
    }
  }

  function handleAddDerivedSeries(label: string, points: Array<{ t_ns: number; v: number }>) {
    const ts = points.map(p => p.t_ns)
    const vs = points.map(p => p.v)
    setDerivedSeries(prev => [...prev, { label, timestamps_ns: ts, values: vs }])
  }

  if (loading || !bag) {
    return <p style={{ color: '#8b949e' }}>Loading...</p>
  }

  return (
    <div>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: '24px',
        }}
      >
        <div>
          <h1 style={{ fontSize: '24px', fontWeight: 600 }}>{basename(bag.path)}</h1>
          <div style={{ color: '#8b949e', fontSize: '14px', marginTop: '4px' }}>
            {bag.duration_sec?.toFixed(1)}s | {formatSize(bag.size_bytes)} |{' '}
            {bag.message_count?.toLocaleString()} messages
          </div>
        </div>
        <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
          <button
            onClick={() => setShowTransformEditor(true)}
            disabled={!selectedTopic || numericColumns.length === 0}
            style={{
              background: '#21262d',
              border: '1px solid #30363d',
              borderRadius: 6,
              padding: '6px 12px',
              color: !selectedTopic || numericColumns.length === 0 ? '#484f58' : '#e1e4e8',
              cursor:
                !selectedTopic || numericColumns.length === 0
                  ? 'not-allowed'
                  : 'pointer',
              fontSize: 13,
            }}
          >
            Transform…
          </button>
          {selectedTopic && (
            <JupyterButton
              bagId={bagId}
              startSec={xRangeSec?.start}
              endSec={xRangeSec?.end}
              topics={[selectedTopic]}
            />
          )}
          <button
            onClick={() => setShowExport(true)}
            style={{
              background: '#21262d',
              border: '1px solid #30363d',
              borderRadius: '6px',
              padding: '6px 12px',
              color: '#e1e4e8',
              cursor: 'pointer',
              fontSize: '13px',
            }}
          >
            Export
          </button>
          <Link
            to={`/bag/${bagId}/health`}
            style={{
              background: '#21262d',
              border: '1px solid #30363d',
              borderRadius: '6px',
              padding: '6px 12px',
              color: '#e1e4e8',
              fontSize: '13px',
              textDecoration: 'none',
            }}
          >
            View Health Report
          </Link>
          <HealthBadge score={bag.health_score} />
        </div>
      </div>

      <Timeline
        topics={bag.topics as any}
        onSelectTopic={setSelectedTopic}
        selectedTopic={selectedTopic}
      />

      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '300px 1fr 260px',
          gap: '24px',
          marginTop: '24px',
        }}
      >
        {/* Topic list */}
        <div
          style={{
            background: '#161b22',
            border: '1px solid #30363d',
            borderRadius: '8px',
            padding: '12px',
            height: 'fit-content',
            position: 'sticky',
            top: 12,
          }}
        >
          <h3
            style={{
              fontSize: '14px',
              fontWeight: 600,
              marginBottom: '12px',
              color: '#8b949e',
            }}
          >
            Topics
          </h3>
          {bag.topics.map(topic => (
            <div
              key={topic.name}
              onClick={() => {
                setSelectedTopic(topic.name)
                setXRangeSec(null)
              }}
              style={{
                padding: '8px 12px',
                borderRadius: '6px',
                cursor: 'pointer',
                marginBottom: '4px',
                background: selectedTopic === topic.name ? '#1f6feb22' : 'transparent',
                border:
                  selectedTopic === topic.name
                    ? '1px solid #1f6feb'
                    : '1px solid transparent',
              }}
            >
              <div
                style={{
                  fontSize: '13px',
                  fontWeight: 500,
                  color: '#58a6ff',
                }}
              >
                {topic.name}
              </div>
              <div style={{ fontSize: '12px', color: '#8b949e' }}>
                {topic.message_type} | {topic.message_count.toLocaleString()} msgs
              </div>
            </div>
          ))}
        </div>

        {/* Center: tabbed data view */}
        <div>
          {!selectedTopic ? (
            <div
              style={{
                background: '#161b22',
                border: '1px solid #30363d',
                borderRadius: '8px',
                padding: '48px',
                textAlign: 'center',
                color: '#8b949e',
              }}
            >
              Select a topic to view its data. The density ribbon and bookmarks
              panel work without a topic selected.
            </div>
          ) : (
            <>
              <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
                {(['plot', 'sync', 'images'] as Tab[]).map(t => {
                  const enabled =
                    t === 'plot' || (t === 'sync' && true) || (t === 'images' && isImageTopic)
                  return (
                    <button
                      key={t}
                      disabled={!enabled}
                      onClick={() => setActiveTab(t)}
                      style={{
                        background: activeTab === t ? '#1f6feb22' : '#21262d',
                        border:
                          activeTab === t ? '1px solid #1f6feb' : '1px solid #30363d',
                        borderRadius: 6,
                        padding: '6px 14px',
                        color: enabled ? '#e1e4e8' : '#484f58',
                        cursor: enabled ? 'pointer' : 'not-allowed',
                        fontSize: 13,
                      }}
                    >
                      {t.charAt(0).toUpperCase() + t.slice(1)}
                    </button>
                  )
                })}
              </div>

              {activeTab === 'plot' && (
                <>
                  {/* Density ribbon spans the full bag duration so the
                      user sees the global picture even when zoomed in. */}
                  <DensityRibbon
                    bagId={bagId}
                    highlightTopic={selectedTopic}
                    zoomRangeSec={xRangeSec}
                    onJumpToTimestampSec={handleJumpToTimestamp}
                  />

                  {topicData && (
                    <>
                      <div
                        style={{
                          fontSize: 12,
                          color: '#8b949e',
                          marginBottom: 8,
                        }}
                      >
                        {topicData.total.toLocaleString()} messages
                        {topicData.downsampled &&
                          ` · downsampled to ${topicData.data.length}`}
                        {xRangeSec &&
                          ` · zoomed to ${xRangeSec.start.toFixed(2)}-${xRangeSec.end.toFixed(2)}s`}
                        {derivedSeries.length > 0 &&
                          ` · ${derivedSeries.length} derived series`}
                        <span style={{ float: 'right', color: '#484f58' }}>
                          tip: shift+drag to select a range for trim/export
                        </span>
                      </div>
                      <TopicPlot
                        bagId={bagId}
                        topicName={selectedTopic}
                        series={series}
                        derivedSeries={derivedSeries}
                        firstTimestampNs={firstTs}
                        onZoom={(s, e) => {
                          if (s === null || e === null) setXRangeSec(null)
                          else setXRangeSec({ start: s, end: e })
                        }}
                        onRangeSelected={(s, e) => setTrimRange({ start: s, end: e })}
                        annotations={annotations}
                        onAnnotationsChanged={setAnnotations}
                      />
                    </>
                  )}
                </>
              )}

              {activeTab === 'sync' && (
                <SyncView
                  bagId={bagId}
                  availableTopics={bag.topics.map(t => t.name)}
                />
              )}

              {activeTab === 'images' && isImageTopic && (
                <ImageViewer
                  bagId={bagId}
                  topic={selectedTopic}
                  totalFrames={selectedTopicInfo!.message_count}
                />
              )}
            </>
          )}
        </div>

        {/* Right rail: bookmarks */}
        <BookmarksPanel
          bagId={bagId}
          firstTimestampNs={bagFirstTs}
          onJumpToTimestampSec={handleJumpToTimestamp}
          annotations={annotations}
          loading={annotationsLoading}
          onAnnotationsChanged={setAnnotations}
        />
      </div>

      {showExport && (
        <ExportDialog
          bagId={bagId}
          availableTopics={bag.topics.map(t => t.name)}
          onClose={() => setShowExport(false)}
        />
      )}

      {showTransformEditor && selectedTopic && (
        <TransformEditor
          bagId={bagId}
          topic={selectedTopic}
          numericColumns={numericColumns}
          onSave={handleAddDerivedSeries}
          onClose={() => setShowTransformEditor(false)}
        />
      )}

      {trimRange && (
        <TrimExportPopover
          bagId={bagId}
          startSec={trimRange.start}
          endSec={trimRange.end}
          availableTopics={bag.topics.map(t => t.name)}
          defaultTopics={selectedTopic ? [selectedTopic] : []}
          onClose={() => setTrimRange(null)}
        />
      )}
    </div>
  )
}
