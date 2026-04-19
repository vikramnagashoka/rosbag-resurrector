import React, { useEffect, useMemo, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import TopicPlot, { PlotSeries } from '../components/TopicPlot'
import Timeline from '../components/Timeline'
import HealthBadge from '../components/HealthBadge'
import ExportDialog from '../components/ExportDialog'
import SyncView from '../components/SyncView'
import ImageViewer from '../components/ImageViewer'
import { api, Bag, TopicDataResponse } from '../api'
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
} {
  if (!topicData.data.length) return { ts_ns: [], series: [] }
  const ts_ns: number[] = topicData.data.map(r => Number(r.timestamp_ns))
  const series: PlotSeries[] = []
  for (const col of topicData.columns) {
    if (col === 'timestamp_ns') continue
    const sampleValue = topicData.data[0][col]
    if (typeof sampleValue !== 'number') continue
    const values = topicData.data.map(r => {
      const v = r[col]
      return typeof v === 'number' ? v : NaN
    })
    series.push({ label: col, timestamps_ns: ts_ns, values })
  }
  return { ts_ns, series }
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

  // Derived plot state.
  const selectedTopicInfo = useMemo(
    () => bag?.topics.find(t => t.name === selectedTopic) || null,
    [bag, selectedTopic],
  )
  const isImageTopic =
    selectedTopicInfo && IMAGE_TYPES.has(selectedTopicInfo.message_type)

  const { ts_ns, series } = useMemo(
    () => (topicData ? timestampsAndSeries(topicData) : { ts_ns: [], series: [] }),
    [topicData],
  )
  const firstTs = ts_ns[0] ?? 0

  // When the user picks an image topic, switch to the images tab.
  useEffect(() => {
    if (isImageTopic) setActiveTab('images')
    else if (activeTab === 'images') setActiveTab('plot')
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isImageTopic])

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
          gridTemplateColumns: '300px 1fr',
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

        {/* Tabbed data view */}
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
              Select a topic to view its data
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

              {activeTab === 'plot' && topicData && (
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
                  </div>
                  <TopicPlot
                    bagId={bagId}
                    topicName={selectedTopic}
                    series={series}
                    firstTimestampNs={firstTs}
                    onZoom={(s, e) => {
                      if (s === null || e === null) setXRangeSec(null)
                      else setXRangeSec({ start: s, end: e })
                    }}
                  />
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
      </div>

      {showExport && (
        <ExportDialog
          bagId={bagId}
          availableTopics={bag.topics.map(t => t.name)}
          onClose={() => setShowExport(false)}
        />
      )}
    </div>
  )
}
