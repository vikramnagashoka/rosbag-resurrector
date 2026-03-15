/**
 * Resurrector Bridge Viewer — Plotly.js live plotter with WebSocket transport.
 */

class ResurrectorViewer {
    constructor() {
        this.ws = null;
        this.plots = {};           // topic -> {div, traces, fieldNames}
        this.maxPoints = 2000;     // Rolling window per trace
        this.topics = [];
        this.subscribedTopics = new Set();
        this.metadata = null;
        this.msgCount = 0;
        this.lastCountTime = Date.now();
        this.lastMsgRate = 0;

        this.plotLayout = {
            paper_bgcolor: '#16213e',
            plot_bgcolor: '#1a1a2e',
            font: { color: '#e0e0e0', size: 11 },
            margin: { l: 50, r: 20, t: 30, b: 40 },
            xaxis: { title: 'Time (s)', gridcolor: '#0f3460', zerolinecolor: '#0f3460' },
            yaxis: { gridcolor: '#0f3460', zerolinecolor: '#0f3460' },
            legend: { orientation: 'h', y: -0.2 },
            showlegend: true,
        };

        this.init();
    }

    async init() {
        // Fetch metadata
        try {
            const resp = await fetch('/api/metadata');
            this.metadata = await resp.json();
        } catch (e) {
            console.warn('Could not fetch metadata:', e);
        }

        // Fetch topics
        try {
            const resp = await fetch('/api/topics');
            const data = await resp.json();
            this.topics = data.available || [];
            this.renderTopicList();
        } catch (e) {
            console.warn('Could not fetch topics:', e);
        }

        // Update time display
        if (this.metadata && this.metadata.duration_sec) {
            document.getElementById('time-display').textContent =
                `0.0s / ${this.metadata.duration_sec.toFixed(1)}s`;
        }

        this.connect();
        this.startStatusPoller();
        this.startRateCounter();
    }

    connect() {
        const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${location.host}/ws`;
        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
            document.getElementById('status-dot').classList.add('connected');
            document.getElementById('status-text').textContent = 'Connected';
            // Re-subscribe to previously selected topics
            if (this.subscribedTopics.size > 0) {
                this.ws.send(JSON.stringify({
                    type: 'subscribe',
                    topics: Array.from(this.subscribedTopics),
                }));
            }
        };

        this.ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            if (data.type === 'topics') {
                this.topics = data.available || [];
                this.renderTopicList();
                return;
            }
            if (data.type === 'status') {
                this.updateStatus(data);
                return;
            }
            // Data message — route to plots
            this.onDataMessage(data);
        };

        this.ws.onclose = () => {
            document.getElementById('status-dot').classList.remove('connected');
            document.getElementById('status-text').textContent = 'Disconnected';
            // Reconnect after 2 seconds
            setTimeout(() => this.connect(), 2000);
        };

        this.ws.onerror = (e) => {
            console.error('WebSocket error:', e);
        };
    }

    renderTopicList() {
        const container = document.getElementById('topic-list');
        container.innerHTML = '';
        for (const topic of this.topics) {
            const div = document.createElement('div');
            div.className = 'topic-item';
            const checked = this.subscribedTopics.has(topic.name) ? 'checked' : '';
            div.innerHTML = `
                <input type="checkbox" ${checked} data-topic="${topic.name}">
                <div>
                    <div class="name">${topic.name}</div>
                    <div class="type">${topic.type || ''} ${topic.hz ? `@ ${topic.hz}Hz` : ''}</div>
                </div>
            `;
            div.querySelector('input').addEventListener('change', (e) => {
                if (e.target.checked) {
                    this.subscribe(topic.name);
                } else {
                    this.unsubscribe(topic.name);
                }
            });
            container.appendChild(div);
        }
    }

    subscribe(topicName) {
        this.subscribedTopics.add(topicName);
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                type: 'subscribe',
                topics: Array.from(this.subscribedTopics),
            }));
        }
        document.getElementById('empty-state')?.remove();
    }

    unsubscribe(topicName) {
        this.subscribedTopics.delete(topicName);
        // Remove plot
        if (this.plots[topicName]) {
            this.plots[topicName].div.remove();
            delete this.plots[topicName];
        }
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                type: 'unsubscribe',
                topics: [topicName],
            }));
        }
    }

    onDataMessage(data) {
        this.msgCount++;
        const timestamp = data.timestamp;
        if (!timestamp) return;

        // Group fields by topic prefix
        const topicFields = {};
        for (const [key, value] of Object.entries(data)) {
            if (key === 'timestamp') continue;
            if (typeof value !== 'number') continue;

            // Find which subscribed topic this field belongs to
            let matchedTopic = null;
            for (const t of this.subscribedTopics) {
                if (key.startsWith(t + '/')) {
                    matchedTopic = t;
                    break;
                }
            }
            if (!matchedTopic) continue;

            if (!topicFields[matchedTopic]) topicFields[matchedTopic] = {};
            const fieldName = key.substring(matchedTopic.length + 1);
            topicFields[matchedTopic][fieldName] = value;
        }

        // Update plots
        for (const [topic, fields] of Object.entries(topicFields)) {
            this.updatePlot(topic, timestamp, fields);
        }
    }

    updatePlot(topic, timestamp, fields) {
        const relativeTime = this.metadata
            ? timestamp - this.metadata.start_time_sec
            : timestamp;

        if (!this.plots[topic]) {
            this.createPlot(topic, Object.keys(fields));
        }

        const plot = this.plots[topic];
        const fieldNames = Object.keys(fields);

        // Add new traces for newly discovered fields
        for (const field of fieldNames) {
            if (!plot.fieldNames.includes(field)) {
                plot.fieldNames.push(field);
                const traceIdx = plot.fieldNames.length - 1;
                Plotly.addTraces(plot.div, {
                    x: [],
                    y: [],
                    name: field,
                    mode: 'lines',
                    line: { width: 1.5 },
                });
            }
        }

        // Extend traces
        const indices = [];
        const xData = [];
        const yData = [];

        for (let i = 0; i < plot.fieldNames.length; i++) {
            const field = plot.fieldNames[i];
            if (field in fields) {
                indices.push(i);
                xData.push([relativeTime]);
                yData.push([fields[field]]);
            }
        }

        if (indices.length > 0) {
            const maxLen = this.maxPoints;
            Plotly.extendTraces(plot.div, { x: xData, y: yData }, indices, maxLen);
        }
    }

    createPlot(topic, fieldNames) {
        const container = document.getElementById('plots-container');
        const div = document.createElement('div');
        div.className = 'plot-container';
        div.id = `plot-${topic.replace(/\//g, '-')}`;
        container.appendChild(div);

        const traces = fieldNames.map((name) => ({
            x: [],
            y: [],
            name: name,
            mode: 'lines',
            line: { width: 1.5 },
        }));

        const layout = {
            ...this.plotLayout,
            title: { text: topic, font: { size: 13, color: '#e94560' } },
        };

        Plotly.newPlot(div, traces, layout, { responsive: true, displayModeBar: false });

        this.plots[topic] = { div, fieldNames: [...fieldNames] };
    }

    updateStatus(status) {
        if (status.progress !== undefined && this.metadata) {
            const slider = document.getElementById('seek-slider');
            slider.value = status.progress * 100;
            const current = status.timestamp - (this.metadata.start_time_sec || 0);
            document.getElementById('time-display').textContent =
                `${current.toFixed(1)}s / ${this.metadata.duration_sec.toFixed(1)}s`;
        }
    }

    // --- Transport controls ---

    play() { fetch('/api/playback/play', { method: 'POST' }); }
    pause() { fetch('/api/playback/pause', { method: 'POST' }); }

    onSeekInput(value) {
        if (!this.metadata) return;
        const fraction = value / 100;
        const t = this.metadata.start_time_sec + fraction * this.metadata.duration_sec;
        fetch(`/api/playback/seek?t=${t}`, { method: 'POST' });
    }

    setSpeed(value) {
        fetch(`/api/playback/speed?v=${value}`, { method: 'POST' });
    }

    startStatusPoller() {
        setInterval(async () => {
            try {
                const resp = await fetch('/api/status');
                const status = await resp.json();
                this.updateStatus(status);
            } catch (e) { /* ignore */ }
        }, 500);
    }

    startRateCounter() {
        setInterval(() => {
            const now = Date.now();
            const elapsed = (now - this.lastCountTime) / 1000;
            this.lastMsgRate = Math.round(this.msgCount / elapsed);
            this.msgCount = 0;
            this.lastCountTime = now;
            document.getElementById('msg-rate').textContent = `${this.lastMsgRate} msg/s`;
        }, 1000);
    }
}

// Initialize
const viewer = new ResurrectorViewer();
