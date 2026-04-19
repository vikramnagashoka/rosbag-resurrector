# RosBag Resurrector — Launch Plan

## Posting Order & Timing

**Best launch day:** Tuesday, Wednesday, or Thursday (avoid Mon/Fri — lower engagement)

| Order | Platform | Time (ET) | Time (PT) | Notes |
|-------|----------|-----------|-----------|-------|
| 1 | HackerNews (Show HN) | 8:00–9:00 AM | 5:00–6:00 AM | Morning = best chance at front page. Earlier submissions have longer to accumulate upvotes. |
| 2 | Reddit r/ROS | 11:00 AM | 8:00 AM | ~2–3 hrs after HN. US + EU robotics engineers active. |
| 3 | Reddit r/robotics | 11:15 AM | 8:15 AM | Space slightly apart so both posts get visibility. |
| 4 | ROS Discourse | 12:00 PM | 9:00 AM | discourse.ros.org — General category. |
| 5 | Twitter/X main tweet | 6:00–7:00 PM | 3:00–4:00 PM | Evening US = peak robotics Twitter. |
| 6 | Twitter reply with link | Immediately after | Immediately after | Algorithm penalizes link-in-main-tweet. |

---

## Usernames

- **HackerNews:** `bag_of_holding`
- **Reddit:** `rosgraveyard`

---

## 1. HackerNews — Show HN

**Title:**
```
Show HN: Pandas-like API for rosbag files with CLIP semantic search
```

**URL:**
```
https://github.com/vikramnagashoka/rosbag-resurrector
```

**First comment (post immediately after submission):**

> Every robotics team collects rosbags during experiments, then dumps them on a NAS and never touches them again. The tooling to actually work with bag files hasn't changed much since 2015.
>
> We built RosBag Resurrector — a Python CLI + web dashboard that treats bag files like dataframes. Key features:
>
> - `BagFrame` API — pandas-like interface, lazy loading, exports to Polars/Pandas
> - Health scoring (0-100) — detects dropped messages, time gaps, out-of-order timestamps
> - CLIP semantic search — embed video frames, then search with natural language ("robot misses grasp but ball touches hand")
> - ML export — one-line conversion to RLDS, LeRobot, HDF5, Parquet, TFRecord
> - PlotJuggler bridge — WebSocket streaming, no plugin needed
>
> No ROS installation required. Works with MCAP and ROS1 bags. Pure Python, pip install.
>
> The CLIP search is the feature I'm most excited about — it indexes frames at 5Hz into DuckDB and does cosine similarity search. For teams with thousands of bags, it turns hours of manual scrubbing into seconds.
>
> Happy to answer questions about the architecture or robotics data workflows.

---

## 2. Reddit — r/ROS

**Title:**
```
We built an open-source tool to actually analyze your rosbag graveyard — semantic search, health checks, ML export, PlotJuggler bridge
```

**Body:**

> Hey r/ROS,
>
> Like most of you, we had terabytes of bags sitting on a NAS that nobody ever looked at again. The existing workflow of `rosbag play` + squinting at RViz + writing throwaway Python scripts wasn't cutting it.
>
> So we built **RosBag Resurrector** — a pandas-like CLI + web UI for MCAP and ROS1 bags. No ROS install required.
>
> **What it does:**
>
> - **BagFrame API** — load any bag, get a Polars/Pandas DataFrame in one line
> - **Health scoring** — automatic quality checks (dropped messages, time gaps, out-of-order timestamps) with a 0-100 score
> - **CLIP semantic search** — search video frames with natural language. Type "robot reaches for cup but misses" and it finds the clips.
> - **ML-ready export** — RLDS, LeRobot, HDF5, Parquet, TFRecord, Zarr, NumPy
> - **PlotJuggler bridge** — WebSocket streaming, just connect to `ws://localhost:9090/ws`
> - **Web dashboard** — browse, compare, and search all your indexed bags
>
> ```bash
> pip install rosbag-resurrector
> resurrector scan ~/bags/
> resurrector search "robot arm collision with table"
> ```
>
> GitHub: github.com/vikramnagashoka/rosbag-resurrector
>
> Would love feedback from anyone dealing with bag file hell. What features would make this actually useful for your workflow?

**Attach:** `assets/demo.gif`

---

## 3. Reddit — r/robotics

**Title:**
```
Built an open-source tool to search rosbag video with natural language using CLIP
```

**Body:**

> If you work with ROS, you probably have hundreds of bag files you've never analyzed.
>
> We built a tool that indexes video frames from your bags using CLIP embeddings, so you can search them with plain English — "robot drops object during handover", "arm collision with obstacle", etc.
>
> Also does health checks, pandas-like data access, and exports to ML training formats (RLDS, LeRobot, HDF5).
>
> No ROS install needed. Just `pip install rosbag-resurrector`.
>
> GitHub: github.com/vikramnagashoka/rosbag-resurrector

**Attach:** `assets/demo.gif`

---

## 4. ROS Discourse

**Category:** General
**URL:** https://discourse.ros.org

**Title:**
```
[Tool] RosBag Resurrector — pandas-like API, health checks, semantic search, PlotJuggler bridge
```

**Body:** (use same content as r/ROS post, slightly more formal)

---

## 5. Twitter — Main Tweet (NO LINK)

> Every robotics lab has a folder called `bags_final_v3_REAL`.
>
> Terabytes of robot experience that nobody will ever look at again.
>
> We built a tool to fix that. Search all your rosbags with plain English. No ROS install needed.

**Attach:** `assets/demo.gif`

---

## 6. Twitter — Reply to your own tweet (immediately after)

> Link: github.com/vikramnagashoka/rosbag-resurrector
>
> pip install rosbag-resurrector

---

## Follow-up Twitter Thread (Day 2 — only if Day 1 gets traction)

Post this the NEXT day if the first tweet gets 5+ retweets or 20+ likes. Lead with:

> People asked how the rosbag search actually works under the hood. Here's the breakdown 🧵

**Tweet 1 — Hook**
> I asked 12 robotics teams what happens to their rosbags after a project ends.
>
> Every single one said the same thing:
>
> "They sit on a NAS somewhere. Nobody touches them again."
>
> That's terabytes of robot experience. Wasted. 🧵

**Tweet 2 — Problem**
> The dirty secret of robotics:
>
> We obsess over collecting data during experiments.
>
> Then we dump bags into `/data/bags_final_v3_REAL/` and never look at them again.
>
> Because the tooling to actually *work* with bag files is stuck in 2015.

**Tweet 3 — Solution**
> What if you could do this:
>
> ```python
> from resurrector import BagFrame
> bf = BagFrame("manipulation_trial.mcap")
> bf["/joint_states"].to_polars()
> ```
>
> pandas for rosbags. No ROS install. Just `pip install` and go.

**Tweet 4 — Wow Moment**
> But the feature that made my jaw drop during development:
>
> **Natural language search across all your bags.**
>
> ```bash
> resurrector search "robot reaches for cup but fingers close too early"
> ```
>
> It finds the exact clips. From thousands of bags. In seconds.

**Tweet 5 — How**
> How?
>
> Every video frame gets CLIP-embedded at 5Hz. Stored in DuckDB.
>
> When you search, it's cosine similarity across every frame you've ever recorded.
>
> Think Google Photos search — but for your robot's entire life.

**Tweet 6 — Health**
> "But my bags are probably garbage anyway"
>
> That's why every bag gets a health score:
>
> ```
> Score: 41/100  ← this bag is cooked
> ⚠ 2,847 dropped messages on /camera/depth
> ⚠ 14 time-travel events on /imu
> ```
>
> Stop training on corrupted data.

**Tweet 7 — PlotJuggler**
> "Cool but I already use PlotJuggler"
>
> So do we. That's why there's a WebSocket bridge:
>
> ```bash
> resurrector bridge playback trial.mcap
> ```
>
> Open PlotJuggler → `ws://localhost:9090/ws`
>
> No plugin. No config. Just streams.

**Tweet 8 — Export**
> Training a manipulation policy?
>
> ```python
> bf.export(format="lerobot")   # LeRobot-ready
> bf.export(format="rlds")      # OpenX/RT-2 compatible
> ```
>
> RLDS, LeRobot, TFRecord, Parquet, Zarr, HDF5, NumPy — pick your poison.

**Tweet 9 — Insight**
> Here's the thing nobody talks about:
>
> The biggest bottleneck in robot learning isn't model architecture.
>
> It's that nobody can find the right training data in their own filesystem.

**Tweet 10 — CTA (reply with link)**
> RosBag Resurrector. Open source. Free.
>
> Go resurrect your graveyard.
>
> (link in reply)

---

## Post-Launch — Day 3+

- **Engage every comment** on HN and Reddit for 24 hrs — responding well to feedback drives the conversation
- **DM 5-10 robotics influencers** on Twitter with a genuine "built this, would love your take"
- **Answer Stack Exchange questions** about rosbag analysis and mention the tool naturally (don't spam)
- **Post to Robohub.org** if HN goes well — they cover open-source robotics tools
- **Submit to Awesome ROS lists** on GitHub

---

## Success Metrics

- **HN front page** = home run (10k+ GitHub visitors)
- **50+ upvotes on r/ROS** = solid win
- **Any robotics company engineer DMing to say "we need this"** = validation gold
- **100+ GitHub stars in first week** = healthy traction
