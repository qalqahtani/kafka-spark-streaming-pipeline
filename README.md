# Kafka-Spark Video Streaming Pipeline

A fully self-contained, Docker Compose–based simulation of a production-grade video streaming backend. Demonstrates real-world streaming architecture: distributed messaging, stream processing, object storage, metadata persistence, and observability — all running locally with a single command.

This project was built as a portfolio piece covering two fundamentally different streaming patterns (VOD and live) sharing the same underlying infrastructure.

---

## Table of Contents

1. [The Problem This Solves](#1-the-problem-this-solves)
2. [Architecture Overview](#2-architecture-overview)
3. [Component Inventory & Design Rationale](#3-component-inventory--design-rationale)
4. [The Two Data Flows Explained](#4-the-two-data-flows-explained)
5. [Why Live Chunks Bypass FastAPI](#5-why-live-chunks-bypass-fastapi)
6. [Video Engineering Considerations](#6-video-engineering-considerations)
7. [Prerequisites](#7-prerequisites)
8. [Getting Started](#8-getting-started)
9. [Lifecycle Commands](#9-lifecycle-commands)
10. [Testing with a Real Video File](#10-testing-with-a-real-video-file)
11. [Understanding What You See: The Two Simultaneous Simulations](#11-understanding-what-you-see-the-two-simultaneous-simulations)
12. [Verifying the Pipeline in Each UI](#12-verifying-the-pipeline-in-each-ui)
13. [UI Access Map](#13-ui-access-map)
14. [Configuration Reference](#14-configuration-reference)
15. [Project Structure](#15-project-structure)
16. [Production Considerations](#16-production-considerations)
17. [Read Path & CDN Integration](#17-read-path--cdn-integration)
18. [Understanding the Codebase](#18-understanding-the-codebase)

---

## 1. The Problem This Solves

A video platform typically handles two content types with radically different delivery requirements:

| Content Type | Upload pattern | Viewer experience | Latency requirement |
|---|---|---|---|
| **VOD (on-demand)** | One-time upload of a finished episode | On-demand, seek anywhere | Minutes to hours acceptable |
| **Live stream** | Continuous 2–4s segment stream for 90+ minutes | Real-time, low latency | Under 5 seconds acceptable |

These two content types have different ingest paths, different processing pipelines, and different playback requirements. A single monolithic architecture cannot serve both efficiently. The solution is to design them as two separate pipelines that share infrastructure (Kafka, Spark, MinIO, MongoDB) but have completely different data flows.

---

## 2. Architecture Overview

### ASCII Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       KAFKA-SPARK STREAMING PIPELINE                            │
└─────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════
 WRITE PATHS
═══════════════════════════════════════════════════════════

──────────────────────────────────────────────────────────
 VOD WRITE PATH
──────────────────────────────────────────────────────────

  ┌──────────────────┐  POST /vod/upload        ┌────────────────┐
  │  Producer Thread │ ─────────────────────────▶│    FastAPI     │
  │  (or test_video) │  (every 10-20 seconds)    │   (api:8000)   │
  └──────────────────┘                           └───────┬────────┘
                                                         │ 1. Write placeholder → MinIO
                                                         │ 2. Insert doc → MongoDB (status=uploaded)
                                                         │ 3. Publish event → Kafka
                                                         ▼
                                          ┌──────────────────────────┐
                                          │    Apache Kafka 4.x      │
                                          │   topic: vod-chunks      │
                                          │    (3 partitions)        │
                                          └─────────────┬────────────┘
                                                        │ readStream (5s trigger)
                                                        ▼
                                          ┌──────────────────────────┐
                                          │      Apache Spark        │
                                          │  Structured Streaming    │
                                          │    (foreachBatch)        │
                                          └────────────┬─────────────┘
                                     ┌─────────────────┼──────────────────┐
                                     │                 │                  │
                                     ▼                 ▼                  ▼
                             ┌───────────────┐ ┌────────────┐ ┌──────────────────┐
                             │     MinIO     │ │   MinIO    │ │     MongoDB      │
                             │   vod-raw     │ │  variants  │ │   vod_metadata   │
                             │  (raw .ts)    │ │ 1080p/720p │ │ status: ready    │
                             │               │ │ 480p/360p  │ │ + manifest_path  │
                             └───────────────┘ └────────────┘ └──────────────────┘
                                                       │
                                                       ▼
                                          ┌──────────────────────────┐
                                          │   MinIO: manifests/      │
                                          │   {stream_id}/           │
                                          │   vod_manifest.m3u8      │
                                          └──────────────────────────┘

──────────────────────────────────────────────────────────
 LIVE (RTMP) WRITE PATH
──────────────────────────────────────────────────────────

  ┌──────────────────┐ POST /streams/start ┌────────────────┐
  │  Producer Thread │ ───────────────────▶│    FastAPI     │ (called ONCE)
  │  (RTMP sim)      │ POST /streams/end   │   (api:8000)   │ (called ONCE)
  │                  │ ◀──────────────────  └────────────────┘
  │                  │
  │  chunk events    │ ──────────────────────────────────────────────────────▶
  │  (every 0.5–1s)  │         direct Kafka publish — NO FastAPI involved
  └──────────────────┘
                               ┌──────────────────────────────────────────┐
                               │           Apache Kafka 4.x               │
                               │       topic: live-chunks                 │
                               │           (6 partitions)                 │
                               └────────────────────┬─────────────────────┘
                                                    │ readStream (1s trigger)
                                                    ▼
                                       ┌────────────────────────┐
                                       │     Apache Spark       │
                                       │  Structured Streaming  │  < 500ms target
                                       │    (foreachBatch)      │
                                       └───────────┬────────────┘
                                    ┌──────────────┴──────────────┐
                                    │                             │
                                    ▼                             ▼
                           ┌─────────────────┐         ┌──────────────────┐
                           │      MinIO      │         │     MongoDB      │
                           │  live-streams   │         │  live_metadata   │
                           │ /chunks/{n}.ts  │         │ status: live     │
                           │                 │         │ + dvr_window     │
                           └─────────────────┘         └──────────────────┘
                                    │
                                    ▼
                           ┌─────────────────────────────┐
                           │   MinIO: manifests/          │
                           │   {stream_id}/               │
                           │   live_manifest.m3u8         │
                           │   (rolling: last 10 chunks)  │
                           └─────────────────────────────┘

═══════════════════════════════════════════════════════════
 READ PATH (same for both VOD and live)
═══════════════════════════════════════════════════════════

  ┌──────────────┐  GET /vod/{id}/manifest        ┌────────────┐
  │    Viewer    │ ──────────────────────────────▶│  FastAPI   │
  │  (VLC / app) │  GET /streams/{id}/live         │            │
  └──────────────┘                                └─────┬──────┘
                                                        │ presigned URL (1hr TTL)
                                                        ▼
                                               ┌─────────────────┐
                                               │      MinIO      │
                                               │  .m3u8 manifest │
                                               │  .ts segments   │
                                               └─────────────────┘

═══════════════════════════════════════════════════════════
 MONITORING (all services scraped by Prometheus)
═══════════════════════════════════════════════════════════

  kafka-exporter  producer:/metrics  api:/metrics  spark-job:/metrics  mongodb-exporter
       └──────────────────┬──────────────────┘
                          ▼
                   ┌─────────────┐
                   │  Prometheus │ ──▶ Grafana (dashboards)
                   └─────────────┘
```

---

## 3. Component Inventory & Design Rationale

### Apache Kafka 4.x — Message Broker
**Image:** `apache/kafka:latest`

Kafka is the central nervous system of the pipeline. Every chunk event — whether from a VOD upload or a live segment — flows through Kafka as a JSON message before being processed by Spark.

**Why Kafka?**
- **Decoupling**: The producer (ingest) is completely decoupled from the consumer (Spark). Either can restart independently without losing messages.
- **Durability**: Messages are persisted to disk. If Spark crashes and restarts, it resumes from the last committed offset — no chunks are lost.
- **Backpressure**: Kafka acts as a buffer. If Spark is slow (e.g., during heavy transcoding), messages queue safely. The producer is never blocked.
- **Replayability**: Messages can be replayed from any offset, enabling backfill or re-processing on schema changes.

**KRaft Mode (no ZooKeeper)**
Kafka 4.x removes ZooKeeper entirely. Cluster metadata is managed through the Raft consensus protocol built into Kafka itself. This eliminates an entire dependency, reduces operational complexity, and improves startup time.

**Why Kafka carries metadata only, never video bytes**
A Kafka message max size is configured to 1 MB (`KAFKA_MESSAGE_MAX_BYTES=1048576`). Real video segments are 500 KB–20 MB. Routing actual video bytes through Kafka would:
- Saturate Kafka's disk I/O with video data
- Prevent proper retention-based cleanup (video content has different lifecycle needs)
- Eliminate Kafka's ability to hold thousands of messages in memory for fast consumer reads

Instead, Kafka carries a JSON payload of ~500 bytes describing the chunk: its ID, size, checksum, timestamps, track IDs. The actual bytes live in the encoder → MinIO path.

**Topics:**
- `vod-chunks`: 3 partitions (lower throughput, one event per episode per upload)
- `live-chunks`: 6 partitions (higher throughput, one event per segment every 0.5–1s)

---

### Apache Spark — Stream Processing Engine
**Image:** `apache/spark:latest` (Spark 4.1.1, Scala 2.13, Python 3.10)

Spark Structured Streaming reads from both Kafka topics simultaneously and processes each message. Two streaming queries run concurrently in the same driver process:
- **VOD query**: 5-second trigger, processes VOD pipeline (transcode simulation, variants, full manifest)
- **Live query**: 1-second trigger, processes live pipeline (gap detection, rolling manifest)

**Why Spark Structured Streaming (not Kafka Streams or Flink)?**
- **foreachBatch**: Spark's `foreachBatch` API lets you process a micro-batch as a regular Python function, giving full access to boto3 (MinIO) and pymongo (MongoDB) without needing Spark connectors for those systems.
- **Fault tolerance**: Checkpoints ensure exactly-once processing semantics. On restart, Spark replays the last uncommitted batch.
- **Familiar Python API**: PySpark lets you write processing logic in Python, which integrates naturally with boto3 and pymongo.

**Three containers, one image:**
`spark-master`, `spark-worker`, and `spark-job` all use the same Docker image (built from `spark_job/Dockerfile`). This eliminates classpath inconsistencies — the Kafka JARs and Python packages are identical across all three roles. The role is determined by the CMD passed in docker-compose.

---

### FastAPI — Control Plane REST API
**Image:** built from `api/Dockerfile` (Python 3.11-slim)

FastAPI handles the **control plane** only — things that happen once per episode or once per stream:

| Endpoint | Called by | When |
|---|---|---|
| `POST /vod/upload` | Producer (or test script) | Every 10–20s, one per episode |
| `POST /streams/start` | Producer | Once, before stream kick-off |
| `POST /streams/end` | Producer | Once, after stream ends |
| `GET /vod/{id}/manifest` | Viewer / player | On-demand read |
| `GET /streams/{id}/live` | Viewer / player | On-demand read |
| `GET /*/metadata` | Viewer / admin | On-demand read |

Live chunk events never go through FastAPI. See [Section 5](#5-why-live-chunks-bypass-fastapi) for the full reasoning.

---

### MinIO — Object Storage
**Image:** `minio/minio:latest`

MinIO is an S3-compatible object store that holds all binary artifacts. It is accessed via the standard AWS S3 API (`boto3`) so the code is portable to any S3-compatible service (AWS S3, Cloudflare R2, Backblaze B2).

**Buckets:**

| Bucket | Contents | Written by |
|---|---|---|
| `vod-raw` | Zero-byte placeholder per raw episode chunk | FastAPI (on upload) |
| `vod-variants` | Zero-byte placeholder per quality variant per chunk | Spark (post-transcode) |
| `live-streams` | Zero-byte placeholder per live segment | Spark (on receive) |
| `manifests` | Real `.m3u8` HLS playlists (both VOD and live) | Spark |

**Presigned URLs**: MinIO generates time-limited URLs that allow clients to fetch objects directly, without proxying through the API. The API generates these URLs and returns them to the viewer. Expires in 1 hour.

**Public vs internal endpoint**: The API uses two boto3 clients:
- **Internal** (`minio:9000`): for put/get operations inside Docker
- **Public** (`localhost:9000`): for presigned URL generation, so URLs work from your Mac's browser or VLC

---

### MongoDB — Metadata Store
**Image:** `mongo:7.0`

MongoDB stores structured metadata for every stream event. Two collections:

**`vod_metadata`** — one document per VOD episode:
```json
{
  "stream_id": "vod-3e87dd00aab5",
  "title": "VOD: The Farmer",
  "status": "ready",
  "processing_latency_ms": 8089.98,
  "variants_generated": ["1080p", "720p", "480p", "360p"],
  "manifest_path": "manifests/vod-3e87dd00aab5/vod_manifest.m3u8",
  "keyframe_aligned": true,
  "audio_track_id": "audio-vod-3e87dd00aab5"
}
```

**`live_metadata`** — one document per live segment:
```json
{
  "stream_id": "live-54390fa408",
  "chunk_index": 999,
  "sequence_number": 999,
  "dvr_window_start": 990,
  "status": "live",
  "processing_latency_ms": 210.4,
  "manifest_updated_at": "2026-02-24T12:51:00+00:00"
}
```

**Why MongoDB (not PostgreSQL)?**
- Schema-free documents — VOD and live events have different shapes; no ALTER TABLE needed
- Embedded arrays for `variant_paths` and `variants_generated`
- Fast upsert on compound key `(stream_id, chunk_index)` for idempotency

---

### Python Producer — Ingest Simulator
**Image:** built from `producer/Dockerfile` (Python 3.11-slim)

Runs two threads simultaneously from the moment the stack starts:

**Thread 1 — VOD uploader** (simulates a content creator's client):
- Calls `POST /vod/upload` on FastAPI every 10–20 seconds
- Rotates through 5 episode titles (configurable in `.env`)
- Simulates file sizes of 5–20 MB

**Thread 2 — Live RTMP ingest** (simulates an encoder output):
- Calls `POST /streams/start` once, then publishes chunk events directly to `live-chunks` Kafka topic every 0.5–1 second
- Simulates 1,000 chunks per "stream" then calls `POST /streams/end` and starts a new stream
- Intentionally injects sequence number gaps (1 gap per stream) to demonstrate Spark's gap detection
- Simulates 2% checksum failure rate to demonstrate monitoring

Exposes Prometheus metrics at `:8765`.

---

### Redpanda Console — Kafka UI
**Image:** `redpandadata/console:latest`
**URL:** http://localhost:8080

A web UI for inspecting Kafka in real time. You can:
- Browse topics and see message counts
- Click into any message and read the full JSON payload
- Monitor consumer group lag (how far behind Spark is)
- See partition distribution

---

### Mongo Express — MongoDB UI
**Image:** `mongo-express:1.0.2`
**URL:** http://localhost:8081

Web UI for browsing MongoDB collections. You can:
- Browse `vod_metadata` and `live_metadata` collections
- Filter documents by stream_id
- Watch `status` field change in real time as Spark processes events

---

### Prometheus — Metrics Aggregation
**Image:** `prom/prometheus:latest`
**URL:** http://localhost:9090

Scrapes metrics from all services every 15 seconds. Configured targets:
- `kafka-exporter:9308` — Kafka broker metrics
- `producer:8765` — ingest simulation metrics
- `api:8000` — FastAPI request/latency metrics
- `spark-job:8766` — Spark processing metrics
- `mongodb-exporter:9216` — MongoDB operation metrics
- `minio:9000/minio/v2/metrics/cluster` — object storage metrics

---

### Grafana — Dashboards
**Image:** `grafana/grafana:latest`
**URL:** http://localhost:3000 (admin / admin)

A pre-built dashboard (`pipeline_dashboard.json`) is auto-provisioned at startup. Panels include:
- Kafka messages/sec per topic
- Producer throughput (messages/s and MB/s by stream type)
- Live chunk gap counter
- VOD and live processing latency (p50, p95, p99)
- Checksum failure rate
- MongoDB operations/sec
- MinIO storage used per bucket
- FastAPI request rate and response time

---

## 4. The Two Data Flows Explained

### VOD Write Path — Step by Step

Using the actual run of `The Farmer.mp4` as the example (timestamps from MongoDB):

```
12:16:35.771  Producer calls POST /vod/upload with:
              { title: "The Farmer", size_bytes: 10258133, resolution: "1920x1080" }

12:16:35.771  FastAPI (api:8000):
              ① Generates stream_id = "vod-3e87dd00aab5"
              ② Writes zero-byte placeholder to MinIO:
                 vod-raw/vod-3e87dd00aab5/raw/0.ts
              ③ Inserts MongoDB document (status: "uploaded")
              ④ Publishes JSON event to Kafka topic: vod-chunks
              ⑤ Returns { stream_id, status: "uploaded" } to caller

12:16:35–40   Event sits in Kafka partition (Spark polls every 5 seconds)

12:16:40.580  Spark fires 5-second trigger, reads the Kafka message:
              ① Updates MongoDB status → "processing"
              ② Verifies simulated MD5 checksum
              ③ Sleeps 2–4 seconds (simulating FFmpeg transcode time)

12:16:43.826  Spark updates MongoDB status → "transcoding"
              ① Writes raw chunk placeholder to MinIO:
                 vod-raw/vod-3e87dd00aab5/raw/0.ts
              ② Writes 4 quality variant placeholders:
                 vod-variants/vod-3e87dd00aab5/1080p/0.ts
                 vod-variants/vod-3e87dd00aab5/720p/0.ts
                 vod-variants/vod-3e87dd00aab5/480p/0.ts
                 vod-variants/vod-3e87dd00aab5/360p/0.ts
              ③ Creates HLS manifest at:
                 manifests/vod-3e87dd00aab5/vod_manifest.m3u8

12:16:43.861  Spark updates MongoDB status → "ready"
              Writes final metadata: processing_latency_ms=8089.98,
              variant_paths=[...], manifest_path="manifests/..."

Total time: ~8 seconds from upload to ready
```

**MongoDB status progression visible in real time:**
```
uploaded  →  processing  →  transcoding  →  ready
  (0s)         (5s)            (5s)         (8-9s)
```

### Live Write Path — Step by Step

```
[Stream start]
Producer calls POST /streams/start once:
  → FastAPI creates MongoDB doc { status: "live" }
  → FastAPI writes empty manifest to MinIO
  → Returns stream_id = "live-54390fa408"

[Every 0.5–1 second, for 1,000 chunks]
Producer publishes directly to Kafka (NO FastAPI):
  {
    stream_id: "live-54390fa408",
    chunk_index: 42,
    sequence_number: 42,
    size_bytes: 1628000,
    duration_ms: 3000,
    keyframe_aligned: true,
    audio_track_id: "audio-live-54390fa408",
    video_track_id: "video-live-54390fa408",
    checksum: "a3f8c2..."
  }

[Every 1 second, Spark fires its live trigger]
Spark reads the batch:
  ① Checks sequence_number continuity:
     if gap detected → increments live_chunk_gaps_total counter
  ② Verifies checksum (2% simulated failure rate)
  ③ Writes chunk placeholder to MinIO:
     live-streams/live-54390fa408/chunks/42.ts
  ④ Fetches current manifest from MinIO
  ⑤ Appends new chunk, removes chunks outside DVR window (last 10)
  ⑥ Writes updated manifest back to MinIO:
     manifests/live-54390fa408/live_manifest.m3u8
     (contains only chunks 33–42, not 0–32)
  ⑦ Upserts MongoDB document for this chunk

[Stream end — after 1,000 chunks]
Producer calls POST /streams/end:
  → FastAPI appends #EXT-X-ENDLIST to the manifest
  → FastAPI updates MongoDB { status: "vod" }
```

---

## 5. Why Live Chunks Bypass FastAPI

This is the most important architectural decision in the pipeline.

### The Problem with HTTP Per-Chunk

In live streaming, a hardware encoder (HEVC, H.264) or software encoder (FFmpeg) outputs HLS segments every 2–4 seconds. At 1 segment every 3 seconds for a 90-minute stream, that's 1,800 API calls — sustained at ~0.33 calls/second. That sounds manageable.

But in real broadcast infrastructure, a single ingest point might be receiving feeds from:
- Multiple camera angles
- Multiple quality renditions (pre-split at the encoder)
- Multiple concurrent streams

At 10 concurrent streams × 3 renditions × 0.5s interval = **60 API calls per second**. With an HTTP round-trip (DNS, TCP handshake, TLS, JSON parsing, response), each call adds 10–50ms of latency. More critically, the API becomes a **single point of failure** — if it restarts, all ongoing streams lose their ingest path.

### The Solution: Direct Kafka Publish

By publishing chunk events directly to Kafka, the ingest path:

| Property | HTTP to API | Direct Kafka |
|---|---|---|
| Latency per chunk | 10–50ms | < 2ms |
| Single point of failure | Yes (API down = stream lost) | No (Kafka persists, Spark catches up) |
| Horizontal scaling | Requires load balancer | Add partitions + consumers |
| Backpressure | Caller must retry on 503 | Kafka queues; producer never blocks |
| Durability | Lost if API crashes mid-write | Kafka persists to disk |

### The Real-World Analogy

This mirrors how AWS MediaLive or Elemental encoder farms work:
- **Pre-stream**: Control plane calls create the stream record (our `POST /streams/start`)
- **During stream**: The encoder uses the Kinesis/Kafka SDK directly — no REST API per segment
- **Post-stream**: Control plane calls finalize the stream (our `POST /streams/end`)

FastAPI is the **control plane**. Kafka is the **data plane**. They serve different purposes and should never be conflated.

---

## 6. Video Engineering Considerations

### Keyframe Alignment (IDR Frames)

Every simulated chunk event includes `keyframe_aligned: true` and a `duration_ms` of 2,000–4,000ms. In real HLS:

- Video is encoded with a fixed keyframe interval (e.g., every 2 seconds at 50fps = every 100 frames)
- Each HLS segment **must** start on an IDR (Instantaneous Decoder Refresh) frame
- If a segment starts mid-GOP (Group of Pictures), a player cannot begin decoding there — it would show artifacts
- The encoder forces an IDR at the start of each segment, ensuring any segment can be independently decoded

This constraint is why HLS segment durations are not arbitrary — they must align with the keyframe interval. Our simulation enforces this by always generating realistic durations rather than random ones.

### Multi-Bitrate Variants (Adaptive Bitrate — ABR)

For VOD, Spark generates 4 quality variant placeholders per chunk:

| Variant | Target resolution | Typical bitrate |
|---|---|---|
| 1080p | 1920×1080 | 4–8 Mbps |
| 720p | 1280×720 | 2–4 Mbps |
| 480p | 854×480 | 1–1.5 Mbps |
| 360p | 640×360 | 0.6–1 Mbps |

A master playlist (HLS variant stream) would reference all four; the client's ABR algorithm selects the rendition based on measured bandwidth. Players like Apple's AVFoundation, ExoPlayer, and HLS.js implement this automatically.

### DVR Window (Live Stream)

The rolling HLS manifest for live streams keeps only the **last 10 segments** (`DVR_WINDOW_SIZE` in `.env`). This is the "DVR window":

```
Chunk 0–990: stored in MinIO forever (for catch-up / clip generation)
Manifest: only references chunks 990–999 (the live window)

#EXTM3U
#EXT-X-VERSION:3
#EXT-X-PLAYLIST-TYPE:EVENT
#EXT-X-MEDIA-SEQUENCE:990      ← base sequence number
#EXTINF:3.000,
live-54390fa408/chunks/990.ts
...
#EXTINF:3.000,
live-54390fa408/chunks/999.ts
```

The `#EXT-X-MEDIA-SEQUENCE` header tells players where the window starts. Players poll this manifest every few seconds; they append new segments and discard old ones from their buffer. This is exactly how YouTube Live, Twitch, and broadcast sports streaming works.

When the stream ends, `#EXT-X-ENDLIST` is appended — the stream transitions from live to VOD.

### Checksum Validation

Every chunk event carries a simulated MD5 checksum. Spark re-computes it and increments `chunk_checksum_failures_total` (a Prometheus counter) on mismatch. In production:

- The checksum is computed at the encoder over the actual `.ts` segment bytes using SHA-256
- Spark (or a dedicated validation service) verifies it after writing to MinIO using `ETag` comparison
- A mismatch means the segment was corrupted in transit → the segment is rejected and the encoder is asked to resend

The 2% simulated failure rate lets you see this counter in Grafana without waiting hours.

### Idempotent Writes (Upsert on Retry)

Spark uses `update_one(..., upsert=True)` with compound key `(stream_id, chunk_index)` in MongoDB. This is critical because:

- Spark may replay a micro-batch on driver restart
- Without upsert, replayed batches create duplicate documents
- With upsert, replaying the same chunk_index is safe — it just overwrites with the same data

The same principle applies to MinIO: `put_object` on the same key is a no-op if the content hasn't changed.

### Audio/Video Track Separation

Each live chunk event carries separate `audio_track_id` and `video_track_id`. In real MPEG-TS:
- Video is encoded as H.264 or HEVC in one PES (Packetized Elementary Stream)
- Audio is encoded as AAC in a separate PES
- Both are multiplexed into the `.ts` container

Storing them separately in MongoDB enables future features: multi-language audio tracks, alternate camera angles, sign-language tracks.

---

## 7. Prerequisites

| Requirement | Minimum | Notes |
|---|---|---|
| Docker Desktop for Mac | Latest stable | Enable the Docker Compose plugin |
| RAM allocated to Docker | **10 GB** | Settings → Resources → Memory |
| CPU allocated to Docker | **4 CPUs** | Settings → Resources → CPUs |
| Disk space | ~8 GB | For Docker images + volumes |
| `ffmpeg` (optional) | Any version | `brew install ffmpeg` — for real video metadata extraction |

---

## 8. Getting Started

```bash
# Clone or enter the project directory
cd streaming_pipeline

# Make scripts executable (first time only)
chmod +x run.sh test_video.sh

# Start everything — first run takes 5–10 minutes to download images
# and compile Kafka JARs. Subsequent starts take ~30 seconds.
./run.sh start
```

When ready, you'll see all services listed with their URLs. The pipeline immediately starts simulating:
- A live stream, with chunks arriving every 0.5–1s
- VOD episode uploads, one every 10–20 seconds

---

## 9. Lifecycle Commands

```bash
./run.sh start          # Build images and start all services in the background
./run.sh stop           # Stop all containers (data volumes are preserved)
./run.sh restart        # Stop then start
./run.sh reset          # Stop AND delete all volumes — full clean slate
./run.sh status         # Show container health and port bindings
./run.sh logs           # Tail logs from all services (Ctrl+C to stop)
./run.sh logs kafka     # Tail logs from a single service
./run.sh logs spark-job # Watch Spark processing in real time
./run.sh logs producer  # Watch the ingest simulation
```

---

## 10. Testing with a Real Video File

Place any video file in `test_videos/` and run:

```bash
./test_video.sh test_videos/your_episode.mp4 --watch
```

What the script does:
1. Checks the pipeline is healthy (`GET /health`)
2. Reads real metadata from the file using `ffprobe` (duration, resolution, file size), or uses estimates if ffprobe is not installed
3. Calls `POST /vod/upload` on FastAPI with that metadata
4. Polls `GET /vod/{stream_id}/metadata` every 3 seconds
5. Prints each status transition as Spark processes the event
6. Returns a presigned MinIO URL for the HLS manifest when `status=ready`

**Example output (The Farmer.mp4):**
```
=== Video File Analysis ===
  File:       The Farmer.mp4
  File size:  9.8 MB

=== Submitting VOD Upload ===
  Stream ID:   vod-3e87dd00aab5

=== Watching Pipeline Progress ===
  [15:16:35] ● Status: uploaded   — event in Kafka, waiting for Spark
  [15:16:41] ● Status: processing — Spark picked up the event
  [15:16:44] ● Status: ready      — variants written to MinIO, manifest generated!

  HLS Manifest URL:
  http://localhost:9000/manifests/vod-3e87dd00aab5/vod_manifest.m3u8?X-Amz-...
```

**To install ffprobe** (reads real video metadata):
```bash
brew install ffmpeg
```

**To open the manifest in VLC:**
- Open VLC → File → Open Network Stream → paste the manifest URL
- The manifest references placeholder `.ts` files (zero bytes), so no video will play,
  but the HLS structure is valid and the URL demonstrates the full read path

---

## 11. Understanding What You See: The Two Simultaneous Simulations

**This is the most important section for understanding what you see in the UIs.**

As soon as you run `./run.sh start`, two independent simulations run continuously in the `producer` container. When you also run `test_video.sh`, that adds a third event on top of the background simulation.

### What runs automatically (background — no action needed)

| Simulation | Source | Rate | Destination |
|---|---|---|---|
| Live chunks | `live_producer_thread` | Every 0.5–1s | Directly to `live-chunks` Kafka topic |
| VOD uploads | `vod_producer_thread` | Every 10–20s | Via FastAPI → `vod-chunks` Kafka topic |

### What your test adds

When you run `./test_video.sh test_videos/The\ Farmer.mp4`:
- It creates **exactly one** VOD upload event with your file's metadata
- This goes through the same FastAPI → Kafka → Spark path as the background VOD simulation
- Your event gets a unique `stream_id` (e.g., `vod-3e87dd00aab5`)

### How to distinguish your test from the background simulation

**Your video's stream_id is printed by the script.** Use it to filter in every UI:

#### In Redpanda Console (http://localhost:8080)
- Topics → `vod-chunks` → Messages
- Use the search/filter to find messages containing `"vod-3e87dd00aab5"` (your stream_id)
- You'll see one message for your video amid many background VOD events

#### In Mongo Express (http://localhost:8081)
- Database: `pipeline` → Collection: `vod_metadata`
- Click the search icon, filter by: `{"stream_id": "vod-3e87dd00aab5"}`
- You'll see your document with `title: "VOD: The Farmer"` and all processing timestamps

#### In MinIO Console (http://localhost:9001)
- `vod-raw` bucket → folder `vod-3e87dd00aab5/` → `raw/0.ts`
- `vod-variants` bucket → folder `vod-3e87dd00aab5/` → `1080p/`, `720p/`, `480p/`, `360p/`
- `manifests` bucket → folder `vod-3e87dd00aab5/` → `vod_manifest.m3u8`

All the other `vod-xxxxxxxxxx/` folders in MinIO are from the **background simulation**, not your test.

### The live chunks you see in MinIO

Every `live-streams/live-xxxxxxxxxx/chunks/N.ts` object was written by the background live simulation. These are entirely from the automatic producer thread — your `test_video.sh` has no connection to live chunks.

### Summary table: who wrote what

| Location | Item | Written by |
|---|---|---|
| `vod-raw/vod-3e87dd00aab5/` | 1 object | Your `test_video.sh` run |
| `vod-variants/vod-3e87dd00aab5/` | 4 objects | Spark, triggered by your test |
| `manifests/vod-3e87dd00aab5/` | 1 manifest | Spark, triggered by your test |
| `vod-raw/vod-xxxxxxxxxx/` (all others) | many folders | Background VOD simulation |
| `live-streams/live-xxxxxxxxxx/` | many chunks | Background live simulation |
| `manifests/live-xxxxxxxxxx/` | Rolling manifest | Spark, from background live |

---

## 12. Verifying the Pipeline in Each UI

### Redpanda Console — http://localhost:8080
**What to look for:**
- **Topics** tab: `vod-chunks` and `live-chunks` should both have messages
- `live-chunks`: message count climbs rapidly (~1/s); click any message to see the JSON with `stream_type: "live"`, `sequence_number`, `keyframe_aligned: true`
- `vod-chunks`: message count grows slowly; click a message to see `stream_type: "vod"` and the full metadata
- **Consumer Groups**: look for the Spark consumer; lag should stay near 0 if Spark keeps up
- Click a message → see the raw JSON payload including `checksum`, `audio_track_id`, `video_track_id`

### Mongo Express — http://localhost:8081
**What to look for:**
- `pipeline` database → `vod_metadata` collection
  - Filter: `{"status": "ready"}` — should show all processed episodes
  - Filter: `{"stream_id": "vod-3e87dd00aab5"}` — shows only The Farmer
  - Note the `processing_started_at`, `transcoding_started_at`, `completed_at` timestamps and calculate the processing stages
- `live_metadata` collection
  - Filter: `{"stream_id": "live-54390fa408"}` — shows all chunks for the current stream
  - Note `dvr_window_start` advancing as the window slides forward
  - Note `manifest_updated_at` changing every second

### MinIO Console — http://localhost:9001
Login: `minioadmin` / `minioadmin123`

**What to look for:**
- `manifests` bucket → click `vod-3e87dd00aab5/vod_manifest.m3u8` → Preview → see the real HLS playlist
- `manifests` bucket → click `live-54390fa408/live_manifest.m3u8` → Preview → see the rolling live playlist (only 10 entries)
- `vod-variants` → browse into `vod-3e87dd00aab5` → see the 4 quality folders
- `live-streams` → see chunk folders accumulating in real time

### Spark Master UI — http://localhost:8090
**What to look for:**
- Active workers registered
- Running application: `StreamingPipeline`
- Memory and CPU allocation per worker

### Spark App UI — http://localhost:4040
**What to look for:**
- **Streaming** tab: two active queries (`vod` and `live`)
  - Input rate: live should show ~1 row/s; vod ~0.07 row/s
  - Processing time: live batches should be < 500ms; vod batches are longer due to simulated transcode
  - Batch duration histogram: should be stable
- **Jobs** tab: completed micro-batch jobs
- **SQL/DataFrame** tab: query plans for each streaming read

### Prometheus — http://localhost:9090
**Key queries to try:**
```
# Messages produced per second by stream type
rate(producer_messages_total[1m])

# Live chunk gap counter (non-zero if gaps detected)
live_chunk_gaps_total

# Processing latency percentiles
histogram_quantile(0.99, rate(chunk_processing_latency_seconds_bucket[1m]))

# All scrape targets up/down
up

# FastAPI request rate
rate(api_requests_total[1m])

# Checksum failures
chunk_checksum_failures_total
```

### Grafana — http://localhost:3000
Login: `admin` / `admin`

Dashboard: **Kafka-Spark Streaming Pipeline** (auto-loaded)

**Key panels:**
- `Producer: Messages/sec by Stream Type`: live ~1/s, vod ~0.07/s
- `Live: Chunk Processing Latency`: p50 should be < 500ms
- `Live: Chunk Gaps Detected`: small non-zero value from injected gaps
- `VOD: Processing Rate & Variants Generated`: 4 variants per chunk
- `Checksum Failures`: ~2% failure rate (simulated)

### FastAPI Swagger — http://localhost:8000/docs
Interactive API documentation. You can:
- Manually submit a VOD upload with custom metadata
- Start and stop live streams
- Fetch manifest URLs for any stream_id
- Inspect request/response shapes for all endpoints

---

## 13. UI Access Map

| Service | URL | Credentials | Purpose |
|---|---|---|---|
| **Redpanda Console** | http://localhost:8080 | — | Kafka topic and message inspection |
| **Mongo Express** | http://localhost:8081 | — | MongoDB collection browser |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin123 | Object storage browser |
| **Spark Master UI** | http://localhost:8090 | — | Spark cluster overview |
| **Spark App UI** | http://localhost:4040 | — | Streaming query stats and job stages |
| **FastAPI Swagger** | http://localhost:8000/docs | — | Interactive REST API |
| **Prometheus** | http://localhost:9090 | — | Raw metrics and PromQL |
| **Grafana** | http://localhost:3000 | admin / admin | Pre-built pipeline dashboard |

---

## 14. Configuration Reference

All tuneable values live in `.env`. Edit the file and run `./run.sh restart` to apply.

| Variable | Default | Description |
|---|---|---|
| `VOD_INTERVAL_MIN/MAX` | 10 / 20 s | How often the VOD producer uploads an episode |
| `LIVE_INTERVAL_MIN/MAX` | 0.5 / 1.0 s | How often the RTMP simulator pushes a chunk to Kafka |
| `DVR_WINDOW_SIZE` | 10 | Number of segments kept in the rolling live manifest |
| `KAFKA_MESSAGE_MAX_BYTES` | 1048576 | 1 MB hard limit — metadata only, never video bytes |
| `SPARK_WORKER_MEMORY` | 2g | Memory per Spark worker |
| `SPARK_WORKER_CORES` | 2 | CPU cores per Spark worker |
| `MINIO_ROOT_USER/PASSWORD` | minioadmin / minioadmin123 | MinIO login |
| `GRAFANA_USER/PASSWORD` | admin / admin | Grafana login |
| `MATCH_HOME_TEAM` | Al-Hilal | Simulated home team name (demo data) |
| `MATCH_AWAY_TEAM` | Al-Nassr | Simulated away team name (demo data) |
| `VOD_SHOW_NAME` | My Streaming Show | Show name prefix for episode titles |

---

## 15. Project Structure

```
streaming_pipeline/
│
├── docker-compose.yml          All 14 services defined here
├── .env                        All tunable configuration values
├── run.sh                      Lifecycle manager (start/stop/reset/logs)
├── test_video.sh               Test with a real video file
├── test_videos/                Place your .mp4/.mkv files here
│
├── api/
│   ├── Dockerfile              Python 3.11-slim
│   ├── requirements.txt
│   └── main.py                 FastAPI application
│                               Endpoints: VOD upload, stream lifecycle, manifest URLs
│
├── producer/
│   ├── Dockerfile              Python 3.11-slim
│   ├── requirements.txt
│   └── producer.py             Dual-thread ingest simulator
│                               Thread 1: calls FastAPI every 10-20s (vod)
│                               Thread 2: publishes to Kafka every 0.5-1s (live)
│
├── spark_job/
│   ├── Dockerfile              apache/spark:latest + Kafka JARs + Python packages
│   └── spark_streaming.py      PySpark Structured Streaming job
│                               Reads: vod-chunks + live-chunks topics
│                               Writes: MinIO (boto3) + MongoDB (pymongo)
│
├── monitoring/
│   └── prometheus.yml          Scrape targets for all services
│
└── grafana/
    └── provisioning/
        ├── datasources/
        │   └── prometheus.yml  Auto-configures Prometheus datasource
        └── dashboards/
            ├── dashboard.yml   Points Grafana at the JSON files
            └── pipeline_dashboard.json  Pre-built dashboard (17 panels)
```

---

## 16. Production Considerations

This simulation intentionally omits several production concerns:

| What this simulation does | What production would use |
|---|---|
| Zero-byte placeholder `.ts` objects in MinIO | Real H.264/HEVC `.ts` segment bytes from FFmpeg or a hardware encoder |
| `time.sleep()` for transcode simulation | AWS Elemental MediaConvert, FFmpeg worker pool, or GPU transcoding cluster |
| Single Kafka broker, no replication | Multi-broker Kafka cluster (min. 3 brokers), replication factor 3, rack-aware assignment |
| No TLS anywhere | TLS on all inter-service communication; mTLS on Kafka; HTTPS for API and MinIO |
| No authentication | OAuth2 / API keys for FastAPI; SASL/SCRAM for Kafka; IAM policies for MinIO; MongoDB auth |
| In-memory DVR window state in Spark driver | External state store (Redis, RocksDB via Spark's StateStore) for fault-tolerant windowing |
| `local[*]` Spark processing fallback | Spark on Kubernetes with HPA; separate executor pods per topic partition |
| Local MinIO | AWS S3 / Cloudflare R2 / Backblaze B2 with CDN (CloudFront, Akamai, Fastly) in front |
| No DRM | Widevine (Android/Chrome), FairPlay (Apple), PlayReady (Windows) content encryption |
| Single MongoDB node, no auth | MongoDB Atlas (managed) or self-hosted replica set (min. 3 nodes) with auth |
| Simulated checksums (2% fake failure rate) | SHA-256 computed at the encoder, verified after MinIO write using S3 ETag |
| Manual `docker-compose up` | Helm chart on Kubernetes; GitOps deployment via ArgoCD or Flux |
| Background simulation for live RTMP | Actual NGINX-RTMP module, Wowza Streaming Engine, or AWS MediaLive receiving from OBS / hardware encoder |
| No CDN | CloudFront / Akamai serving `.ts` segments with edge caching; CDN key rotation for DRM |
| No cold storage tiering | S3 Lifecycle Rules: Standard → Standard-IA (30 days) → Glacier (90 days) → Glacier Deep Archive (1 year) |
| Grafana dashboard manually provisioned | Automated alert rules; PagerDuty integration; SLO dashboards with burn rate alerts |

---

## 17. Read Path & CDN Integration

### How Processed Content Reaches a Viewer

After Spark finishes processing, the pipeline has written two categories of artifacts to MinIO:
- **HLS manifests** (`.m3u8` text files) in the `manifests/` bucket
- **Video segments** (`.ts` binary files) in `vod-variants/`, `vod-raw/`, or `live-streams/`

Getting those bytes to a viewer's device involves two problems:
1. **Discovery** — how does the player know which manifest URL to request?
2. **Delivery** — how do the bytes travel from storage to the viewer's screen efficiently?

---

### Current Simulation: Presigned MinIO URLs

In this simulation, FastAPI answers both questions via a single presigned URL:

```
GET /vod/{stream_id}/manifest
  → FastAPI looks up stream_id in MongoDB (vod_metadata)
  → Reads manifest_path: "manifests/vod-3e87dd00aab5/vod_manifest.m3u8"
  → Calls MinIO to generate a presigned URL (1-hour TTL)
  → Returns the URL to the caller

Viewer pastes URL into VLC or HLS.js → player fetches the manifest → player fetches .ts segments
```

This works locally because MinIO acts as both the origin store and the delivery server. It does not scale — MinIO is not a CDN.

---

### Production: CDN Sits in Front of Object Storage

In production, a CDN (CloudFront, Akamai, Fastly) is placed in front of S3 or MinIO as the **origin**. The CDN caches objects at edge nodes close to viewers. The URL structure becomes deterministic:

```
https://cdn.example.com/{bucket}/{key}
```

Because the MinIO/S3 key structure in this pipeline is already deterministic — built entirely from `stream_id` and `chunk_index` — the CDN URL for any piece of content can be computed without a database lookup:

```
# VOD manifest
https://cdn.example.com/manifests/{stream_id}/vod_manifest.m3u8

# VOD variant segment (chunk 0, 1080p)
https://cdn.example.com/vod-variants/{stream_id}/1080p/0.ts

# Live manifest (rolling window, polled every ~2s by the player)
https://cdn.example.com/manifests/{stream_id}/live_manifest.m3u8

# Live segment
https://cdn.example.com/live-streams/{stream_id}/chunks/{n}.ts
```

The platform only needs the `stream_id` to construct any URL. No manifest URL is stored explicitly in MongoDB — `manifest_path` in MongoDB is a storage key, not a CDN URL.

---

### `stream_id` — The Universal Content Key

`stream_id` is the single identifier that ties together every system in the stack:

| System | How `stream_id` is used |
|---|---|
| **Kafka** | `stream_id` field in every JSON message on `vod-chunks` and `live-chunks` |
| **MongoDB** | Primary lookup key in `vod_metadata` and `live_metadata` |
| **MinIO** | Root prefix for all objects belonging to this stream (`{bucket}/{stream_id}/...`) |
| **CDN** | Embedded in the URL path — every CDN edge node can serve by key |
| **Player** | Requested in every manifest and segment fetch (`/manifests/{stream_id}/...`) |
| **DRM** | Content key identifier used when requesting a licence (`content_id = stream_id`) |

A platform that integrates with this pipeline needs to store exactly one thing per piece of content: the `stream_id`. Everything else (manifest URL, segment URLs, metadata) is derivable from it.

---

### CDN Caching Strategy: VOD vs Live

VOD and live content have opposite caching requirements:

| Object | Cache-Control | Reason |
|---|---|---|
| `vod_manifest.m3u8` | `max-age=31536000, immutable` | VOD manifest never changes once `status=ready` |
| VOD `.ts` segments | `max-age=31536000, immutable` | Segments are write-once; content never changes |
| `live_manifest.m3u8` | `max-age=2, s-maxage=1` | Rolling window updates every ~1s; CDN must revalidate frequently |
| Live `.ts` segments | `max-age=31536000, immutable` | Once written, a segment's bytes never change |

The live manifest is the only object that must be fetched fresh. Everything else — including live `.ts` segments — can be cached indefinitely at the CDN edge. This is why live streaming CDN costs are dominated by manifest requests, not segment bandwidth: manifest objects are small (~500 bytes) but fetched every 2 seconds per viewer.

When the stream ends and `#EXT-X-ENDLIST` is appended, the platform updates the manifest's Cache-Control to `immutable`. From that moment, the stream is served as pure VOD.

---

### MongoDB as Content Catalog

MongoDB (`vod_metadata` and `live_metadata`) is the authoritative **content catalog** — the source of truth for content identity and state. A platform API would expose it like this:

```
GET /content/{stream_id}
```

Example response for a finished VOD episode:

```json
{
  "stream_id": "vod-3e87dd00aab5",
  "title": "VOD: The Farmer",
  "status": "ready",
  "stream_type": "vod",
  "duration_ms": 127000,
  "variants": ["1080p", "720p", "480p", "360p"],
  "manifest_url": "https://cdn.example.com/manifests/vod-3e87dd00aab5/vod_manifest.m3u8",
  "processing_latency_ms": 8089,
  "created_at": "2026-02-24T12:16:35Z",
  "ready_at": "2026-02-24T12:16:44Z"
}
```

Example response for an active live stream:

```json
{
  "stream_id": "live-54390fa408",
  "status": "live",
  "stream_type": "live",
  "manifest_url": "https://cdn.example.com/manifests/live-54390fa408/live_manifest.m3u8",
  "dvr_window_start": 990,
  "latest_chunk": 999,
  "started_at": "2026-02-24T11:00:00Z"
}
```

The platform constructs `manifest_url` at response time by combining the CDN base URL with the known key pattern. It does not need to store the full URL — only the `stream_id` and the CDN base URL (configuration).

---

### DRM and Signed CDN URLs

In a DRM-protected deployment, the flow is extended:

```
1. Player requests manifest URL from platform API (authenticated)
2. Platform API:
   a. Verifies the user is entitled to this content
   b. Issues a short-lived signed CDN URL (e.g., CloudFront signed URL, 15-minute TTL)
   c. Returns the signed manifest URL to the player
3. Player fetches manifest from CDN using the signed URL
4. Player encounters an encrypted segment (AES-128 or CENC)
5. Player requests a licence from the DRM licence server (Widevine / FairPlay / PlayReady)
   — using content_id = stream_id
6. DRM server verifies the user's entitlement, issues a decryption key
7. Player decrypts and plays segments
```

The signed CDN URL serves the same conceptual role as the presigned MinIO URL in this simulation — both are time-limited tokens granting access to a specific object path. The difference is scale: a CDN signed URL is valid for millions of edge-cached fetches; a MinIO presigned URL hits a single origin every time.

`stream_id` remains the content key at every step: the platform entitlement check, the CDN URL path, the DRM licence server lookup, and the player's HLS request.

---

## 18. Guide: How to Read and Understand This Codebase

This section is a structured guide for anyone — including the original author returning after a break — who wants to build a real mental model of how the pipeline works, not just what it does. It assumes you have the stack running (`./run.sh start`) and walks you through the code in the order that builds understanding fastest.

**What you'll have at the end:** You'll be able to answer "where does X happen?" for any behaviour you observe in the UIs, explain why each architectural decision was made, and make changes to the code with confidence.

**Time required:** 3–4 focused hours.

---

### The Core Principle

Don't read code like a book from top to bottom. Read it by **following one piece of data through the system**. A single live chunk event touches 5 files. Trace it all the way through and you'll understand the entire pipeline.

---

### Step 1 — Read `.env` first

Before touching any code, read the config file. Every variable in there is a knob the system responds to. When you later see `os.getenv("VOD_TOPIC")` in code, you'll already know what it means.

Pay attention to: topic names, bucket names, interval timings, port numbers. These are the vocabulary of the whole project.

---

### Step 2 — Read `docker-compose.yml` as an architecture diagram

Don't read it as config syntax. Read it as answers to these questions per service:

- What image does this run?
- What ports does it expose?
- What environment variables does it receive?
- Which other services does it depend on?

After this, you should be able to draw the network on paper: which containers can talk to which, and on what port.

---

### Step 3 — Read `producer/producer.py` while watching it run

This is the entry point of all data. Start the logs in one terminal:

```bash
./run.sh logs producer
```

Then open the file and read it alongside the logs. It has two threads — find where each thread starts (`vod_producer_thread`, `live_producer_thread`) and read them separately. Focus on:

- What JSON payload does the VOD thread send to FastAPI?
- What JSON payload does the live thread publish directly to Kafka?
- Where are the Prometheus counters incremented?

Open **Redpanda Console** at `localhost:8080` → Topics → `live-chunks` → click a message. That JSON is exactly what `live_producer_thread` builds. Find the line in the code that constructs it.

---

### Step 4 — Read `api/main.py` while watching FastAPI handle a request

Open `localhost:8000/docs` in your browser. Find the `POST /vod/upload` endpoint and submit a request manually from the Swagger UI.

Now find that endpoint in the code and trace exactly what it does:

1. Generates a `stream_id`
2. Writes a placeholder to MinIO
3. Inserts a document into MongoDB
4. Publishes a JSON event to Kafka
5. Returns the `stream_id`

Then open **Mongo Express** at `localhost:8081` → database `pipeline` → `vod_metadata`. Your document is there with `status: uploaded` — that's step 3 you just read.

The question to ask yourself after reading this file: **why does the live path skip all of this?** The answer is in [Section 5](#5-why-live-chunks-bypass-fastapi).

---

### Step 5 — Read `spark_job/spark_streaming.py` in three passes

This is the most complex file. Don't read it once — read it three times with different goals.

**First pass — read only the metric definitions (lines ~74–104)**

These ~8 lines define everything that shows up in Grafana. After this pass you'll understand where every dashboard chart originates.

**Second pass — read `process_vod_chunk()` only**

This function receives one dict (the Kafka message you read in Step 3), then:

1. Verifies the checksum
2. Simulates a transcode with `time.sleep()`
3. Writes raw + variant placeholders to MinIO
4. Builds an HLS manifest string and writes it to MinIO
5. Upserts the MongoDB document through all status transitions

While reading this, have **MinIO Console** open at `localhost:9001` and watch a new VOD folder appear.

**Third pass — read `process_live_chunk()` only**

Similar to VOD but adds two things that don't exist in VOD:

- **Gap detection** — compares `sequence_number` to `_live_last_seq`. Find where `live_chunk_gaps` is incremented.
- **DVR window management** — find where `DVR_WINDOW_SIZE` is used. The manifest keeps only the last N entries and updates `#EXT-X-MEDIA-SEQUENCE` so the player knows where the window starts.

After both passes, find the bottom of the file where the Spark session is created and the two streaming queries are started. This is the `main` that wires everything together.

---

### Step 6 — Run `test_video.sh` and trace the journey yourself

```bash
./test_video.sh test_videos/your_file.mp4 --watch
```

Take the `stream_id` it prints and manually find it in every system:

| Where | What to look for |
|---|---|
| Redpanda Console | Filter messages by your `stream_id` in `vod-chunks` |
| Spark logs | `./run.sh logs spark-job` — find your `stream_id` being processed |
| Mongo Express | Filter `{"stream_id": "vod-xxxx"}` — watch `status` change in real time |
| MinIO Console | Find the folder `vod-xxxx/` in each bucket |
| Grafana | Watch the VOD latency panel spike when Spark processes your upload |

This is the most valuable exercise. You're not reading — you're **observing the code run**.

---

### Reading Order Summary

```
1. .env                        → vocabulary of the whole project
2. docker-compose.yml          → architecture and service wiring
3. producer/producer.py        → what data enters the system and in what shape
4. api/main.py                 → VOD write path (3 writes per upload)
5. spark_streaming.py — pass 1 → where Grafana metrics come from
   spark_streaming.py — pass 2 → VOD processing (follow one event end-to-end)
   spark_streaming.py — pass 3 → live processing (gaps + DVR window)
6. test_video.sh (live run)    → observe everything you just read, in motion
```

The whole thing takes 3–4 focused hours. The most important thing: **run the system while reading** — don't read the code dry.

---

### Three Concepts Worth Looking Up

If any part of the code is unclear, the answer almost always comes from understanding one of these:

**Kafka fundamentals** — understand producer, consumer, topic, partition, offset, and consumer group. Once you understand offsets, the consumer lag metric in Grafana becomes completely obvious. The Kafka documentation's [Introduction](https://kafka.apache.org/documentation/#gettingStarted) covers this in one page.

**HLS (HTTP Live Streaming)** — understand what an `.m3u8` file is and why it references `.ts` segment files. Open one of the manifests in MinIO Console (`manifests/` bucket → any `.m3u8` → Preview) and read it. Once you understand the format, the entire manifest-building code in `spark_streaming.py` is trivial to follow.

**Spark `foreachBatch`** — Spark Structured Streaming normally processes data using distributed DataFrame operations. `foreachBatch` breaks out of that model and gives you each micro-batch as a plain Python function. This is why the code can use `boto3` and `pymongo` directly without needing Spark connectors. The official [foreachBatch documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#foreachbatch) explains this in one page.
