"""
Ingest Producer
================
Simulates two simultaneous real-world ingest sources:

  Thread 1 — VOD uploader client:
    Calls POST /vod/upload on FastAPI every 10-20 seconds.
    FastAPI validates the request, writes to MinIO, inserts into MongoDB,
    and publishes the Kafka event. This is the VOD write path.

  Thread 2 — RTMP ingest server:
    Pushes live chunk metadata DIRECTLY to Kafka every 0.5-1 second.
    No HTTP call to FastAPI per chunk; only POST /streams/start (once) and
    POST /streams/end (once) use the API. This mirrors real RTMP encoder
    output where per-chunk latency must be sub-second.

Why live chunks bypass FastAPI:
  In live streaming, the ingest server (NGINX-RTMP, Wowza, FFmpeg) produces
  segments at ~2-4s intervals. An HTTP round-trip per chunk at that rate
  adds unnecessary latency and creates a single point of failure. Instead,
  the ingest server publishes metadata directly to Kafka, and Spark consumers
  process asynchronously. FastAPI only handles the control plane (start/end).

Exposes Prometheus /metrics on port 8765.
"""

import os
import json
import uuid
import hashlib
import random
import time
import threading
import logging
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_client import Counter, Gauge, start_http_server

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [PRODUCER] %(message)s"
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration from environment variables
# ---------------------------------------------------------------------------
KAFKA_BROKER             = os.getenv("KAFKA_BROKER", "kafka:9092")
VOD_TOPIC                = os.getenv("VOD_TOPIC", "vod-chunks")
LIVE_TOPIC               = os.getenv("LIVE_TOPIC", "live-chunks")
API_URL                  = os.getenv("API_URL", "http://api:8000")
VOD_INTERVAL_MIN         = float(os.getenv("VOD_INTERVAL_MIN", "10"))
VOD_INTERVAL_MAX         = float(os.getenv("VOD_INTERVAL_MAX", "20"))
LIVE_INTERVAL_MIN        = float(os.getenv("LIVE_INTERVAL_MIN", "0.5"))
LIVE_INTERVAL_MAX        = float(os.getenv("LIVE_INTERVAL_MAX", "1.0"))
VOD_MIN_SIZE_BYTES       = int(os.getenv("VOD_MIN_SIZE_BYTES", "5000000"))
VOD_MAX_SIZE_BYTES       = int(os.getenv("VOD_MAX_SIZE_BYTES", "20000000"))
LIVE_MIN_SIZE_BYTES      = int(os.getenv("LIVE_MIN_SIZE_BYTES", "500000"))
LIVE_MAX_SIZE_BYTES      = int(os.getenv("LIVE_MAX_SIZE_BYTES", "2000000"))
MATCH_HOME_TEAM          = os.getenv("MATCH_HOME_TEAM", "Al-Hilal")
MATCH_AWAY_TEAM          = os.getenv("MATCH_AWAY_TEAM", "Al-Nassr")
MATCH_COMPETITION        = os.getenv("MATCH_COMPETITION", "Saudi Pro League")
VOD_SHOW_NAME            = os.getenv("VOD_SHOW_NAME", "My Streaming Show")
METRICS_PORT             = int(os.getenv("METRICS_PORT", "8765"))

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
messages_produced = Counter(
    "producer_messages_total",
    "Total Kafka messages produced",
    ["stream_type", "topic"]
)
bytes_simulated = Counter(
    "producer_bytes_simulated_total",
    "Total simulated bytes represented by produced messages",
    ["stream_type"]
)
messages_per_second = Gauge(
    "producer_messages_per_second",
    "Approximate messages produced per second",
    ["stream_type"]
)
api_calls_total = Counter(
    "producer_api_calls_total",
    "Total FastAPI calls made by the producer",
    ["endpoint", "status"]
)
active_streams = Gauge(
    "producer_active_streams",
    "Number of active streams being simulated",
    ["stream_type"]
)

# ---------------------------------------------------------------------------
# Kafka producer — retry until Kafka is ready
# ---------------------------------------------------------------------------
def create_kafka_producer(retries: int = 20, delay: float = 5.0) -> KafkaProducer:
    """Blocks until a Kafka connection is established."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                # Serialise Python dicts to UTF-8 JSON bytes
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Kafka should only carry metadata — hard limit mirrors broker config
                max_request_size=1_048_576,
                acks="all",       # wait for leader + replicas to acknowledge
                retries=3,
            )
            log.info("Kafka producer connected (attempt %d)", attempt)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (attempt %d/%d) — retrying in %.0fs", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after %d attempts" % retries)


kafka_producer: KafkaProducer = None   # initialised in main()

# ---------------------------------------------------------------------------
# Helper — simulated checksum
# ---------------------------------------------------------------------------
def fake_md5(stream_id: str, chunk_index: int) -> str:
    """
    Simulates an MD5 checksum for a video chunk.
    In production this would be computed over the actual .ts segment bytes.
    Spark re-computes this and compares it to detect corruption.
    """
    return hashlib.md5(f"{stream_id}:{chunk_index}:{random.random()}".encode()).hexdigest()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Thread 1 — VOD producer
# ---------------------------------------------------------------------------
VOD_RESOLUTIONS = ["1920x1080", "1280x720", "1920x1080", "3840x2160"]
VOD_EPISODES = [
    "Episode 101: Season Preview",
    "Episode 102: Round of 16 Analysis",
    "Episode 103: Transfer Window Deep Dive",
    "Episode 104: Rules and Decisions Explained",
    "Episode 105: Youth Academy Spotlight",
]


def vod_producer_thread():
    """
    Simulates a VOD client calling POST /vod/upload every 10-20s.
    FastAPI then publishes the Kafka event, creating the asynchronous
    processing chain: FastAPI → Kafka → Spark → MinIO + MongoDB.

    Each VOD episode is treated as a single 'upload' trigger.
    The stream_id is reused for subsequent chunks of the same episode.
    """
    active_streams.labels(stream_type="vod").set(1)
    episode_idx = 0

    while True:
        episode_title = VOD_EPISODES[episode_idx % len(VOD_EPISODES)]
        stream_id = f"vod-{uuid.uuid4().hex[:10]}"
        duration = round(random.uniform(600, 3600), 1)   # 10 min to 1 hour
        resolution = random.choice(VOD_RESOLUTIONS)
        size_bytes = random.randint(VOD_MIN_SIZE_BYTES, VOD_MAX_SIZE_BYTES)

        payload = {
            "stream_id": stream_id,
            "title": f"{VOD_SHOW_NAME}: {episode_title}",
            "duration_seconds": duration,
            "resolution": resolution,
            "file_size_bytes": size_bytes,
            "audio_track_id": f"audio-{stream_id}",
        }

        try:
            log.info("[VOD] Uploading episode '%s' (stream_id=%s, %.0f MB)",
                     episode_title, stream_id, size_bytes / 1e6)
            resp = requests.post(
                f"{API_URL}/vod/upload",
                json=payload,
                timeout=10,
            )
            if resp.status_code == 200:
                api_calls_total.labels(endpoint="/vod/upload", status="success").inc()
                messages_produced.labels(stream_type="vod", topic=VOD_TOPIC).inc()
                bytes_simulated.labels(stream_type="vod").inc(size_bytes)
                log.info("[VOD] Accepted by API → Kafka event published")
            else:
                api_calls_total.labels(endpoint="/vod/upload", status="error").inc()
                log.warning("[VOD] API returned %d: %s", resp.status_code, resp.text)
        except requests.exceptions.RequestException as e:
            api_calls_total.labels(endpoint="/vod/upload", status="error").inc()
            log.error("[VOD] API call failed: %s", e)

        episode_idx += 1
        sleep = random.uniform(VOD_INTERVAL_MIN, VOD_INTERVAL_MAX)
        log.info("[VOD] Next upload in %.1fs", sleep)
        time.sleep(sleep)


# ---------------------------------------------------------------------------
# Thread 2 — Live RTMP ingest simulator
# ---------------------------------------------------------------------------

def live_producer_thread():
    """
    Simulates an RTMP ingest server (e.g., NGINX-RTMP or FFmpeg) that:
      1. Calls POST /streams/start ONCE (pre-stream API call)
      2. Pushes chunk metadata DIRECTLY to Kafka every 0.5-1s (no API)
      3. Calls POST /streams/end ONCE (post-stream API call)

    Direct Kafka publishing models the real broadcast pipeline where
    latency per chunk must be under 100ms — an HTTP call would add ~10-50ms
    per chunk and create a bottleneck at peak throughput.

    Each chunk event represents one HLS segment sliced at a keyframe boundary.
    """
    match_number = 0

    while True:
        match_number += 1
        stream_id = f"live-{uuid.uuid4().hex[:10]}"
        video_track_id = f"video-{stream_id}"
        audio_track_id = f"audio-{stream_id}"

        # --- STEP 1: Call FastAPI to start the stream (once) ---
        log.info("[LIVE] Starting stream #%d: %s vs %s (stream_id=%s)",
                 match_number, MATCH_HOME_TEAM, MATCH_AWAY_TEAM, stream_id)
        try:
            resp = requests.post(
                f"{API_URL}/streams/start",
                json={
                    "match_id": stream_id,
                    "home_team": MATCH_HOME_TEAM,
                    "away_team": MATCH_AWAY_TEAM,
                    "competition": MATCH_COMPETITION,
                },
                timeout=10,
            )
            if resp.status_code == 200:
                api_calls_total.labels(endpoint="/streams/start", status="success").inc()
                log.info("[LIVE] Stream started via API")
            else:
                api_calls_total.labels(endpoint="/streams/start", status="error").inc()
                log.warning("[LIVE] /streams/start returned %d", resp.status_code)
        except requests.exceptions.RequestException as e:
            api_calls_total.labels(endpoint="/streams/start", status="error").inc()
            log.error("[LIVE] /streams/start failed: %s", e)

        active_streams.labels(stream_type="live").set(1)

        # --- STEP 2: Push chunks directly to Kafka (the hot path) ---
        # Simulate 90 minutes of live at one chunk every 0.5-1s
        # At 0.75s avg interval and 3s per chunk, that's ~7200 chunks for 90min
        # For demo purposes we run continuously until a new stream starts

        chunk_index = 0
        sequence_number = 0
        # Simulate occasional chunk gaps (for checksum/gap detection demo)
        inject_gap_at = random.randint(50, 200)   # inject one gap in this range

        # Run for a simulated stream duration (300 chunks ≈ 5min demo, adjust as needed)
        # Set to a high number for continuous demo
        CHUNKS_PER_STREAM = 1000   # ~10-16 min of simulated real-time at 1s interval

        for _ in range(CHUNKS_PER_STREAM):
            # Occasionally skip a sequence number to trigger gap detection in Spark
            if sequence_number == inject_gap_at:
                skipped = random.randint(1, 3)
                log.warning("[LIVE] Simulating chunk gap: skipping sequence %d→%d",
                            sequence_number, sequence_number + skipped)
                sequence_number += skipped   # gap injected

            chunk_duration_ms = random.randint(2000, 4000)   # 2-4s per HLS segment
            size_bytes = random.randint(LIVE_MIN_SIZE_BYTES, LIVE_MAX_SIZE_BYTES)
            checksum = fake_md5(stream_id, chunk_index)

            # This is the live chunk event — it goes DIRECTLY to Kafka.
            # FastAPI is not called. This is the key architectural difference.
            event = {
                "stream_id": stream_id,
                "chunk_index": chunk_index,
                "sequence_number": sequence_number,
                "timestamp": now_iso(),
                "size_bytes": size_bytes,
                "stream_type": "live",
                "status": "received",
                "checksum": checksum,
                # HLS segment duration — must align with keyframes for seamless playback
                "duration_ms": chunk_duration_ms,
                # Every chunk starts at a keyframe (IDR frame) for correct seeking
                "keyframe_aligned": True,
                "audio_track_id": audio_track_id,
                "video_track_id": video_track_id,
                "match_home": MATCH_HOME_TEAM,
                "match_away": MATCH_AWAY_TEAM,
                "competition": MATCH_COMPETITION,
            }

            try:
                kafka_producer.send(LIVE_TOPIC, value=event)
                messages_produced.labels(stream_type="live", topic=LIVE_TOPIC).inc()
                bytes_simulated.labels(stream_type="live").inc(size_bytes)

                if chunk_index % 20 == 0:
                    log.info("[LIVE] stream=%s chunk=%d seq=%d size=%.1fKB",
                             stream_id, chunk_index, sequence_number, size_bytes / 1024)
            except Exception as e:
                log.error("[LIVE] Kafka send failed for chunk %d: %s", chunk_index, e)

            chunk_index += 1
            sequence_number += 1

            # Flush periodically to avoid buffering too many messages
            if chunk_index % 50 == 0:
                kafka_producer.flush()

            interval = random.uniform(LIVE_INTERVAL_MIN, LIVE_INTERVAL_MAX)
            time.sleep(interval)

        # Final flush
        kafka_producer.flush()

        # --- STEP 3: Call FastAPI to end the stream (once) ---
        log.info("[LIVE] Stream #%d ended — sending /streams/end", match_number)
        try:
            resp = requests.post(
                f"{API_URL}/streams/end",
                json={"stream_id": stream_id},
                timeout=10,
            )
            if resp.status_code == 200:
                api_calls_total.labels(endpoint="/streams/end", status="success").inc()
                log.info("[LIVE] Stream %s finalized (status=vod)", stream_id)
            else:
                api_calls_total.labels(endpoint="/streams/end", status="error").inc()
        except requests.exceptions.RequestException as e:
            api_calls_total.labels(endpoint="/streams/end", status="error").inc()
            log.error("[LIVE] /streams/end failed: %s", e)

        active_streams.labels(stream_type="live").set(0)

        # Brief pause between streams
        pause = random.uniform(5, 15)
        log.info("[LIVE] Next stream starts in %.0fs", pause)
        time.sleep(pause)


# ---------------------------------------------------------------------------
# Metrics rate calculator — updates Gauge every second
# ---------------------------------------------------------------------------
def metrics_rate_updater():
    """
    Samples the Counter values every second and updates the Gauge
    for messages/sec. This gives Grafana a real-time rate gauge.
    """
    prev_vod  = 0.0
    prev_live = 0.0
    while True:
        time.sleep(1)
        cur_vod  = messages_produced.labels(stream_type="vod",  topic=VOD_TOPIC)._value.get()
        cur_live = messages_produced.labels(stream_type="live", topic=LIVE_TOPIC)._value.get()
        messages_per_second.labels(stream_type="vod").set(cur_vod - prev_vod)
        messages_per_second.labels(stream_type="live").set(cur_live - prev_live)
        prev_vod  = cur_vod
        prev_live = cur_live


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def wait_for_api(retries: int = 30, delay: float = 5.0):
    """Block until the FastAPI service is reachable."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(f"{API_URL}/health", timeout=3)
            if r.status_code == 200:
                log.info("FastAPI ready (attempt %d)", attempt)
                return
        except requests.exceptions.RequestException:
            pass
        log.warning("FastAPI not ready (attempt %d/%d) — retrying in %.0fs", attempt, retries, delay)
        time.sleep(delay)
    raise RuntimeError("FastAPI did not become ready")


if __name__ == "__main__":
    # Start Prometheus metrics server
    start_http_server(METRICS_PORT)
    log.info("Prometheus metrics server started on :%d", METRICS_PORT)

    # Wait for dependencies
    wait_for_api()
    kafka_producer = create_kafka_producer()

    # Launch threads
    threads = [
        threading.Thread(target=vod_producer_thread,     name="vod-producer",     daemon=True),
        threading.Thread(target=live_producer_thread,    name="live-producer",    daemon=True),
        threading.Thread(target=metrics_rate_updater,    name="metrics-updater",  daemon=True),
    ]
    for t in threads:
        log.info("Starting thread: %s", t.name)
        t.start()

    log.info("All producer threads running. Ctrl+C to stop.")
    # Keep main thread alive — daemon threads stop when main exits
    for t in threads:
        t.join()
