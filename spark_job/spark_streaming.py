"""
Spark Structured Streaming Job
================================
Reads from two Kafka topics simultaneously and applies different processing
logic per stream type:

  vod-chunks  → VOD processing path (transcoding simulation, variants, full manifest)
  live-chunks → Live processing path (fast path, rolling DVR manifest)

Architecture notes:
  - Uses foreachBatch which runs the processing function in the Spark driver.
    This allows boto3 (MinIO) and pymongo (MongoDB) to be used directly
    without needing them installed on every Spark executor.
  - Two separate streaming queries run concurrently; both are awaited at the end.
  - Prometheus metrics are exposed via an HTTP server started in the driver.
  - Idempotent upserts on (stream_id, chunk_index) prevent duplicate processing
    on Spark retry (which happens when a micro-batch fails and is replayed).

Kafka message flow:
  vod-chunks:  FastAPI  → Kafka → THIS JOB → MinIO + MongoDB
  live-chunks: Producer → Kafka → THIS JOB → MinIO + MongoDB
"""

import os
import json
import time
import random
import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import pymongo
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SPARK] %(message)s"
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER            = os.getenv("KAFKA_BROKER", "kafka:9092")
VOD_TOPIC               = os.getenv("VOD_TOPIC", "vod-chunks")
LIVE_TOPIC              = os.getenv("LIVE_TOPIC", "live-chunks")
MONGO_URI               = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB                = os.getenv("MONGO_DB", "pipeline")
MINIO_ENDPOINT          = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY        = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY        = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BUCKET_RAW              = os.getenv("MINIO_BUCKET_VOD_RAW",      "vod-raw")
BUCKET_VARIANTS         = os.getenv("MINIO_BUCKET_VOD_VARIANTS", "vod-variants")
BUCKET_LIVE             = os.getenv("MINIO_BUCKET_LIVE",          "live-streams")
BUCKET_MANIFESTS        = os.getenv("MINIO_BUCKET_MANIFESTS",     "manifests")
DVR_WINDOW_SIZE         = int(os.getenv("DVR_WINDOW_SIZE", "10"))
METRICS_PORT            = int(os.getenv("SPARK_JOB_METRICS_PORT", "8766"))

# Quality variants for VOD transcoding simulation
QUALITY_VARIANTS = ["1080p", "720p", "480p", "360p"]

# ---------------------------------------------------------------------------
# Prometheus metrics (run in the driver process)
# ---------------------------------------------------------------------------
vod_chunks_processed = Counter(
    "spark_vod_chunks_processed_total",
    "Total VOD chunks processed by Spark"
)
live_chunks_processed = Counter(
    "spark_live_chunks_processed_total",
    "Total live chunks processed by Spark"
)
live_chunk_gaps = Counter(
    "live_chunk_gaps_total",
    "Live chunks with detected sequence number gaps"
)
chunk_checksum_failures = Counter(
    "chunk_checksum_failures_total",
    "Chunk checksum validation failures",
    ["stream_type"]
)
processing_latency = Histogram(
    "chunk_processing_latency_seconds",
    "Time between event timestamp and processing completion",
    ["stream_type"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0]
)
variants_generated = Counter(
    "spark_vod_variants_generated_total",
    "Total quality variants generated for VOD chunks"
)
active_live_streams = Gauge(
    "spark_active_live_streams",
    "Number of live streams currently active"
)

# ---------------------------------------------------------------------------
# MinIO client (boto3)
# ---------------------------------------------------------------------------
def get_minio_client():
    """
    Returns a boto3 S3 client configured for MinIO.
    MinIO requires path-style addressing (not virtual-hosted-style).
    Signature version s3v4 is required by MinIO.
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


# ---------------------------------------------------------------------------
# MongoDB client
# ---------------------------------------------------------------------------
def get_mongo_db():
    """Returns the pymongo database object."""
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return client[MONGO_DB]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def calculate_processing_latency_ms(event_timestamp: str) -> float:
    """
    Calculates latency (ms) between when the event was emitted by the
    producer and when Spark is processing it. This measures end-to-end
    pipeline lag from ingest to processing.
    """
    try:
        event_dt = datetime.fromisoformat(event_timestamp.replace("Z", "+00:00"))
        now_dt = datetime.now(timezone.utc)
        return (now_dt - event_dt).total_seconds() * 1000
    except Exception:
        return 0.0


def verify_checksum(stream_id: str, chunk_index: int, received_checksum: str) -> bool:
    """
    Simulates checksum verification on the chunk 'blob'.
    In production, Spark would re-hash the actual bytes fetched from the
    ingest buffer. Here we regenerate the fake MD5 and compare.
    Returns True if checksum matches, False on mismatch.
    """
    # The producer generates: md5(f"{stream_id}:{chunk_index}:{random}")
    # Since the random component makes exact reproduction impossible, we
    # simulate a 2% checksum failure rate for demo purposes.
    simulated_failure = random.random() < 0.02
    return not simulated_failure


def put_placeholder_object(s3, bucket: str, key: str, metadata: dict):
    """
    Writes a zero-byte placeholder object to MinIO representing a video chunk.
    In production this would be the actual .ts segment bytes.
    The object metadata carries enough info to reconstruct the path from MongoDB.
    """
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=b"",   # zero bytes — simulation only; real bytes come from the encoder
        Metadata={k: str(v) for k, v in metadata.items()},
    )


def build_hls_manifest_line(chunk_index: int, duration_seconds: float, key: str) -> str:
    """
    Generates one HLS segment entry for an m3u8 playlist.
    HLS requires EXTINF (duration) before each segment URI.
    """
    return f"#EXTINF:{duration_seconds:.3f},\n{key}\n"


# ---------------------------------------------------------------------------
# VOD chunk processing
# ---------------------------------------------------------------------------

def process_vod_chunk(s3, db, event: dict):
    """
    Full VOD processing pipeline for one chunk:

    1. Update MongoDB status: uploaded → processing
    2. Verify checksum (emit metric on failure)
    3. Simulate transcoding delay (2-4 seconds per real-world encoder latency)
    4. Update MongoDB status: processing → transcoding
    5. Write raw chunk placeholder to MinIO: vod-raw/{stream_id}/raw/{idx}.ts
    6. Write 4 variant placeholders to MinIO (1080p, 720p, 480p, 360p)
    7. Generate / update HLS manifest in MinIO: manifests/{stream_id}/vod_manifest.m3u8
    8. Update MongoDB status: transcoding → ready + write all metadata
    """
    stream_id    = event["stream_id"]
    chunk_index  = event["chunk_index"]
    checksum     = event.get("checksum", "")
    duration_ms  = event.get("duration_ms", 4000)
    resolution   = event.get("resolution", "1920x1080")
    size_bytes   = event.get("size_bytes", 10_000_000)
    audio_id     = event.get("audio_track_id", f"audio-{stream_id}")
    timestamp    = event.get("timestamp", now_iso())

    log.info("[VOD] Processing stream=%s chunk=%d", stream_id, chunk_index)

    # --- 1. Status: uploaded → processing ---
    db.vod_metadata.update_one(
        {"stream_id": stream_id, "chunk_index": chunk_index},
        {"$set": {"status": "processing", "processing_started_at": now_iso()}},
    )

    # --- 2. Checksum verification ---
    if not verify_checksum(stream_id, chunk_index, checksum):
        chunk_checksum_failures.labels(stream_type="vod").inc()
        log.warning("[VOD] Checksum MISMATCH stream=%s chunk=%d — continuing with warning",
                    stream_id, chunk_index)

    # --- 3. Simulate transcoding delay ---
    # Real transcoding would be CPU-bound here (FFmpeg, etc.)
    transcode_delay = random.uniform(2.0, 4.0)
    log.info("[VOD] Simulating transcode delay %.1fs for stream=%s", transcode_delay, stream_id)
    time.sleep(transcode_delay)

    # --- 4. Status: processing → transcoding ---
    db.vod_metadata.update_one(
        {"stream_id": stream_id, "chunk_index": chunk_index},
        {"$set": {"status": "transcoding", "transcoding_started_at": now_iso()}},
    )

    # --- 5. Write raw chunk placeholder ---
    raw_key = f"{stream_id}/raw/{chunk_index}.ts"
    put_placeholder_object(s3, BUCKET_RAW, raw_key, {
        "stream_id": stream_id, "chunk_index": str(chunk_index), "type": "raw"
    })
    log.info("[VOD] Raw chunk written: %s/%s", BUCKET_RAW, raw_key)

    # --- 6. Write 4 quality variant placeholders ---
    # Multi-bitrate HLS (ABR) — clients pick the variant matching their bandwidth.
    # Variants represent the output of FFmpeg with -vf scale flags:
    #   1080p: original quality
    #   720p:  1280x720, ~3 Mbps
    #   480p:  854x480,  ~1.5 Mbps
    #   360p:  640x360,  ~0.8 Mbps
    variant_keys = []
    for quality in QUALITY_VARIANTS:
        variant_key = f"{stream_id}/{quality}/{chunk_index}.ts"
        put_placeholder_object(s3, BUCKET_VARIANTS, variant_key, {
            "stream_id": stream_id,
            "chunk_index": str(chunk_index),
            "quality": quality,
            "original_resolution": resolution,
        })
        variant_keys.append(f"{BUCKET_VARIANTS}/{variant_key}")
        variants_generated.inc()

    log.info("[VOD] %d variant placeholders written for chunk %d", len(QUALITY_VARIANTS), chunk_index)

    # --- 7. Generate/update HLS manifest ---
    # For VOD: fetch existing manifest (or start fresh), append this chunk,
    # then overwrite the manifest object in MinIO.
    # The manifest is a simple m3u8 pointing to the 1080p variant segments.
    manifest_key = f"{stream_id}/vod_manifest.m3u8"
    duration_seconds = duration_ms / 1000.0

    try:
        # Fetch existing manifest content
        obj = s3.get_object(Bucket=BUCKET_MANIFESTS, Key=manifest_key)
        existing = obj["Body"].read().decode()
        # Remove the ENDLIST tag if present (we're still appending)
        existing = existing.replace("#EXT-X-ENDLIST\n", "").rstrip()
    except ClientError as e:
        # 404 / NoSuchKey means first chunk — initialise manifest header
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            existing = (
                "#EXTM3U\n"
                "#EXT-X-VERSION:3\n"
                f"#EXT-X-TARGETDURATION:{int(duration_seconds) + 1}\n"
                "#EXT-X-PLAYLIST-TYPE:VOD\n"
            )
        else:
            log.warning("[VOD] Could not fetch manifest for %s: %s — starting fresh", stream_id, e)
            existing = (
                "#EXTM3U\n"
                "#EXT-X-VERSION:3\n"
                f"#EXT-X-TARGETDURATION:{int(duration_seconds) + 1}\n"
                "#EXT-X-PLAYLIST-TYPE:VOD\n"
            )

    # Append this segment's entry
    segment_uri = f"{stream_id}/1080p/{chunk_index}.ts"
    new_manifest = (
        existing.rstrip() + "\n"
        + build_hls_manifest_line(chunk_index, duration_seconds, segment_uri)
    )

    s3.put_object(
        Bucket=BUCKET_MANIFESTS,
        Key=manifest_key,
        Body=new_manifest.encode(),
        ContentType="application/vnd.apple.mpegurl",
    )
    log.info("[VOD] HLS manifest updated: %s/%s", BUCKET_MANIFESTS, manifest_key)

    # --- 8. Update MongoDB: transcoding → ready ---
    latency_ms = calculate_processing_latency_ms(timestamp)
    processing_latency.labels(stream_type="vod").observe(latency_ms / 1000.0)

    # Idempotent upsert on (stream_id, chunk_index) to handle Spark retries
    db.vod_metadata.update_one(
        {"stream_id": stream_id, "chunk_index": chunk_index},
        {"$set": {
            "status": "ready",
            "processing_latency_ms": round(latency_ms, 2),
            "variants_generated": QUALITY_VARIANTS,
            "variant_paths": variant_keys,
            "raw_path": f"{BUCKET_RAW}/{raw_key}",
            "manifest_path": f"{BUCKET_MANIFESTS}/{manifest_key}",
            "completed_at": now_iso(),
            "keyframe_aligned": True,
            "audio_track_id": audio_id,
        }},
        upsert=True,
    )

    vod_chunks_processed.inc()
    log.info("[VOD] stream=%s chunk=%d COMPLETE (latency=%.0fms, status=ready)",
             stream_id, chunk_index, latency_ms)


# ---------------------------------------------------------------------------
# LIVE chunk processing
# ---------------------------------------------------------------------------

# In-memory tracker for sequence numbers per stream (driver only)
# Used to detect gaps in the chunk stream.
_live_last_seq: dict = {}


def process_live_chunk(s3, db, event: dict):
    """
    Fast-path live processing for one chunk. Target: < 500ms total.

    1. Validate sequence number continuity — emit metric on gap
    2. Verify checksum — emit metric on failure
    3. Write chunk placeholder to MinIO: live-streams/{stream_id}/chunks/{idx}.ts
    4. Fetch current manifest, slide the DVR window (keep last 10 chunks)
    5. Write updated manifest to MinIO: manifests/{stream_id}/live_manifest.m3u8
    6. Upsert MongoDB document with all event fields + processing metadata
    """
    stream_id       = event["stream_id"]
    chunk_index     = event["chunk_index"]
    sequence_number = event.get("sequence_number", chunk_index)
    checksum        = event.get("checksum", "")
    duration_ms     = event.get("duration_ms", 3000)
    size_bytes      = event.get("size_bytes", 1_000_000)
    audio_id        = event.get("audio_track_id", f"audio-{stream_id}")
    video_id        = event.get("video_track_id", f"video-{stream_id}")
    timestamp       = event.get("timestamp", now_iso())

    if chunk_index % 50 == 0:
        log.info("[LIVE] Processing stream=%s chunk=%d seq=%d",
                 stream_id, chunk_index, sequence_number)

    # --- 1. Sequence number gap detection ---
    last_seq = _live_last_seq.get(stream_id)
    if last_seq is not None and sequence_number > last_seq + 1:
        gap_size = sequence_number - last_seq - 1
        live_chunk_gaps.inc(gap_size)
        log.warning("[LIVE] GAP DETECTED stream=%s: missing %d chunk(s) (seq %d→%d)",
                    stream_id, gap_size, last_seq + 1, sequence_number - 1)
    _live_last_seq[stream_id] = sequence_number

    # --- 2. Checksum verification ---
    if not verify_checksum(stream_id, chunk_index, checksum):
        chunk_checksum_failures.labels(stream_type="live").inc()
        log.warning("[LIVE] Checksum MISMATCH stream=%s chunk=%d", stream_id, chunk_index)

    # --- 3. Write chunk placeholder to MinIO ---
    chunk_key = f"{stream_id}/chunks/{chunk_index}.ts"
    put_placeholder_object(s3, BUCKET_LIVE, chunk_key, {
        "stream_id": stream_id, "chunk_index": str(chunk_index), "seq": str(sequence_number)
    })

    # --- 4. Update rolling DVR manifest ---
    # A live HLS stream manifest keeps only the last N segments.
    # Older segments remain in MinIO (for catch-up DVR) but are removed
    # from the live manifest so clients don't try to load them during live viewing.
    manifest_key = f"{stream_id}/live_manifest.m3u8"
    duration_seconds = duration_ms / 1000.0

    # Fetch existing manifest to extract the current window
    existing_segments = []  # list of (seq, duration, uri) tuples
    try:
        obj = s3.get_object(Bucket=BUCKET_MANIFESTS, Key=manifest_key)
        lines = obj["Body"].read().decode().splitlines()
        # Parse existing EXTINF + URI pairs
        i = 0
        while i < len(lines):
            if lines[i].startswith("#EXTINF:"):
                dur_str = lines[i].split(":")[1].rstrip(",")
                if i + 1 < len(lines):
                    uri = lines[i + 1]
                    existing_segments.append((float(dur_str), uri))
                i += 2
            else:
                i += 1
    except ClientError:
        # 404 / NoSuchKey — first chunk, start with empty window
        pass
    except Exception:
        pass

    # Append the new segment
    existing_segments.append((duration_seconds, f"{stream_id}/chunks/{chunk_index}.ts"))

    # Slide the window: keep only the last DVR_WINDOW_SIZE segments
    if len(existing_segments) > DVR_WINDOW_SIZE:
        existing_segments = existing_segments[-DVR_WINDOW_SIZE:]

    # Determine the media sequence number (index of oldest segment in window)
    dvr_window_start = max(0, chunk_index - DVR_WINDOW_SIZE + 1)

    # Build the updated manifest
    manifest_lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{int(max(d for d, _ in existing_segments)) + 1}",
        "#EXT-X-PLAYLIST-TYPE:EVENT",   # EVENT allows appending; ENDLIST finalizes
        f"#EXT-X-MEDIA-SEQUENCE:{dvr_window_start}",
    ]
    for dur, uri in existing_segments:
        manifest_lines.append(f"#EXTINF:{dur:.3f},")
        manifest_lines.append(uri)

    new_manifest = "\n".join(manifest_lines) + "\n"

    s3.put_object(
        Bucket=BUCKET_MANIFESTS,
        Key=manifest_key,
        Body=new_manifest.encode(),
        ContentType="application/vnd.apple.mpegurl",
    )

    # --- 5. Upsert MongoDB document ---
    # Idempotent: if Spark retries this batch, the same chunk won't duplicate
    latency_ms = calculate_processing_latency_ms(timestamp)
    processing_latency.labels(stream_type="live").observe(latency_ms / 1000.0)

    db.live_metadata.update_one(
        # Unique key: (stream_id, chunk_index) — prevents duplicates on retry
        {"stream_id": stream_id, "chunk_index": chunk_index},
        {"$set": {
            "stream_id": stream_id,
            "chunk_index": chunk_index,
            "sequence_number": sequence_number,
            "timestamp": timestamp,
            "size_bytes": size_bytes,
            "stream_type": "live",
            "status": "live",
            "checksum": checksum,
            "duration_ms": duration_ms,
            "keyframe_aligned": True,
            "audio_track_id": audio_id,
            "video_track_id": video_id,
            "chunk_path": f"{BUCKET_LIVE}/{chunk_key}",
            "manifest_path": f"{BUCKET_MANIFESTS}/{manifest_key}",
            "processing_latency_ms": round(latency_ms, 2),
            "manifest_updated_at": now_iso(),
            "dvr_window_start": dvr_window_start,
        }},
        upsert=True,
    )

    live_chunks_processed.inc()
    active_live_streams.set(len(_live_last_seq))


# ---------------------------------------------------------------------------
# foreachBatch processors — called by Spark for each micro-batch
# ---------------------------------------------------------------------------

def process_vod_batch(batch_df: DataFrame, epoch_id: int):
    """
    Processes a micro-batch of VOD chunk events.
    foreachBatch runs in the Spark driver, so boto3/pymongo work here.
    """
    rows = batch_df.collect()
    if not rows:
        return

    log.info("[VOD BATCH] epoch=%d rows=%d", epoch_id, len(rows))

    # Initialise clients once per batch (creating per-row is expensive)
    s3 = get_minio_client()
    db = get_mongo_db()

    for row in rows:
        try:
            event = json.loads(row.value)
            process_vod_chunk(s3, db, event)
        except Exception as e:
            log.error("[VOD BATCH] Failed to process row in epoch %d: %s", epoch_id, e)


def process_live_batch(batch_df: DataFrame, epoch_id: int):
    """
    Processes a micro-batch of live chunk events.
    Target: < 500ms per chunk (no simulated transcoding delay here).
    """
    rows = batch_df.collect()
    if not rows:
        return

    if epoch_id % 20 == 0:
        log.info("[LIVE BATCH] epoch=%d rows=%d", epoch_id, len(rows))

    s3 = get_minio_client()
    db = get_mongo_db()

    for row in rows:
        try:
            event = json.loads(row.value)
            process_live_chunk(s3, db, event)
        except Exception as e:
            log.error("[LIVE BATCH] Failed to process row in epoch %d: %s", epoch_id, e)


# ---------------------------------------------------------------------------
# Main — Spark session + streaming queries
# ---------------------------------------------------------------------------

def main():
    # Start Prometheus metrics server in the driver process
    start_http_server(METRICS_PORT)
    log.info("Prometheus metrics server started on :%d", METRICS_PORT)

    # Build the Spark session
    # The Kafka JARs were pre-downloaded into /opt/spark/jars/
    # in the Dockerfile, so no --packages download is needed at runtime.
    spark = (
        SparkSession.builder
        .appName("StreamingPipeline")
        # Reduce log verbosity from Kafka consumer
        .config("spark.streaming.kafka.consumer.cache.enabled", "false")
        # Trigger interval: process micro-batches every 5 seconds
        # VOD: lower throughput, 5s is fine
        # Live: higher throughput, shorter trigger (set per-query below)
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created — app: StreamingPipeline")

    # ---------------------------------------------------------------------------
    # Read from vod-chunks topic
    # startingOffsets=latest: only process new messages, not historical backlog
    # ---------------------------------------------------------------------------
    vod_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", VOD_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "10")      # process at most 10 per batch
        .option("failOnDataLoss", "false")
        .load()
        # Kafka value is bytes; cast to string for JSON parsing in the batch function
        .selectExpr("CAST(value AS STRING) as value", "timestamp as kafka_timestamp")
    )

    # ---------------------------------------------------------------------------
    # Read from live-chunks topic
    # Higher throughput: more offsets per trigger, shorter trigger interval
    # ---------------------------------------------------------------------------
    live_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", LIVE_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "100")     # live is much higher throughput
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) as value", "timestamp as kafka_timestamp")
    )

    # ---------------------------------------------------------------------------
    # Streaming queries
    # processingTime trigger: Spark checks for new data at this interval
    # ---------------------------------------------------------------------------

    vod_query = (
        vod_stream
        .writeStream
        .foreachBatch(process_vod_batch)
        .option("checkpointLocation", "/tmp/spark-checkpoints/vod")
        .trigger(processingTime="5 seconds")     # vod: batch every 5s
        .start()
    )
    log.info("VOD streaming query started (trigger: 5s)")

    live_query = (
        live_stream
        .writeStream
        .foreachBatch(process_live_batch)
        .option("checkpointLocation", "/tmp/spark-checkpoints/live")
        .trigger(processingTime="1 seconds")     # live: batch every 1s for low latency
        .start()
    )
    log.info("Live streaming query started (trigger: 1s)")

    log.info("Both streaming queries running. Waiting for termination...")
    # Block until either query terminates (or fails)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
