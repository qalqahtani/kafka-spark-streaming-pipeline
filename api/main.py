"""
Video Streaming Pipeline API
=============================
FastAPI service for VOD upload lifecycle and live stream lifecycle.

IMPORTANT ARCHITECTURE NOTE:
  FastAPI is NOT involved in the live chunk ingest path.
  Live chunks flow: RTMP ingest server → Kafka directly (bypassing this API).
  FastAPI is only called:
    - POST /vod/upload         (every 10-20s, triggers Kafka event for Spark)
    - POST /streams/start      (once, before stream kick-off)
    - POST /streams/end        (once, after stream ends)

  This design mirrors real RTMP architectures where the ingest server
  (e.g., NGINX-RTMP, Wowza) pushes segments directly to a message bus
  without an HTTP round-trip per chunk — critical for sub-second latency.
"""

import os
import uuid
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pymongo
from kafka import KafkaProducer
import boto3
from botocore.client import Config
from prometheus_client import Counter, Histogram, make_asgi_app
from starlette.routing import Mount

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [API] %(message)s"
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER        = os.getenv("KAFKA_BROKER", "kafka:9092")
VOD_TOPIC           = os.getenv("VOD_TOPIC", "vod-chunks")
MONGO_URI           = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB            = os.getenv("MONGO_DB", "pipeline")
MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
# MINIO_PUBLIC_ENDPOINT is used for presigned URL generation so the URLs are
# resolvable from outside Docker (e.g. the host browser / VLC / ffplay).
# Defaults to localhost:9000 for local dev; set to your CDN domain in production.
MINIO_PUBLIC_ENDPOINT = os.getenv("MINIO_PUBLIC_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY    = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BUCKET_RAW          = os.getenv("MINIO_BUCKET_VOD_RAW", "vod-raw")
BUCKET_MANIFESTS    = os.getenv("MINIO_BUCKET_MANIFESTS", "manifests")

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
api_requests_total = Counter(
    "api_requests_total",
    "Total API requests",
    ["endpoint", "method", "status"]
)
api_latency = Histogram(
    "api_request_duration_seconds",
    "API request duration",
    ["endpoint"]
)
kafka_events_published = Counter(
    "api_kafka_events_published_total",
    "Kafka events published from API",
    ["topic"]
)

# ---------------------------------------------------------------------------
# FastAPI app — Prometheus /metrics mounted as ASGI sub-app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Streaming Pipeline API",
    description=(
        "REST API for VOD upload and live stream lifecycle. "
        "Live chunk ingest bypasses this API and goes directly to Kafka."
    ),
    version="1.0.0",
)

# Mount Prometheus metrics at /metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# ---------------------------------------------------------------------------
# Startup: connect to Kafka, MongoDB, MinIO
# ---------------------------------------------------------------------------
kafka_producer: Optional[KafkaProducer] = None
mongo_client: Optional[pymongo.MongoClient] = None
s3_client = None         # internal ops (put_object, get_object) via minio:9000
s3_public_client = None  # presigned URL generation via localhost:9000


@app.on_event("startup")
async def startup_event():
    global kafka_producer, mongo_client, s3_client, s3_public_client

    # Kafka producer — serialises Python dicts to JSON bytes
    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_request_size=1_048_576,   # 1 MB — metadata only, never video bytes
        retries=3,
        acks="all",
    )
    log.info("Kafka producer connected to %s", KAFKA_BROKER)

    # MongoDB client
    mongo_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client.server_info()   # raises if unreachable
    log.info("MongoDB connected at %s", MONGO_URI)

    # Internal MinIO client — used for put_object / get_object inside Docker
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    log.info("MinIO internal client connected at %s", MINIO_ENDPOINT)

    # Public MinIO client — used only for presigned URL generation.
    # The generated URLs embed the endpoint hostname, so they must use
    # a hostname reachable from outside Docker (localhost:9000 for local dev).
    s3_public_client = boto3.client(
        "s3",
        endpoint_url=MINIO_PUBLIC_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    log.info("MinIO public client configured at %s (for presigned URLs)", MINIO_PUBLIC_ENDPOINT)


@app.on_event("shutdown")
async def shutdown_event():
    if kafka_producer:
        kafka_producer.flush()
        kafka_producer.close()
    if mongo_client:
        mongo_client.close()


# ---------------------------------------------------------------------------
# Request/response models
# ---------------------------------------------------------------------------

class VODUploadRequest(BaseModel):
    """
    Simulates the metadata a client sends when uploading a VOD episode.
    In a real system this would be a multipart file upload; here we carry
    metadata only (matching our Kafka-carries-metadata-only principle).
    """
    title: str                          # episode title
    duration_seconds: float             # total episode duration
    resolution: str = "1920x1080"       # source resolution
    file_size_bytes: int                # simulated raw file size
    audio_track_id: Optional[str] = None  # optional pre-set track id
    stream_id: Optional[str] = None     # optional pre-set id (for test_video.sh)


class StreamStartRequest(BaseModel):
    """Body for POST /streams/start — called once before the live stream begins."""
    match_id: Optional[str] = None   # optional external match identifier
    home_team: str
    away_team: str
    competition: str


class StreamEndRequest(BaseModel):
    """Body for POST /streams/end — called once after the live stream ends."""
    stream_id: str


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health", tags=["System"])
async def health():
    """Used by Docker healthcheck and load balancers."""
    return {"status": "ok", "service": "pipeline-api"}


# ---------------------------------------------------------------------------
# VOD endpoints
# ---------------------------------------------------------------------------

@app.post("/vod/upload", tags=["VOD"])
async def upload_vod(req: VODUploadRequest):
    """
    Simulates a VOD episode upload.

    Flow:
      1. Generate a unique stream_id
      2. Create a placeholder 'raw file' reference in MinIO (vod-raw bucket)
      3. Insert an initial MongoDB document with status=uploaded
      4. Publish a Kafka event to vod-chunks topic (triggers Spark processing)
      5. Return the stream_id so the client can poll status

    The actual video bytes are never sent here; only metadata travels through
    the system. MinIO holds a zero-byte placeholder representing the raw file
    location that a real CDN origin would resolve.
    """
    with api_latency.labels(endpoint="/vod/upload").time():
        db = mongo_client[MONGO_DB]

        # 1. Unique stream identifier
        stream_id = req.stream_id or f"vod-{uuid.uuid4().hex[:12]}"
        chunk_index = 0   # VOD upload is treated as a single trigger event
        audio_track_id = req.audio_track_id or f"audio-{stream_id}"
        # Fake checksum — in production this would be the SHA-256 of the file
        checksum = hashlib.md5(f"{stream_id}-{chunk_index}-{req.file_size_bytes}".encode()).hexdigest()
        now = datetime.now(timezone.utc).isoformat()

        # 2. Write placeholder object to MinIO
        raw_object_key = f"{stream_id}/raw/{chunk_index}.ts"
        try:
            s3_client.put_object(
                Bucket=BUCKET_RAW,
                Key=raw_object_key,
                Body=b"",   # zero bytes — placeholder; real bytes come from the encoder
                Metadata={
                    "stream_id": stream_id,
                    "chunk_index": str(chunk_index),
                    "title": req.title,
                    "size_bytes": str(req.file_size_bytes),
                },
            )
            log.info("[%s] MinIO placeholder created: %s/%s", stream_id, BUCKET_RAW, raw_object_key)
        except Exception as e:
            log.error("[%s] MinIO write failed: %s", stream_id, e)
            raise HTTPException(status_code=500, detail=f"MinIO error: {e}")

        # 3. Insert MongoDB document with status=uploaded
        doc = {
            "stream_id": stream_id,
            "chunk_index": chunk_index,
            "title": req.title,
            "timestamp": now,
            "size_bytes": req.file_size_bytes,
            "stream_type": "vod",
            "status": "uploaded",   # will be updated by Spark: uploaded→processing→transcoding→ready
            "checksum": checksum,
            "duration_ms": int(req.duration_seconds * 1000),
            "resolution": req.resolution,
            "keyframe_aligned": True,   # always true for HLS-compatible segments
            "audio_track_id": audio_track_id,
            "raw_path": f"{BUCKET_RAW}/{raw_object_key}",
            "created_at": now,
        }
        try:
            db.vod_metadata.insert_one(doc)
            log.info("[%s] MongoDB document inserted (status=uploaded)", stream_id)
        except Exception as e:
            log.error("[%s] MongoDB insert failed: %s", stream_id, e)
            raise HTTPException(status_code=500, detail=f"MongoDB error: {e}")

        # 4. Publish Kafka event — this is what triggers Spark processing
        kafka_event = {
            "stream_id": stream_id,
            "chunk_index": chunk_index,
            "timestamp": now,
            "size_bytes": req.file_size_bytes,
            "stream_type": "vod",
            "status": "uploaded",
            "checksum": checksum,
            "duration_ms": int(req.duration_seconds * 1000),
            "resolution": req.resolution,
            "keyframe_aligned": True,
            "audio_track_id": audio_track_id,
            "title": req.title,
            # raw_path tells Spark where to find the source object in MinIO
            "raw_path": f"{BUCKET_RAW}/{raw_object_key}",
        }
        try:
            kafka_producer.send(VOD_TOPIC, value=kafka_event)
            kafka_producer.flush()
            kafka_events_published.labels(topic=VOD_TOPIC).inc()
            log.info("[%s] Kafka event published to %s", stream_id, VOD_TOPIC)
        except Exception as e:
            log.error("[%s] Kafka publish failed: %s", stream_id, e)
            raise HTTPException(status_code=500, detail=f"Kafka error: {e}")

        api_requests_total.labels(endpoint="/vod/upload", method="POST", status="200").inc()
        return {
            "stream_id": stream_id,
            "status": "uploaded",
            "message": "VOD upload accepted; Spark will process asynchronously",
            "kafka_topic": VOD_TOPIC,
        }


@app.get("/vod/{stream_id}/manifest", tags=["VOD"])
async def get_vod_manifest(stream_id: str):
    """
    Returns a presigned MinIO URL valid for 1 hour for the HLS manifest.
    The client (video player) uses this URL to fetch the .m3u8 playlist,
    which in turn references individual .ts segment URLs.
    """
    with api_latency.labels(endpoint="/vod/manifest").time():
        db = mongo_client[MONGO_DB]

        # Find the latest document for this stream (could have multiple chunks)
        doc = db.vod_metadata.find_one(
            {"stream_id": stream_id, "manifest_path": {"$exists": True}},
            sort=[("chunk_index", -1)],
        )
        if not doc or not doc.get("manifest_path"):
            raise HTTPException(status_code=404, detail="Manifest not yet available — processing may still be in progress")

        manifest_path = doc["manifest_path"]
        # manifest_path format: "manifests/{stream_id}/vod_manifest.m3u8"
        bucket, key = manifest_path.split("/", 1)

        try:
            url = s3_public_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=3600,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Could not generate presigned URL: {e}")

        api_requests_total.labels(endpoint="/vod/manifest", method="GET", status="200").inc()
        return {"stream_id": stream_id, "manifest_url": url, "expires_in_seconds": 3600}


@app.get("/vod/{stream_id}/metadata", tags=["VOD"])
async def get_vod_metadata(stream_id: str):
    """Returns the full MongoDB document for a VOD stream."""
    db = mongo_client[MONGO_DB]
    doc = db.vod_metadata.find_one({"stream_id": stream_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Stream not found")
    api_requests_total.labels(endpoint="/vod/metadata", method="GET", status="200").inc()
    return doc


# ---------------------------------------------------------------------------
# LIVE STREAM endpoints (lifecycle only — NOT chunk ingest)
# ---------------------------------------------------------------------------

@app.post("/streams/start", tags=["Live Stream"])
async def start_stream(req: StreamStartRequest):
    """
    Called ONCE before a live stream begins.

    Creates the MongoDB document and an empty manifest placeholder in MinIO.
    After this call, the RTMP ingest server takes over and pushes chunk events
    directly to Kafka — this endpoint is never called again until the stream ends.

    This separation models real broadcast architecture:
      Pre-stream setup → API (HTTP)
      Live chunk ingest → Kafka (binary protocol, no HTTP overhead)
    """
    with api_latency.labels(endpoint="/streams/start").time():
        db = mongo_client[MONGO_DB]

        stream_id = req.match_id or f"live-{uuid.uuid4().hex[:12]}"
        now = datetime.now(timezone.utc).isoformat()

        # Create empty manifest placeholder in MinIO so the URL is resolvable
        # before any chunks arrive (important for CDN pre-warming)
        manifest_key = f"{stream_id}/live_manifest.m3u8"
        empty_manifest = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n"
        try:
            s3_client.put_object(
                Bucket=BUCKET_MANIFESTS,
                Key=manifest_key,
                Body=empty_manifest.encode(),
                ContentType="application/vnd.apple.mpegurl",
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"MinIO error: {e}")

        # Insert MongoDB document
        doc = {
            "stream_id": stream_id,
            "match_id": req.match_id,
            "home_team": req.home_team,
            "away_team": req.away_team,
            "competition": req.competition,
            "stream_type": "live",
            "status": "live",
            "started_at": now,
            "manifest_path": f"{BUCKET_MANIFESTS}/{manifest_key}",
            "chunk_count": 0,
            "dvr_window_start": 0,
        }
        try:
            db.live_metadata.insert_one(doc)
            log.info("[%s] Live stream started: %s vs %s", stream_id, req.home_team, req.away_team)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"MongoDB error: {e}")

        api_requests_total.labels(endpoint="/streams/start", method="POST", status="200").inc()
        return {
            "stream_id": stream_id,
            "status": "live",
            "message": "Stream started. RTMP ingest server should now push chunks directly to Kafka.",
        }


@app.post("/streams/end", tags=["Live Stream"])
async def end_stream(req: StreamEndRequest):
    """
    Called ONCE after the live stream ends.
    Updates the MongoDB document status to 'vod' and appends EXT-X-ENDLIST
    to the HLS manifest, signalling players to stop polling for new segments.
    """
    with api_latency.labels(endpoint="/streams/end").time():
        db = mongo_client[MONGO_DB]
        stream_id = req.stream_id

        doc = db.live_metadata.find_one({"stream_id": stream_id})
        if not doc:
            raise HTTPException(status_code=404, detail="Stream not found")

        now = datetime.now(timezone.utc).isoformat()

        # Fetch current manifest from MinIO and append EXT-X-ENDLIST
        manifest_key = f"{stream_id}/live_manifest.m3u8"
        try:
            obj = s3_client.get_object(Bucket=BUCKET_MANIFESTS, Key=manifest_key)
            current_manifest = obj["Body"].read().decode()
            if "#EXT-X-ENDLIST" not in current_manifest:
                current_manifest += "\n#EXT-X-ENDLIST\n"
            s3_client.put_object(
                Bucket=BUCKET_MANIFESTS,
                Key=manifest_key,
                Body=current_manifest.encode(),
                ContentType="application/vnd.apple.mpegurl",
            )
        except Exception as e:
            log.warning("[%s] Could not finalize manifest: %s", stream_id, e)

        # Update MongoDB status to 'vod'
        db.live_metadata.update_one(
            {"stream_id": stream_id},
            {"$set": {"status": "vod", "ended_at": now}},
        )
        log.info("[%s] Live stream ended — status set to vod", stream_id)

        api_requests_total.labels(endpoint="/streams/end", method="POST", status="200").inc()
        return {"stream_id": stream_id, "status": "vod", "ended_at": now}


@app.get("/streams/{stream_id}/live", tags=["Live Stream"])
async def get_live_manifest(stream_id: str):
    """
    Returns a presigned MinIO URL for the current rolling HLS manifest.
    Clients poll this endpoint every few seconds to get new segment URLs
    as the DVR window slides forward.
    """
    with api_latency.labels(endpoint="/streams/live").time():
        db = mongo_client[MONGO_DB]
        doc = db.live_metadata.find_one({"stream_id": stream_id})
        if not doc:
            raise HTTPException(status_code=404, detail="Stream not found")

        manifest_path = doc.get("manifest_path", "")
        if not manifest_path:
            raise HTTPException(status_code=404, detail="Manifest not yet available")

        bucket, key = manifest_path.split("/", 1)
        try:
            url = s3_public_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=3600,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Could not generate presigned URL: {e}")

        api_requests_total.labels(endpoint="/streams/live", method="GET", status="200").inc()
        return {
            "stream_id": stream_id,
            "status": doc.get("status"),
            "manifest_url": url,
            "dvr_window_start": doc.get("dvr_window_start", 0),
        }


@app.get("/streams/{stream_id}/metadata", tags=["Live Stream"])
async def get_stream_metadata(stream_id: str):
    """Returns the full MongoDB document for a live stream."""
    db = mongo_client[MONGO_DB]
    doc = db.live_metadata.find_one({"stream_id": stream_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Stream not found")
    api_requests_total.labels(endpoint="/streams/metadata", method="GET", status="200").inc()
    return doc
