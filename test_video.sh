#!/usr/bin/env bash
# =============================================================================
# Streaming Pipeline — Video File Test Script
# =============================================================================
# Usage:
#   ./test_video.sh /path/to/your/video.mp4
#   ./test_video.sh /path/to/your/video.mp4 --watch
#
# What it does:
#   1. Reads real metadata from your video file using ffprobe (if available)
#      or falls back to reasonable defaults
#   2. Submits the video as a VOD upload to FastAPI
#   3. Optionally starts a simulated live stream alongside
#   4. Polls the pipeline and reports status changes in real time
#   5. Fetches and displays the generated HLS manifest URL when ready
#
# Requirements:
#   - The pipeline must be running: ./run.sh start
#   - curl (always available on macOS)
#   - ffprobe (optional, install via: brew install ffmpeg)
#
# The video file itself does NOT travel through the pipeline — only its
# metadata does. This matches the architecture: Kafka carries metadata only,
# MinIO holds placeholder objects, the actual CDN would serve the real bytes.
# =============================================================================

set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
VIDEO_FILE="${1:-}"
WATCH="${2:-}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'
BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERR]${NC}  $*" >&2; }
header()  { echo -e "\n${BOLD}${CYAN}=== $* ===${NC}\n"; }

# ---------------------------------------------------------------------------
# Validate input
# ---------------------------------------------------------------------------
if [[ -z "$VIDEO_FILE" ]]; then
    echo ""
    echo -e "${BOLD}Usage:${NC} ./test_video.sh <video_file> [--watch]"
    echo ""
    echo "  <video_file>   Path to any video file (MP4, MKV, MOV, etc.)"
    echo "  --watch        Keep polling until Spark marks the stream as 'ready'"
    echo ""
    echo "Example:"
    echo "  ./test_video.sh test_videos/episode.mp4 --watch"
    exit 1
fi

if [[ ! -f "$VIDEO_FILE" ]]; then
    error "File not found: $VIDEO_FILE"
    exit 1
fi

# ---------------------------------------------------------------------------
# Check that the pipeline API is reachable
# ---------------------------------------------------------------------------
header "Pipeline Health Check"
if ! curl -sf "$API_URL/health" >/dev/null 2>&1; then
    error "FastAPI is not reachable at $API_URL"
    error "Make sure the pipeline is running: ./run.sh start"
    exit 1
fi
success "FastAPI is healthy at $API_URL"

# ---------------------------------------------------------------------------
# Extract video metadata using ffprobe (if available)
# ---------------------------------------------------------------------------
header "Video File Analysis"

FILENAME=$(basename "$VIDEO_FILE")
EXTENSION="${FILENAME##*.}"

if command -v ffprobe >/dev/null 2>&1; then
    info "Using ffprobe to read real video metadata..."

    # Extract key metadata fields
    DURATION=$(ffprobe -v quiet -show_entries format=duration \
        -of default=noprint_wrappers=1:nokey=1 "$VIDEO_FILE" 2>/dev/null || echo "3600")
    DURATION=$(printf "%.1f" "$DURATION")

    FILE_SIZE=$(wc -c < "$VIDEO_FILE" | tr -d ' ')

    # Get video stream info
    VIDEO_INFO=$(ffprobe -v quiet -select_streams v:0 \
        -show_entries stream=width,height,codec_name,bit_rate,r_frame_rate \
        -of default=noprint_wrappers=1 "$VIDEO_FILE" 2>/dev/null || echo "")

    WIDTH=$(echo "$VIDEO_INFO"  | grep "^width="  | cut -d= -f2 | head -1)
    HEIGHT=$(echo "$VIDEO_INFO" | grep "^height=" | cut -d= -f2 | head -1)
    CODEC=$(echo "$VIDEO_INFO"  | grep "^codec_name=" | cut -d= -f2 | head -1)

    WIDTH="${WIDTH:-1920}"
    HEIGHT="${HEIGHT:-1080}"
    CODEC="${CODEC:-h264}"
    RESOLUTION="${WIDTH}x${HEIGHT}"

    echo ""
    echo -e "  File:       ${BOLD}$FILENAME${NC}"
    echo -e "  Duration:   ${BOLD}${DURATION}s${NC} ($(awk "BEGIN{printf \"%d:%02d\", int($DURATION/60), int($DURATION%60)}") min)"
    echo -e "  Resolution: ${BOLD}${RESOLUTION}${NC}"
    echo -e "  Codec:      ${BOLD}${CODEC}${NC}"
    echo -e "  File size:  ${BOLD}$(python3 -c "print(f'{$FILE_SIZE/1048576:.1f} MB')")${NC}"
    echo ""
else
    warn "ffprobe not found — using estimated metadata (install ffmpeg for real values)"
    warn "To install: brew install ffmpeg"
    echo ""

    DURATION="3600"
    FILE_SIZE=$(wc -c < "$VIDEO_FILE" | tr -d ' ')
    RESOLUTION="1920x1080"
    CODEC="h264"
    echo -e "  File:       ${BOLD}$FILENAME${NC}"
    echo -e "  Duration:   ${BOLD}estimated 60:00${NC}"
    echo -e "  Resolution: ${BOLD}${RESOLUTION}${NC} (estimated)"
    echo -e "  File size:  ${BOLD}$(python3 -c "print(f'{$FILE_SIZE/1048576:.1f} MB')")${NC}"
    echo ""
fi

# Episode title from filename (strip extension, replace underscores/dashes)
EPISODE_TITLE=$(basename "$VIDEO_FILE" ".$EXTENSION" | tr '_-' ' ' | sed 's/  / /g')

# ---------------------------------------------------------------------------
# Submit as VOD upload to FastAPI
# ---------------------------------------------------------------------------
header "Submitting VOD Upload"
info "Calling POST $API_URL/vod/upload ..."

UPLOAD_RESPONSE=$(curl -sf -X POST "$API_URL/vod/upload" \
    -H "Content-Type: application/json" \
    -d "{
        \"title\": \"VOD: $EPISODE_TITLE\",
        \"duration_seconds\": $DURATION,
        \"resolution\": \"$RESOLUTION\",
        \"file_size_bytes\": $FILE_SIZE
    }")

if [[ $? -ne 0 ]] || [[ -z "$UPLOAD_RESPONSE" ]]; then
    error "Upload failed — check that the pipeline is running"
    exit 1
fi

STREAM_ID=$(echo "$UPLOAD_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['stream_id'])" 2>/dev/null || echo "")

if [[ -z "$STREAM_ID" ]]; then
    error "Could not parse stream_id from response:"
    echo "$UPLOAD_RESPONSE"
    exit 1
fi

success "Upload accepted!"
echo ""
echo -e "  Stream ID:   ${BOLD}${STREAM_ID}${NC}"
echo -e "  Kafka topic: ${BOLD}vod-chunks${NC}"
echo -e "  Status:      ${BOLD}uploaded${NC} (Spark will process asynchronously)"
echo ""
echo -e "  Full response: $UPLOAD_RESPONSE"
echo ""

# ---------------------------------------------------------------------------
# Poll metadata until status = ready (or timeout)
# ---------------------------------------------------------------------------
if [[ "$WATCH" == "--watch" ]]; then
    header "Watching Pipeline Progress"
    info "Polling GET $API_URL/vod/$STREAM_ID/metadata every 3s..."
    info "Spark status progression: uploaded → processing → transcoding → ready"
    info "(Press Ctrl+C to stop watching at any time)"
    echo ""

    PREV_STATUS=""
    TIMEOUT=300   # 5 minutes
    ELAPSED=0

    while [[ $ELAPSED -lt $TIMEOUT ]]; do
        METADATA=$(curl -sf "$API_URL/vod/$STREAM_ID/metadata" 2>/dev/null || echo "{}")
        STATUS=$(echo "$METADATA" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status','unknown'))" 2>/dev/null || echo "unknown")

        if [[ "$STATUS" != "$PREV_STATUS" ]]; then
            TIMESTAMP=$(date "+%H:%M:%S")
            case "$STATUS" in
                "uploaded")    echo -e "  [$TIMESTAMP] ${YELLOW}●${NC} Status: ${BOLD}uploaded${NC}  — event in Kafka, waiting for Spark" ;;
                "processing")  echo -e "  [$TIMESTAMP] ${YELLOW}●${NC} Status: ${BOLD}processing${NC} — Spark picked up the event" ;;
                "transcoding") echo -e "  [$TIMESTAMP] ${YELLOW}●${NC} Status: ${BOLD}transcoding${NC} — simulating FFmpeg encoding (2-4s)" ;;
                "ready")
                    echo -e "  [$TIMESTAMP] ${GREEN}●${NC} Status: ${BOLD}ready${NC} — variants written to MinIO, manifest generated!"
                    echo ""
                    success "Pipeline processing COMPLETE for stream $STREAM_ID"
                    echo ""

                    # Fetch the manifest URL
                    MANIFEST_RESP=$(curl -sf "$API_URL/vod/$STREAM_ID/manifest" 2>/dev/null || echo "{}")
                    MANIFEST_URL=$(echo "$MANIFEST_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('manifest_url','N/A'))" 2>/dev/null || echo "N/A")

                    echo -e "  ${BOLD}HLS Manifest URL:${NC}"
                    echo -e "  $MANIFEST_URL"
                    echo ""
                    echo -e "  This URL is valid for 1 hour and can be opened in:"
                    echo -e "    • ${CYAN}VLC${NC}:  File → Open Network Stream → paste URL"
                    echo -e "    • ${CYAN}ffplay${NC}: ffplay \"$MANIFEST_URL\""
                    echo ""
                    break
                    ;;
                *)             echo -e "  [$(date '+%H:%M:%S')] ${CYAN}●${NC} Status: $STATUS" ;;
            esac
            PREV_STATUS="$STATUS"
        fi

        sleep 3
        ELAPSED=$((ELAPSED + 3))
    done

    if [[ $ELAPSED -ge $TIMEOUT ]]; then
        warn "Timed out after ${TIMEOUT}s — Spark may still be processing."
        warn "Check: http://localhost:4040 (Spark UI), http://localhost:8081 (MongoDB)"
    fi
else
    # Non-watch mode: just show where to look
    echo -e "To watch pipeline progress, rerun with --watch:"
    echo -e "  ${CYAN}./test_video.sh $VIDEO_FILE --watch${NC}"
    echo ""
fi

# ---------------------------------------------------------------------------
# Quick reference for verifying end-to-end
# ---------------------------------------------------------------------------
header "Where to Verify the Pipeline"
echo -e "  ${BOLD}Redpanda Console${NC}  http://localhost:8080"
echo -e "    → Topics → vod-chunks → look for stream_id: $STREAM_ID"
echo ""
echo -e "  ${BOLD}Mongo Express${NC}     http://localhost:8081"
echo -e "    → pipeline → vod_metadata → filter: {\"stream_id\": \"$STREAM_ID\"}"
echo ""
echo -e "  ${BOLD}MinIO Console${NC}     http://localhost:9001  (minioadmin / minioadmin123)"
echo -e "    → vod-raw      → $STREAM_ID/raw/0.ts"
echo -e "    → vod-variants → $STREAM_ID/{1080p,720p,480p,360p}/0.ts"
echo -e "    → manifests    → $STREAM_ID/vod_manifest.m3u8"
echo ""
echo -e "  ${BOLD}Spark App UI${NC}      http://localhost:4040  (streaming queries, micro-batches)"
echo -e "  ${BOLD}Grafana${NC}           http://localhost:3000  (metrics dashboards — admin/admin)"
echo ""
