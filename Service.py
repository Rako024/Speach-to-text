import os
import sys
import signal
import time
import datetime
import threading
import subprocess
import queue
import logging
import psycopg2
from faster_whisper import WhisperModel

# --- Configuration ---
HLS_URL         = "https://live.itv.az/itv.m3u8"
SEGMENT_TIME    = 8             # seconds per segment
OVERLAP_TIME    = 1             # seconds overlap
WAV_DIR         = "wav_segments"
OUTPUT_DIR      = "transcripts"
MODEL_SIZE      = "large"       # tiny, base, small, medium, large
BEAM_SIZE       = 4
BEST_OF         = 4
VAD_FILTER      = True
WORKERS         = 3             # number of transcription threads
BACKLOG_WARN    = WORKERS * 3   # threshold to warn about queue backlog

# --- Database Configuration ---
DB_HOST         = "localhost"  # Database host
DB_NAME         = "speach_to_text"
DB_USER         = "postgres"
DB_PASSWORD     = "!2627251Rr"
DB_PORT         = 5432  # Default PostgreSQL port

# Internal state
segment_queue = queue.Queue()
shutdown_event = threading.Event()
ffmpeg_proc = None
model = None

# Setup logging
def setup_logging():
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
        level=logging.INFO
    )

# PostgreSQL connection
def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

# Ensure directories exist
def ensure_dirs():
    os.makedirs(WAV_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

# Start ffmpeg to segment HLS into WAV files
def start_ffmpeg():
    cmd = [
        "ffmpeg", "-y",
        "-i", HLS_URL,
        "-vn", "-ac", "1", "-ar", "16000",
        "-f", "segment",
        "-segment_time", str(SEGMENT_TIME),
        "-segment_time_delta", str(OVERLAP_TIME),
        "-reset_timestamps", "1",
        os.path.join(WAV_DIR, "segment_%03d.wav")
    ]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# Producer: watch for new WAV segments
def watch_segments():
    idx = 0
    while not shutdown_event.is_set():
        path = os.path.join(WAV_DIR, f"segment_{idx:03d}.wav")
        # Wait until file appears
        while not os.path.exists(path) and not shutdown_event.is_set():
            time.sleep(0.1)
        if shutdown_event.is_set():
            break
        # Wait until writing completes
        prev_size = -1
        while True:
            curr = os.path.getsize(path)
            if curr == prev_size and curr > 0:
                break
            prev_size = curr
            time.sleep(0.1)
        end_ts = datetime.datetime.now(datetime.timezone.utc)
        start_ts = end_ts - datetime.timedelta(seconds=SEGMENT_TIME)
        segment_queue.put((path, start_ts, end_ts))
        idx += 1

# Monitor: print queue size and backlog warnings
def monitor_queue():
    while not shutdown_event.is_set():
        qsize = segment_queue.qsize()
        logging.info(f"[Monitor] Queue size={qsize}")
        if qsize > BACKLOG_WARN:
            logging.warning(f"[Monitor] Backlog={qsize} exceeds threshold ({BACKLOG_WARN})")
        time.sleep(5)

# Consumer: transcribe segments
def transcribe_worker(worker_id):
    global model
    logging.info(f"[W{worker_id}] Ready")
    while not shutdown_event.is_set():
        try:
            path, st, en = segment_queue.get(timeout=1)
        except queue.Empty:
            continue
        logging.info(f"[W{worker_id}] Transcribing segment @ {st.time()}–{en.time()}")

        # Whisper transcription (batch or single-file)
        try:
            segs_list, _ = model.transcribe(
                [path],
                language="az",
                beam_size=BEAM_SIZE,
                best_of=BEST_OF,
                vad_filter=VAD_FILTER,
                batch_size=1
            )
            segments = segs_list[0]
        except TypeError:
            segments, _ = model.transcribe(
                path,
                language="az",
                beam_size=BEAM_SIZE,
                best_of=BEST_OF,
                vad_filter=VAD_FILTER
            )

        # Connect to the database
        conn = connect_db()
        cursor = conn.cursor()

        # Insert the segments into PostgreSQL
        for seg in segments:
            s = st + datetime.timedelta(seconds=seg.start)
            e = st + datetime.timedelta(seconds=seg.end)
            text = seg.text.strip()

            # Insert data into the database
            cursor.execute("""
                INSERT INTO transcripts (start_time, end_time, text)
                VALUES (%s, %s, %s)
            """, (s, e, text))

        conn.commit()
        cursor.close()
        conn.close()

        # Cleanup
        try:
            os.remove(path)
        except OSError:
            pass
        segment_queue.task_done()
        logging.info(f"[W{worker_id}] Done & removed {path}")

# Signal handler for graceful shutdown
def signal_handler(sig, frame):
    logging.info("Shutting down...")
    shutdown_event.set()
    if ffmpeg_proc:
        ffmpeg_proc.terminate()
    sys.exit(0)

# Main entry
def main():
    global ffmpeg_proc, model
    setup_logging()
    signal.signal(signal.SIGINT, signal_handler)

    ensure_dirs()
    logging.info("Starting HLS transcription service...")

    # Load Whisper once
    logging.info(f"Loading Whisper model '{MODEL_SIZE}' on cuda float16...")
    model = WhisperModel(MODEL_SIZE, device="cuda", compute_type="float16")
    logging.info("Whisper model ready.")

    # Start ffmpeg and worker threads
    ffmpeg_proc = start_ffmpeg()
    logging.info(f"FFmpeg started (pid={ffmpeg_proc.pid})")

    threading.Thread(target=watch_segments, daemon=True).start()
    threading.Thread(target=monitor_queue, daemon=True).start()
    for i in range(WORKERS):
        threading.Thread(target=transcribe_worker, args=(i,), daemon=True).start()

    # Keep alive until Ctrl+C
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        signal_handler(None, None)

if __name__ == "__main__":
    main()
