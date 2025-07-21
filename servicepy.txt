#!/usr/bin/env python3
import os
import sys
import signal
import subprocess
import threading
import queue
import time
import datetime
import logging
import psycopg2
from faster_whisper import WhisperModel

# --- Configuration ---
HLS_URL         = "https://live.itv.az/itv.m3u8"
ARCHIVE_DIR     = "archive"
TS_SEGMENT_TIME = 8       # seconds per TS segment
TS_LIST_SIZE    = 10800   # number of segments to retain (~24h)

WAV_DIR         = "wav_segments"
SEGMENT_TIME    = 8       # seconds per WAV segment
OVERLAP_TIME    = 1       # seconds overlap

# Whisper config
MODEL_SIZE      = "large"
BEAM_SIZE       = 4
BEST_OF         = 4
VAD_FILTER      = True
WORKERS         = 3
BACKLOG_WARN    = WORKERS * 3

# Database config
DB_HOST         = "localhost"
DB_NAME         = "speach_to_text"
DB_USER         = "postgres"
DB_PASSWORD     = "!2627251Rr"
DB_PORT         = 5432

# Internal state
segment_queue   = queue.Queue()
shutdown_event  = threading.Event()
ffmpeg_ts_proc  = None
ffmpeg_wav_proc = None
model           = None

# Setup logging
def setup_logging():
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        level=logging.INFO
    )
    logging.getLogger('ffmpeg').setLevel(logging.WARNING)

# Database connection helper
def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

# Initialize DB: create transcripts table if not exists
def init_db():
    conn = connect_db()
    cur  = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS transcripts (
            id SERIAL PRIMARY KEY,
            start_time TIMESTAMP WITH TIME ZONE NOT NULL,
            end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
            text       TEXT NOT NULL,
            segment_filename TEXT NOT NULL,
            offset_secs       REAL NOT NULL,
            duration_secs     REAL NOT NULL
        )
        """
    )
    conn.commit()
    cur.close()
    conn.close()

# Ensure directories exist
def ensure_dirs():
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    os.makedirs(WAV_DIR, exist_ok=True)

# Start TS archiver: live -> .ts + index.m3u8
def start_ts_archiver():
    global ffmpeg_ts_proc
    cmd = [
        "ffmpeg", "-y",
        "-i", HLS_URL,
        "-c", "copy",
        "-f", "hls",
        "-hls_time", str(TS_SEGMENT_TIME),
        "-hls_list_size", str(TS_LIST_SIZE),
        "-hls_flags", "delete_segments+append_list",
        "-hls_segment_filename", os.path.join(ARCHIVE_DIR, "segment_%05d.ts"),
        os.path.join(ARCHIVE_DIR, "index.m3u8")
    ]
    logging.info(f"Starting TS archiver: {' '.join(cmd)}")
    ffmpeg_ts_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# Start WAV segmenter: live audio -> WAV
def start_wav_segmenter():
    global ffmpeg_wav_proc
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
    logging.info(f"Starting WAV segmenter: {' '.join(cmd)}")
    ffmpeg_wav_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# Producer: watch WAV_DIR and enqueue segments
def watch_wavs():
    idx = 0
    while not shutdown_event.is_set():
        path = os.path.join(WAV_DIR, f"segment_{idx:03d}.wav")
        while not os.path.exists(path) and not shutdown_event.is_set():
            time.sleep(0.1)
        if shutdown_event.is_set():
            break
        prev_size = -1
        while True:
            curr_size = os.path.getsize(path)
            if curr_size == prev_size and curr_size > 0:
                break
            prev_size = curr_size
            time.sleep(0.1)
        end_ts = datetime.datetime.now(datetime.timezone.utc)
        start_ts = end_ts - datetime.timedelta(seconds=SEGMENT_TIME)
        segment_queue.put((path, start_ts, end_ts))
        idx += 1

# Monitor queue backlog
def monitor_queue():
    while not shutdown_event.is_set():
        qsize = segment_queue.qsize()
        logging.info(f"[Monitor] Queue size={qsize}")
        if qsize > BACKLOG_WARN:
            logging.warning(f"Backlog {qsize} exceeds {BACKLOG_WARN}")
        time.sleep(5)

# Consumer: transcribe WAV -> DB
def transcribe_worker(worker_id):
    global model
    logging.info(f"Worker {worker_id} started")
    while not shutdown_event.is_set():
        try:
            path, st, en = segment_queue.get(timeout=1)
        except queue.Empty:
            continue
        logging.info(f"Worker {worker_id} transcribing {path}")
        try:
            segs_list, _ = model.transcribe(
                [path], language="az",
                beam_size=BEAM_SIZE, best_of=BEST_OF,
                vad_filter=VAD_FILTER, batch_size=1
            )
            segments = segs_list[0]
        except TypeError:
            segments, _ = model.transcribe(
                path, language="az",
                beam_size=BEAM_SIZE, best_of=BEST_OF,
                vad_filter=VAD_FILTER
            )
        conn = connect_db()
        cur  = conn.cursor()
        for seg in segments:
            real_start = st + datetime.timedelta(seconds=seg.start)
            real_end   = st + datetime.timedelta(seconds=seg.end)
            txt        = seg.text.strip()
            wav_name   = os.path.basename(path)
            idx        = int(wav_name.replace("segment_", "").replace(".wav", ""))
            ts_file    = f"segment_{idx:05d}.ts"
            offset     = float(seg.start)
            dur        = float(seg.end - seg.start)
            cur.execute(
                """
                INSERT INTO transcripts
                  (start_time, end_time, text,
                   segment_filename, offset_secs, duration_secs)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (real_start, real_end, txt, ts_file, offset, dur)
            )
        conn.commit()
        cur.close(); conn.close()
        try:
            os.remove(path)
        except OSError:
            pass
        segment_queue.task_done()
        logging.info(f"Worker {worker_id} done {path}")

# Handle termination signals
def handle_sig(sig, frame):
    logging.info("Shutdown requested")
    shutdown_event.set()
    if ffmpeg_ts_proc:
        ffmpeg_ts_proc.terminate()
    if ffmpeg_wav_proc:
        ffmpeg_wav_proc.terminate()
    sys.exit(0)

# Main entrypoint
def main():
    global model
    setup_logging()
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)
    ensure_dirs()
    init_db()                    # ensure transcripts table exists
    start_ts_archiver()
    start_wav_segmenter()
    logging.info(f"Loading Whisper model '{MODEL_SIZE}'")
    model = WhisperModel(MODEL_SIZE, device="cuda", compute_type="float16")
    threading.Thread(target=watch_wavs, daemon=True).start()
    threading.Thread(target=monitor_queue, daemon=True).start()
    for i in range(WORKERS):
        threading.Thread(target=transcribe_worker, args=(i,), daemon=True).start()
    logging.info("Service started")
    while not shutdown_event.is_set():
        time.sleep(1)

if __name__ == "__main__":
    main()
