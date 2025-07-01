#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Service_dynamic_batch_v2.py

Optimized real-time transcription for Ryzen5 6-core + RTX3050:
- faster-whisper ONNX float16 backend
- dynamic queue of 10s segments with 1s overlap
- multiple transcription worker threads (configurable)
- per-segment beam search + VAD filter for quality
- optional punctuation restoration
- monitor thread for queue/backlog alerts
- human-readable timestamps (seconds precision) and segment indices
"""

import subprocess
import os
import time
import datetime
import threading
import queue
from faster_whisper import WhisperModel

# Optional punctuation restoration
try:
    from deepmultilingualpunctuation import PunctuationModel
    USE_PUNCT = True
except ImportError:
    print("[WARN] deepmultilingualpunctuation not installed; punctuation disabled.")
    USE_PUNCT = False

# --- Configuration ---
HLS_URL           = "https://live.itv.az/itv.m3u8"
SEGMENT_TIME      = 10            # seconds per segment
OVERLAP_TIME      = 1             # seconds overlap
WAV_DIR           = "wav_segments"
OUTPUT_DIR        = "transcripts"
MODEL_SIZE        = "large"       # tiny, base, small, medium, large
TRANSCRIPT_FILE   = os.path.join(OUTPUT_DIR, "transcript.txt")
BEAM_SIZE         = 4             # beam search width
BEST_OF           = 4             # best_of candidates
VAD_FILTER        = True          # faster-whisper vad_filter
WORKER_COUNT      = 2             # number of transcription threads
MONITOR_INTERVAL  = 5             # seconds between monitor checks
BACKLOG_WARNING   = WORKER_COUNT * 2  # queue size threshold for warnings

# Internal queue for segments
segment_queue = queue.Queue()


def ensure_dirs():
    os.makedirs(WAV_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if not os.path.exists(TRANSCRIPT_FILE):
        open(TRANSCRIPT_FILE, "w", encoding="utf-8").close()


def start_ffmpeg():
    # produce SEGMENT_TIME-second files sliding by (SEGMENT_TIME - OVERLAP_TIME)
    return subprocess.Popen([
        "ffmpeg", "-y",
        "-i", HLS_URL,
        "-vn", "-ac", "1", "-ar", "16000",
        "-f", "segment",
        "-segment_time", str(SEGMENT_TIME),
        "-segment_time_delta", str(OVERLAP_TIME),
        "-reset_timestamps", "1",
        os.path.join(WAV_DIR, "segment_%03d.wav")
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def watch_segments():
    """Producer: waits for new wav files and enqueues them along with index and timestamps."""
    idx = 0
    while True:
        path = os.path.join(WAV_DIR, f"segment_{idx:03d}.wav")
        while not os.path.exists(path):
            time.sleep(0.1)
        prev_size = -1
        while True:
            curr_size = os.path.getsize(path)
            if curr_size == prev_size and curr_size > 0:
                break
            prev_size = curr_size
            time.sleep(0.1)
        end_ts = datetime.datetime.now(datetime.timezone.utc)
        start_ts = end_ts - datetime.timedelta(seconds=SEGMENT_TIME)
        segment_queue.put((idx, path, start_ts, end_ts))
        idx += 1


def transcribe_worker(worker_id):
    """Consumer: dequeues segments, runs inference, writes outputs."""
    print(f"[Worker {worker_id}] Loading model '{MODEL_SIZE}' (float16 GPU)...")
    model = WhisperModel(MODEL_SIZE, device="cuda", compute_type="float16")
    print(f"[Worker {worker_id}] Model ready.")
    if USE_PUNCT:
        punct_model = PunctuationModel("oliverguhr/fullstop-punctuation-multilang-large")

    while True:
        idx, path, st, en = segment_queue.get()
        print(f"[Worker {worker_id}] Transcribing segment #{idx:03d} @ {st.strftime('%H:%M:%S')}–{en.strftime('%H:%M:%S')}")
        try:
            segments_list, _ = model.transcribe(
                [path],
                language="az",
                beam_size=BEAM_SIZE,
                best_of=BEST_OF,
                vad_filter=VAD_FILTER,
                batch_size=1
            )
            segments = segments_list[0]
        except TypeError:
            segments, _ = model.transcribe(
                path,
                language="az",
                beam_size=BEAM_SIZE,
                best_of=BEST_OF,
                vad_filter=VAD_FILTER
            )

        # Write transcript block
        with open(TRANSCRIPT_FILE, "a", encoding="utf-8") as f:
            header = f"[Segment {idx:03d} @ {st.strftime('%Y-%m-%dT%H:%M:%S')} --> {en.strftime('%Y-%m-%dT%H:%M:%S')}]\n"
            f.write(header)
            for seg in segments:
                s = st + datetime.timedelta(seconds=seg.start)
                e = st + datetime.timedelta(seconds=seg.end)
                text = seg.text.strip()
                if USE_PUNCT and text:
                    try:
                        text = punct_model([text])[0]
                    except Exception:
                        pass
                f.write(f"[{s.strftime('%Y-%m-%dT%H:%M:%S')} --> {e.strftime('%Y-%m-%dT%H:%M:%S')}] {text}\n")
            f.write("\n")
        try:
            os.remove(path)
        except OSError:
            pass
        print(f"[Worker {worker_id}] Done segment #{idx:03d}, removed file.")


def monitor():
    """Monitor: periodically logs queue size and warns if backlog grows."""
    while True:
        qsize = segment_queue.qsize()
        print(f"[Monitor] Queue size: {qsize}")
        if qsize > BACKLOG_WARNING:
            print(f"[Monitor] WARNING: backlog of {qsize} segments")
        time.sleep(MONITOR_INTERVAL)


def main():
    ensure_dirs()
    ff = start_ffmpeg()
    print(f"[Main] FFmpeg started (pid={ff.pid})")

    # Start producer thread
    t_watch = threading.Thread(target=watch_segments, daemon=True)
    t_watch.start()
    # Start monitor thread
    t_mon = threading.Thread(target=monitor, daemon=True)
    t_mon.start()
    # Start transcription worker threads
    for i in range(WORKER_COUNT):
        t = threading.Thread(target=transcribe_worker, args=(i,), daemon=True)
        t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[Main] Shutting down...")
        ff.terminate()

if __name__ == "__main__":
    main()
