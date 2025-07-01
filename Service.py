#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Service_dynamic_batch_fallback.py

Optimized real-time transcription for Ryzen5 6-core + RTX3050:
- faster-whisper ONNX float16 backend
- dynamic queue of 10s segments with 1s overlap
- per-segment beam search + VAD filter for quality
- optional punctuation restoration
- fallback: no batch_size if unsupported
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
HLS_URL         = "https://live.itv.az/itv.m3u8"
SEGMENT_TIME    = 10            # seconds per segment
OVERLAP_TIME    = 1             # seconds overlap
WAV_DIR         = "wav_segments"
OUTPUT_DIR      = "transcripts"
MODEL_SIZE      = "large"       # tiny, base, small, medium, large
TRANSCRIPT_FILE = os.path.join(OUTPUT_DIR, "transcript.txt")
BEAM_SIZE       = 4             # beam search width
BEST_OF         = 4             # best_of candidates
VAD_FILTER      = True          # faster-whisper vad_filter

# internal queue for segments
segment_queue = queue.Queue()


def ensure_dirs():
    os.makedirs(WAV_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if not os.path.exists(TRANSCRIPT_FILE):
        open(TRANSCRIPT_FILE, "w", encoding="utf-8").close()


def start_ffmpeg():
    # produce 10s segments sliding by (10 - overlap) seconds
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
    """Producer: waits for new wav files and enqueues them."""
    idx = 0
    while True:
        path = os.path.join(WAV_DIR, f"segment_{idx:03d}.wav")
        # wait until file stable
        while not os.path.exists(path):
            time.sleep(0.1)
        prev = -1
        while True:
            curr = os.path.getsize(path)
            if curr == prev and curr > 0:
                break
            prev = curr
            time.sleep(0.1)
        end_ts = datetime.datetime.now(datetime.timezone.utc)
        start_ts = end_ts - datetime.timedelta(seconds=SEGMENT_TIME)
        segment_queue.put((path, start_ts, end_ts))
        idx += 1


def transcribe_worker():
    """Consumer: dequeues segments, runs inference, writes outputs."""
    print(f"[INFO] Loading model '{MODEL_SIZE}'... (float16 GPU)")
    model = WhisperModel(MODEL_SIZE, device="cuda", compute_type="float16")
    print("[INFO] Model ready.")
    if USE_PUNCT:
        punct = PunctuationModel("oliverguhr/fullstop-punctuation-multilang-large")

    while True:
        path, st, en = segment_queue.get()
        print(f"[INFO] Transcribing segment @ {st.time()}–{en.time()}")
        # try batch-style (some versions support batch_size)
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
            # fallback single-file call
            segments, _ = model.transcribe(
                path,
                language="az",
                beam_size=BEAM_SIZE,
                best_of=BEST_OF,
                vad_filter=VAD_FILTER
            )

        # write transcript
        with open(TRANSCRIPT_FILE, "a", encoding="utf-8") as f:
            f.write(f"[Segment @ {st.isoformat()} --> {en.isoformat()}]\n")
            for seg in segments:
                s = st + datetime.timedelta(seconds=seg.start)
                e = st + datetime.timedelta(seconds=seg.end)
                text = seg.text.strip()
                if USE_PUNCT and text:
                    try:
                        text = punct([text])[0]
                    except Exception:
                        pass
                f.write(f"[{s.isoformat()} --> {e.isoformat()}] {text}\n")
            f.write("\n")
        # cleanup
        try:
            os.remove(path)
        except OSError:
            pass
        print(f"[INFO] Done & removed {path}")


def main():
    ensure_dirs()
    ff = start_ffmpeg()
    print(f"[INFO] FFmpeg started (pid={ff.pid})")
    # start threads
    t1 = threading.Thread(target=watch_segments, daemon=True)
    t2 = threading.Thread(target=transcribe_worker, daemon=True)
    t1.start(); t2.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        ff.terminate()

if __name__ == "__main__":
    main()
