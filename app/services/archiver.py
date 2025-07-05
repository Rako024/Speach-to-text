#!/usr/bin/env python3
import os
import time
import queue
import threading
import subprocess
import datetime
import logging

from app.config import Settings

logger = logging.getLogger(__name__)

class Archiver:
    def __init__(self, settings: Settings):
        # HLS → TS archiving
        self.hls_url         = settings.hls_url
        self.archive_dir     = settings.archive_dir
        self.ts_seg_time     = settings.ts_segment_time
        self.ts_list_size    = settings.ts_list_size

        # HLS → WAV segmentation
        self.wav_dir          = settings.wav_dir
        self.wav_seg_time     = settings.wav_segment_time
        self.wav_overlap      = settings.wav_overlap_time

        # daxili queue & stop-flag
        self.wav_queue        = queue.Queue()
        self._shutdown        = threading.Event()

    def start_ts(self):
        """HLS-dən .ts və index.m3u8 yaradır."""
        os.makedirs(self.archive_dir, exist_ok=True)
        logger.info("TS archiver işə düşdü, m3u8 yazılır → %s", self.archive_dir)
        cmd = [
            "ffmpeg", "-y", "-i", self.hls_url,
            "-c", "copy", "-f", "hls",
            "-hls_time", str(self.ts_seg_time),
            "-hls_list_size", str(self.ts_list_size),
            "-hls_flags", "delete_segments+append_list",
            "-hls_segment_filename", os.path.join(self.archive_dir, "segment_%05d.ts"),
            os.path.join(self.archive_dir, "index.m3u8")
        ]
        self.ts_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def start_wav(self):
        """HLS-dən .wav seqmentləri yaradır və watch thread-i işə salır."""
        os.makedirs(self.wav_dir, exist_ok=True)
        logger.info("WAV segmenter işə düşdü, fayllar → %s", self.wav_dir)
        cmd = [
            "ffmpeg", "-y", "-i", self.hls_url,
            "-vn", "-ac", "1", "-ar", "16000",
            "-f", "segment",
            "-segment_time", str(self.wav_seg_time),
            "-segment_time_delta", str(self.wav_overlap),
            "-reset_timestamps", "1",
            os.path.join(self.wav_dir, "segment_%03d.wav")
        ]
        self.wav_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        threading.Thread(target=self._watch_wavs, daemon=True).start()

    def _watch_wavs(self):
        """Yazılmış wav fayllarını gözləyir, tamalananda queue-ya atır."""
        idx = 0
        while not self._shutdown.is_set():
            path = os.path.join(self.wav_dir, f"segment_{idx:03d}.wav")
            if not os.path.exists(path):
                time.sleep(0.1)
                continue

            logger.debug("Yeni WAV tapıldı: %s", path)

            # Yazılmanın tamamlanmasını gözləyirik
            prev_size = -1
            while True:
                size = os.path.getsize(path)
                if size == prev_size and size > 0:
                    break
                prev_size = size
                time.sleep(0.05)

            # Başlanğıc zamanını epoch şəklində hesablayırıq
            end_dt   = datetime.datetime.now(datetime.timezone.utc)
            start_dt = end_dt - datetime.timedelta(seconds=self.wav_seg_time)
            self.wav_queue.put((path, start_dt.timestamp()))
            logger.info("WAV hazırlandı və queue-yə göndərildi: %s", path)

            idx += 1

    def wav_generator(self):
        """Daemon thread-lər üçün iterator: (wav_path, start_ts)."""
        while True:
            yield self.wav_queue.get()

    def stop(self):
        """Həm ts, həm wav process-lərini dayandırır."""
        logger.info("Archiver dayandırılır…")
        self._shutdown.set()
        if hasattr(self, "ts_proc"):
            self.ts_proc.terminate()
        if hasattr(self, "wav_proc"):
            self.wav_proc.terminate()
