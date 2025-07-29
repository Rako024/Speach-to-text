#!/usr/bin/env python3
import os
import time
import queue
import threading
import subprocess
import datetime
import logging
from app.config import Settings, Channel

logger = logging.getLogger(__name__)

class Archiver:
    def __init__(self, channel: Channel, settings: Settings):
        self.channel     = channel
        self.hls_url     = channel.hls_url
        self.archive_dir = os.path.join(settings.archive_base, channel.id)
        self.wav_dir     = os.path.join(settings.wav_base,     channel.id)

        # Seqmentləmə parametrləri
        self.ts_seg_time = settings.ts_segment_time

        # WAV üçün queue + stop‑flag
        self.wav_queue   = queue.Queue()
        self._shutdown   = threading.Event()

        # Startup zamanı mövcud .ts faylları emal olundu sayılacaq
        self._processed = set()

    def start_ts(self):
        """
        HLS → .ts seqmentləri yazır:
        fayllar audio‑copy + video H.264/CRF=28, 360p
        """
        os.makedirs(self.archive_dir, exist_ok=True)
        logger.info("[%s] TS archiver started → %s", self.channel.id, self.archive_dir)

        ts_pattern = os.path.join(
            self.archive_dir,
            f"{self.channel.id}_" + "%Y%m%dT%H%M%S.ts"
        )

        cmd = [
            "ffmpeg", "-y", "-i", self.hls_url,
            "-c:a", "copy",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "28",
            "-vf", "scale=-2:360",
            "-f", "segment",
            "-segment_time",    str(self.ts_seg_time),
            "-reset_timestamps","1",
            "-strftime",        "1",
            ts_pattern
        ]
        logger.debug("[%s] TS cmd: %s", self.channel.id, " ".join(cmd))

        self.ts_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

    def start_watcher(self):
        """
        Arxiv qovluğundakı yeni .ts fayllarını gözləyir,
        yalnız startup-dan sonra yarananları .wav-ə çevirib queue-ya atır.
        """
        os.makedirs(self.wav_dir, exist_ok=True)
        logger.info("[%s] WAV‑watcher started → %s", self.channel.id, self.wav_dir)

        # Startup zamanı mövcud .ts fayllarını artıq emal edildi say
        self._processed = {
            fname for fname in os.listdir(self.archive_dir)
            if fname.endswith(".ts")
        }

        threading.Thread(target=self._watch_ts_and_generate_wav, daemon=True).start()

    def _watch_ts_and_generate_wav(self):
        while not self._shutdown.is_set():
            for fname in sorted(os.listdir(self.archive_dir)):
                if not fname.endswith(".ts") or fname in self._processed:
                    continue

                ts_path = os.path.join(self.archive_dir, fname)
                wav_name = os.path.splitext(fname)[0] + ".wav"
                wav_path = os.path.join(self.wav_dir, wav_name)

                # Faylın tamam yazılmasını gözlə
                prev = -1
                while True:
                    size = os.path.getsize(ts_path)
                    if size == prev and size > 0:
                        break
                    prev = size
                    time.sleep(0.05)

                # .wav çıxar
                cmd = [
                    "ffmpeg", "-y", "-i", ts_path,
                    "-vn", "-ac", "1", "-ar", "16000",
                    wav_path
                ]
                logger.debug("[%s] WAV gen cmd: %s", self.channel.id, " ".join(cmd))
                subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

                # Başlanğıc timestamp
                start_ts = self._extract_ts_from_filename(fname)

                # Queue‑ya at
                self.wav_queue.put((self.channel.id, wav_path, start_ts))
                logger.info("[%s] WAV generated and queued: %s", self.channel.id, wav_path)

                # Bu fayl artıq emal edildi sayılır
                self._processed.add(fname)

            time.sleep(0.1)

    def _extract_ts_from_filename(self, fname: str) -> float:
        """
        itv_20250721T153012.ts → epoch saniyəsi
        """
        try:
            ts_str = os.path.splitext(fname)[0].split("_",1)[1]
            dt = datetime.datetime.strptime(ts_str, "%Y%m%dT%H%M%S")
            dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt.timestamp()
        except Exception as e:
            logger.warning("[%s] TS parse error: %s", self.channel.id, e)
            return datetime.datetime.now(datetime.timezone.utc).timestamp()

    def wav_generator(self):
        """
        Hər çağırışda (channel_id, wav_path, start_ts) qaytarır.
        """
        while True:
            yield self.wav_queue.get()

    def stop(self):
        """
        Prosesləri dayandır.
        """
        self._shutdown.set()
        if hasattr(self, 'ts_proc'):
            self.ts_proc.terminate()
