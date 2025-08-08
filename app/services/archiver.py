#!/usr/bin/env python3
import os
import time
import threading
import subprocess
import datetime
import logging
import queue
from zoneinfo import ZoneInfo
logger = logging.getLogger(__name__)

class Archiver:
    def __init__(self, channel, settings, wav_queue):
        self.channel     = channel
        self.settings    = settings
        self.archive_dir = os.path.join(settings.archive_base, channel.id)
        self.wav_dir     = os.path.join(settings.wav_base,     channel.id)
        self.ts_seg_time = settings.ts_segment_time

        # Ortak bounded queue (dispatcher loop istifadə edəcək)
        self.wav_queue   = wav_queue

        # Watcher thread
        self._watcher    = None
        # stop siqnalı üçün Event
        self._shutdown   = threading.Event()
        # artıq emal edilmiş .ts fayllar
        self._processed  = set()
        # TS yazan ffmpeg prosesinə handle
        self._ts_proc    = None

    def start_ts(self):
        """FFmpeg ilə HLS → .ts seqmentlərinə yazır."""
        os.makedirs(self.archive_dir, exist_ok=True)
        pattern = os.path.join(
            self.archive_dir,
            f"{self.channel.id}_%Y%m%dT%H%M%S.ts"
        )

        # Əgər artıq ffmpeg işləyirsə, yenisini açma
        if self._ts_proc is not None and self._ts_proc.poll() is None:
            logger.debug("[%s] TS archiver already running (pid=%s)",
                         self.channel.id, self._ts_proc.pid)
            return

        if self.channel.media_type == "video":
            cmd = [
                "ffmpeg", "-y", "-i", self.channel.hls_url,
                "-c:a", "copy", "-c:v", "libx264",
                "-preset", "veryfast", "-crf", "28",
                "-vf", "scale=-2:360",
                "-f", "segment",
                "-segment_time", str(self.ts_seg_time),
                "-reset_timestamps", "1",
                "-strftime", "1",
                pattern
            ]
        else:
            cmd = [
                "ffmpeg", "-y", "-i", self.channel.hls_url,
                "-c", "copy",
                "-f", "segment",
                "-segment_time", str(self.ts_seg_time),
                "-reset_timestamps", "1",
                "-strftime", "1",
                pattern
            ]
        logger.info("[%s] Starting TS archiver", self.channel.id)
        # Proses handle-ı saxlayırıq
        self._ts_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logger.debug("[%s] ffmpeg started (pid=%s)", self.channel.id, getattr(self._ts_proc, "pid", None))

    def start_watcher(self):
        """Yenilənən .tsləri götür, .wav çevir, queue-ya at."""
        # Əvvəlki stop siqnalını təmizləyirik
        self._shutdown.clear()
        if self._watcher and self._watcher.is_alive():
             return

        os.makedirs(self.wav_dir, exist_ok=True)
        # startup-dan qalma wavləri sil
        for f in os.listdir(self.wav_dir):
            if f.lower().endswith(".wav"):
                try: os.remove(os.path.join(self.wav_dir, f))
                except: pass

        self._processed = {
            f for f in os.listdir(self.archive_dir)
            if f.endswith(".ts")
        }

        self._watcher = threading.Thread(
            target=self._watch_loop,
            daemon=True
        )
        self._watcher.start()

    def _watch_loop(self):
        # Loop-u stop() çağırılana qədər davam etdir
        while not self._shutdown.is_set():
            try:
                for fname in sorted(os.listdir(self.archive_dir)):
                    if not fname.endswith(".ts") or fname in self._processed:
                        continue

                    ts_path = os.path.join(self.archive_dir, fname)
                    # tamamlanmağı gözlə
                    prev = -1
                    while True:
                        sz = os.path.getsize(ts_path)
                        if sz == prev and sz > 0:
                            break
                        prev = sz
                        time.sleep(0.05)

                    wav_name = fname[:-3] + ".wav"
                    wav_path = os.path.join(self.wav_dir, wav_name)
                    cmd = [
                        "ffmpeg", "-y", "-i", ts_path,
                        "-vn", "-ac", "1", "-ar", "16000",
                        wav_path
                    ]
                    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

                    start_ts = self._parse_ts(fname)
                    # **bloklanan put**: queue full olduqda burda dayanacaq
                    try:
                        # sıranı bloklamadan doldurmağa çalışır
                        self.wav_queue.put_nowait((self.channel.id, wav_path, start_ts))
                    except queue.Full:
                        # əgər sıra doludursa, heç bloklamadan xəbərdarlıq yazır və seqment itir
                        logger.warning("[%s] Queue dolu, seqment atlandı: %s",
                                    self.channel.id, wav_path)
                    logger.info("[%s] WAV queued (q=%d): %s",
                                self.channel.id,
                                self.wav_queue.qsize(),
                                wav_path)

                    self._processed.add(fname)
                # qısa fasilə, sonra yenidən yoxla
                time.sleep(0.1)
            except Exception as e:
                logger.error("[%s] Watcher error: %s", self.channel.id, e)

    def _parse_ts(self, fname):
        try:
            t = fname.split("_", 1)[1].rsplit(".", 1)[0]  # 20250721T143236
            dt = datetime.datetime.strptime(t, "%Y%m%dT%H%M%S")

            # settings.timezone varsa onu istifadə et, yoxdursa UTC
            tz_name = getattr(self.settings, "timezone", "UTC")
            try:
                tz = ZoneInfo(tz_name)
            except Exception:
                tz = ZoneInfo("UTC")

            # Fayl adı lokal vaxtdır → əvvəlcə həmin TZ ver, sonra UTC epoch-a çevir
            local_dt = dt.replace(tzinfo=tz)
            utc_dt = local_dt.astimezone(datetime.timezone.utc)
            return utc_dt.timestamp()

        except Exception:
            return time.time()

    def stop(self):
        """Watcher-i təmiz dayandırır və ffmpeg prosesini söndürür."""
        # Stop siqnalı göndər
        self._shutdown.set()
        # Thread bitməsini gözlə (max 1 saniyə)
        if self._watcher:
            self._watcher.join(timeout=1)

        # FFmpeg prosesini dayandır
        if self._ts_proc is not None:
            try:
                if self._ts_proc.poll() is None:
                    logger.info("[%s] Stopping TS archiver (pid=%s)", self.channel.id, self._ts_proc.pid)
                    self._ts_proc.terminate()
                    try:
                        self._ts_proc.wait(timeout=1)
                    except Exception:
                        self._ts_proc.kill()
                        try:
                            self._ts_proc.wait(timeout=1)
                        except Exception:
                            pass
            finally:
                self._ts_proc = None

    def resume(self):
        """
        SchedulerManager.enable_all çağıranda işə düşür.
        Lazım gələrsə həm TS seqmenting, həm watcher yenidən start edir.
        """
        # Əgər artıq işləyirsə, start_* metodları artıq check edir
        self.start_ts()
        self.start_watcher()
