# app/services/archiver.py
#!/usr/bin/env python3
import os
import time
import threading
import subprocess
import datetime
import logging
import queue
import tempfile
import errno
from typing import Optional
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor

from app.services.storage import WasabiClient

logger = logging.getLogger(__name__)

WORKSPACE_ROOT = os.getenv("WORKSPACE_ROOT", "/workspace")

def _safe_remove(path: str, retries: int = 15, delay: float = 0.2) -> bool:
    """Faylı etibarlı sil (Windows kilid halları üçün retry-lə)."""
    for _ in range(retries):
        try:
            os.remove(path)
            return True
        except FileNotFoundError:
            return True
        except OSError as e:
            if getattr(e, "winerror", None) == 32 or e.errno in (errno.EACCES, errno.EBUSY):
                time.sleep(delay)
                continue
            else:
                break
    return False

def _get_ts_root(settings) -> str:
    """
    TS staging directory seçimi:
      - settings.ts_staging_dir verilmişsə → DƏQİQ HƏMİN QOVLUQ
      - verilməyibsə və ya boşdursa → OS tmp (tempfile.gettempdir()).
    """
    tsd = getattr(settings, "ts_staging_dir", None)
    if tsd and str(tsd).strip():
        return str(tsd).strip()
    return tempfile.gettempdir()

def _resolve_under_workspace(path_like: Optional[str], default_subdir: str) -> str:
    """
    Nisbi yol gəlirsə /workspace altında qur, absolute-dursa olduğu kimi saxla.
    (Mailə görə WAV_BASE çox vaxt 'wav_segments' olur → /workspace/wav_segments)
    """
    if path_like and os.path.isabs(path_like):
        base = path_like
    elif path_like and str(path_like).strip():
        base = os.path.join(WORKSPACE_ROOT, str(path_like).strip())
    else:
        base = os.path.join(WORKSPACE_ROOT, default_subdir)
    return os.path.abspath(base)

class Archiver:
    def __init__(self, channel, settings, wav_queue: queue.Queue):
        self.channel  = channel
        self.settings = settings

        # ❗ YENİ: TS staging logic razılaşmaya uyğun
        base_dir = _get_ts_root(settings)

        # Yollar
        self.archive_dir = os.path.abspath(os.path.join(base_dir, channel.id))
        self.wav_dir     = os.path.abspath(os.path.join(
            _resolve_under_workspace(getattr(settings, "wav_base", "wav_segments"), "wav_segments"),
            channel.id
        ))
        self.ts_seg_time = int(settings.ts_segment_time)
        self.wav_queue   = wav_queue

        # daxili vəziyyət
        self._watcher: Optional[threading.Thread] = None
        self._shutdown = threading.Event()
        self._processed: set[str] = set()
        self._ts_proc: Optional[subprocess.Popen] = None
        self._monitor: Optional[threading.Thread] = None
        self._ffmpeg_log_path: Optional[str] = None

        # Wasabi
        self._storage: Optional[WasabiClient] = None
        self._uploader: Optional[ThreadPoolExecutor] = None
        if getattr(self.settings, "wasabi_upload_enabled", False):
            try:
                self._storage = WasabiClient(self.settings)
                self._uploader = ThreadPoolExecutor(max_workers=2)
                logger.info("[%s] Wasabi uploader enabled", self.channel.id)
            except Exception as e:
                logger.warning("[%s] Wasabi disabled: %s", self.channel.id, e)
                self._storage = None
                self._uploader = None

        # Kanal header-ları (User-Agent, Referer, və s.)
        self._header_args = self._build_header_args(getattr(self.channel, "headers", None))

        logger.debug("[%s] archive_dir=%s wav_dir=%s", self.channel.id, self.archive_dir, self.wav_dir)

    # ----------------------------- Public API -----------------------------

    def resume(self):
        """SchedulerManager.enable_all → işə sal."""
        self.start_ts()
        self.start_watcher()

    def stop(self):
        """Watcher-i və ffmpeg-i təmiz dayandır."""
        self._shutdown.set()
        if self._watcher and self._watcher.is_alive():
            try: self._watcher.join(timeout=1)
            except Exception: pass

        if self._monitor and self._monitor.is_alive():
            try: self._monitor.join(timeout=1)
            except Exception: pass

        if self._ts_proc is not None:
            try:
                if self._ts_proc.poll() is None:
                    logger.info("[%s] Stopping TS archiver (pid=%s)", self.channel.id, self._ts_proc.pid)
                    self._ts_proc.terminate()
                    try:
                        self._ts_proc.wait(timeout=2)
                    except Exception:
                        self._ts_proc.kill()
                        try: self._ts_proc.wait(timeout=2)
                        except Exception: pass
            finally:
                self._ts_proc = None

        if self._uploader:
            try: self._uploader.shutdown(wait=False)
            except Exception: pass

    # ----------------------------- Internals -----------------------------

    def start_ts(self):
        """FFmpeg ilə HLS → .ts seqmentləri (segment muxer)."""
        os.makedirs(self.archive_dir, exist_ok=True)

        # artıq işləyirsə, bir də açma
        if self._ts_proc is not None and self._ts_proc.poll() is None:
            logger.debug("[%s] TS archiver already running (pid=%s)", self.channel.id, self._ts_proc.pid)
        else:
            self._spawn_ts_proc()

        # monitor thread: ffmpeg ölərsə, avtomatik yenidən başlat
        if (self._monitor is None) or (not self._monitor.is_alive()):
            self._monitor = threading.Thread(target=self._monitor_loop, daemon=True)
            self._monitor.start()

    def _spawn_ts_proc(self):
        pattern = os.path.join(self.archive_dir, f"{self.channel.id}_%Y%m%dT%H%M%S.ts")

        cmd = [
            "ffmpeg",
            "-hide_banner", "-loglevel", os.getenv("FFMPEG_LOGLEVEL", "info"),
            "-nostats",
            "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "10",
            "-rw_timeout", "15000000",  # ~15s (mikrosaniyə)
        ]
        cmd += self._header_args
        cmd += [
            "-y", "-i", self.channel.hls_url,
            "-c", "copy",
            "-f", "segment",
            "-segment_time", str(self.ts_seg_time),
            "-reset_timestamps", "1",
            "-strftime", "1",
            pattern
        ]

        logger.info("[%s] Starting TS archiver", self.channel.id)
        self._ffmpeg_log_path = f"/tmp/ffmpeg_{self.channel.id}.log"
        try:
            logf = open(self._ffmpeg_log_path, "a", buffering=1)
        except Exception:
            logf = subprocess.DEVNULL  # son çarə

        self._ts_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=logf
        )
        logger.debug("[%s] TS cmd: %s", self.channel.id, " ".join(cmd))
        logger.debug("[%s] ffmpeg started (pid=%s, log=%s)", self.channel.id, getattr(self._ts_proc, "pid", None), self._ffmpeg_log_path)

    def _monitor_loop(self):
        """ffmpeg prosesini izləyir; qapanarsa backoff ilə yenidən açır."""
        backoff = 2
        while not self._shutdown.is_set():
            if self._ts_proc is None or (self._ts_proc.poll() is not None):
                rc = None if self._ts_proc is None else self._ts_proc.returncode
                logger.warning("[%s] ffmpeg exited (rc=%s). Restarting in %ss…", self.channel.id, rc, backoff)
                time.sleep(backoff)
                if backoff < 30:
                    backoff = min(30, backoff * 2)
                self._spawn_ts_proc()
            else:
                backoff = 2
                time.sleep(3)

    def start_watcher(self):
        """Yeni .ts faylları → .wav çevril, queue-ya at, .ts-i (varsa) Wasabi-yə yüklə."""
        self._shutdown.clear()
        if self._watcher and self._watcher.is_alive():
            return

        os.makedirs(self.wav_dir, exist_ok=True)

        # startup-da qalan wav-ları təmizlə (sadə)
        for f in os.listdir(self.wav_dir):
            if f.lower().endswith(".wav"):
                try: os.remove(os.path.join(self.wav_dir, f))
                except Exception: pass

        # ❗ YENİ: mövcud .ts-ləri processed-ə ATMIRIQ — hamısını emal edəcəyik
        os.makedirs(self.archive_dir, exist_ok=True)
        self._processed = set()

        self._watcher = threading.Thread(target=self._watch_loop, daemon=True)
        self._watcher.start()

    def _watch_loop(self):
        del_retries = int(getattr(self.settings, "wasabi_delete_retries", 15))
        del_delay_s = float(getattr(self.settings, "wasabi_delete_delay_ms", 200)) / 1000.0
        grace_s     = float(getattr(self.settings, "wasabi_post_upload_delete_grace_ms", 250)) / 1000.0

        while not self._shutdown.is_set():
            try:
                # stabil sıralama
                for fname in sorted(os.listdir(self.archive_dir)):
                    if not fname.endswith(".ts") or fname in self._processed:
                        continue

                    ts_path = os.path.join(self.archive_dir, fname)

                    # Fayl tamamlandı mı? (ölçü sabitlənsin)
                    prev = -1
                    while True:
                        try:
                            sz = os.path.getsize(ts_path)
                        except FileNotFoundError:
                            sz = -1
                        if sz == prev and sz > 0:
                            break
                        prev = sz
                        time.sleep(0.05)

                    # WAV-a çevir
                    wav_name = fname[:-3] + ".wav"
                    wav_path = os.path.join(self.wav_dir, wav_name)

                    cmd = [
                        "ffmpeg", "-hide_banner", "-loglevel", "warning",
                        "-y", "-i", ts_path,
                        "-vn", "-ac", "1", "-ar", "16000",
                        wav_path
                    ]
                    rc = subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    if rc != 0:
                        logger.error("[%s] WAV convert failed (rc=%s): %s", self.channel.id, rc, ts_path)
                        # bu TS-i işlənmiş kimi qeyd etməyək – növbəti iterasiyada yenə cəhd olunsun
                        time.sleep(0.1)
                        continue

                    # Dispatcher növbəsinə at
                    start_ts = self._parse_ts(fname)
                    try:
                        self.wav_queue.put_nowait((self.channel.id, wav_path, start_ts))
                    except queue.Full:
                        logger.warning("[%s] Queue dolu, seqment atlandı: %s", self.channel.id, wav_path)
                        # ❗ YENİ: disk dolmasın deyə yaranmış WAV-ı dərhal sil
                        try: os.remove(wav_path)
                        except Exception: pass
                        # processed-ə əlavə etmirik ki, növbəti dövrdə yenə cəhd etsin
                    else:
                        logger.info("[%s] WAV queued (q=%d): %s", self.channel.id, self.wav_queue.qsize(), wav_path)
                        self._processed.add(fname)

                        # Wasabi-yə .ts upload (asinxron)
                        if self._storage and self._uploader:
                            key = f"{self.channel.id}/{fname}"

                            def _do_upload(local_path: str, s3_key: str):
                                try:
                                    self._storage.upload_file(local_path, s3_key, content_type="video/mp2t")
                                except Exception as e:
                                    logger.error("[%s] Wasabi upload FAILED for %s: %s", self.channel.id, s3_key, e)
                                    return

                                if getattr(self.settings, "wasabi_delete_local_after_upload", True) and os.path.exists(local_path):
                                    time.sleep(grace_s)
                                    if _safe_remove(local_path, retries=del_retries, delay=del_delay_s):
                                        logger.debug("[%s] Local TS removed after upload: %s", self.channel.id, local_path)
                                    else:
                                        logger.warning("[%s] Could not remove TS after retries: %s", self.channel.id, local_path)

                            self._uploader.submit(_do_upload, ts_path, key)

                time.sleep(0.1)

            except Exception as e:
                logger.error("[%s] Watcher error: %s", self.channel.id, e)
                time.sleep(0.5)

    # ----------------------------- Helpers -----------------------------

    def _build_header_args(self, headers: Optional[dict]) -> list[str]:
        """FFmpeg üçün -user_agent və -headers parametrinə çevrilmə."""
        args: list[str] = []
        if not headers:
            return args

        # User-Agent ayrıca flag
        for k in list(headers.keys()):
            if k.lower() == "user-agent":
                ua = headers.pop(k)
                if ua:
                    args += ["-user_agent", str(ua)]
                break

        # qalan başlıqları -headers-ə yaz (CRLF ilə)
        if headers:
            lines = [f"{k}: {v}" for k, v in headers.items() if v is not None]
            if lines:
                # ffmpeg CRLF tələb edir
                args += ["-headers", "\\r\\n".join(lines) + "\\r\\n"]
        return args

    def _parse_ts(self, fname: str) -> float:
        """fname: <channel>_YYYYmmddTHHMMSS.ts → UTC timestamp."""
        try:
            t = fname.split("_", 1)[1].rsplit(".", 1)[0]
            dt = datetime.datetime.strptime(t, "%Y%m%dT%H%M%S")
            tz_name = getattr(self.settings, "timezone", "UTC")
            try:
                tz = ZoneInfo(tz_name)
            except Exception:
                tz = ZoneInfo("UTC")
            local_dt = dt.replace(tzinfo=tz)
            utc_dt = local_dt.astimezone(datetime.timezone.utc)
            return utc_dt.timestamp()
        except Exception:
            return time.time()
