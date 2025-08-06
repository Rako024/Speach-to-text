#!/usr/bin/env python3
import os
import signal
import sys
import time
import logging
import queue
import subprocess

# ——————————————————————————————————————————————
# CUDA/cuDNN Diaqnostikası
# ——————————————————————————————————————————————
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# LD_LIBRARY_PATH yoxlayırıq
logger.debug(f"LD_LIBRARY_PATH = {os.environ.get('LD_LIBRARY_PATH')}")

# ldconfig -p siyahısını çıxarırıq
try:
    out = subprocess.check_output(["ldconfig", "-p"], stderr=subprocess.DEVNULL).decode("utf-8")
    logger.debug("ldconfig -p:\n" + out)
except Exception as e:
    logger.warning("ldconfig -p xətası: %s", e)

# Torch / CUDA / cuDNN versiya məlumatı
try:
    import torch
    logger.debug(f"torch.cuda.is_available(): {torch.cuda.is_available()}")
    logger.debug(f"torch.version.cuda: {torch.version.cuda}")
    logger.debug(f"torch.backends.cudnn.version(): {torch.backends.cudnn.version()}")
except ImportError as e:
    logger.error("Torch import xətası: %s", e)

# Hər bir libcudnn kitabxanasının yüklənmə testi
import ctypes
for lib in [
    "libcudnn.so.8",
    "libcudnn_ops_infer.so.8",
    "libcudnn_cnn_infer.so.8",
    "libcudnn_adv_infer.so.8"
]:
    try:
        ctypes.CDLL(lib)
        logger.debug(f"✅ {lib} yükləndi")
    except OSError as e:
        logger.error(f"❌ {lib} yüklənmədi: {e}")
# ——————————————————————————————————————————————
# Diaqnostika tamamlandı
# ——————————————————————————————————————————————

from prometheus_client import start_http_server
from apscheduler.schedulers.background import BackgroundScheduler
from concurrent.futures import ThreadPoolExecutor

from app.metrics              import PROCESSED, ERRORS, QUEUE_LEN, ACTIVE_WORKERS
from app.config               import Settings
from app.services.db          import DBClient
from app.services.transcriber import Transcriber
from app.services.archiver    import Archiver
from app.services.cleanup     import cleanup_old_ts
from app.scheduler_manager    import SchedulerManager


def init_worker():
    """Hər yeni prosess açıldıqda çağırılır: model və DB client yüklənir."""
    global transcriber_w, db_client_w
    s = Settings()
    transcriber_w = Transcriber(s)
    db_client_w  = DBClient(s)


def worker_process_segment(args):
    """Prosess daxilində iş görən funksiya."""
    ch_id, wav_path, start_ts = args
    QUEUE_LEN.labels(channel=ch_id).dec()
    ACTIVE_WORKERS.labels(channel=ch_id).inc()
    try:
        raw = transcriber_w.transcribe(wav_path, start_ts)
        segments = [{"channel_id": ch_id, **r} for r in raw]
        db_client_w.insert_segments(segments)
        PROCESSED.labels(channel=ch_id).inc(len(segments))
        logging.getLogger().info(f"[{ch_id}] Written {len(segments)} segs")
    except Exception as e:
        ERRORS.labels(channel=ch_id).inc()
        logging.getLogger().error(f"[{ch_id}] Transcribe error: {e}")
    finally:
        try: os.remove(wav_path)
        except: pass
        ACTIVE_WORKERS.labels(channel=ch_id).dec()


def process_segment(ch_id, wav_path, start_ts, transcriber, db):
    """ThreadPoolExecutor içindən çağırılan funksiya."""
    QUEUE_LEN.labels(channel=ch_id).dec()
    ACTIVE_WORKERS.labels(channel=ch_id).inc()
    try:
        raw = transcriber.transcribe(wav_path, start_ts)
        segments = [{"channel_id": ch_id, **r} for r in raw]
        db.insert_segments(segments)
        PROCESSED.labels(channel=ch_id).inc(len(segments))
        logging.getLogger().info(f"[{ch_id}] Written {len(segments)} segs")
    except Exception as e:
        ERRORS.labels(channel=ch_id).inc()
        logging.getLogger().error(f"[{ch_id}] Transcribe error: {e}")
    finally:
        try: os.remove(wav_path)
        except: pass
        ACTIVE_WORKERS.labels(channel=ch_id).dec()


def get_free_gpu_memory():
    """nvidia-smi-dən birinci GPU-nun boş yaddaşını MB ilə qaytarır."""
    try:
        out = subprocess.check_output([
            "nvidia-smi", "--query-gpu=memory.free",
            "--format=csv,nounits,noheader"], encoding="utf-8")
        return int(out.splitlines()[0])
    except Exception:
        return 0


def main():
    # 1) Settings & Logging
    settings = Settings()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logger = logging.getLogger()

    # 2) Prometheus metrics server
    start_http_server(8001)
    logger.info("Prometheus metrics server started on :8001")

    # 3) APScheduler qurulumu
    scheduler = BackgroundScheduler(timezone=settings.timezone)

    # 3a) daily .ts cleanup
    scheduler.add_job(
        cleanup_old_ts,
        trigger="cron",
        hour=settings.cleanup_hour,
        minute=settings.cleanup_minute,
        id="cleanup_old_ts",
        replace_existing=True
    )
    # 3b) APScheduler üçün dummy date-trigger
    scheduler.add_job(lambda: None, trigger="date", run_date=None)

    # 3c) hər dəqiqə 3 dəqiqədən köhnə .wav fayllarını silən job
    def cleanup_old_wavs():
        now = time.time()
        max_age = 3 * 60   # 3 dəqiqə
        for ch in settings.channels:
            wav_dir = os.path.join(settings.wav_base, ch.id)
            if not os.path.isdir(wav_dir):
                continue
            for fname in os.listdir(wav_dir):
                if not fname.lower().endswith(".wav"):
                    continue
                path = os.path.join(wav_dir, fname)
                if now - os.path.getmtime(path) > max_age:
                    try:
                        os.remove(path)
                        logger.debug("Removed old WAV %s/%s", ch.id, fname)
                    except Exception as e:
                        logger.warning("Could not remove old WAV %s/%s: %s", ch.id, fname, e)

    scheduler.add_job(
        cleanup_old_wavs,
        trigger="interval",
        minutes=1,
        id="cleanup_old_wavs",
        replace_existing=True
    )
    logger.info("Scheduled cleanup_old_wavs(): every minute, deleting >3min old files")
    scheduler.start()

    # 4) DB client & shared Transcriber
    db_client   = DBClient(settings)
    transcriber = Transcriber(settings)
    try:
        db_client.init_db()
        db_client.init_schedule_table()
        logger.info("DB tables initialized")
    except Exception as e:
        logger.error("DB init failed: %s", e)
        sys.exit(1)

    # 5) Dispatcher üçün bounded queue + ThreadPoolExecutor
    wav_queue = queue.Queue(maxsize=settings.max_queue_size)
    executor  = ThreadPoolExecutor(max_workers=settings.gpu_max_jobs)

    # 6) Archiver obyektləri yaradılır
    archivers = [
        Archiver(ch, settings, wav_queue)
        for ch in settings.channels
    ]
    for arch in archivers:
        logger.info("Archiver for channel %s created", arch.channel.id)

    # 7) SchedulerManager ilə interval-a görə enable/disable
    sched_mgr = SchedulerManager(scheduler, db_client, archivers)
    sched_mgr.load_and_schedule_intervals()
    scheduler.add_job(
        sched_mgr.load_and_schedule_intervals,
        trigger="interval",
        minutes=1,
        id="reload_intervals",
        replace_existing=True
    )
    logger.info("Scheduled reload_intervals every minute")

    # 8) Hər Archiver üçün TS segmentation və watcher start et
    for arch in archivers:
        arch.start_ts()
        arch.start_watcher()

    # 9) Dispatcher loop
    def shutdown(sig, frame):
        logger.info("Shutdown signal (%s) received, stopping…", sig)
        for arch in archivers:
            arch.stop()
        executor.shutdown(wait=False)
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info("Dispatcher running; waiting on queue…")
    try:
        while True:
            ch_id, wav_path, start_ts = wav_queue.get()
            QUEUE_LEN.labels(channel=ch_id).set(wav_queue.qsize())

            # boş GPU yaddaşı yoxla
            while get_free_gpu_memory() < settings.min_free_gpu_mb:
                logger.info(
                    "GPU boş yaddaş aşağıdı (%d MB), %d MB-a çatana qədər gözləyirəm",
                    get_free_gpu_memory(), settings.min_free_gpu_mb
                )
                time.sleep(1)

            executor.submit(
                process_segment,
                ch_id, wav_path, start_ts,
                transcriber,
                db_client
            )

    except (KeyboardInterrupt, SystemExit):
        shutdown("SIGINT", None)


if __name__ == "__main__":
    main()
