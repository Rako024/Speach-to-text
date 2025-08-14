#!/usr/bin/env python3
import os
import signal
import sys
import time
import logging
import queue
import subprocess

# .env faylını ən əvvəl yüklə ki, bütün os.getenv çağırışları və Settings bundan faydalansın
from prometheus_client import start_http_server
from apscheduler.schedulers.background import BackgroundScheduler
from concurrent.futures import ThreadPoolExecutor

from app.metrics              import PROCESSED, ERRORS, QUEUE_LEN, ACTIVE_WORKERS
from app.config               import Settings
from app.services.db          import DBClient
from app.services.transcriber import Transcriber
from app.services.archiver    import Archiver, _get_ts_root
from app.services.cleanup     import cleanup_old_ts, cleanup_local_ts
from app.scheduler_manager    import SchedulerManager
from dotenv import load_dotenv
load_dotenv(override=False)
executor = None
wav_queue = None
# ——————————————————————————————————————————————
# Logging konfiqurasiyası (ENV ilə idarə)
# ——————————————————————————————————————————————
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger("nintel")

# ——————————————————————————————————————————————
# CUDA/cuDNN Diaqnostikası (yalnız NINTEL_DIAG=1 olduqda)
# ——————————————————————————————————————————————
if os.getenv("NINTEL_DIAG", "0") == "1":
    logger.debug(f"LD_LIBRARY_PATH = {os.environ.get('LD_LIBRARY_PATH')}")
    try:
        out = subprocess.check_output(["ldconfig", "-p"], stderr=subprocess.DEVNULL).decode("utf-8")
        logger.debug("ldconfig -p:\n" + out)
    except Exception as e:
        logger.warning("ldconfig -p xətası: %s", e)

    try:
        import importlib.util as _iu
        if _iu.find_spec("torch") is not None:
            import torch  # type: ignore[import-not-found]
            logger.debug(f"torch.cuda.is_available(): {torch.cuda.is_available()}")
            logger.debug(f"torch.version.cuda: {getattr(torch.version, 'cuda', None)}")
            cudnn_ver = None
            try:
                cudnn_ver = getattr(torch.backends.cudnn, "version", lambda: None)()
            except Exception:
                pass
            logger.debug(f"torch.backends.cudnn.version(): {cudnn_ver}")
        else:
            logger.debug("torch tapılmadı; PyTorch diaqnostikası ötürülür")
    except Exception as e:
        logger.debug("PyTorch diaqnostikası atlandı: %s", e)

    try:
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
    except Exception as e:
        logger.debug("cuDNN kitabxana yoxlaması atlandı: %s", e)
# ——————————————————————————————————————————————
# Diaqnostika tamamlandı (yalnız NINTEL_DIAG=1)
# ——————————————————————————————————————————————



def init_worker():
    """Hər yeni prosess açıldıqda çağırılır: model və DB client yüklənir."""
    global transcriber_w, db_client_w
    s = Settings()
    transcriber_w = Transcriber(s)
    db_client_w   = DBClient(s)


def worker_process_segment(args):
    """Multiprocessing üçün nümunə worker (hazırda istifadə olunmur)."""
    ch_id, wav_path, start_ts = args
    ACTIVE_WORKERS.labels(channel=ch_id).inc()
    try:
        raw = transcriber_w.transcribe(wav_path, start_ts)
        segments = [{"channel_id": ch_id, **r} for r in raw]
        db_client_w.insert_segments(segments)
        PROCESSED.labels(channel=ch_id).inc(len(segments))
        logger.info(f"[{ch_id}] Written {len(segments)} segs")
    except Exception as e:
        ERRORS.labels(channel=ch_id).inc()
        logger.error(f"[{ch_id}] Transcribe error: {e}")
    finally:
        try:
            os.remove(wav_path)
        except Exception:
            pass
        ACTIVE_WORKERS.labels(channel=ch_id).dec()


def process_segment(ch_id, wav_path, start_ts, transcriber, db):
    """ThreadPoolExecutor içindən çağırılan funksiya."""
    ACTIVE_WORKERS.labels(channel=ch_id).inc()
    try:
        raw = transcriber.transcribe(wav_path, start_ts)
        segments = [{"channel_id": ch_id, **r} for r in raw]
        db.insert_segments(segments)
        PROCESSED.labels(channel=ch_id).inc(len(segments))
        logger.info(f"[{ch_id}] Written {len(segments)} segs")
    except Exception as e:
        ERRORS.labels(channel=ch_id).inc()
        logger.error(f"[{ch_id}] Transcribe error: {e}")
    finally:
        try:
            os.remove(wav_path)
        except Exception:
            pass
        ACTIVE_WORKERS.labels(channel=ch_id).dec()


def get_free_gpu_memory():
    """nvidia-smi-dən birinci GPU-nun boş yaddaşını MB ilə qaytarır; xəta olarsa None."""
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=memory.free", "--format=csv,nounits,noheader"],
            encoding="utf-8"
        )
        line = out.splitlines()[0].strip()
        return int(line)
    except Exception:
        return None


def main():
    global executor, wav_queue  # global dəyişənləri istifadə edirik

    # 1) Settings
    settings = Settings()

    try:
        ts_root = _get_ts_root(settings)
        os.makedirs(ts_root, exist_ok=True)
        logger.info("TS root ensured at: %s", ts_root)
    except Exception as e:
        logger.warning("Could not ensure TS root: %s", e)
        ts_root = "<unknown>"

    logger.info(
        "TS cleanup cfg: interval=%d min, max_age=%d min, ts_root=%s",
        settings.ts_local_clean_interval_min,
        settings.ts_local_max_age_min,
        ts_root,
    )

    # 2) Prometheus metrics
    start_http_server(8001)
    logger.info("Prometheus metrics server started on :8001")

    # 3) APScheduler
    scheduler = BackgroundScheduler(timezone=settings.timezone)

    scheduler.add_job(
        cleanup_old_ts,
        trigger="cron",
        hour=settings.cleanup_hour,
        minute=settings.cleanup_minute,
        id="cleanup_old_ts",
        replace_existing=True
    )

    scheduler.add_job(
        lambda: cleanup_local_ts(settings.ts_local_max_age_min),
        trigger="interval",
        minutes=settings.ts_local_clean_interval_min,
        id="cleanup_local_ts",
        replace_existing=True
    )

    def cleanup_old_wavs():
        now = time.time()
        max_age = 3 * 60
        for ch in settings.channels:
            wav_dir = os.path.join(settings.wav_base, ch.id)
            if not os.path.isdir(wav_dir):
                continue
            for fname in os.listdir(wav_dir):
                if not fname.lower().endswith(".wav"):
                    continue
                path = os.path.join(wav_dir, fname)
                try:
                    if now - os.path.getmtime(path) > max_age:
                        os.remove(path)
                        logger.debug("Removed old WAV %s/%s", ch.id, fname)
                except Exception as e:
                    logger.warning("Could not remove WAV %s/%s: %s", ch.id, fname, e)

    scheduler.add_job(
        cleanup_old_wavs,
        trigger="interval",
        minutes=1,
        id="cleanup_old_wavs",
        replace_existing=True
    )

    scheduler.start()

    try:
        cleanup_local_ts(settings.ts_local_max_age_min)
    except Exception as e:
        logger.warning("Initial cleanup_local_ts failed: %s", e)

    # 4) DB client & Transcriber
    db_client = DBClient(settings)
    transcriber = Transcriber(settings)
    try:
        db_client.init_db()
        db_client.init_schedule_table()
        logger.info("DB tables initialized")
    except Exception as e:
        logger.error("DB init failed: %s", e)
        sys.exit(1)

    # 5) Dispatcher queue və executor
    wav_queue = queue.Queue(maxsize=settings.max_queue_size)
    executor = ThreadPoolExecutor(max_workers=settings.gpu_max_jobs)

    # 6) Archiver-lər
    archivers = [Archiver(ch, settings, wav_queue) for ch in settings.channels]
    for arch in archivers:
        logger.info("Archiver for channel %s created", arch.channel.id)

    # 7) Interval idarəçisi
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

    # 8) Shutdown funksiyası
    def shutdown(sig, frame):
        logger.info("Shutdown signal (%s) received, stopping…", sig)

        # 1) Bütün archiver-ləri dayandır (ffmpeg + watcher)
        for arch in archivers:
            arch.stop()

        # 2) Yalnız proqramdan çıxışda uploader-i bağla
        for arch in archivers:
            arch.close()   # uploader burada bağlanır

        # 3) Transcribe üçün qlobal executor-u bağla
        global executor
        try:
            if executor:
                executor.shutdown(wait=True, cancel_futures=False)
        except Exception as e:
            logger.warning("Executor shutdown warning: %s", e)

        # 4) Scheduler-i bağla
        try:
            scheduler.shutdown(wait=False)
        except Exception as e:
            logger.warning("Scheduler shutdown warning: %s", e)

        # 5) DB pool bağla
        try:
            db_client.close()
        except Exception as e:
            logger.warning("DB pool close warning: %s", e)

        sys.exit(0)


    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info("Dispatcher running; waiting on queue…")

    try:
        while True:
            ch_id, wav_path, start_ts = wav_queue.get()
            QUEUE_LEN.labels(channel=ch_id).set(wav_queue.qsize())

            use_gpu = (getattr(settings, "device", "cpu").lower() != "cpu")
            if use_gpu and settings.min_free_gpu_mb > 0:
                free_mb = get_free_gpu_memory()
                while (free_mb is not None) and (free_mb < settings.min_free_gpu_mb):
                    logger.info(
                        "GPU boş yaddaş %d MB; tələb olunan %d MB. Gözləyirəm...",
                        free_mb, settings.min_free_gpu_mb
                    )
                    time.sleep(1)
                    free_mb = get_free_gpu_memory()

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
