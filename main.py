#!/usr/bin/env python3
import os
import signal
import sys
import time
import threading
import logging

from prometheus_client import start_http_server, Counter, Gauge
from apscheduler.schedulers.background import BackgroundScheduler

from app.api.schemas import SegmentInfo
from app.config import Settings
from app.services.archiver import Archiver
from app.services.transcriber import Transcriber
from app.services.db import DBClient
from app.services.cleanup import cleanup_old_ts
from app.scheduler_manager import SchedulerManager

# 1) Settings & Logging
settings = Settings()
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# 2) Prometheus metrics
PROCESSED = Counter('processed_segments_total', 'Emal olunmuş seqment sayı', ['channel'])
ERRORS    = Counter('worker_errors_total',      'Worker xətaları',       ['channel'])
QUEUE_LEN = Gauge('wav_queue_length',           'WAV queue uzunluğu',     ['channel'])

# 3) Start Prometheus HTTP server
start_http_server(8001)
logger.info("Prometheus metrics server started on :8001")

# 4) Scheduler (for cleanup + dynamic intervals)
scheduler = BackgroundScheduler(timezone=settings.timezone)  # e.g. "Asia/Baku"
# daily cleanup job
scheduler.add_job(
    cleanup_old_ts, 'cron',
    hour   = settings.cleanup_hour,
    minute = settings.cleanup_minute
)
scheduler.start()
logger.info(
    "Scheduled cleanup_old_ts(): every day at %02d:%02d %s, retaining %d days",
    settings.cleanup_hour,
    settings.cleanup_minute,
    settings.timezone,
    settings.cleanup_retention_days
)

# 5) DB client & transcriber
db_client   = DBClient(settings)
transcriber = Transcriber(settings)

# 6) Initialize DB schemas
try:
    db_client.init_db()
    db_client.init_schedule_table()   # ← yeni: schedule_intervals cədvəlini yaradır
    logger.info("DB tables initialized")
except Exception as e:
    logger.error("DB init error: %s", e)
    sys.exit(1)

# 7) Create Archiver instances (amma start_ts/start_watcher yalnız enable zamanı işə düşəcək)
archivers = []
for channel in settings.channels:
    arch = Archiver(channel, settings)
    archivers.append(arch)
    logger.info("Archiver created for channel: %s", channel.id)

    # worker factory
    def make_worker(a: Archiver):
        def worker():
            while True:
                ch_id, wav_path, start_ts = a.wav_generator().__next__()
                # yeni: yoxla fayl hələ də disklərdədirsə…
                if not os.path.exists(wav_path):
                    logger.warning("[%s] WAV tapılmadı, ötüşdürülür: %s", ch_id, wav_path)
                    continue

                QUEUE_LEN.labels(channel=ch_id).set(a.wav_queue.qsize())
                logger.info("[%s] Processing WAV: %s", ch_id, wav_path)
                try:
                    raw_segs = transcriber.transcribe(wav_path, start_ts)
                    segments = [
                        SegmentInfo(
                            channel_id       = ch_id,
                            start_time       = r["start_time"],
                            end_time         = r["end_time"],
                            text             = r["text"],
                            segment_filename = r["segment_filename"],
                            offset_secs      = r["offset_secs"],
                            duration_secs    = r["duration_secs"]
                        )
                        for r in raw_segs
                    ]
                    db_client.insert_segments(segments)
                    PROCESSED.labels(channel=ch_id).inc(len(segments))
                    logger.info("[%s] %d seqment bazaya yazıldı", ch_id, len(segments))
                except Exception as e:
                    logger.error("[%s] Worker error: %s", ch_id, e)
                    ERRORS.labels(channel=ch_id).inc()
                finally:
                    try:
                        os.remove(wav_path)
                        logger.debug("[%s] WAV removed: %s", ch_id, wav_path)
                    except Exception:
                        logger.warning("[%s] Could not remove WAV: %s", ch_id, wav_path)
        return threading.Thread(target=worker, daemon=True)

    # iki işçi hər archiver üçün
    for _ in range(2):
        t = make_worker(arch)
        t.start()

# 8) SchedulerManager for dynamic on/off
sched_mgr = SchedulerManager(scheduler, db_client, archivers)
sched_mgr.load_and_schedule_intervals()  # ← DB-dən intervaları yükləyir və cron-job-lar planlayır

# Yeni: hər dəqiqə DB-dən intervaları yenidən yükləyəcək,
# beləcə /schedule/ ilə edilən dəyişikliklər real vaxtda tətbiq olunur
scheduler.add_job(
    sched_mgr.load_and_schedule_intervals,
    trigger='interval',
    minutes=1,
    id='reload_intervals',
    replace_existing=True
)
logger.info("Added reload_intervals job: will re-load schedule every minute")

logger.info("SchedulerManager loaded intervals; system will auto-enable/disable archiving per schedule.")

# 9) Signal handler for graceful shutdown
def shutdown(sig, frame):
    logger.info("Shutdown signal (%s) received, stopping…", sig)
    for arch in archivers:
        arch.stop()
    sys.exit(0)

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

logger.info("Worker process running; waiting for scheduled enables/disables…")
while True:
    time.sleep(1)
