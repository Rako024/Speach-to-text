#!/usr/bin/env python3
import os, signal, sys, time, threading, logging

from prometheus_client import start_http_server, Counter, Gauge
from app.api.schemas import SegmentInfo
from app.config import Settings
from app.services.archiver import Archiver
from app.services.transcriber import Transcriber
from app.services.db import DBClient

# 1) Loglama konfiqurasiyası
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# 2) Prometheus metrikləri
PROCESSED = Counter('processed_segments_total', 'Emal olunmuş seqment sayı', ['channel'])
ERRORS    = Counter('worker_errors_total',      'Worker xətaları',       ['channel'])
QUEUE_LEN = Gauge('wav_queue_length',           'WAV queue uzunluğu',     ['channel'])

# 3) Prometheus server
start_http_server(8001)
logger.info("Prometheus metrics server started on :8001")

# 4) Servislər
settings    = Settings()
db_client   = DBClient(settings)
transcriber = Transcriber(settings)

# 5) DB sxemi
try:
    db_client.init_db()
    logger.info("DB uğurla yaradıldı və ya yoxlanıldı")
except Exception as e:
    logger.error("DB init xətası: %s", e)
    sys.exit(1)

archivers = []

# 6) Hər kanal üçün .ts → .wav watcher + worker-lar
for channel in settings.channels:
    arch = Archiver(channel, settings)
    arch.start_ts()       # TS seqmentlə
    arch.start_watcher()  # TS→WAV çevir və queue
    archivers.append(arch)
    logger.info("Archiver started for channel: %s", channel.id)

    def make_worker(a: Archiver):
        def worker():
            while True:
                ch_id, wav_path, start_ts = a.wav_generator().__next__()
                QUEUE_LEN.labels(channel=ch_id).set(a.wav_queue.qsize())
                logger.info("[%s] Processing WAV: %s", ch_id, wav_path)
                try:
                    raw_segs = transcriber.transcribe(wav_path, start_ts)
                    segments = [
                        SegmentInfo(channel_id       = ch_id,
                                    start_time       = r["start_time"],
                                    end_time         = r["end_time"],
                                    text             = r["text"],
                                    segment_filename = r["segment_filename"],
                                    offset_secs      = r["offset_secs"],
                                    duration_secs    = r["duration_secs"])
                        for r in raw_segs
                    ]
                    db_client.insert_segments(segments)
                    PROCESSED.labels(channel=ch_id).inc(len(segments))
                    logger.info("[%s] %d seqment bazaya yazıldı", ch_id, len(segments))
                except Exception as e:
                    logger.error("[%s] Worker xətası: %s", ch_id, e)
                    ERRORS.labels(channel=ch_id).inc()
                finally:
                    try:
                        os.remove(wav_path)
                        logger.debug("[%s] WAV silindi: %s", ch_id, wav_path)
                    except Exception:
                        logger.warning("[%s] WAV silinə bilmədi: %s", ch_id, wav_path)
        return threading.Thread(target=worker, daemon=True)

    # 2 paralel worker
    for _ in range(2):
        t = make_worker(arch)
        t.start()

# 7) Siqnal handler
def shutdown(sig, frame):
    logger.info("Shutdown signal (%s) alındı, dayandırılır…", sig)
    for a in archivers:
        a.stop()
    sys.exit(0)

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

logger.info("Workers işləyir; siqnal gözlənilir…")
while True:
    time.sleep(1)
