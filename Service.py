#!/usr/bin/env python3
import os
import logging
import threading
import signal
import sys
import time

from app.config import Settings
from app.services.archiver import Archiver
from app.services.transcriber import Transcriber
from app.services.db import DBClient

# 1) Loglama səviyyəsini qururuq
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# 2) Konfiqurasiya obyektini yaradırıq
settings = Settings()

# 3) Xidmət komponentlərini ilkinizə edirik
archiver    = Archiver(settings)
transcriber = Transcriber(settings)
db_client   = DBClient(settings)

# 4) Archiver-i işə salırıq (TS + WAV)
archiver.start_ts()
archiver.start_wav()
logger.info("Xidmət başladı: TS archiver və WAV segmenter işə düşdü.")

# 5) Transkripsiya worker funksiyası
def transcription_worker():
    for wav_path, start_ts in archiver.wav_generator():
        logger.info("Worker: yeni WAV gəldi → %s", wav_path)
        try:
            segments = transcriber.transcribe(wav_path, start_ts)
            db_client.insert_segments(segments)
            logger.info("Worker: %d seqment DB-ə yazıldı", len(segments))
        except Exception as e:
            logger.error("Worker xəta: %s", e)

        # 6) İşlənən WAV faylını silirik
        try:
            os.remove(wav_path)
            logger.info("WAV silindi: %s", wav_path)
        except Exception as e:
            logger.warning("WAV silinərkən xəta: %s", e)

# 7) Worker thread-i daemon kimi işə salırıq
threading.Thread(target=transcription_worker, daemon=True).start()

# 8) Sinyal handler – Ctrl+C ilə shutdown
def shutdown(sig, frame):
    logger.info("Shutdown siqnalı alındı (%s), xidmət dayandırılır…", sig)
    archiver.stop()
    sys.exit(0)

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

# 9) Proqram blokda qalır
logger.info("Service hazırdır, siqnal gözlənilir...")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    shutdown(None, None)
