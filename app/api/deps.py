# app/api/deps.py
from __future__ import annotations
from typing import Optional
from app.services.storage import WasabiClient
import os
from typing import Optional
from zoneinfo import ZoneInfo
from apscheduler.schedulers.background import BackgroundScheduler

from app.config import Settings
from app.services.db import DBClient
from app.services.summarizer import DeepSeekClient
from app.scheduler_manager import SchedulerManager

# -------------------------------------------------
# Settings
# -------------------------------------------------
settings = Settings()

# -------------------------------------------------
# DB client (singleton) + cədvəllərin init-i
# -------------------------------------------------
_db_client: DBClient = DBClient(settings)
_db_client.init_db()
_db_client.init_schedule_table()
_storage: Optional[WasabiClient] = None
def get_db() -> DBClient:
    """FastAPI dependency: shared DB client."""
    return _db_client

# -------------------------------------------------
# DeepSeek client (lazy singleton)
# -------------------------------------------------
_summ_client: Optional[DeepSeekClient] = None

def get_summarizer() -> DeepSeekClient:
    """FastAPI dependency: DeepSeek client (lazy)."""
    global _summ_client
    if _summ_client is None:
        _summ_client = DeepSeekClient(settings)
    return _summ_client

# -------------------------------------------------
# Scheduler (lazy + optional start via env)
# - API prosesində birdən çox nüsxənin qarşısını almaq üçün
#   yalnız RUN_SCHEDULER_IN_API=1 olduqda start edilir.
# - Adətən scheduler-i main.py (worker) idarə edir.
# -------------------------------------------------
_scheduler: Optional[BackgroundScheduler] = None
_sched_mgr: Optional[SchedulerManager] = None

def get_scheduler_manager() -> SchedulerManager:
    """FastAPI dependency: SchedulerManager (lazy)."""
    global _scheduler, _sched_mgr
    if _sched_mgr is not None:
        return _sched_mgr

    tz_name = getattr(settings, "timezone", "UTC")
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")

    _scheduler = BackgroundScheduler(timezone=tz)

    # Yalnız env=1 olduqda API prosesində scheduler-i start et
    if os.getenv("RUN_SCHEDULER_IN_API", "0") == "1":
        _scheduler.start()

    _sched_mgr = SchedulerManager(_scheduler, _db_client, archivers=[])
    return _sched_mgr


def get_storage() -> Optional[WasabiClient]:
    global _storage
    if not getattr(settings, "wasabi_upload_enabled", False):
        return None
    if _storage is None:
        try:
            _storage = WasabiClient(settings)
        except Exception:
            _storage = None
    return _storage
