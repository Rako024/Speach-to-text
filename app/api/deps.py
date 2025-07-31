# app/api/deps.py

from app.config import Settings
from app.services.db import DBClient
from app.services.summarizer import DeepSeekClient
from app.scheduler_manager import SchedulerManager
from apscheduler.schedulers.background import BackgroundScheduler

# Load settings
settings = Settings()

# Initialize DB client and ensure tables exist
_db_client = DBClient(settings)
_db_client.init_db()
_db_client.init_schedule_table()

# Initialize DeepSeek client lazily
_summ_client = None

# Dependency: get DB client
def get_db() -> DBClient:
    return _db_client

# Dependency: get DeepSeek client
def get_summarizer() -> DeepSeekClient:
    global _summ_client
    if _summ_client is None:
        _summ_client = DeepSeekClient(settings)
    return _summ_client

# Set up scheduler for intervals
# Use TIMEZONE from settings if available, else default to UTC
tz = getattr(settings, 'timezone', 'UTC')
scheduler = BackgroundScheduler(timezone=tz)
scheduler.start()

# Create SchedulerManager with no archivers (API only reloads jobs)
sched_mgr = SchedulerManager(scheduler, _db_client, archivers=[])

# Dependency: get SchedulerManager
def get_scheduler_manager() -> SchedulerManager:
    return sched_mgr
