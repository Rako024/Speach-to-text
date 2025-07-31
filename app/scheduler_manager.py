import datetime
import logging
from apscheduler.schedulers.base import BaseScheduler
from app.services.db import DBClient
from app.services.archiver import Archiver

logger = logging.getLogger(__name__)

class SchedulerManager:
    def __init__(self, scheduler: BaseScheduler, db: DBClient, archivers: list[Archiver]):
        self.scheduler = scheduler
        self.db        = db
        self.archivers = archivers
        # Cari vəziyyəti yadda saxlayırıq:
        self._enabled  = False

    def clear_interval_jobs(self):
        # Keçmiş enable_/disable_ job-ları sil
        for job in self.scheduler.get_jobs():
            if job.id.startswith("enable_") or job.id.startswith("disable_"):
                self.scheduler.remove_job(job.id)

    def load_and_schedule_intervals(self):
        # 1) Keçmiş job-ları sil
        self.clear_interval_jobs()

        # 2) DB-dən intervaları götür
        intervals = self.db.get_intervals()

        # 3) Hər interval üçün enable/disable job planla (saniyə səviyyəsinə qədər)
        for interval in intervals:
            sid = interval.id
            sh, sm, ss = (
                interval.start_time.hour,
                interval.start_time.minute,
                interval.start_time.second
            )
            eh, em, es = (
                interval.end_time.hour,
                interval.end_time.minute,
                interval.end_time.second
            )

            # Enable job
            self.scheduler.add_job(
                func=self.enable_all,
                trigger='cron',
                hour=sh, minute=sm, second=ss,
                id=f"enable_{sid}"
            )
            # Disable job
            self.scheduler.add_job(
                func=self.disable_all,
                trigger='cron',
                hour=eh, minute=em, second=es,
                id=f"disable_{sid}"
            )
            logger.info(
                "Scheduled interval %d: %02d:%02d:%02d → %02d:%02d:%02d",
                sid, sh, sm, ss, eh, em, es
            )

        # 4) Startup zamanı cari vaxtı yoxlayıb dərhal tətbiq et
        now = datetime.datetime.now(self.scheduler.timezone).time()
        should_enable = False
        for interval in intervals:
            st, et = interval.start_time, interval.end_time
            if st <= et:
                # adi interval
                if st <= now < et:
                    should_enable = True
                    break
            else:
                # gecə yarısından sonra davam edən interval
                if now >= st or now < et:
                    should_enable = True
                    break

        if should_enable:
            logger.info("Current time %s inside an interval → enabling archivers", now)
            self.enable_all()
        else:
            logger.info("Current time %s outside intervals → disabling archivers", now)
            self.disable_all()

    def enable_all(self):
        # Yalnız indiyədək disabled idisə, resume et
        if not self._enabled:
            for arch in self.archivers:
                arch.resume()
            self._enabled = True
            logger.info("→ Archiving ENABLED for all channels")
        else:
            logger.debug("enable_all çağırıldı, amma artıq enabled vəziyyətdədir; keçilir")

    def disable_all(self):
        # Yalnız indiyədək enabled idisə, stop et
        if self._enabled:
            for arch in self.archivers:
                arch.stop()
            self._enabled = False
            logger.info("→ Archiving DISABLED for all channels")
        else:
            logger.debug("disable_all çağırıldı, amma artıq disabled vəziyyətdədir; keçilir")
