# app/scheduler_manager.py
import datetime
import logging
from apscheduler.schedulers.base import BaseScheduler

logger = logging.getLogger(__name__)

class SchedulerManager:
    def __init__(self, scheduler: BaseScheduler, db, archivers: list):
        self.scheduler = scheduler
        self.db        = db
        self.archivers = archivers
        self._enabled  = False

    def clear_interval_jobs(self):
        for job in self.scheduler.get_jobs():
            if job.id.startswith("enable_") or job.id.startswith("disable_"):
                self.scheduler.remove_job(job.id)

    # --- YENİ: indi hər hansı intervalın içindəyikmi? ---
    def _now_in_any_interval(self) -> bool:
        now = datetime.datetime.now(self.scheduler.timezone).time()
        for it in self.db.get_intervals():
            st, et = it.start_time, it.end_time
            if st <= et:
                if st <= now < et:
                    return True
            else:
                # gecədən keçən interval (22:00–06:00)
                if now >= st or now < et:
                    return True
        return False

    def load_and_schedule_intervals(self):
        self.clear_interval_jobs()
        intervals = self.db.get_intervals()

        for interval in intervals:
            sid = interval.id
            sh, sm, ss = interval.start_time.hour, interval.start_time.minute, interval.start_time.second
            eh, em, es = interval.end_time.hour,   interval.end_time.minute,   interval.end_time.second

            self.scheduler.add_job(
                func=self.enable_all, trigger='cron',
                hour=sh, minute=sm, second=ss,
                id=f"enable_{sid}", replace_existing=True
            )
            self.scheduler.add_job(
                func=self.disable_all, trigger='cron',
                hour=eh, minute=em, second=es,
                id=f"disable_{sid}", replace_existing=True
            )
            logger.info("Scheduled interval %d: %02d:%02d:%02d → %02d:%02d:%02d", sid, sh, sm, ss, eh, em, es)

        # Startup-da cari vəziyyəti dərhal tətbiq et
        if self._now_in_any_interval():
            logger.info("Startup: current time is INSIDE an interval → enabling archivers")
            self.enable_all()
        else:
            logger.info("Startup: current time is OUTSIDE intervals → disabling archivers")
            self.disable_all()

    def enable_all(self):
        if not self._enabled:
            for arch in self.archivers:
                arch.resume()  # start_ts() + start_watcher()
            self._enabled = True
            logger.info("→ Archiving ENABLED for all channels")
        else:
            logger.debug("enable_all çağırıldı, amma artıq enabled vəziyyətdədir; keçilir")

    def disable_all(self):
        # ƏSAS DÜZƏLİŞ: HƏLƏ də intervalın içindəyiksə, söndürmə
        if self._now_in_any_interval():
            logger.info("disable_all trigger gəldi, amma hələ aktiv interval var → ENABLED saxlanılır")
            return

        if self._enabled:
            for arch in self.archivers:
                arch.stop()
            self._enabled = False
            logger.info("→ Archiving DISABLED for all channels")
        else:
            logger.debug("disable_all çağırıldı, amma artıq disabled vəziyyətdədir; keçilir")
