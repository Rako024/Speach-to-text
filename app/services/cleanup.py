# app/services/cleanup.py
#!/usr/bin/env python3
import os
import datetime
import logging
from app.config import Settings
from app.services.db import DBClient

logger = logging.getLogger(__name__)

def cleanup_old_ts():
    """
    end_time sütununa görə `cleanup_retention_days` gündən köhnə
    .ts fayllarını diskdən silir və DB-də deleted = TRUE qeyd edir.
    """
    settings = Settings()
    db       = DBClient(settings)

    days   = settings.cleanup_retention_days
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)

    logger.info(
        "Cleanup: deleting segments older than %d days (end_time < %s)",
        days, cutoff.isoformat()
    )

    old_segs = db.get_segments_older_than(cutoff)
    logger.info("Found %d segments to process for deletion", len(old_segs))

    deleted_ids = []
    for seg in old_segs:
        path = os.path.join(settings.archive_base, seg.channel_id, seg.segment_filename)
        try:
            if os.path.exists(path):
                os.remove(path)
                logger.debug("Removed TS file: %s", path)
                deleted_ids.append(seg.id)
        except Exception as e:
            logger.warning("Could not remove TS %s: %s", path, e)

    # DB-də deleted flag-ı güncəllə
    db.mark_segments_deleted(deleted_ids)
    logger.info("Marked %d records as deleted in DB", len(deleted_ids))
