# app/services/cleanup.py
#!/usr/bin/env python3
import os
import datetime
import logging
from typing import Optional

from app.config import Settings
from app.services.db import DBClient
from app.services.storage import WasabiClient

logger = logging.getLogger(__name__)

def cleanup_old_ts():
    """
    end_time sütununa görə `cleanup_retention_days` gündən köhnə
    .ts fayllarını lokal diskdən və Wasabi-dən silir,
    DB-də deleted = TRUE qeyd edir.
    """
    settings = Settings()
    db       = DBClient(settings)

    # Wasabi (opsional)
    wc: Optional[WasabiClient] = None
    if getattr(settings, "wasabi_upload_enabled", False):
        try:
            wc = WasabiClient(settings)
        except Exception as e:
            logger.warning("Wasabi init failed; remote cleanup skipped: %s", e)
            wc = None

    days   = settings.cleanup_retention_days
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)

    logger.info(
        "Cleanup: deleting segments older than %d days (end_time < %s)",
        days, cutoff.isoformat()
    )

    # Bu metodun DB-də olduğuna əmin ol: deleted=FALSE və end_time<cutoff qaytarır
    old_segs = db.get_segments_older_than(cutoff)
    logger.info("Found %d segments to process for deletion", len(old_segs))

    deleted_ids = []
    for seg in old_segs:
        ch_id = seg.channel_id
        fname = seg.segment_filename

        local_path = os.path.join(settings.archive_base, ch_id, fname)
        remote_key = f"{ch_id}/{fname}"

        removed_local = False
        removed_remote_or_missing = False

        # Lokal sil
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
                removed_local = True
                logger.debug("Removed local TS: %s", local_path)
            else:
                removed_local = True  # lokalda onsuz da yoxdur → “silinmiş” kimi sayırıq
        except Exception as e:
            logger.warning("Could not remove local TS %s: %s", local_path, e)

        # Wasabi sil
        if wc is not None:
            try:
                if wc.exists(remote_key):
                    wc.delete_object(remote_key)
                    removed_remote_or_missing = True
                    logger.debug("Removed Wasabi TS: %s", remote_key)
                else:
                    removed_remote_or_missing = True  # artıq yoxdur
            except Exception as e:
                logger.warning("Could not remove Wasabi TS %s: %s", remote_key, e)
        else:
            removed_remote_or_missing = True  # Wasabi deaktiv → yalnız lokala baxırıq

        if removed_local and removed_remote_or_missing:
            deleted_ids.append(seg.id)

    # DB-də deleted flag-ı güncəllə
    if deleted_ids:
        db.mark_segments_deleted(deleted_ids)
    logger.info("Marked %d records as deleted in DB", len(deleted_ids))
