# app/services/cleanup.py
#!/usr/bin/env python3
import os
import datetime
import logging
import tempfile
from typing import Optional

from app.config import Settings
from app.services.db import DBClient
from app.services.storage import WasabiClient
from app.services.archiver import _get_ts_root

logger = logging.getLogger(__name__)

def _get_ts_root(settings: Settings) -> str:
    """
    TS fayllarının saxlandığı kök qovluq:
      - settings.ts_staging_dir verilmişsə: DƏQİQ HƏMİN QOVLUQ (dəyişdirmirik)
      - verilməyibsə və ya boşdursa: OS temp dir (tempfile.gettempdir()).
    """
    ts_dir = getattr(settings, "ts_staging_dir", None)
    if ts_dir and str(ts_dir).strip():
        return str(ts_dir).strip()
    return tempfile.gettempdir()


def cleanup_old_ts():
    """
    DB-də end_time sütununa görə `cleanup_retention_days` gündən köhnə
    segmentləri silir:
      - Lokal diskdən (yalnız TS kökündə: ts_staging_dir və ya OS temp)
      - Wasabi-dən (əgər aktivdirsə)
    və DB-də deleted = TRUE qeyd edir.
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
        "Cleanup(DB): deleting segments older than %d days (end_time < %s)",
        days, cutoff.isoformat()
    )

    # DB: deleted=FALSE və end_time<cutoff olanlar
    old_segs = db.get_segments_older_than(cutoff)
    logger.info("Found %d segments to process for deletion", len(old_segs))

    ts_root = _get_ts_root(settings)
    deleted_ids = []

    for seg in old_segs:
        ch_id = seg.channel_id
        fname = seg.segment_filename

        # Lokal yol: <TS_ROOT>/<channel_id>/<filename.ts>
        local_path = os.path.join(ts_root, ch_id, fname)
        removed_local = False

        # Lokal sil
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
                removed_local = True
                logger.debug("Removed local TS: %s", local_path)
            else:
                # Lokalda yoxdursa da "silinmiş" kimi qəbul edirik
                removed_local = True
        except Exception as e:
            logger.warning("Could not remove local TS %s: %s", local_path, e)

        # Uzaq (Wasabi) sil
        removed_remote_or_missing = True  # default: Wasabi yoxdur → TRUE
        if wc is not None:
            removed_remote_or_missing = False
            remote_key = f"{ch_id}/{fname}"
            try:
                if wc.exists(remote_key):
                    wc.delete_object(remote_key)
                    logger.debug("Removed Wasabi TS: %s", remote_key)
                # yoxdursa da problem deyil
                removed_remote_or_missing = True
            except Exception as e:
                logger.warning("Could not remove Wasabi TS %s: %s", remote_key, e)

        # Hər iki tərəf OK-dursa, DB-də işarələ
        if removed_local and removed_remote_or_missing:
            deleted_ids.append(seg.id)

    # DB-də deleted = TRUE
    if deleted_ids:
        db.mark_segments_deleted(deleted_ids)
    logger.info("Marked %d records as deleted in DB", len(deleted_ids))


def cleanup_local_ts(max_age_minutes: Optional[int] = None):
    """
    DB-dən asılı olmadan lokal TS təmizliyi.
    TS kök qovluğunda (ts_staging_dir və ya OS temp) yaşı X dəqiqəni
    keçmiş BÜTÜN *.ts fayllarını silir.
    - max_age_minutes: env/parametr; default 120 dəqiqə.
      Env: TS_LOCAL_MAX_AGE_MIN
    """
    settings = Settings()

    ts_root = _get_ts_root(settings)
    logger.debug("cleanup_local_ts: max_age=%dmin, ts_root=%s", max_age_minutes, ts_root)

    if max_age_minutes is None:
        try:
            max_age_minutes = int(os.getenv("TS_LOCAL_MAX_AGE_MIN", "120"))
        except Exception:
            max_age_minutes = 120

    cutoff_ts = datetime.datetime.now().timestamp() - max_age_minutes * 60

    total_removed = 0
    if not os.path.isdir(ts_root):
        logger.info("cleanup_local_ts: TS root does not exist: %s", ts_root)
        
        logger.info(f"!!!! current directory is: {os.getcwd()}")   # Prints the current working 
        logger.info(f"root directory list: {os.listdir('/')}")
        logger.info(f"current directory list: {os.listdir()}")
        return

    # <TS_ROOT>/<channel_id> altına bax
    for ch_id in os.listdir(ts_root):
        ch_dir = os.path.join(ts_root, ch_id)
        if not os.path.isdir(ch_dir):
            continue

        for fname in os.listdir(ch_dir):
            if not fname.endswith(".ts"):
                continue
            fpath = os.path.join(ch_dir, fname)
            try:
                if os.path.getmtime(fpath) < cutoff_ts:
                    os.remove(fpath)
                    total_removed += 1
                    logger.debug("cleanup_local_ts: removed %s", fpath)
            except FileNotFoundError:
                continue
            except Exception as e:
                logger.warning("cleanup_local_ts: could not remove %s: %s", fpath, e)

    logger.info("cleanup_local_ts: removed %d stale TS files (>%d min) from %s",
                total_removed, max_age_minutes, ts_root)
