# app/api/routers.py - FIXED
import os
import subprocess
import tempfile
import logging
import threading
import time
from datetime import datetime, timedelta, date
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from pydantic import BaseModel

from app.config import Settings
from app.services.db import DBClient
from app.services.summarizer import DeepSeekClient
from app.scheduler_manager import SchedulerManager
from app.api.deps import get_db, get_summarizer, get_scheduler_manager
from app.api.auth_deps import require_auth
from app.api.schemas import SegmentInfo, IntervalIn, IntervalOut
from app.services.storage import WasabiClient

logger = logging.getLogger(__name__)
router = APIRouter()
settings = Settings()


# --- presign URL cache (sadə, sürətli) ---
_presign_cache: dict[str, tuple[str, float]] = {}  # rel_key -> (url, expire_ts)

def _presign_get_cached(wc: WasabiClient, rel_key: str, ttl_sec: int) -> str:
    """Wasabi üçün presigned URL-ni qısa müddət cache et (sürət üçün)."""
    now = time.time()
    url, exp = _presign_cache.get(rel_key, (None, 0.0)) if rel_key in _presign_cache else (None, 0.0)
    if url and exp > now + 3:  # 3s bufer
        return url
    # Qeyd: S3 presign özü obyektin varlığını yoxlamır; URL 404 verərsə ffmpeg-dən görəcəyik.
    url = wc.presign_get(rel_key, expires=ttl_sec)
    # cache müddətini TTL-in 90%-i götürək (təxminən)
    _presign_cache[rel_key] = (url, now + max(10, int(ttl_sec * 0.9)))
    return url

# --------------------------
# Wasabi client (lazy)
# --------------------------
_wasabi: Optional[WasabiClient] = None
def _get_wasabi() -> Optional[WasabiClient]:
    global _wasabi
    if _wasabi is None and getattr(settings, "wasabi_upload_enabled", False):
        try:
            _wasabi = WasabiClient(settings)
            logger.info("WasabiClient initialized successfully")
        except Exception as e:
            logger.error("Wasabi init failed: %s", e)
            _wasabi = None
    return _wasabi

# --------------------------
# Helpers: TS adından zaman al / qonşuları qur
# --------------------------
_TS_FMT = "%Y%m%dT%H%M%S"

def _parse_ts_name(video_file: str) -> datetime:
    base = os.path.basename(video_file)
    try:
        t = base.split("_", 1)[1].rsplit(".", 1)[0]
        return datetime.strptime(t, _TS_FMT)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Bad TS filename format: {video_file}")

def _format_ts_name(channel: str, dt: datetime) -> str:
    return f"{channel}_{dt.strftime(_TS_FMT)}.ts"

def _local_ts_path(channel: str, video_file: str) -> str:
    return os.path.join(settings.archive_base, channel, video_file)

def _rel_key(channel: str, video_file: str) -> str:
    """Wasabi üçün NİSBİ key (prefixsiz): '<channel>/<file>'"""
    return f"{channel.strip('/')}/{os.path.basename(video_file)}"

def _full_key_for_log(rel_key: str) -> str:
    """Yalnız LOG üçün tam key (prefix + rel). Storage çağırışlarında istifadə ETMƏ."""
    prefix = (getattr(settings, "wasabi_prefix", "") or "").strip().strip("/")
    return f"{prefix}/{rel_key}" if prefix else rel_key

def _basename_from_source(src: str) -> str:
    if src.startswith("http"):
        return src.split("?", 1)[0].rstrip("/").split("/")[-1]
    return os.path.basename(src)

def _try_source(channel: str, fname: str) -> Optional[str]:
    """
    Lokal path varsa onu qaytar; yoxdursa Wasabi-də obyekt varsa presigned URL qaytar.
    Tapılmazsa None.
    """
    # 1) Local
    lpath = _local_ts_path(channel, fname)
    if os.path.exists(lpath):
        logger.debug("[LOCAL] Found: %s", lpath)
        return os.path.abspath(lpath)

    # 2) Wasabi
    wc = _get_wasabi()
    if wc:
        exp = int(getattr(settings, "wasabi_presign_expire_seconds",
                          getattr(settings, "wasabi_presign_expire", 3600)))
        rel = _rel_key(channel, fname)
        try:
            if wc.exists(rel):
                url = wc.presign_get(rel, expires=exp)
                logger.info("[WASABI] Found %s", _full_key_for_log(rel))
                logger.debug("[WASABI] URL: %s", url)
                return url
            else:
                logger.error("[WASABI] Not found: %s", _full_key_for_log(rel))
        except Exception as e:
            logger.error("[WASABI] Error on key %s: %s", _full_key_for_log(rel), e)

    logger.warning("[NOT FOUND] %s/%s not found locally or in Wasabi", channel, fname)
    return None

def _probe_duration(src: str) -> float:
    """ffprobe ilə saniyə cinsindən uzunluq; URL-ləri birbaşa verir."""
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default:nokey=1:noprint_wrappers=1",
            src
        ]
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, timeout=10)
        val = float(out.decode().strip())
        return max(0.0, val)
    except Exception as e:
        logger.warning("Failed to probe duration for %s: %s",
                       src[:80] + ("..." if len(src) > 80 else ""), e)
        return float(getattr(settings, "ts_segment_time", 8))

# -------- Mərkəzin ətrafında ardıcıl zənciri topla (min 30s) --------
def _collect_chain_around(
    channel: str,
    center_file: str,
    pad_before: float,
    total_needed: float,
    scan_sec: Optional[int] = None,
    max_segments: int = 300,
):
    """
    center_file ətrafında ard-arda TS-ləri yığır.
    Qaytarır: (inputs[list[str]], durs[list[float]], center_idx[int])
    """
    import itertools

    scan_sec = scan_sec or int(getattr(settings, "ts_neighbor_scan_sec", 300))
    center_dt = _parse_ts_name(center_file)

    # mərkəz
    center_src = _try_source(channel, center_file)
    if not center_src:
        logger.error("Center file not found: %s/%s", channel, center_file)
        return [], [], -1

    inputs: List[str] = [center_src]
    durs:   List[float] = [_probe_duration(center_src)]
    center_idx = 0

    # yaxın→uzaq prev/next
    prev_srcs: List[str] = []
    next_srcs: List[str] = []
    for s in range(1, scan_sec + 1):
        if len(prev_srcs) < max_segments:
            fprev = _format_ts_name(channel, center_dt - timedelta(seconds=s))
            sp = _try_source(channel, fprev)
            if sp and (not prev_srcs or _basename_from_source(prev_srcs[-1]) != _basename_from_source(sp)):
                prev_srcs.append(sp)
        if len(next_srcs) < max_segments:
            fnext = _format_ts_name(channel, center_dt + timedelta(seconds=s))
            sn = _try_source(channel, fnext)
            if sn and (not next_srcs or _basename_from_source(next_srcs[-1]) != _basename_from_source(sn)):
                next_srcs.append(sn)
        if len(prev_srcs) >= max_segments and len(next_srcs) >= max_segments:
            break

    prev_durs = [_probe_duration(s) for s in prev_srcs]
    next_durs = [_probe_duration(s) for s in next_srcs]

    pref_prev = [0.0] + list(itertools.accumulate(prev_durs))
    pref_next = [0.0] + list(itertools.accumulate(next_durs))

    # pad_before qədər prev-dən
    k_prev = 0
    for i in range(1, len(pref_prev)):
        if pref_prev[i] >= max(0.0, pad_before - 0.01):
            k_prev = i
            break
    k_prev = min(k_prev or len(prev_srcs), len(prev_srcs))

    base = pref_prev[k_prev] + durs[0]
    k_next = 0
    for j in range(0, len(pref_next)):
        if base + pref_next[j] >= total_needed - 0.01:
            k_next = j
            break
    if base + pref_next[k_next] < total_needed and k_next < len(next_srcs):
        k_next = len(next_srcs)

    use_prev = list(reversed(prev_srcs[:k_prev]))
    use_next = next_srcs[:k_next]
    inputs = use_prev + inputs + use_next
    durs = [_probe_duration(s) for s in inputs]
    center_idx = len(use_prev)

    logger.info("Collected %d segments around %s", len(inputs), center_file)
    return inputs, durs, center_idx

# --- 1) SEARCH ---
@router.get(
    "/search/",
    response_model=List[SegmentInfo],
    summary="Transkriptlərdə axtarış: seqment siyahısı qaytarır"
)
def search(
    keyword: str = Query(..., min_length=1),
    channel: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date:   Optional[date] = Query(None),
    threshold: float = Query(0.2, ge=0.0, le=1.0),
    limit:     int = Query(50, ge=1, le=500),
    db: DBClient = Depends(get_db),
    claims: dict = Depends(require_auth)
):
    results = db.search(
        keyword=keyword,
        channel=channel,
        start_date=start_date,
        end_date=end_date,
        threshold=threshold,
        limit=limit
    )
    return [
        SegmentInfo(
            id               = r['id'],
            channel_id       = r['channel_id'],
            start_time       = r['start_time'].isoformat(),
            end_time         = r['end_time'].isoformat(),
            text             = r['text'],
            segment_filename = r['segment_filename'],
            offset_secs      = r['offset_secs'],
            duration_secs    = r['duration_secs'],
            score            = r['score'],
        )
        for r in results
    ]

# --- 2) SUMMARIZE ---
class SummarizeOut(BaseModel):
    summary: str
    segments: List[SegmentInfo]

@router.get(
    "/summarize/{segment_id}",
    response_model=SummarizeOut,
    summary="Seçilmiş seqmentin ±15s kontekstini xülasə et",
)
def summarize_segment(
    segment_id: int,
    db: DBClient = Depends(get_db),
    ds: DeepSeekClient = Depends(get_summarizer),
    claims: dict = Depends(require_auth)
):
    seg = db.get_segment(segment_id)
    if not seg:
        raise HTTPException(status_code=404, detail="Segment not found")

    base = SegmentInfo(
        id               = seg['id'],
        channel_id       = seg['channel_id'],
        start_time       = seg['start_time'].isoformat(),
        end_time         = seg['end_time'].isoformat(),
        text             = seg['text'],
        segment_filename = seg['segment_filename'],
        offset_secs      = seg['offset_secs'],
        duration_secs    = seg['duration_secs'],
        score            = seg.get('score'),
    )

    st_dt = datetime.fromisoformat(base.start_time)
    en_dt = datetime.fromisoformat(base.end_time)
    window_start = st_dt - timedelta(seconds=15)
    window_end   = en_dt + timedelta(seconds=15)

    ctx = db.fetch_segments_in_window(
        channel=base.channel_id,
        start_iso=window_start.isoformat(),
        end_iso=window_end.isoformat()
    )
    if not ctx:
        raise HTTPException(status_code=404, detail="No context segments found")

    segments = [
        SegmentInfo(
            id               = r['id'],
            channel_id       = r['channel_id'],
            start_time       = r['start_time'].isoformat(),
            end_time         = r['end_time'].isoformat(),
            text             = r['text'],
            segment_filename = r['segment_filename'],
            offset_secs      = r['offset_secs'],
            duration_secs    = r['duration_secs'],
            score            = r.get('score'),
        )
        for r in ctx
    ]

    summary = ds.summarize(segments)
    return SummarizeOut(summary=summary, segments=segments)

# --- 3) VIDEO_CLIP (single source) — lokal + Wasabi fallback (FAST by default) ---
@router.get(
    "/video_clip/",
    response_class=StreamingResponse,
    summary="Stream MP4 clip from a single TS (start/duration)"
)
def clip(
    channel: str = Query(..., description="Channel ID"),
    video_file: str = Query(..., description="TS filename"),
    start: float = Query(..., description="Start offset in seconds"),
    duration: float = Query(..., description="Duration in seconds"),
    fast: int = Query(1, ge=0, le=1, description="1=stream-copy (tez), 0=re-encode (dəqiq)"),
    claims: dict = Depends(require_auth)
):
    # Sərhədləri təmizlə
    start = max(0.0, float(start))
    duration = max(0.1, float(duration))

    # 1) Lokal varsa — ən sürətlisi
    local_path = _local_ts_path(channel, video_file)
    if os.path.exists(local_path):
        input_url = os.path.abspath(local_path)
        logger.info("[video_clip] local: %s (start=%.3f, dur=%.3f, fast=%d)", input_url, start, duration, fast)
    else:
        # 2) Wasabi — prefikssiz nisbi key
        wc = _get_wasabi()
        if wc is None:
            raise HTTPException(status_code=404, detail="TS not found locally and Wasabi disabled")

        rel = _rel_key(channel, video_file)
        ttl = int(getattr(settings, "wasabi_presign_expire_seconds",
                          getattr(settings, "wasabi_presign_expire", 3600)))
        try:
            input_url = _presign_get_cached(wc, rel, ttl)   # exists() yox, birbaşa presign (sürət)
            logger.info("[video_clip] wasabi: %s (rel=%s, fast=%d)", input_url, rel, fast)
        except Exception as e:
            logger.error("[video_clip] presign failed for %s: %s", _full_key_for_log(rel), e)
            raise HTTPException(status_code=404, detail=f"TS not found: {_full_key_for_log(rel)}")

    # 3) FFmpeg əmri
    if fast == 1:
        # FAST: -ss İNPUTDAN ƏVVƏL + stream-copy
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-ss", f"{start:.3f}",
            "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
            "-i", input_url,
            "-t", f"{duration:.3f}",
            # yalnız video/audio axınlarını map et (data/ID3-ləri at)
            "-map", "0:v:0?", "-map", "0:a:0?",
            # ADTS → MP4 üçün zəruri filter (0 saniyə & muxer errorlarını həll edir)
            "-c:v", "copy", "-c:a", "copy",
            "-bsf:a", "aac_adtstoasc",
            # timestamp sağlamlığı
            "-fflags", "+genpts",
            "-avoid_negative_ts", "make_zero",
            # streamləmə üçün
            "-movflags", "frag_keyframe+empty_moov",
            "-f", "mp4", "pipe:1"
        ]
    else:
        # SLOW: -ss inputdan SONRA + re-encode (frame-accurate, amma yavaşdır)
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
            "-i", input_url,
            "-ss", f"{start:.3f}",
            "-t", f"{duration:.3f}",
            "-map", "0:v:0?", "-map", "0:a:0?",
            "-fflags", "+genpts",
            "-avoid_negative_ts", "make_zero",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "28",
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "frag_keyframe+empty_moov",
            "-f", "mp4", "pipe:1"
        ]

    logger.debug("[video_clip] ffmpeg: %s", " ".join(cmd[:12] + ["..."]))

    # 4) Prosesi işə sal — stderr-i background-da yalnız error-ları logla
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        def _stderr_logger():
            for line in proc.stderr:
                if not line:
                    continue
                txt = line.decode("utf-8", "ignore").strip()
                if ("error" in txt.lower()) or ("failed" in txt.lower()):
                    logger.error("[ffmpeg] %s", txt)
        threading.Thread(target=_stderr_logger, daemon=True).start()
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="ffmpeg not found on system")
    except Exception as e:
        logger.exception("[video_clip] start failed")
        raise HTTPException(status_code=500, detail=f"Failed to start ffmpeg: {e}")

    # 5) Təmizlik
    def _cleanup():
        try:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=1.5)
        except Exception:
            try: proc.kill()
            except Exception: pass

    return StreamingResponse(
        proc.stdout,
        media_type="video/mp4",
        background=BackgroundTask(_cleanup),
        headers={"Cache-Control": "no-store"}
    )


# --- 4) VIDEO_TRIPLET — concat + dəqiq kəsim ---
@router.get(
    "/video_triplet/",
    response_class=StreamingResponse,
    summary="Concat adjacent TS files and stream as one MP4"
)
def video_triplet(
    channel: str = Query(..., description="Channel ID"),
    video_file: str = Query(..., description="Center TS filename (e.g. itv_20250808T163002.ts)"),
    offset: float = Query(0.0, description="Offset inside center.ts (seconds)"),
    duration: float = Query(3.0, description="Matched speech duration (seconds)"),
    pad_before: float = Query(15.0, description="Extra seconds before"),
    pad_after: float = Query(15.0, description="Extra seconds after"),
    fast: int = Query(0, description="1 = stream-copy (FAST), 0 = re-encode (SLOW)"),
    claims: dict = Depends(require_auth)
):
    min_clip = float(getattr(settings, "ts_min_clip_sec", 30.0))
    desired  = max(min_clip, pad_before + duration + pad_after)

    inputs, durs, center_idx = _collect_chain_around(
        channel, video_file, pad_before=pad_before, total_needed=desired
    )
    if not inputs:
        logger.error("[video_triplet] No segments found for %s/%s", channel, video_file)
        raise HTTPException(status_code=404, detail=f"No TS segments found around {video_file}")

    logger.info("[video_triplet] %s | center=%s | fast=%d | segments=%d",
                channel, video_file, fast, len(inputs))

    # Concat list
    listfile = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".txt", encoding='utf-8')
    listpath = listfile.name
    try:
        for src in inputs:
            # URL və lokal üçün eyni format işləyir (ffmpeg concat demuxer)
            safe = src.replace("\\", "/").replace("'", "'\\''")
            listfile.write(f"file '{safe}'\n")
        listfile.flush()
        listfile.close()
        logger.debug("[video_triplet] Concat list: %s", listpath)
    except Exception as e:
        try: listfile.close()
        except: pass
        try: os.remove(listpath)
        except: pass
        logger.error("[video_triplet] Failed to create concat list: %s", e)
        raise HTTPException(status_code=500, detail="Internal error building concat list")

    total_len = sum(durs)
    center_start_in_concat = sum(durs[:center_idx])
    clip_start = max(0.0, center_start_in_concat + max(0.0, offset - pad_before))
    clip_len   = min(max(min_clip, pad_before + duration + pad_after),
                     max(0.5, total_len - clip_start - 0.05))

    logger.info("[video_triplet] Clip timing: start=%.3f len=%.3f (total=%.3f desired>=%.3f)",
                clip_start, clip_len, total_len, desired)

    if fast == 1:
        # Stream-copy (sürətli)
        cmd = [
            "ffmpeg", "-y",
            "-f", "concat", "-safe", "0",
            "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
            "-i", listpath,
            "-ss", f"{clip_start:.3f}",
            "-t", f"{clip_len:.3f}",
            "-c:v", "copy", "-c:a", "copy",
            "-movflags", "frag_keyframe+empty_moov",
            "-f", "mp4", "pipe:1"
        ]
    else:
        # Yenidən kodla (dəqiq kəsim)
        cmd = [
            "ffmpeg", "-y",
            "-f", "concat", "-safe", "0",
            "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
            "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
            "-i", listpath,
            "-ss", f"{clip_start:.3f}",
            "-t", f"{clip_len:.3f}",
            "-fflags", "+genpts",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "28",
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "frag_keyframe+empty_moov",
            "-f", "mp4", "pipe:1"
        ]

    logger.debug("[video_triplet] FFmpeg: %s", " ".join(cmd[:8] + ["..."]))

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        import threading
        def log_stderr():
            for line in proc.stderr:
                if line:
                    decoded = line.decode('utf-8', errors='ignore').strip()
                    if 'error' in decoded.lower() or 'failed' in decoded.lower():
                        logger.error("[ffmpeg] %s", decoded)
        threading.Thread(target=log_stderr, daemon=True).start()
    except FileNotFoundError:
        try: os.remove(listpath)
        except: pass
        logger.error("[video_triplet] ffmpeg not found")
        raise HTTPException(status_code=500, detail="ffmpeg not found on system")
    except Exception as e:
        try: os.remove(listpath)
        except: pass
        logger.error("[video_triplet] Failed to start ffmpeg: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to start ffmpeg: {str(e)}")

    def _cleanup():
        try:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=2)
        except:
            try: proc.kill()
            except: pass
        try: os.remove(listpath)
        except: pass
        logger.debug("[video_triplet] Cleanup completed")

    return StreamingResponse(proc.stdout, media_type="video/mp4", background=BackgroundTask(_cleanup))

# --- 5) SCHEDULE endpoints ---
@router.get(
    "/schedule/",
    response_model=List[IntervalOut],
    summary="List all scheduling intervals"
)
def list_intervals(
    db: DBClient = Depends(get_db),
    claims: dict = Depends(require_auth)
):
    return db.get_intervals()

@router.post(
    "/schedule/",
    response_model=IntervalOut,
    status_code=201,
    summary="Create a new scheduling interval"
)
def create_interval(
    data: IntervalIn,
    db: DBClient = Depends(get_db),
    sched_mgr: SchedulerManager = Depends(get_scheduler_manager),
    claims: dict = Depends(require_auth)
):
    new = db.add_interval(data.start_time, data.end_time)
    sched_mgr.load_and_schedule_intervals()
    return new

@router.put(
    "/schedule/{interval_id}",
    status_code=204,
    summary="Update an existing scheduling interval"
)
def update_interval(
    interval_id: int,
    data: IntervalIn,
    db: DBClient = Depends(get_db),
    sched_mgr: SchedulerManager = Depends(get_scheduler_manager),
    claims: dict = Depends(require_auth)
):
    db.update_interval(interval_id, data.start_time, data.end_time)
    sched_mgr.load_and_schedule_intervals()

@router.delete(
    "/schedule/{interval_id}",
    status_code=204,
    summary="Delete a scheduling interval"
)
def delete_interval(
    interval_id: int,
    db: DBClient = Depends(get_db),
    sched_mgr: SchedulerManager = Depends(get_scheduler_manager),
    claims: dict = Depends(require_auth)
):
    db.delete_interval(interval_id)
    sched_mgr.load_and_schedule_intervals()
