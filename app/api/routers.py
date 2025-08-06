# app/api/routers.py

import os
import subprocess
from datetime import datetime, timedelta, date
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.config import Settings
from app.services.db import DBClient, ScheduleInterval
from app.services.summarizer import DeepSeekClient
from app.scheduler_manager import SchedulerManager
from app.api.deps import get_db, get_summarizer, get_scheduler_manager
from app.api.schemas import (
    SegmentInfo,
    IntervalIn,
    IntervalOut,
)

router = APIRouter()
settings = Settings()


def _get_db() -> DBClient:
    return DBClient(settings)

_ds: Optional[DeepSeekClient] = None
def _get_ds() -> DeepSeekClient:
    global _ds
    if _ds is None:
        _ds = DeepSeekClient(settings)
    return _ds


# --- 1) YENİLƏNMİŞ SEARCH: sadəcə seqmentləri qaytarır ---
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
    db: DBClient = Depends(_get_db),
):
    results = db.search(
        keyword=keyword,
        channel=channel,
        start_date=start_date,
        end_date=end_date,
        threshold=threshold,
        limit=limit
    )
    if not results:
        raise HTTPException(status_code=404, detail="Matching transcripts not found")

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


# --- 2) YENİ SUMMARIZE endpoint: seçilmiş seqmentin ±15s kontekstini xülasə et ---
class SummarizeOut(BaseModel):
    summary: str
    segments: List[SegmentInfo]

@router.get(
    "/summarize/{segment_id}",
    response_model=SummarizeOut,
    summary="Seçilmiş seqmentin ±15s kontekstini xülasə et"
)
def summarize_segment(
    segment_id: int,
    db: DBClient = Depends(_get_db),
    ds: DeepSeekClient = Depends(_get_ds),
):
    # 1) Seçilmiş seqmenti götür
    seg = db.get_segment(segment_id)
    if not seg:
        raise HTTPException(status_code=404, detail="Segment not found")

    # 2) Pydantic modelə çevir
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

    # 3) 15 saniyə əvvəli və sonranı hesablamaq
    st_dt = datetime.fromisoformat(base.start_time)
    en_dt = datetime.fromisoformat(base.end_time)
    window_start = st_dt - timedelta(seconds=15)
    window_end   = en_dt + timedelta(seconds=15)

    # 4) O kanal üçün həmin pəncərədəki bütün seqmentləri gətir
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

    # 5) DeepSeek ilə xülasə et
    summary = ds.summarize(segments)

    return SummarizeOut(summary=summary, segments=segments)


# --- 3) VIDEO_CLIP endpoint ---
@router.get(
    "/video_clip/",
    response_class=StreamingResponse,
    summary="Stream MP4 clip from a TS segment"
)
def clip(
    channel: str = Query(..., description="Channel ID"),
    video_file: str = Query(..., description="TS filename"),
    start: float = Query(..., description="Start offset in seconds"),
    duration: float = Query(..., description="Duration in seconds")
):
    folder = os.path.join(settings.archive_base, channel)
    path = os.path.join(folder, video_file)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="TS file not found")

    cmd = [
        "ffmpeg", "-ss", str(start), "-i", path,
        "-t", str(duration), "-c", "copy",
        "-bsf:a", "aac_adtstoasc",
        "-movflags", "frag_keyframe+empty_moov",
        "-f", "mp4", "pipe:1"
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    return StreamingResponse(proc.stdout, media_type="video/mp4")


# --- 4) SCHEDULE endpoints ---
@router.get(
    "/schedule/",
    response_model=List[IntervalOut],
    summary="List all scheduling intervals"
)
def list_intervals(
    db: DBClient = Depends(_get_db)
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
    db: DBClient = Depends(_get_db),
    sched_mgr: SchedulerManager = Depends(get_scheduler_manager)
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
    db: DBClient = Depends(_get_db),
    sched_mgr: SchedulerManager = Depends(get_scheduler_manager)
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
    db: DBClient = Depends(_get_db),
    sched_mgr: SchedulerManager = Depends(get_scheduler_manager)
):
    db.delete_interval(interval_id)
    sched_mgr.load_and_schedule_intervals()
