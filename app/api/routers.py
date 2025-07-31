# app/api/routers.py

import os
import subprocess
from datetime import datetime, timedelta, time
from typing import List

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.config import Settings
from app.services.db import DBClient, ScheduleInterval
from app.services.summarizer import DeepSeekClient
from app.scheduler_manager import SchedulerManager  # worker-da yaradılan sched_mgr-i əldə etmək üçün
from app.api.deps import get_db, get_scheduler_manager   # bu dependencyləri deps.py-ə qoyun

from app.api.schemas import SearchResponse, SegmentInfo

router = APIRouter()

# ————————————————————————————————
# Defaults: Settings & clients for search/clip
# ————————————————————————————————
settings = Settings()
db       = DBClient(settings)
ds       = DeepSeekClient(settings)


# ————————————————————————————————
# 1) SEARCH endpoint
# ————————————————————————————————
@router.get("/search/", response_model=SearchResponse)
def search(
    keyword: str = Query(..., min_length=1),
    channel: str | None = Query(None),
):
    segments = db.search(keyword, channel)
    if not segments:
        raise HTTPException(404, "Açar söz tapılmadı")

    starts = [datetime.fromisoformat(s.start_time) for s in segments]
    ends   = [datetime.fromisoformat(s.end_time)   for s in segments]
    window_start = min(starts) - timedelta(minutes=3)
    window_end   = max(ends)   + timedelta(minutes=3)

    context = db.fetch_text(
        window_start.isoformat(),
        window_end.isoformat(),
        channel
    )
    summary = ds.summarize_text(context)
    return SearchResponse(summary=summary, segments=segments)


# ————————————————————————————————
# 2) VIDEO_CLIP endpoint
# ————————————————————————————————
@router.get("/video_clip/", response_class=StreamingResponse)
def clip(
    channel: str = Query(..., description="Kanal ID"),
    video_file: str = Query(..., description="TS fayl adı"),
    start: float = Query(..., description="Başlanğıc offset, saniyə ilə"),
    duration: float = Query(..., description="Müddət, saniyə ilə"),
):
    folder = os.path.join(settings.archive_base, channel)
    path   = os.path.join(folder, video_file)
    if not os.path.exists(path):
        raise HTTPException(404, "Video seqment tapılmadı")

    cmd = [
        "ffmpeg", "-ss", str(start), "-i", path,
        "-t", str(duration), "-c", "copy",
        "-bsf:a", "aac_adtstoasc",
        "-movflags", "frag_keyframe+empty_moov",
        "-f", "mp4", "pipe:1"
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    return StreamingResponse(proc.stdout, media_type="video/mp4")


# ————————————————————————————————
# 3) SCHEDULE endpoints
# ————————————————————————————————

class IntervalIn(BaseModel):
    start_time: time = Field(..., description="Başlama vaxtı (HH:MM)")
    end_time:   time = Field(..., description="Bitmə vaxtı (HH:MM)")

class IntervalOut(IntervalIn):
    id: int

@router.get(
    "/schedule/",
    response_model=List[IntervalOut],
    summary="Mövcud intervaları siyahıla"
)
def list_intervals(
    db: DBClient = Depends(get_db)
):
    return db.get_intervals()

@router.post(
    "/schedule/",
    response_model=IntervalOut,
    status_code=201,
    summary="Yeni interval əlavə et"
)
def create_interval(
    data: IntervalIn,
    db: DBClient = Depends(get_db),
    sched_mgr: SchedulerManager = Depends(get_scheduler_manager)
):
    new = db.add_interval(data.start_time, data.end_time)
    sched_mgr.load_and_schedule_intervals()
    return new

@router.put(
    "/schedule/{interval_id}",
    status_code=204,
    summary="Mövcud intervalı yenilə"
)
def update_interval(
    interval_id: int,
    data: IntervalIn,
    db: DBClient = Depends(get_db),
    sched_mgr: SchedulerManager = Depends(get_scheduler_manager)
):
    db.update_interval(interval_id, data.start_time, data.end_time)
    sched_mgr.load_and_schedule_intervals()

@router.delete(
    "/schedule/{interval_id}",
    status_code=204,
    summary="Interval sil"
)
def delete_interval(
    interval_id: int,
    db: DBClient = Depends(get_db),
    sched_mgr: SchedulerManager = Depends(get_scheduler_manager)
):
    db.delete_interval(interval_id)
    sched_mgr.load_and_schedule_intervals()
