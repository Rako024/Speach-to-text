# app/api/routers.py

import os
import subprocess

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.config import Settings
from app.services.db import DBClient
from app.services.deepseek_client import DeepSeekClient
from app.api.schemas import SearchResponse, SegmentInfo

router = APIRouter()

# Settings & clients
settings = Settings()
db = DBClient(settings)
ds = DeepSeekClient(settings)


@router.get("/search/", response_model=SearchResponse)
def search(
    keyword: str = Query(..., min_length=1),
    channel: str | None = Query(None),
):
    """
    Açar söz üzrə transkript seqmentlərini axtarır,
    DeepSeek ilə xülasə yaradır və həm mətn, həm də
    metadata (fayl adı, offset, duration) qaytarır.
    """
    # 1) DB-dən tapılan seqmentlər
    segments = db.search(keyword, channel)
    if not segments:
        raise HTTPException(status_code=404, detail="Açar söz tapılmadı")

    # 2) DeepSeek ilə xülasə
    summary = ds.summarize(segments, keyword)

    # 3) Cavabı qaytar
    return SearchResponse(summary=summary, segments=segments)


@router.get("/video_clip/", response_class=StreamingResponse)
def clip(
    channel: str = Query(..., description="Kanal ID"),
    video_file: str = Query(..., description="TS fayl adı"),
    start: float = Query(..., description="Başlanğıc offset, saniyə ilə"),
    duration: float = Query(..., description="Müddət, saniyə ilə"),
):
    """
    Verilən kanal və fayl adı üçün start‑offset və müddət əsasında
    MP4 klip çıxarır.
    """
    folder = os.path.join(settings.archive_base, channel)
    path = os.path.join(folder, video_file)

    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Video seqment tapılmadı")

    cmd = [
        "ffmpeg",
        "-ss", str(start),
        "-i", path,
        "-t", str(duration),
        "-c", "copy",
        "-bsf:a", "aac_adtstoasc",
        "-movflags", "frag_keyframe+empty_moov",
        "-f", "mp4",
        "pipe:1",
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    return StreamingResponse(proc.stdout, media_type="video/mp4")
