# api.py
import os
import subprocess
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from app.config import Settings
from app.services.db import DBClient
from app.services.summarizer import DeepSeekClient
from app.api.schemas import SearchResponse, SegmentInfo

settings = Settings()
app = FastAPI()

# ————————————————————————————————
# 1) Kök (“/”) üçün index.html route’u
# ————————————————————————————————
@app.get("/", include_in_schema=False)
def index():
    html_path = os.path.join(settings.archive_base, "index.html")
    if not os.path.exists(html_path):
        raise HTTPException(404, "index.html tapılmadı")
    return FileResponse(html_path)

# ————————————————————————————————
# 2) Arxiv faylları: /archive altında mount
# ————————————————————————————————
os.makedirs(settings.archive_base, exist_ok=True)
app.mount(
    "/archive",
    StaticFiles(directory=settings.archive_base),
    name="archive",
)

# ————————————————————————————————
# 3) DB və DeepSeek klientləri
# ————————————————————————————————
db = DBClient(settings)
db.init_db()
ds = DeepSeekClient(settings)

# ————————————————————————————————
# 4) Axtarış endpoint
# ————————————————————————————————
@app.get("/search/", response_model=SearchResponse)
def search(
    keyword: str = Query(..., min_length=1),
    channel: str | None = Query(None)
):
    segments = db.search(keyword, channel)
    if not segments:
        raise HTTPException(404, "Keyword tapılmadı")

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
# 5) Video klip endpoint
# ————————————————————————————————
@app.get("/video_clip/", response_class=StreamingResponse)
def clip(
    channel: str,
    video_file: str,
    start: float,
    duration: float
):
    # Faylın tam yolunu qururuq: archive/itv/itv_20250721Txxxxxx.ts
    folder = os.path.join(settings.archive_base, channel)
    path = os.path.join(folder, video_file)

    # Fayl mövcuddursa davam et
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"Fayl tapılmadı: {path}")

    # ffmpeg komandası ilə faylı MP4 formatında kəs
    cmd = [
        "ffmpeg",
        "-ss", str(start),        # Başlanğıc nöqtəsi
        "-i", path,               # Giriş .ts faylı
        "-t", str(duration),      # Müddət
        "-c", "copy",             # Yenidən kodlaşdırma yox
        "-bsf:a", "aac_adtstoasc",
        "-movflags", "frag_keyframe+empty_moov",
        "-f", "mp4",              # Çıxış formatı
        "pipe:1"                  # Standart çıxışa yönləndir
    ]

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        return StreamingResponse(
            proc.stdout,
            media_type="video/mp4"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"FFmpeg xətası: {str(e)}")@app.get("/video_clip/", response_class=StreamingResponse)
    
def clip(
    channel: str,
    video_file: str,
    start: float,
    duration: float
):
    # Faylın tam yolunu qururuq: archive/itv/itv_20250721Txxxxxx.ts
    folder = os.path.join(settings.archive_base, channel)
    path = os.path.join(folder, video_file)

    # Fayl mövcuddursa davam et
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"Fayl tapılmadı: {path}")

    # ffmpeg komandası ilə faylı MP4 formatında kəs
    cmd = [
        "ffmpeg",
        "-ss", str(start),        # Başlanğıc nöqtəsi
        "-i", path,               # Giriş .ts faylı
        "-t", str(duration),      # Müddət
        "-c", "copy",             # Yenidən kodlaşdırma yox
        "-bsf:a", "aac_adtstoasc",
        "-movflags", "frag_keyframe+empty_moov",
        "-f", "mp4",              # Çıxış formatı
        "pipe:1"                  # Standart çıxışa yönləndir
    ]

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        return StreamingResponse(
            proc.stdout,
            media_type="video/mp4"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"FFmpeg xətası: {str(e)}")

# ————————————————————————————————
# 6) Lokal server üçün
# ————————————————————————————————
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
