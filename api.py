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

# Serve our SPA/UI
@app.get("/", include_in_schema=False)
def index():
    path = os.path.join(settings.archive_dir, "index.html")
    if not os.path.exists(path):
        raise HTTPException(404, "index.html yoxdu")
    return FileResponse(path)

# Expose TS files and m3u8
os.makedirs(settings.archive_dir, exist_ok=True)
app.mount("/archive", StaticFiles(directory=settings.archive_dir), name="archive")

db = DBClient(settings)
db.init_db()           # make sure table exists
ds = DeepSeekClient(settings)

@app.get("/search/", response_model=SearchResponse)
def search(keyword: str = Query(..., min_length=1)):
    # 1) find matching segments
    segments = db.search(keyword)
    if not segments:
        raise HTTPException(404, "Keyword tapılmadı")

    # 2) compute ±3min window around matches
    starts = [datetime.fromisoformat(s.start_time) for s in segments]
    ends   = [datetime.fromisoformat(s.end_time)   for s in segments]
    window_start = min(starts) - timedelta(minutes=3)
    window_end   = max(ends)   + timedelta(minutes=3)

    # 3) fetch full text in that window
    context = db.fetch_text(
        window_start.isoformat(),
        window_end.isoformat()
    )

    # 4) summarize with DeepSeek
    summary = ds.summarize_text(context)

    return SearchResponse(summary=summary, segments=segments)

@app.get("/video_clip/", response_class=StreamingResponse)
def clip(video_file: str, start: float, duration: float):
    path = os.path.join(settings.archive_dir, video_file)
    if not os.path.exists(path):
        raise HTTPException(404, "Segment yoxdu")
    cmd = [
        "ffmpeg", "-ss", str(start), "-i", path,
        "-t", str(duration), "-c", "copy",
        "-bsf:a", "aac_adtstoasc",
        "-movflags", "frag_keyframe+empty_moov",
        "-f", "mp4", "pipe:1"
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    return StreamingResponse(proc.stdout, media_type="video/mp4")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
