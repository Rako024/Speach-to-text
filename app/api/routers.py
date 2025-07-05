from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
import os, subprocess
from app.services.db import DBClient
from app.services.summarizer import DeepSeekClient
from app.api.schemas import SearchResponse
from app.config import Settings

router = APIRouter()
s = Settings()
db = DBClient(s)
ds = DeepSeekClient(s)

@router.get("/search/", response_model=SearchResponse)
def search(keyword: str = Query(..., min_length=1)):
    rows = db.search(keyword)
    if not rows:
        raise HTTPException(404, "Not found")
    summary = ds.summarize(rows, keyword)
    from app.api.schemas import SegmentInfo
    segments = [ SegmentInfo(**dict(zip(["start_time","end_time","text","segment_filename","offset_secs","duration_secs"], r))) for r in rows ]
    return SearchResponse(summary=summary, segments=segments)

@router.get("/video_clip/")
def clip(video_file: str, start: float, duration: float):
    path = os.path.join(s.archive_dir, video_file)
    if not os.path.exists(path): raise HTTPException(404)
    cmd = ["ffmpeg","-ss",str(start),"-i",path,"-t",str(duration),"-c","copy","-f","mp4","pipe:1"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    return StreamingResponse(proc.stdout, media_type="video/mp4")