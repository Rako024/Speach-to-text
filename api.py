#!/usr/bin/env python3
import os
import logging
import subprocess 
import psycopg2
import requests
from typing import List
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# --- Configuration ---
ARCHIVE_DIR      = "archive"
DB_HOST          = "localhost"
DB_NAME          = "speach_to_text"
DB_USER          = "postgres"
DB_PASSWORD      = "!2627251Rr"
DB_PORT          = 5432
DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_API_KEY = "sk-415ea7ff259945b386d57c216e2bc77d"

# Logger setup
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger("api")

# FastAPI app
app = FastAPI()

# Serve UI
@app.get("/", include_in_schema=False)
def serve_index():
    path = os.path.join(ARCHIVE_DIR, "index.html")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(path, media_type="text/html")

# Mount static archive
os.makedirs(ARCHIVE_DIR, exist_ok=True)
app.mount("/archive", StaticFiles(directory=ARCHIVE_DIR), name="archive")

# DB helper
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

# DeepSeek summarization
def get_summary_from_deepseek(text: str, keyword: str) -> str:
    system = (
        "Sən transkript mətinlərini xülasə etmək üçün ixtisaslaşmış modelisən. "
        "Cavabını Azərbaycan dilində, qısa və dəqiq ver."
    )
    user = (
        f"Verilmiş mətndə “{keyword}” sözü ilə bağlı cümlələri birləşdirərək"
        f" 2–3 cümləlik xülasə ver:\n\n{text}"
    )
    payload = {
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ],
        "model": "deepseek-chat",
        "max_tokens": 1024,
        "temperature": 0.3,
        "stream": False
    }
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }
    resp = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload)
    if resp.status_code != 200:
        logger.error("DeepSeek error: %s", resp.text)
        raise HTTPException(status_code=resp.status_code, detail="DeepSeek API error")
    return resp.json()["choices"][0]["message"]["content"]

# Pydantic models
class AnalyzeRequest(BaseModel):
    start_time: str
    end_time:   str

class SegmentInfo(BaseModel):
    segment_filename: str
    offset_secs:      float
    duration_secs:    float
    start_time:       str
    end_time:         str
    text:             str

class SearchResponse(BaseModel):
    summary:  str
    segments: List[SegmentInfo]

# Analyze endpoint
@app.post("/analyze/", response_model=dict)
async def analyze_text(req: AnalyzeRequest):
    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute(
            "SELECT text FROM transcripts WHERE start_time >= %s AND end_time <= %s",  
            (req.start_time, req.end_time)
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    if not rows:
        raise HTTPException(status_code=404, detail="No transcripts in this range")
    full_text = " ".join(r[0] for r in rows)
    summary   = get_summary_from_deepseek(full_text, keyword="")
    return {"summary": summary}

# Search endpoint
@app.get("/search/", response_model=SearchResponse)
def search_keyword(keyword: str = Query(..., min_length=1)):
    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute(
            """
            SELECT start_time, end_time, text,
                   segment_filename, offset_secs, duration_secs
            FROM transcripts
            WHERE text ILIKE %s
            ORDER BY start_time
            """,
            (f"%{keyword}%",)
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="Keyword not found")

    full_text = " ".join(r[2] for r in rows)
    summary   = get_summary_from_deepseek(full_text, keyword)
    segments  = []
    for st, en, txt, segfn, off, dur in rows:
        segments.append(SegmentInfo(
            segment_filename=segfn,
            offset_secs=     float(off),
            duration_secs=   float(dur),
            start_time=      st.isoformat(),
            end_time=        en.isoformat(),
            text=            txt
        ))
    return SearchResponse(summary=summary, segments=segments)

# Video clip streaming
@app.get("/video_clip/", response_class=StreamingResponse)
def get_clip(
    video_file: str = Query(...),
    start:      float  = Query(...),
    duration:   float  = Query(...)
):
    path = os.path.join(ARCHIVE_DIR, video_file)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Segment not found")

    cmd = [
        "ffmpeg",
        "-ss", str(start),
        "-i", path,
        "-t", str(duration),
        "-c", "copy",
        "-bsf:a", "aac_adtstoasc",
        "-movflags", "frag_keyframe+empty_moov",
        "-f", "mp4",
        "pipe:1"
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    return StreamingResponse(proc.stdout, media_type="video/mp4")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
