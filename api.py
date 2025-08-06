# api.py
#!/usr/bin/env python3
import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import Settings
from app.services.db import DBClient
from app.api.routers import router

settings = Settings()

# DB cədvəllərinin hazır olduğuna əmin ol
db = DBClient(settings)
db.init_db()
db.init_schedule_table()

app = FastAPI(title="TV Transcript API")

# 1) Kök (“/”) üçün index.html
@app.get("/", include_in_schema=False)
def index():
    html_path = os.path.join(settings.archive_base, "index.html")
    if not os.path.exists(html_path):
        raise HTTPException(status_code=404, detail="index.html tapılmadı")
    return FileResponse(html_path)

# 2) Arxiv faylları: /archive altında mount
os.makedirs(settings.archive_base, exist_ok=True)
app.mount(
    "/archive",
    StaticFiles(directory=settings.archive_base),
    name="archive",
)

# 3) Bütün routeləri (search, clip, schedule) əlavə et
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
