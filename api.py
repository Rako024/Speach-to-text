# api.py
#!/usr/bin/env python3
import os
import sys, logging

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from app.config import Settings
from app.services.db import DBClient
from app.api.routers import router
from app.api.auth_deps import require_auth

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("app.api.routers").setLevel(logging.INFO)

settings = Settings()

# DB cədvəllərinin hazır olduğuna əmin ol
db = DBClient(settings)
db.init_db()
db.init_schedule_table()

app = FastAPI(title="TV Transcript API")

# --- CORS (env ilə idarə) ---
# .env: CORS_ORIGINS=http://localhost:3000,https://myapp.com
origins_env = os.getenv("CORS_ORIGINS", "*")
if origins_env.strip() == "*":
    allow_origins = ["*"]
else:
    allow_origins = [o.strip() for o in origins_env.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Healthcheck ---
@app.get("/healthz", tags=["health"])
def healthz():
    return {"status": "ok"}

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
