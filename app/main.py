from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from app.api.routes import router
from app.config import Settings
import os

s = Settings()
app = FastAPI()

# Serve index.html + /archive statics
app.mount("/archive", StaticFiles(directory=s.archive_dir), name="archive")
@app.get("/", include_in_schema=False)
def index():
    return open(os.path.join(s.archive_dir, "index.html")).read()

# Include our router
app.include_router(router)