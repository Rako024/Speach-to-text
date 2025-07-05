from fastapi import Depends
from app.config import Settings
from app.services.db import DBClient
from app.services.summarizer import DeepSeekClient

settings = Settings()
_db = None
_summ = None

def get_db():
    global _db
    if not _db:
        _db = DBClient(settings)
    return _db

def get_summarizer():
    global _summ
    if not _summ:
        _summ = DeepSeekClient(settings)
    return _summ