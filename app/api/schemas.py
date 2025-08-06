# app/api/schemas.py
from pydantic import BaseModel, Field
from datetime import time
from typing import List, Optional

class SegmentInfo(BaseModel):
    id:                Optional[int]   = None
    channel_id:        str
    start_time:        str
    end_time:          str
    text:              str
    segment_filename:  str
    offset_secs:       float
    duration_secs:     float
    score:             Optional[float] = None

class SearchResponse(BaseModel):
    summary:  str
    segments: List[SegmentInfo]

class IntervalIn(BaseModel):
    start_time: time = Field(..., description="Başlama vaxtı (HH:MM)")
    end_time:   time = Field(..., description="Bitmə vaxtı (HH:MM)")

class IntervalOut(IntervalIn):
    id: int

class SummarizeResponse(BaseModel):
    summary: str
