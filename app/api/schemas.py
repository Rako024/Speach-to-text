from pydantic import BaseModel
from typing import List

class SegmentInfo(BaseModel):
    start_time: str
    end_time:   str
    text:       str
    segment_filename: str
    offset_secs: float
    duration_secs: float

class SearchResponse(BaseModel):
    summary: str
    segments: List[SegmentInfo]