from pydantic import BaseModel

class SegmentInfo(BaseModel):
    channel_id: str
    start_time: str
    end_time:   str
    text:       str
    segment_filename: str
    offset_secs: float
    duration_secs: float
    deleted:         bool

class SearchResponse(BaseModel):
    summary: str
    segments: list[SegmentInfo]