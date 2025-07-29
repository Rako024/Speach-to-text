from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List

class Channel(BaseModel):
    id: str          # unique channel identifier
    hls_url: str     # channel’s HLS URL

class Settings(BaseSettings):
    # List of TV channels to archive
    channels: List[Channel]

    # Base directories; per-channel subfolders are created under these
    archive_base: str = "archive"
    wav_base:    str = "wav_segments"

    # HLS TS parameters (shared)
    ts_segment_time: int = 8
    ts_list_size:    int = 10800

    # WAV segmentation parameters (shared)
    wav_segment_time: int = 8
    wav_overlap_time: int = 1

    # Whisper model
    whisper_model: str = "large"
    device:        str
    compute_type:  str

    # DeepSeek API
    deepseek_api_url: str
    deepseek_key:     str

    # PostgreSQL connection
    db_host:     str
    db_port:     int
    db_name:     str
    db_user:     str
    db_password: str

    # Cleanup schedule & retention
    #   CLEANUP_HOUR           – hour of day (UTC) to run cleanup
    #   CLEANUP_MINUTE         – minute of the hour to run cleanup
    #   CLEANUP_RETENTION_DAYS – how many days to keep before deleting
    cleanup_hour:           int = 3
    cleanup_minute:         int = 0
    cleanup_retention_days: int = 30

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8"
    )
