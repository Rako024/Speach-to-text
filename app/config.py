from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import List


class Channel(BaseModel):
    id: str
    hls_url: str
    media_type: str = "video"

class Settings(BaseSettings):
    # List of TV channels to archive
    channels: List[Channel]
    # GPU üçün minimum boş yaddaş (MB) – .env-də MIN_FREE_GPU_MB şəklində override olar
    min_free_gpu_mb: int = 1024

    # Base directories; per-channel subfolders are created under these
    archive_base: str = "archive"
    wav_base:    str = "wav_segments"

    # HLS TS parameters
    ts_segment_time: int = 8
    ts_list_size:    int = 10800
    
    # WAV segmentation parameters
    wav_segment_time: int = 8
    wav_overlap_time: int = 1

    # Yeni: bounded queue üçün maksimum ölçü
    max_queue_size: int = 16

    # Yeni: GPU üzərində eyni anda neçə iş
    gpu_max_jobs: int = 2

    # Whisper model
    whisper_model: str = "large"
    device:        str = "cuda"
    compute_type:  str = "float16"

    timezone: str = "UTC"

    # DeepSeek API
    deepseek_api_url: str
    deepseek_key:     str

    # PostgreSQL connection
    db_host:     str
    db_port:     int
    db_name:     str
    db_user:     str
    db_password: str
    db_sslmode: str = Field("require", env="DB_SSLMODE")
    # Cleanup schedule & retention
    cleanup_hour:           int = 3
    cleanup_minute:         int = 0
    cleanup_retention_days: int = 30

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8"
    )
