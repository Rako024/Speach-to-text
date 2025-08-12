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
     # Yeni env-lər üçün field-lar (alias ilə)
    log_level: str = Field('INFO', alias='LOG_LEVEL')
    nintel_diag: int = Field(0, alias='NINTEL_DIAG')
    run_scheduler_in_api: int = Field(0, alias='RUN_SCHEDULER_IN_API')

    db_pool_min: int = Field(1, alias='DB_POOL_MIN')
    db_pool_max: int = Field(10, alias='DB_POOL_MAX')

    gpu_max_jobs: int = Field(2, alias='GPU_MAX_JOBS')
    max_queue_size: int = Field(200, alias='MAX_QUEUE_SIZE')
    
    
    
     # JWT / Auth
    jwt_secret: str | None = Field(None, alias="JWT_SECRET")      # HS256 üçün
    static_token: str | None = Field(None, alias="STATIC_TOKEN")  # X-API-Key / Bearer statik açar


    wasabi_upload_enabled: bool = Field(True, alias="WASABI_UPLOAD_ENABLED")
    wasabi_access_key_id: str | None = Field(None, alias="WASABI_ACCESS_KEY_ID")
    wasabi_secret_access_key: str | None = Field(None, alias="WASABI_SECRET_ACCESS_KEY")
    wasabi_region: str | None = Field(None, alias="WASABI_REGION")
    wasabi_bucket: str | None = Field(None, alias="WASABI_BUCKET")
    wasabi_endpoint: str | None = Field("https://s3.wasabisys.com", alias="WASABI_ENDPOINT")
    wasabi_prefix: str | None = Field("", alias="WASABI_PREFIX")
    wasabi_delete_local_after_upload: bool = Field(True, alias="WASABI_DELETE_LOCAL_AFTER_UPLOAD")
    wasabi_presign_expire: int = Field(3600, alias="WASABI_PRESIGN_EXPIRE")

    ts_staging_dir: str | None = Field(None, alias="TS_STAGING_DIR")  
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra='ignore'         
    )
