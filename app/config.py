from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Optional


class Channel(BaseModel):
    id: str
    hls_url: str
    media_type: str = "video"


class Settings(BaseSettings):
    # List of TV channels to archive
    channels: List[Channel]

    # GPU üçün minimum boş yaddaş (MB)
    min_free_gpu_mb: int = Field(1024, alias="MIN_FREE_GPU_MB")

    # Base directories
    archive_base: str = Field("archive", alias="ARCHIVE_BASE")
    wav_base: str = Field("wav_segments", alias="WAV_BASE")

    # HLS TS parameters
    ts_segment_time: int = Field(8, alias="TS_SEGMENT_TIME")
    ts_list_size: int = Field(10800, alias="TS_LIST_SIZE")

    # WAV segmentation parameters
    wav_segment_time: int = Field(8, alias="WAV_SEGMENT_TIME")
    wav_overlap_time: int = Field(1, alias="WAV_OVERLAP_TIME")

    # Yeni: bounded queue üçün maksimum ölçü
    max_queue_size: int = Field(200, alias="MAX_QUEUE_SIZE")

    # Yeni: GPU üzərində eyni anda neçə iş
    gpu_max_jobs: int = Field(2, alias="GPU_MAX_JOBS")

    # Whisper model
    whisper_model: str = Field("large", alias="WHISPER_MODEL")
    device: str = Field("cuda", alias="DEVICE")
    compute_type: str = Field("float16", alias="COMPUTE_TYPE")

    timezone: str = Field("UTC", alias="TIMEZONE")

    # DeepSeek API
    deepseek_api_url: str = Field(..., alias="DEEPSEEK_API_URL")
    deepseek_key: str = Field(..., alias="DEEPSEEK_KEY")

    # PostgreSQL connection
    db_host: str = Field(..., alias="DB_HOST")
    db_port: int = Field(..., alias="DB_PORT")
    db_name: str = Field(..., alias="DB_NAME")
    db_user: str = Field(..., alias="DB_USER")
    db_password: str = Field(..., alias="DB_PASSWORD")
    db_sslmode: str = Field("require", alias="DB_SSLMODE")

    # Cleanup schedule & retention
    cleanup_hour: int = Field(3, alias="CLEANUP_HOUR")
    cleanup_minute: int = Field(0, alias="CLEANUP_MINUTE")
    cleanup_retention_days: int = Field(30, alias="CLEANUP_RETENTION_DAYS")

    # Logging & diagnostics
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    nintel_diag: int = Field(0, alias="NINTEL_DIAG")

    # API scheduler control
    run_scheduler_in_api: int = Field(0, alias="RUN_SCHEDULER_IN_API")

    # DB pool size
    db_pool_min: int = Field(1, alias="DB_POOL_MIN")
    db_pool_max: int = Field(10, alias="DB_POOL_MAX")

    # JWT / Auth
    jwt_secret: Optional[str] = Field(None, alias="JWT_SECRET")
    static_token: Optional[str] = Field(None, alias="STATIC_TOKEN")

    # TS local cleanup
    ts_local_max_age_min: int = Field(10, alias="TS_LOCAL_MAX_AGE_MIN")
    ts_local_clean_interval_min: int = Field(5, alias="TS_LOCAL_CLEAN_INTERVAL_MIN")

    # Wasabi
    wasabi_upload_enabled: bool = Field(True, alias="WASABI_UPLOAD_ENABLED")
    wasabi_access_key_id: Optional[str] = Field(None, alias="WASABI_ACCESS_KEY_ID")
    wasabi_secret_access_key: Optional[str] = Field(None, alias="WASABI_SECRET_ACCESS_KEY")
    wasabi_region: Optional[str] = Field(None, alias="WASABI_REGION")
    wasabi_bucket: Optional[str] = Field(None, alias="WASABI_BUCKET")
    wasabi_endpoint: Optional[str] = Field("https://s3.wasabisys.com", alias="WASABI_ENDPOINT")
    wasabi_prefix: Optional[str] = Field("", alias="WASABI_PREFIX")
    wasabi_delete_local_after_upload: bool = Field(True, alias="WASABI_DELETE_LOCAL_AFTER_UPLOAD")
    wasabi_presign_expire: int = Field(3600, alias="WASABI_PRESIGN_EXPIRE")

    # TS staging
    ts_staging_dir: Optional[str] = Field(None, alias="TS_STAGING_DIR")

    # Wasabi delete retries/delay
    wasabi_delete_retries: int = Field(60, alias="WASABI_DELETE_RETRIES")
    wasabi_delete_delay_ms: int = Field(500, alias="WASABI_DELETE_DELAY_MS")

    # TS neighbor scan & min clip sec
    ts_neighbor_scan_sec: int = Field(30, alias="TS_NEIGHBOR_SCAN_SEC")
    ts_min_clip_sec: int = Field(30, alias="TS_MIN_CLIP_SEC")

    # Admin signing
    admin_signing_key: Optional[str] = Field(None, alias="ADMIN_SIGNING_KEY")
    admin_key_id: Optional[str] = Field(None, alias="ADMIN_KEY_ID")
    admin_clock_skew_sec: int = Field(300, alias="ADMIN_CLOCK_SKEW_SEC")
    admin_nonce_ttl: int = Field(600, alias="ADMIN_NONCE_TTL")
    admin_allowlist: Optional[str] = Field(None, alias="ADMIN_ALLOWLIST")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra='ignore'
    )
