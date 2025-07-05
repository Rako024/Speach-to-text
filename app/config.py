# app/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # HLS stream URL
    hls_url: str
    # Arxiv TS faylları harada saxlanır
    archive_dir: str = "archive"
    # WAV faylların çıxacağı qovluq
    wav_dir: str = "wav_segments"
    # HLS TS segment parametrləri
    ts_segment_time: int = 8
    ts_list_size: int = 10800

    # WAV segment parametrləri
    wav_segment_time: int = 8
    wav_overlap_time: int = 1

    # Whisper model üçün
    whisper_model: str = "large"
    device: str         # məsələn "cuda" və ya "cpu"
    compute_type: str   # məsələn "float16"

    # DeepSeek API
    deepseek_api_url: str
    deepseek_key:     str

    # PostgreSQL bağlantısı
    db_host:     str
    db_port:     int
    db_name:     str
    db_user:     str
    db_password: str

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
