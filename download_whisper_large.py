from huggingface_hub import snapshot_download

# Yükləyəcəyin model
model_id = "openai/whisper-large-v3"

# Yükləmə və keşə yerləşdirmə
snapshot_download(
    repo_id=model_id,
    cache_dir=None  # None istifadə etsən default keşə müvəqqəti yüklənir
)