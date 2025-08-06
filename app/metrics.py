from prometheus_client import Counter, Gauge

# Emal olunmuş seqment sayı
PROCESSED = Counter(
    'processed_segments_total',
    'Emal olunmuş seqment sayı',
    ['channel']
)

# Worker xətaları
ERRORS = Counter(
    'worker_errors_total',
    'Worker xətaları',
    ['channel']
)

# WAV queue uzunluğu
QUEUE_LEN = Gauge(
    'wav_queue_length',
    'WAV queue uzunluğu',
    ['channel']
)

# Kanal üzrə aktiv process (worker) sayı
ACTIVE_WORKERS = Gauge(
    'active_workers_total',
    'Kanal üzrə aktiv worker sayı',
    ['channel']
)
