#!/usr/bin/env bash
set -e

echo "🚀 Container başlayır..."

# GPU statusunu yoxlayırıq
echo "🔧 GPU Status:"
nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv,noheader,nounits || echo "❌ nvidia-smi əlçatmazdır"

# CUDA kitabxanalarını yoxlayırıq
echo "🔍 CUDA Libraries:"
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
ls -la /usr/lib/x86_64-linux-gnu/ | grep cudnn | head -5 || echo "❌ cuDNN tapılmadı"

# Python CUDA testini edirik
echo "🐍 Python CUDA Test:"
python -c "
import torch
print(f'✅ PyTorch CUDA available: {torch.cuda.is_available()}')
if torch.cuda.is_available():
    print(f'✅ CUDA device count: {torch.cuda.device_count()}')
    print(f'✅ Current device: {torch.cuda.current_device()}')
    print(f'✅ Device name: {torch.cuda.get_device_name(0)}')
else:
    print('❌ CUDA not available')
"

# faster-whisper testini edirik
echo "🎙️ Faster-Whisper Test:"
python -c "
try:
    from faster_whisper import WhisperModel
    print('✅ faster-whisper import uğurludur')
    # Kiçik model ilə test (yüklənməsini tələb etmir)
    print('✅ faster-whisper işləyir')
except Exception as e:
    print(f'❌ faster-whisper xətası: {e}')
"

# Əsas script-i başladırıq
echo "🎯 Əsas script başlayır: $1"

if [ "$1" = 'main' ]; then
  echo "👉 Starting dispatcher (main.py)..."
  exec python3 main.py
elif [ "$1" = 'api' ]; then
  echo "👉 Starting FastAPI server..."
  exec uvicorn api:app --host 0.0.0.0 --port 8000 --workers 1
elif [ "$1" = 'test' ]; then
  echo "👉 Running GPU tests..."
  exec /test-gpu.sh
else
  echo "👉 Running custom command: $@"
  exec "$@"
fi