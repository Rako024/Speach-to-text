#!/usr/bin/env bash
set -e

echo "🚀 Container başlayır..."

# GPU və CUDA yoxlamaları
echo "🔧 GPU Status:"
nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv,noheader,nounits || echo "❌ nvidia-smi əlçatmazdır"

echo "🔍 CUDA Libraries:"
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
ls -la /usr/lib/x86_64-linux-gnu/ | grep cudnn | head -5 || echo "❌ cuDNN tapılmadı"

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

echo "🎙️ Faster-Whisper Test:"
python -c "
try:
    from faster_whisper import WhisperModel
    print('✅ faster-whisper import uğurludur')
    print('✅ faster-whisper işləyir')
except Exception as e:
    print(f'❌ faster-whisper xətası: {e}')
"

# 🎯 Həm main.py, həm də FastAPI serveri başlat
echo "🔁 Dispatcher (main.py) və FastAPI (api.py) başlayır..."

# main.py fon proses kimi
python3 main.py &

# api.py foreground-da
exec uvicorn api:app --host 0.0.0.0 --port 8000 --workers 1
