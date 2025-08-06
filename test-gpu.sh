# test-gpu.sh
#!/bin/bash
python -c "import torch; print('CUDA available:', torch.cuda.is_available())"
echo "GPU test completed"