# Təmiz PyTorch base image
FROM pytorch/pytorch:2.1.0-cuda12.1-cudnn8-runtime

# Timezone avtomatik konfiqurasiyası (Asia/Baku)
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Baku
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Lazımi sistem paketlərini qur
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    software-properties-common \
    gnupg2 \
    ffmpeg \
    libsndfile1 \
    git \
    wget && \
    which wget && \
    wget --version

# cuDNN repozitorisini əlavə et və quraşdır
COPY cudnn-local-repo-ubuntu2004-8.9.0.131_1.0-1_amd64.deb /tmp/cudnn-repo.deb
RUN dpkg -i /tmp/cudnn-repo.deb && \
    cp /var/cudnn-local-repo-ubuntu2004-8.9.0.131/cudnn-local-*-keyring.gpg /usr/share/keyrings/ && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    libcudnn8 \
    libcudnn8-dev && \
    rm -rf /var/lib/apt/lists/* && \
    rm /tmp/cudnn-repo.deb

# Layihə qovluğu
WORKDIR /app

# Requirements faylını kopyala və pip ilə paketləri quraşdır
COPY requirements.txt ./
RUN pip install --no-cache-dir --default-timeout=100 --retries=5 -r requirements.txt -i https://pypi.org/simple

# Kod və skriptləri əlavə et
COPY . .

# Entrypoint və test-gpu üçün icra icazəsi ver
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && chmod +x /app/test-gpu.sh

# Portları aç
EXPOSE 8000 8001

# Konteynerin başlanğıc nöqtəsi
ENTRYPOINT ["/entrypoint.sh"]
CMD []
