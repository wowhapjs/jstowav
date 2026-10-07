FROM node:22-bookworm-slim
ARG WHISPER_CPP_REF=v1.9.5
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates libgomp1 curl \
 && curl -fsSL "https://github.com/ggml-org/whisper.cpp/releases/download/${WHISPER_CPP_REF}/whisper-bin-x64.zip" -o /tmp/whisper.zip \
 && apt-get install -y --no-install-recommends unzip \
 && unzip -q /tmp/whisper.zip -d /tmp/whisper \
 && install -m 0755 "$(find /tmp/whisper -type f -name whisper-cli -print -quit)" /usr/local/bin/whisper-cli \
 && mkdir -p /opt/whisper/models \
 && curl -fsSL https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-tiny.bin -o /opt/whisper/models/ggml-tiny.bin \
 && rm -rf /tmp/whisper /tmp/whisper.zip /var/lib/apt/lists/*
WORKDIR /app
COPY package.json server.mjs ./
ENV PORT=3000 NODE_ENV=production WHISPER_MODEL=/opt/whisper/models/ggml-tiny.bin WHISPER_THREADS=2
EXPOSE 3000
USER node
CMD ["node","server.mjs"]
