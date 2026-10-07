FROM debian:bookworm-slim AS whisper-build
ARG WHISPER_CPP_REF=v1.9.5
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git cmake build-essential curl && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git clone --depth 1 --branch "${WHISPER_CPP_REF}" https://github.com/ggml-org/whisper.cpp.git
WORKDIR /src/whisper.cpp
RUN cmake -S . -B build -DWHISPER_BUILD_TESTS=OFF -DWHISPER_BUILD_EXAMPLES=ON -DBUILD_SHARED_LIBS=OFF -DGGML_NATIVE=OFF \
 && cmake --build build --config Release -j1 --target whisper-cli \
 && ./models/download-ggml-model.sh tiny

FROM node:22-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates libgomp1 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=whisper-build /src/whisper.cpp/build/bin/whisper-cli /usr/local/bin/whisper-cli
COPY --from=whisper-build /src/whisper.cpp/models/ggml-tiny.bin /opt/whisper/models/ggml-tiny.bin
COPY package.json server.mjs ./
ENV PORT=3000 NODE_ENV=production WHISPER_MODEL=/opt/whisper/models/ggml-tiny.bin WHISPER_THREADS=2
EXPOSE 3000
USER node
CMD ["node","server.mjs"]
