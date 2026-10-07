FROM node:22-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package.json server.mjs ./
ENV PORT=3000 NODE_ENV=production
EXPOSE 3000
USER node
CMD ["node","server.mjs"]
