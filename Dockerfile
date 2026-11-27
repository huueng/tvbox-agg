# syntax=docker/dockerfile:1
# TVBox 聚合器 —— 纯标准库，无第三方依赖（镜像约 50MB）
FROM python:3.12-alpine

# 时区：日志和「上次成功」时间要和本地一致
RUN apk add --no-cache tzdata \
 && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
 && echo "Asia/Shanghai" > /etc/timezone

WORKDIR /app
COPY app/ /app/
RUN mkdir -p /app/data/out

ENV PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai \
    PORT=5680 \
    BIND=0.0.0.0 \
    INTERVAL=6 \
    WORKERS=3 \
    TIMEOUT=20 \
    DROP_AFTER=3

EXPOSE 5680

# 数据（源库 / 输出 / 状态）全部走挂载，容器随时可重建
VOLUME ["/app/data"]

HEALTHCHECK --interval=60s --timeout=8s --start-period=30s --retries=3 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:5680/healthz',timeout=5)"

CMD ["python", "server.py", \
     "--seeds", "/app/data/sources.json", \
     "--out", "/app/data/out", \
     "--state", "/app/data/state.json"]
