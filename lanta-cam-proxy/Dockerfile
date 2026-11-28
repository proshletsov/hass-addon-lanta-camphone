ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:latest
FROM $BUILD_FROM

RUN apk add --no-cache python3 py3-pip jq ffmpeg curl && \
    pip3 install --no-cache-dir --break-system-packages aiohttp aiomqtt

COPY rootfs /
COPY proxy.py /proxy.py
RUN sed -i 's/\r$//' /etc/services.d/lanta-cam-proxy/run && \
    chmod +x /etc/services.d/lanta-cam-proxy/run
