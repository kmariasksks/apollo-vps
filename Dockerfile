FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb x11vnc fluxbox wget gnupg ca-certificates \
    fonts-liberation libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libasound2 libpango-1.0-0 libcairo2 libatspi2.0-0 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r /tmp/requirements.txt || true
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN patchright install chrome --with-deps

WORKDIR /app
COPY browser_engine.py /app/browser_engine.py
COPY api_server.py /app/api_server.py
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV DISPLAY=:99

ENTRYPOINT ["/app/entrypoint.sh"]