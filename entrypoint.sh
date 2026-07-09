#!/bin/bash
set -e

Xvfb :99 -screen 0 1280x800x24 -ac +extension GLX +render -noreset &
sleep 2

fluxbox > /dev/null 2>&1 &
sleep 1

x11vnc -display :99 -nopw -forever -shared -rfbport 5900 -bg -o /tmp/x11vnc.log

echo "==============================================="
echo " Xvfb + VNC запущені (без пароля, через тунель)."
echo " Запускаю API-сервер (api_server.py)."
echo "==============================================="

exec python /app/api_server.py