#!/bin/bash
set -e

# ── Очищення застряглих замків від попереднього (аварійного) запуску ──
# Після reboot/краху лишаються lock-файли, через які Xvfb і Chrome не стартують.
rm -f /tmp/.X99-lock /tmp/.X11-unix/X99 2>/dev/null || true
rm -f /app/profile/SingletonLock /app/profile/SingletonCookie /app/profile/SingletonSocket 2>/dev/null || true
# на випадок, якщо процеси якось лишились живими
pkill -9 Xvfb 2>/dev/null || true
pkill -9 x11vnc 2>/dev/null || true
sleep 1

Xvfb :99 -screen 0 1280x800x24 -ac +extension GLX +render -noreset &
sleep 2

fluxbox > /dev/null 2>&1 &
sleep 1

x11vnc -display :99 -nopw -forever -shared -rfbport 5900 -bg -o /tmp/x11vnc.log

echo "==============================================="
echo " Xvfb + VNC запущені (без пароля, через тунель)."
echo " Запускаю API-сервер (api_server.py)."
echo "==============================================="

exec python