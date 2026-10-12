#!/bin/sh
# Установка очереди на биоимпеданс на чистый Ubuntu 22/24 одной командой (запускать под root):
#   curl -fsSL https://zdravcity-med.github.io/srv/install.sh | sh
# Переменные: PIN=2604 DOMAIN=<домен или пусто -> <ip>.sslip.io> KEY="ssh-ed25519 ..." (добавить ключ в authorized_keys)
set -e
BASE=https://zdravcity-med.github.io/srv
PIN=${PIN:-2604}
IP=$(curl -fsS -4 https://ifconfig.me/ip || hostname -I | awk '{print $1}')
DOMAIN=${DOMAIN:-$(echo "$IP" | tr . -).sslip.io}
KEY=${KEY:-"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIB/KsCRkSCRPo0dz/2e0IOtXvl4VTubN79Fxgt5FJ7Js zdravcity@MacBook-Pro-Zdravcity.local"}

mkdir -p /opt/bioqueue /root/.ssh && chmod 700 /root/.ssh
grep -qF "$KEY" /root/.ssh/authorized_keys 2>/dev/null || echo "$KEY" >> /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys
for f in server.py index.html admin.html board.html; do curl -fsSL "$BASE/$f" -o "/opt/bioqueue/$f"; done

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq python3 debian-keyring debian-archive-keyring apt-transport-https curl gpg >/dev/null
if ! command -v caddy >/dev/null; then
  curl -1sLf https://dl.cloudsmith.io/public/caddy/stable/gpg.key | gpg --dearmor --yes -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
  curl -1sLf https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt > /etc/apt/sources.list.d/caddy-stable.list
  apt-get update -qq && apt-get install -y -qq caddy >/dev/null
fi
cat > /etc/systemd/system/bioqueue.service <<UNIT
[Unit]
Description=Bioimpedance queue
After=network.target
[Service]
WorkingDirectory=/opt/bioqueue
ExecStart=/usr/bin/python3 /opt/bioqueue/server.py --port 8765 --pin $PIN
Restart=always
RestartSec=2
[Install]
WantedBy=multi-user.target
UNIT
cat > /etc/caddy/Caddyfile <<CADDY
$DOMAIN {
    reverse_proxy 127.0.0.1:8765
    encode gzip
}
CADDY
timedatectl set-timezone Europe/Moscow || true
systemctl daemon-reload
systemctl enable --now bioqueue >/dev/null 2>&1
systemctl restart bioqueue
systemctl enable --now caddy >/dev/null 2>&1
systemctl reload caddy || systemctl restart caddy
sleep 2
echo "bioqueue: $(systemctl is-active bioqueue)  caddy: $(systemctl is-active caddy)"
echo "ГОТОВО: https://$DOMAIN/   оператор: https://$DOMAIN/admin  (PIN $PIN)"
