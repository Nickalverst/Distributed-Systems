#!/usr/bin/env bash
set -euo pipefail

# go to project root
cd "$(dirname "$0")"

# ---- deps (optional) ----
#if [ ! -d ".venv" ]; then
#  python3 -m venv .venv
#fi
source .venv/bin/activate

#pip -q install -r requirements.txt

# ---- rabbitmq (optional) ----
#if ! docker ps --format '{{.Names}}' | grep -q '^rabbit$'; then
#  docker run -d --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management >/dev/null
#fi

# ---- ports / commands ----
GATEWAY_CMD="python3 -m rest_api.gateway"
RANKING_CMD="python3 -m rest_api.ranking"
PROMO_CMD="python3 -m rest_api.promocao"
NOTIF_CMD="export RESEND_API_KEY='re_BxWEPtsH_3Yhz2gH3AGn9yz1c6GkRXN59' && python3 -m rest_api.notification"
STORE_CMD="python3 -m store_client"
FRONT_CMD="python3 -m http.server 8000"

# tmux helper
tmux kill-session -t promo 2>/dev/null || true

tmux new-session -d -s promo -n rabbit 'docker ps -q >/dev/null || true; tail -f /dev/null'

tmux new-window -t promo:1 -n gateway "$GATEWAY_CMD"
tmux new-window -t promo:2 -n ranking "$RANKING_CMD"
tmux new-window -t promo:3 -n promocao "$PROMO_CMD"
tmux new-window -t promo:4 -n frontend "$FRONT_CMD"
tmux new-window -t promo:5 -n store   "$STORE_CMD"

tmux new-window -t promo:6 -n notification "$NOTIF_CMD"

tmux select-window -t promo:5
tmux attach -t promo

echo "Frontend: http://localhost:8000/index.html"
