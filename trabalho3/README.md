# PromoRadar (trabalho3) — Frontend + Gateway README

## Overview
This folder contains a simple frontend (single-page HTML) and a small Flask "Gateway" REST + SSE service used in the project. The frontend expects the Gateway to run at `http://localhost:5000` by default and uses REST endpoints and Server-Sent Events (SSE) to show promotions, votes and real-time notifications.

## Quick test (frontend-only)
If you only want to check the UI and local interactions (no backend required):

```bash
cd trabalho3
python3 -m http.server 8000
```

Open in your browser: http://localhost:8000/index.html

Notes:
- Without the Gateway running, REST calls will fail and SSE will not connect; the UI includes local fallbacks so you can still click around.
- To change the frontend REST target edit the `BASE_URL` constant near the top of `index.html`.

## Full setup (recommended)
This runs the Gateway and the frontend so SSE and vote/publish flows work.

1. Create & activate a Python virtual environment and install dependencies:

```bash
cd trabalho3
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Ensure RabbitMQ is available. The Gateway uses RabbitMQ (default hostname `localhost:5672`). You can run a local RabbitMQ with Docker:

```bash
sudo docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

3. Start the Gateway (Flask service) and others microservices (in separate terminals):
We recommend to use tmux or multiple terminal windows to run all the microservices.

The Gateway listens on port `5000` by default.

```bash
python3 -m rest_api.gateway
python3 -m rest_api.ranking
python3 -m rest_api.promocao

#if you want to test notification emails
export RESEND_API_KEY=your_api_key_here  
python3 -m rest_api.notification
```
4. Run store service (in a separate terminal) to create promotions and publish them to the Gateway:

```bash
python3 -m store_client
```

5. Serve the frontend in a static server (separate terminal):

```bash
cd trabalho3
python3 -m http.server 8000
```

Open the frontend: http://localhost:8000/index.html

## Configuration
- `BASE_URL` in `index.html` (near the top of the inline script) points to the Gateway. Change it if the Gateway runs on a different host/port.
- Gateway file: `rest_api/gateway.py` — this file subscribes to events on RabbitMQ and exposes endpoints used by the frontend:
  - `GET /promocoes` — list promotions
  - `POST /promocoes` — publish a promotion (used by store service)
  - `POST /promocoes/<id>/votos` — submit vote
  - `POST /interesses` and `DELETE /interesses/<categoria>` — follow/unfollow categories
  - `GET /sse?user_id=...` — SSE endpoint for real-time notifications

## Important files
- `index.html` — frontend single-page app
- `rest_api/gateway.py` — Flask gateway (SSE + REST)
- `requirements.txt` — Python dependencies for the project
- `rest_api/requirements.txt` — requirements for the gateway (subset)