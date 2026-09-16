#!/usr/bin/env bash
# 启动 MiroFish（后端 Flask 5001 + 前端 Vite 3000，图谱走本地 Zep 后端）
set -u

cd /workspaces/mirofish
pkill -f "backend/run.py" >/dev/null 2>&1 || true
pkill -f "vite" >/dev/null 2>&1 || true
sleep 1

cd /workspaces/mirofish/backend
setsid nohup /workspaces/mirofish/backend/.venv/bin/python run.py > /tmp/mf_backend.log 2>&1 < /dev/null &

cd /workspaces/mirofish/frontend
setsid nohup env PATH="/workspaces/dsh_stack/node24/bin:$PATH" /workspaces/dsh_stack/node24/bin/npm run dev > /tmp/mf_frontend.log 2>&1 < /dev/null &

sleep 12
echo "--- backend ---"
curl -s -o /dev/null -w "api=%{http_code}\n" http://127.0.0.1:5001/api/graph/project/list
echo "--- frontend ---"
curl -s -o /dev/null -w "web=%{http_code}\n" http://127.0.0.1:3000/
tail -3 /tmp/mf_backend.log
tail -3 /tmp/mf_frontend.log
