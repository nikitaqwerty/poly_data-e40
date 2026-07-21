#!/bin/bash
# E40.H V1 tape top-up wrapper — resumes update_chain_v1.py from its cursor,
# retries transient HyperSync connection drops (the 2026-07-21 20:49Z death),
# and stops at V1_STOP_BLOCK (~2026-05-07, past the 2026-05-05 cutover margin).
# Token is fetched into the environment only; never echoed or logged.
set -u
cd /home/nikita/poly_data-e40
export V1_STOP_BLOCK=86480000
HYPERSYNC_API=$(/home/nikita/rust_polytrader/scripts/gh_env.sh get HYPERSYNC_API)
export HYPERSYNC_API
if [ -z "$HYPERSYNC_API" ]; then
  echo "[wrapper] FATAL: HYPERSYNC_API empty from gh_env.sh $(date -u +%FT%TZ)" >> data/topup_v1.log
  exit 1
fi
for i in $(seq 1 20); do
  .venv/bin/python update_utils/update_chain_v1.py >> data/topup_v1.log 2>&1
  rc=$?
  if [ $rc -eq 0 ]; then
    echo "[wrapper] clean exit rc=0 attempt=$i $(date -u +%FT%TZ)" >> data/topup_v1.log
    exit 0
  fi
  echo "[wrapper] attempt $i died rc=$rc, retrying in 30s $(date -u +%FT%TZ)" >> data/topup_v1.log
  sleep 30
done
echo "[wrapper] gave up after 20 attempts $(date -u +%FT%TZ)" >> data/topup_v1.log
exit 1
