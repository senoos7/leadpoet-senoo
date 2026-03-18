#!/usr/bin/env bash

# ============================================================
# run_multi_miner.sh — manage multiple Leadpoet miners (pm2)
#
# Each miner entry needs a unique:
#   HOTKEY      — bittensor hotkey name (under the same coldkey)
#   PM2_NAME    — unique pm2 process name
#
# Usage:
#   ./run_multi_miner.sh start          # start all miners
#   ./run_multi_miner.sh stop           # stop all miners
#   ./run_multi_miner.sh restart        # restart all miners
#   ./run_multi_miner.sh status         # show pm2 status
#   ./run_multi_miner.sh logs <name>    # tail logs for one miner
# ============================================================

set -e

# ----------------------------------------------------------------
# Shared settings (same for all miners)
# ----------------------------------------------------------------
WALLET_NAME="${WALLET_NAME:-superbit-darnsin}"        # coldkey (shared)
NETWORK="${NETWORK:-finney}"
NETUID="${NETUID:-71}"
WALLET_PATH="${WALLET_PATH:-}"             # if empty: use bittensor default
MINER_SCRIPT="./neurons/miner.py"

# ----------------------------------------------------------------
# Miner definitions — edit this section for your hotkeys/names
# Each row: "HOTKEY PM2_NAME"
# ----------------------------------------------------------------
MINERS=(
  # "default leadpoet_miner_1"
  # "hotkey2 leadpoet_miner_2"
  "leadpoet-miner-63181 leadpoet_miner_1"
  "leadpoet-miner-63182 leadpoet_miner_2"
)

# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------
require_pm2() {
  if ! command -v pm2 &>/dev/null; then
    echo "Error: pm2 is not installed. Install with: npm install -g pm2"
    exit 1
  fi
}

miner_args() {
  local hotkey="$1"
  local args="--wallet_name ${WALLET_NAME} \
    --wallet_hotkey ${hotkey} \
    --netuid ${NETUID} \
    --subtensor_network ${NETWORK}"

  if [ -n "${WALLET_PATH}" ]; then
    args="${args} --wallet_path ${WALLET_PATH}"
  fi

  echo "${args}"
}

start_all() {
  require_pm2

  if [ "${#MINERS[@]}" -eq 0 ]; then
    echo "No miners configured yet."
    echo "Edit run_multi_miner.sh and add entries to the MINERS array, e.g.:"
    echo '  MINERS=('
    echo '    "default leadpoet_miner_1"'
    echo '    "hotkey2 leadpoet_miner_2"'
    echo '  )'
    exit 1
  fi

  export PYTHONPATH="$(pwd)"

  for entry in "${MINERS[@]}"; do
    read -r hotkey pm2_name <<< "${entry}"
    echo "Starting miner: pm2=${pm2_name} hotkey=${hotkey}"
    pm2 delete "${pm2_name}" 2>/dev/null || true
    # shellcheck disable=SC2046
    pm2 start "${MINER_SCRIPT}" \
      --name "${pm2_name}" -- \
      $(miner_args "${hotkey}")
  done

  pm2 save
  echo ""
  echo "All miners started. Check status with: pm2 list"
}

stop_all() {
  require_pm2
  for entry in "${MINERS[@]}"; do
    read -r hotkey pm2_name <<< "${entry}"
    echo "Stopping: ${pm2_name}"
    pm2 stop "${pm2_name}" 2>/dev/null || echo "  (already stopped)"
  done
}

restart_all() {
  require_pm2
  for entry in "${MINERS[@]}"; do
    read -r hotkey pm2_name <<< "${entry}"
    echo "Restarting: ${pm2_name}"
    pm2 restart "${pm2_name}" 2>/dev/null || echo "  (not running, starting instead)"
  done
}

status_all() {
  require_pm2
  pm2 list
}

logs_one() {
  local name="$1"
  if [ -z "${name}" ]; then
    echo "Usage: $0 logs <pm2_name>"
    echo "Configured miners:"
    for entry in "${MINERS[@]}"; do
      read -r hotkey pm2_name <<< "${entry}"
      echo "  ${pm2_name} (hotkey=${hotkey})"
    done
    exit 1
  fi
  pm2 logs "${name}"
}

# ----------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------
case "${1:-}" in
  start)    start_all ;;
  stop)     stop_all ;;
  restart)  restart_all ;;
  status)   status_all ;;
  logs)     logs_one "${2:-}" ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs <pm2_name>}"
    echo ""
    echo "Configured miners:"
    if [ "${#MINERS[@]}" -eq 0 ]; then
      echo "  (none yet — edit MINERS array in run_multi_miner.sh)"
    else
      for entry in "${MINERS[@]}"; do
        read -r hotkey pm2_name <<< "${entry}"
        echo "  pm2=${pm2_name}  hotkey=${hotkey}"
      done
    fi
    exit 1
    ;;
esac

