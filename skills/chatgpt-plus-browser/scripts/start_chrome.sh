#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
PROFILE_DIR="${CHATGPT_PLUS_PROFILE_DIR:-$HOME/.case_data/browser-profiles/chatgpt-plus-browser}"
DEBUG_PORT="${CHATGPT_PLUS_DEBUG_PORT:-9222}"
START_URL="${1:-https://chatgpt.com/}"
CHROME_APP_NAME="${CHATGPT_PLUS_CHROME_APP_NAME:-Google Chrome}"
CHROME_APP_PATH="${CHATGPT_PLUS_CHROME_APP_PATH:-/Applications/Google Chrome.app}"
CHROME_BIN_PATH="${CHATGPT_PLUS_CHROME_BIN_PATH:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"

mkdir -p "$PROFILE_DIR"
mkdir -p "$PROJECT_ROOT/skills/chatgpt-plus-browser/.state"

has_existing_profile_data() {
  [ -d "$PROFILE_DIR/Default" ] || return 1
  [ -f "$PROFILE_DIR/Local State" ] || return 1
  return 0
}

if curl -fsS "http://127.0.0.1:${DEBUG_PORT}/json/version" >/dev/null 2>&1; then
  echo "Chrome debugging endpoint already available on port ${DEBUG_PORT}."
  exit 0
fi

open_args=(-na "$CHROME_APP_NAME")
if has_existing_profile_data; then
  open_args=(-g "${open_args[@]}")
fi

launch_with_open() {
  open "${open_args[@]}" --args \
    --user-data-dir="$PROFILE_DIR" \
    --remote-debugging-port="$DEBUG_PORT" \
    --new-window \
    "$START_URL"
}

launch_with_binary() {
  "$CHROME_BIN_PATH" \
    --user-data-dir="$PROFILE_DIR" \
    --remote-debugging-port="$DEBUG_PORT" \
    --new-window \
    "$START_URL" >/tmp/chatgpt-plus-browser.log 2>&1 &
}

if ! launch_with_open; then
  if [ -x "$CHROME_BIN_PATH" ]; then
    launch_with_binary
  else
    echo "Unable to find application named '$CHROME_APP_NAME' or executable '$CHROME_BIN_PATH'" >&2
    exit 1
  fi
fi

for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:${DEBUG_PORT}/json/version" >/dev/null 2>&1; then
    if has_existing_profile_data; then
      echo "Chrome automation profile ready in background on port ${DEBUG_PORT}."
    else
      echo "Chrome automation profile ready on port ${DEBUG_PORT}."
    fi
    exit 0
  fi
  sleep 1
done

echo "Chrome launched, but debugging endpoint did not appear on port ${DEBUG_PORT}." >&2
exit 1
