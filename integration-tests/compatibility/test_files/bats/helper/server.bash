# Server lifecycle helpers for SQL server compatibility tests.
# Loaded alongside helper/common (for old_dolt and new_dolt) and the main suite's
# helper/query-server-common (for definePORT and IS_WINDOWS).

# new_dolt_cli forwards arguments to new_dolt with connection flags pinned so the
# command reaches the running server without sql-server auto-discovery.
new_dolt_cli() {
  new_dolt --host 127.0.0.1 --port "$PORT" --user root --password "" --no-tls --use-db "$DB_NAME" "$@"
}

# wait_for_old_server polls new_dolt_cli until it connects to the server or the
# timeout in milliseconds elapses.
wait_for_old_server() {
  local port="$1"
  local timeout_ms="$2"
  local end_time=$((SECONDS + (timeout_ms / 1000)))

  while [ $SECONDS -lt $end_time ]; do
    if new_dolt_cli sql -q "SELECT 1;" > /dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "wait_for_server_connection: failed to connect on port $port within ${timeout_ms}ms" >&2
  return 1
}

# start_old_sql_server starts old_dolt sql-server on a random available port and
# sets PORT, DB_NAME, and SERVER_PID in the calling scope.
start_old_sql_server() {
  PORT=$(definePORT)
  DB_NAME=$(basename "$PWD")

  if [ "$IS_WINDOWS" != true ]; then
    old_dolt sql-server --host 0.0.0.0 --port="$PORT" --socket "dolt.$PORT.sock" > server.log 2>&1 3>&- &
  else
    old_dolt sql-server --host 0.0.0.0 --port="$PORT" > server.log 2>&1 3>&- &
  fi
  SERVER_PID=$!

  wait_for_old_server "$PORT" 8500
}

# stop_old_sql_server terminates the running sql-server and removes its socket file.
stop_old_sql_server() {
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    while ps -p "$SERVER_PID" > /dev/null 2>&1; do
      sleep 0.1
    done
  fi
  SERVER_PID=""
  if [ -n "$PORT" ] && [ -f "dolt.$PORT.sock" ]; then
    rm -f "dolt.$PORT.sock"
  fi
}

# is_dev_build returns 0 when the binary path matches DOLT_DEV_BUILD_PATH, which the runner
# exports for the freshly-built dolt so version-literal compares do not skip the dev build.
is_dev_build() {
  [ -n "${DOLT_DEV_BUILD_PATH:-}" ] && [ "$1" = "$DOLT_DEV_BUILD_PATH" ]
}

# skip_if_new_lte skips when new_dolt is at or below max_version. The dev build is exempt
# only when its reported version is already at or above max_version.
skip_if_new_lte() {
  local max_version="$1"
  local reason="$2"
  local new_version
  new_version=$(new_dolt version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
  if [ -z "$new_version" ]; then
    return 0
  fi
  if is_dev_build "${DOLT_NEW_BIN:-}" && [ "$(printf '%s\n' "$new_version" "$max_version" | sort -V | head -n1)" = "$max_version" ]; then
    return 0
  fi
  if [ "$(printf '%s\n' "$max_version" "$new_version" | sort -V | head -n1)" = "$new_version" ]; then
    skip "$reason (new_dolt version: $new_version)"
  fi
}

# skip_if_old_lte skips when old_dolt is at or below max_version. The dev build is exempt
# only when its reported version is already at or above max_version.
skip_if_old_lte() {
  local max_version="$1"
  local reason="$2"
  local old_version
  old_version=$(old_dolt version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
  if [ -z "$old_version" ]; then
    return 0
  fi
  if is_dev_build "${DOLT_OLD_BIN:-}" && [ "$(printf '%s\n' "$old_version" "$max_version" | sort -V | head -n1)" = "$max_version" ]; then
    return 0
  fi
  if [ "$(printf '%s\n' "$max_version" "$old_version" | sort -V | head -n1)" = "$old_version" ]; then
    skip "$reason (old_dolt version: $old_version)"
  fi
}

# strip_ansi removes ANSI color escape sequences from the given string.
strip_ansi() {
  printf "%s\n" "$1" | sed 's/\x1b\[[0-9;]*m//g'
}

# extract_commit_hash returns the first commit hash from dolt log output, after
# stripping ANSI escape sequences.
extract_commit_hash() {
  printf "%s\n" "$1" | sed 's/\x1b\[[0-9;]*m//g' | grep -m1 '^commit ' | awk '{print $2}'
}

# latest_commit returns the hash of the most recent commit. It queries dolt_log via
# new_dolt_cli using only legacy columns so it works against old sql-servers.
latest_commit() {
  new_dolt_cli sql -r csv -q "SELECT commit_hash FROM dolt_log ORDER BY date DESC LIMIT 1;" | tail -n1 | tr -d '\r'
}
