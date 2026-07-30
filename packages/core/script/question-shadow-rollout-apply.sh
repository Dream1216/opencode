#!/usr/bin/env bash
set -euo pipefail

sample_rate="${1:-}"
case "$sample_rate" in
  0|0.01|0.05|0.25|1) ;;
  *) echo "sample rate must be 0, 0.01, 0.05, 0.25, or 1" >&2; exit 64 ;;
esac

env_file="${OPENCODE_ENV_FILE:-/www/wwwroot/code.jxhh.com/.env}"
service="${OPENCODE_SYSTEMD_SERVICE:-opencode-web.service}"
lock_file="${OPENCODE_QUESTION_SHADOW_ROLLOUT_LOCK:-/run/opencode-question-shadow-rollout.lock}"
exec 9>"$lock_file"
flock -n 9 || { echo "another Question shadow rollout is active" >&2; exit 75; }

backup="${env_file}.p7.3.7.$(date +%s).bak"
cp -p "$env_file" "$backup"

upsert() {
  local key="$1"
  local value="$2"
  local temporary
  temporary="$(mktemp "${env_file}.XXXXXX")"
  awk -v key="$key" -v value="$value" '
    BEGIN { found=0 }
    index($0, key "=") == 1 { print key "=" value; found=1; next }
    { print }
    END { if (!found) print key "=" value }
  ' "$env_file" > "$temporary"
  chmod --reference="$env_file" "$temporary"
  chown --reference="$env_file" "$temporary"
  mv "$temporary" "$env_file"
}

if [ "$sample_rate" = "0" ]; then
  upsert OPENCODE_EXPERIMENTAL_QUESTION_COMMAND_GATEWAY_SHADOW false
else
  upsert OPENCODE_EXPERIMENTAL_QUESTION_COMMAND_GATEWAY_SHADOW true
fi
upsert OPENCODE_QUESTION_SHADOW_SAMPLE_RATE "$sample_rate"
upsert OPENCODE_QUESTION_SHADOW_STAGE_STARTED_AT "$(date +%s000)"

if ! systemctl restart "$service"; then
  cp -p "$backup" "$env_file"
  systemctl restart "$service" || true
  exit 1
fi

set -a
. "$env_file"
set +a
health_url="${OPENCODE_HEALTH_URL:-http://127.0.0.1:4096/global/health}"
for _ in $(seq 1 40); do
  if [ -n "${OPENCODE_SERVER_PASSWORD:-}" ]; then
    status="$(curl -sS -o /tmp/opencode-question-shadow-health.json -w '%{http_code}' \
      -u "${OPENCODE_SERVER_USERNAME:-opencode}:$OPENCODE_SERVER_PASSWORD" "$health_url" || true)"
  else
    status="$(curl -sS -o /tmp/opencode-question-shadow-health.json -w '%{http_code}' "$health_url" || true)"
  fi
  if [ "$status" = "200" ]; then
    printf '{"status":"ok","sampleRate":"%s","backup":"%s"}\n' "$sample_rate" "$backup"
    exit 0
  fi
  sleep 1
done

cp -p "$backup" "$env_file"
systemctl restart "$service" || true
echo "OpenCode health check failed; environment rolled back" >&2
exit 1
