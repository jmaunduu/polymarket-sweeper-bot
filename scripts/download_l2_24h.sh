#!/usr/bin/env bash
set -euo pipefail

API_KEY="${PMDATA_API_KEY:-sk-25Ipq9WV1iMwXMvXgfDPSqlvuolh3r3g}"
OUT_DIR="${1:-/root/bot/data/l2}"

mkdir -p "$OUT_DIR"

now_floor=$(date +%s | awk '{print $1 - ($1 % 300)}')
start_ts=$((now_floor - 24 * 60 * 60))

assets=(btc eth sol)
timeframes=(5m 15m)

downloaded=0
skipped=0
failed=0
retried=0

for asset in "${assets[@]}"; do
  for tf in "${timeframes[@]}"; do
    ts=$start_ts
    while [ "$ts" -le "$now_floor" ]; do
      slug="${asset}-updown-${tf}-${ts}"
      dest="${OUT_DIR}/${slug}.parquet"
      url="https://api.pmdata.dev/download/poly_l2/${slug}.parquet"

      if [ -f "$dest" ]; then
        skipped=$((skipped + 1))
        ts=$((ts + 300))
        continue
      fi

      attempt=0
      code=""
      while :; do
        code=$(curl -sS -o /tmp/pmdata_download.$$ -w '%{http_code}' \
          -X GET "$url" \
          -H "api_key: ${API_KEY}" \
          -H "Content-Type: application/json")
        if [ "$code" != "429" ] || [ "$attempt" -ge 4 ]; then
          break
        fi
        attempt=$((attempt + 1))
        retried=$((retried + 1))
        sleep $((attempt * 2))
      done

      case "$code" in
        200)
          mv /tmp/pmdata_download.$$ "$dest"
          downloaded=$((downloaded + 1))
          ;;
        404)
          rm -f /tmp/pmdata_download.$$
          skipped=$((skipped + 1))
          ;;
        *)
          rm -f /tmp/pmdata_download.$$
          echo "download failed for ${slug}: HTTP ${code}" >&2
          failed=$((failed + 1))
          ;;
      esac

      ts=$((ts + 300))
    done
  done
done

echo "downloaded=${downloaded} skipped=${skipped} failed=${failed} retried=${retried}"
