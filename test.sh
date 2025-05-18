#!/usr/bin/env bash
set -euo pipefail

pretty_bytes() {
  num=$1
  if [[ $num -lt 1024 ]]; then
    echo "${num} B"
    return
  fi
  awk -v n="$num" '
    BEGIN {
      split("KiB MiB GiB TiB PiB EiB", u)
      i = 0
      while (n >= 1024 && i < 5) {
        n /= 1024
        i++
      }
      printf "%.1f %s\n", n, u[i]
    }
  '
}

API_ROOT="http://127.0.0.1:8000/api"
DATASET_URL="${API_ROOT}/datasets"
AUTH_URL="${API_ROOT}/auth"
USERS_URL="${API_ROOT}/users"
FOLLOWS_URL="${API_ROOT}/follows"

DATASET_LIMIT="${1:-5}"
EMAIL="${EMAIL:-}"
PASSWORD="${PASSWORD:-}"
AUTH_TOKEN="${AUTH_TOKEN:-}"

echo "Fetching first ${DATASET_LIMIT} datasets (with size)…"
cat_json=$(curl -sG "${DATASET_URL}" \
  --data-urlencode "limit=${DATASET_LIMIT}" \
  --data-urlencode "with_size=true")

dataset_lines=$(echo "${cat_json}" | jq -r '.[] | [.id, (.size_bytes // 0)] | @tsv')

if [[ -z "${dataset_lines}" ]]; then
  echo "No datasets returned – is the API running?" >&2
  exit 1
fi
echo "Retrieved $(echo "${dataset_lines}" | wc -l | tr -d ' ') ids."
echo

echo "${dataset_lines}" | while IFS=$'\t' read -r id size_bytes; do
  echo "DATASET: ${id}"
  echo "Total repo size: $(pretty_bytes "${size_bytes}") (${size_bytes} bytes)"
  echo

  echo "Last 3 commits:"
  curl -s "${DATASET_URL}/${id}/commits" | jq '.[0:3]'
  echo

  echo "File list:"
  files_json=$(curl -s "${DATASET_URL}/${id}/files")

  files=$(echo "${files_json}" | jq -r '
      if type == "array"        then .[]
      elif has("files")         then .files[]
      else error("unexpected /files payload")
      end')

  if [[ -z "${files}" ]]; then
    echo "   (no files returned)"
    echo
    continue
  fi

  echo "${files}" | sed 's/^/   • /'

  target_file=$(echo "${files}" | grep -m1 -E '^README(\.md)?$' || true)
  [[ -z "${target_file}" ]] && target_file=$(echo "${files}" | head -n1)

  printf "\n Resolving URL for '%s' …\n" "${target_file}"
  dl_url=$(curl -sG "${DATASET_URL}/${id}/file-url" \
             --data-urlencode "filename=${target_file}" | jq -r '.download_url // empty')

  if [[ -n "${dl_url}" ]]; then
    echo "     ${dl_url}"
    status=$(curl -s -o /dev/null -w '%{http_code}' -I "${dl_url}")
    echo "     (HTTP HEAD status: ${status})"
  else
    echo "     Unable to obtain download URL"
  fi
  echo
done

if [[ -n "$EMAIL" && -n "$PASSWORD" ]]; then
  echo "\n== Testing auth endpoints =="
  curl -s -X POST "$AUTH_URL/register" \
    -H 'Content-Type: application/json' \
    -d '{"email":"'"$EMAIL"'","password":"'"$PASSWORD"'"}' | jq .

  login_json=$(curl -s -X POST "$AUTH_URL/login" \
    -H 'Content-Type: application/json' \
    -d '{"email":"'"$EMAIL"'","password":"'"$PASSWORD"'"}')
  AUTH_TOKEN=$(echo "$login_json" | jq -r '.access_token // empty')
  echo "$login_json" | jq .
else
  echo "\nSkipping auth tests – EMAIL or PASSWORD env vars missing"
fi

if [[ -n "$AUTH_TOKEN" ]]; then
  echo "\n== Testing users endpoints =="
  curl -s -H "Authorization: Bearer $AUTH_TOKEN" "$USERS_URL/me" | jq .

  echo "\n== Testing follows endpoints =="
  first_dataset=$(echo "$dataset_lines" | head -n1 | cut -f1)
  curl -s -X POST "$FOLLOWS_URL" \
    -H "Authorization: Bearer $AUTH_TOKEN" \
    -H 'Content-Type: application/json' \
    -d '{"dataset_id":"'"$first_dataset"'"}' -o /dev/null -w 'POST %s -> %s\n' "$first_dataset" '%{http_code}'
  curl -s "$FOLLOWS_URL" -H "Authorization: Bearer $AUTH_TOKEN" | jq .
  curl -s -X DELETE "$FOLLOWS_URL/$first_dataset" -H "Authorization: Bearer $AUTH_TOKEN" -o /dev/null -w 'DELETE %s -> %s\n' "$first_dataset" '%{http_code}'

  echo "\n-- Dataset follow/unfollow via dataset routes --"
  curl -s -X POST "${DATASET_URL}/${first_dataset}/follow" \
    -H "Authorization: Bearer $AUTH_TOKEN" \
    -o /dev/null -w 'POST %s/follow -> %s\n' "$first_dataset" '%{http_code}'
  curl -s -X DELETE "${DATASET_URL}/${first_dataset}/follow" \
    -H "Authorization: Bearer $AUTH_TOKEN" \
    -o /dev/null -w 'DELETE %s/follow -> %s\n' "$first_dataset" '%{http_code}'

  echo "\n-- My follows via /users/me/follows --"
  curl -s "$USERS_URL/me/follows" -H "Authorization: Bearer $AUTH_TOKEN" | jq .
else
  echo "\nSkipping user/follow tests – no AUTH_TOKEN available"
fi

