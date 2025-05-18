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

BASE_URL='http://127.0.0.1:8000/api/datasets'
FOLLOW_URL='http://127.0.0.1:8000/api/follows'
DATASET_LIMIT="${1:-5}"

echo "Fetching first ${DATASET_LIMIT} datasets (with size)…"
cat_json=$(curl -sG "${BASE_URL}" \
  --data-urlencode "limit=${DATASET_LIMIT}" \
  --data-urlencode "with_size=true")

dataset_lines=$(echo "${cat_json}" | jq -r '.[] | [.id, (.size_bytes // 0)] | @tsv')

if [[ -z "${dataset_lines}" ]]; then
  echo "No datasets returned – is the API running?" >&2
  exit 1
fi
echo "Retrieved $(echo "${dataset_lines}" | wc -l | tr -d ' ') ids."
echo

first_id=$(echo "${dataset_lines}" | head -n1 | cut -f1)
if [[ -n "${AUTH_TOKEN:-}" ]]; then
  auth=(-H "Authorization: Bearer ${AUTH_TOKEN}")
  echo "Testing follow endpoints with dataset ${first_id}"
  curl -s -X POST "${FOLLOW_URL}" "${auth[@]}" \
       -H 'Content-Type: application/json' \
       -d "{\"dataset_id\":\"${first_id}\"}" -o /dev/null -w 'POST %{{http_code}}\n'
  echo "Follow list:" && curl -s "${FOLLOW_URL}" "${auth[@]}" | jq
  curl -s -X DELETE "${FOLLOW_URL}/${first_id}" "${auth[@]}" -o /dev/null -w 'DELETE %{{http_code}}\n'
  echo
else
  echo "AUTH_TOKEN not set – skipping follow endpoint tests." >&2
fi

echo "${dataset_lines}" | while IFS=$'\t' read -r id size_bytes; do
  echo "DATASET: ${id}"
  echo "Total repo size: $(pretty_bytes "${size_bytes}") (${size_bytes} bytes)"
  echo

  echo "Last 3 commits:"
  curl -s "${BASE_URL}/${id}/commits" | jq '.[0:3]'
  echo

  echo "File list:"
  files_json=$(curl -s "${BASE_URL}/${id}/files")

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
  dl_url=$(curl -sG "${BASE_URL}/${id}/file-url" \
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
