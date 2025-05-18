#!/usr/bin/env bash

BASE_URL='http://127.0.0.1:8000/api/datasets'
DATASET_LIMIT="${1:-5}"   

echo "Fetching first ${DATASET_LIMIT} datasets …"
dataset_ids=$(curl -sG "${BASE_URL}" --data-urlencode "limit=${DATASET_LIMIT}" | jq -r '.[].id')

if [[ -z "${dataset_ids}" ]]; then
  echo "No datasets returned – is the API running?" >&2
  exit 1
fi

echo "Retrieved $(echo "${dataset_ids}" | wc -l | tr -d ' ') ids."


echo "${dataset_ids}" | while IFS= read -r id; do
  echo "DATASET: ${id}"
  echo "Last 3 commits:"
  curl -s "${BASE_URL}/${id}/commits" | jq '.[0:3]'

  echo "File list:"
  files_json=$(curl -s "${BASE_URL}/${id}/files")

  files=$(echo "${files_json}" | jq -r '
      if type == "array"            then .[]
      elif has("files")             then .files[]
      else error("unexpected /files payload")
      end')

  if [[ -z "${files}" ]]; then
    echo "     (no files returned)"
    continue
  fi

  echo "${files}" | sed 's/^/   • /'

  target_file=$(echo "${files}" | grep -m1 -E '^README(\.md)?$' || true)
  [[ -z "${target_file}" ]] && target_file=$(echo "${files}" | head -n1)

  printf "\nResolving URL for '%s' …\n" "${target_file}"
  dl_url=$(curl -sG "${BASE_URL}/${id}/file-url" --data-urlencode "filename=${target_file}" | jq -r '.download_url // empty')

  if [[ -n "${dl_url}" ]]; then
    echo "     ${dl_url}"
    status=$(curl -s -o /dev/null -w '%{http_code}' -I "${dl_url}")
    echo "(HTTP HEAD status: ${status})"
  else
    echo "Unable to obtain download URL"
  fi
done

