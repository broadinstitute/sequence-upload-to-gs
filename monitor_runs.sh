#!/bin/bash

# Chris Tomkins-Tinch
# tomkinsc@broadinstitute.org

# depends on:
# google-cloud-sdk
# pstree (separate install on mac, 'brew install pstree')
# Uses optimized tar settings (--blocking-factor=1, --sparse, EOF trimming) for efficient concatenation

if [[ "$#" -ne 2 ]]; then
    echo "--------------------------------------------------------------------"
    echo ""
    echo ""
    echo "        +-----LOCAL------+                               "
    echo "        |      Dir       |              +----GS BUCKET---+"
    echo "        |   containing   |-------+      |                |"
    echo "        |   Illumina     |       |      | run/run.tar.gz |"
    echo "        |   run dirs     |       +------>                |"
    echo "        +----------------+              +---------------+"
    echo ""
    echo ""
    echo "This script monitors for new run directorties and launches a script."
    echo "to upload them."
    echo ""
    echo "Usage: $(basename $0) /path/to/monitored-directory gs://bucket-prefix"
    echo "--------------------------------------------------------------------"
    exit 1
fi

set -x

function absolute_path() {
    local SOURCE="$1"
    while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        if [[ "$OSTYPE" == "darwin"* ]]; then
            SOURCE="$(readlink "$SOURCE")"
        else
            SOURCE="$(readlink -f "$SOURCE")"
        fi
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
    done
    echo "$SOURCE"
}
SOURCE="${BASH_SOURCE[0]}"
SCRIPT=$(absolute_path "$SOURCE")
SCRIPT_DIRNAME="$(dirname "$SOURCE")"
SCRIPTPATH="$(cd -P "$(echo $SCRIPT_DIRNAME)" &> /dev/null && pwd)"
SCRIPT="$SCRIPTPATH/$(basename "$SCRIPT")"

# if this script appears to be running on an Illumina machine
if [ -d "/usr/local/illumina" ]; then
    # default to write the tmp files the drive/partition presumed to be larger
    DEFAULT_STAGING_AREA="/usr/local/illumina/seq-run-uploads"
else
    # otherwise write to /tmp
    DEFAULT_STAGING_AREA="/tmp/seq-run-uploads"
fi

PATH_TO_MONITOR="$1"
PATH_TO_MONITOR="$(realpath ${PATH_TO_MONITOR})"
DESTINATION_BUCKET_PREFIX="$2"

INCLUSION_TIME_INTERVAL_DAYS=${INCLUSION_TIME_INTERVAL_DAYS:-'7'}
DELAY_BETWEEN_INCREMENTS_SEC=${DELAY_BETWEEN_INCREMENTS_SEC:-'10'}
STAGING_AREA_PATH="${STAGING_AREA_PATH:-$DEFAULT_STAGING_AREA}"

GSUTIL_CMD='gsutil'
if [ "$(uname)" == "Darwin" ]; then
    #export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=true # workaround for https://bugs.python.org/issue33725
    GSUTIL_CMD='gsutil -o "GSUtil:parallel_process_count=1"'
fi

echo "Location for temp files: ${STAGING_AREA_PATH}"

# detect if running via cron, and only run infinitely if not running via cron
CRON="$( pstree -s $$ | grep -c cron )"
while true; do
    echo ""
    echo "==="
    echo "$(date +%Y-%m-%dT%H:%M)"
    echo "Checking for new run directories in: ${PATH_TO_MONITOR}"
    for found_dir in $(find "${PATH_TO_MONITOR}" -maxdepth 2 -type f -name '*RunInfo.xml' -exec dirname {} \; | sort -u); do
        found_dir="$(realpath "$found_dir")"
        echo "---"
        echo "Found potential run directory:           ${found_dir}"
        # if the run dir is new enough to be uploaded, continue
        if [ $(find "${found_dir}" -maxdepth 0 -mtime -"${INCLUSION_TIME_INTERVAL_DAYS}" -type d) ]; then
            echo "Path is new enough to attempt an upload: ${found_dir}"
            RUN_BASENAME="$(basename ${found_dir})"
            # if the run does not already exist on the destination, commence upload process...
            RUN_BUCKET_PATH="${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/${RUN_BASENAME}.tar.gz"
            if ! $GSUTIL_CMD ls "${RUN_BUCKET_PATH}" &> /dev/null; then
                echo "Run does not exist in bucket:            ${RUN_BUCKET_PATH}"
                if ! [ -d "${STAGING_AREA_PATH}/${RUN_BASENAME}" ]; then
                  echo "Run upload not yet in progress; no dir:  ${STAGING_AREA_PATH}/${RUN_BASENAME}"
                  echo "Initiating incremental upload..."
                  upload_cmd="${SCRIPTPATH}/incremental_illumina_upload_to_gs.sh ${found_dir} ${DESTINATION_BUCKET_PREFIX}"
                  echo "    ${upload_cmd}"
                  # fork incremental upload to separate process
                  # pass cron detection info via environment variable
                  if [[ $CRON -gt 0 ]]; then
                      (STAGING_AREA_PATH="${STAGING_AREA_PATH}" CRON_INVOKED="true" ${upload_cmd}) &
                  else
                      (STAGING_AREA_PATH="${STAGING_AREA_PATH}" CRON_INVOKED="false" ${upload_cmd}) &
                  fi
                else
                    echo "Skipping initiation of new upload (upload in progress): ${STAGING_AREA_PATH}/${RUN_BASENAME}"
                fi
            else
                echo "Skipping (already uploaded):             ${RUN_BUCKET_PATH}"
            fi
        fi
    done
    if [[ $CRON -gt 0 ]]; then
        break
    fi
    echo "Sleeping for ${DELAY_BETWEEN_INCREMENTS_SEC} sec before re-checking for new run directories..."
    sleep $DELAY_BETWEEN_INCREMENTS_SEC
done
