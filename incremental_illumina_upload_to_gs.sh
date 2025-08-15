#!/bin/bash

# Chris Tomkins-Tinch
# tomkinsc@broadinstitute.org

# depends on:
# GNU tar (separate install on mac)
# google-cloud-sdk
# Uses optimized tar settings (--blocking-factor=1, --sparse, EOF trimming) for efficient concatenation

if [[ "$#" -ne 2 ]]; then
    echo "--------------------------------------------------------------------"
    echo ""
    echo ""
    echo "        +-----LOCAL------+                               "
    echo "        |                |              +----GS BUCKET---+"
    echo "        |   Illumina     |-------+      |                |"
    echo "        |      Run       |       |      | run/run.tar.gz |"
    echo "        |      Dir       |       +------>                |"
    echo "        +----------------+              +----------------+"
    echo ""
    echo ""
    echo "This script creates incremental gzipped tarballs and syncs them"
    echo "to a single tarball in a GS bucket. The tarballs use optimized"
    echo "tar settings for efficient concatenation and standard extraction."
    echo ""
    echo "Dependencies: GNU tar, google-cloud-sdk (with crcmod installed)"
    echo ""
    echo "Usage: $(basename $0) /path/of/run_to_upload gs://bucket-prefix"
    echo "--------------------------------------------------------------------"
    exit 1
fi

set -x

PATH_TO_UPLOAD="$1"
PATH_TO_UPLOAD="$(realpath ${PATH_TO_UPLOAD})"
DESTINATION_BUCKET_PREFIX="$2"

SOURCE_PATH_IS_ON_NFS="true" # passes '--no-check-device' to incremental tar; see: https://www.gnu.org/software/tar/manual/html_node/Incremental-Dumps.html

# if this script appears to be running on an Illumina machine
if [ -d "/usr/local/illumina" ]; then
    # default to write the tmp files the drive/partition presumed to be larger
    DEFAULT_STAGING_AREA="/usr/local/illumina/seq-run-uploads"
else
    # otherwise write to /tmp
    DEFAULT_STAGING_AREA="/tmp/seq-run-uploads"
fi

CHUNK_SIZE_MB=${CHUNK_SIZE_MB:-'100'}
DELAY_BETWEEN_INCREMENTS_SEC=${DELAY_BETWEEN_INCREMENTS_SEC:-'30'}
RUN_COMPLETION_TIMEOUT_DAYS=${RUN_COMPLETION_TIMEOUT_DAYS:-'16'}
RUN_BASENAME="$(basename ${PATH_TO_UPLOAD})"
STAGING_AREA_PATH="${STAGING_AREA_PATH:-$DEFAULT_STAGING_AREA}"
RSYNC_RETRY_MAX_ATTEMPTS=${RSYNC_RETRY_MAX_ATTEMPTS:-"12"}
RSYNC_RETRY_DELAY_SEC=${RSYNC_RETRY_DELAY_SEC:-"600"}

# -------------------------------

function cleanup(){
    echo "Cleaning up archive; exit code: $?"
    if [ -d "${STAGING_AREA_PATH}/${RUN_BASENAME}" ]; then
      rm -r "${STAGING_AREA_PATH}/${RUN_BASENAME}"
    fi
    #exit 0
}
trap cleanup SIGINT SIGQUIT SIGTERM

mkdir -p "${STAGING_AREA_PATH}/${RUN_BASENAME}"

chunk_size_bytes=$(expr $CHUNK_SIZE_MB \* 1048576) # $CHUNK_SIZE_MB*1024^2
RUN_COMPLETION_TIMEOUT_SEC=$(expr $RUN_COMPLETION_TIMEOUT_DAYS \* 86400)

size_at_last_check=0

TAR_BIN="tar"
GSUTIL_CMD='gsutil'
if [ "$(uname)" == "Darwin" ]; then
    TAR_BIN="gtar" # GNU tar must be installed and available on the path as gtar

    #export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=true # workaround for https://bugs.python.org/issue33725
    GSUTIL_CMD='gsutil -o "GSUtil:parallel_process_count=1"'
fi

$GSUTIL_CMD version -l

# if the run does not already exist on the destination, commence upload process...
if ! $GSUTIL_CMD ls "${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/${RUN_BASENAME}.tar.gz" &> /dev/null; then
    START_TIME=$(date +%s)

    echo "Does not already exist in bucket: ${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/${RUN_BASENAME}.tar.gz"

    # quit if the run is stale based on mtime of RunInfo.xml
    if [ "$(uname)" != "Darwin" ]; then
        run_info_mtime="$(date +%s -r "${PATH_TO_UPLOAD}/RunInfo.xml")"
    else
        run_info_mtime="$(stat -f "%B" "${PATH_TO_UPLOAD}/RunInfo.xml")"
    fi
    if [[ $(($START_TIME - $run_info_mtime)) -ge $RUN_COMPLETION_TIMEOUT_SEC ]]; then
        # this should be a noop unless the mtime of the parent directory is newer and we got here
        echo "Run is too old to upload: ${PATH_TO_UPLOAD}"
        cleanup
        exit 1
    fi


    sleep 5 # sleep 5 sec to allow early files to appear
    # separate upload of individual files which should exist outside the tarball
    # these are relative to $PATH_TO_UPLOAD
    declare -a individual_files_to_upload=(
            "SampleSheet.csv" 
            "RunInfo.xml"
            )
    for filename in "${individual_files_to_upload[@]}"; do
        if [[ -f "${PATH_TO_UPLOAD}/${filename}" ]]; then
            file_basename="$(basename ${filename})"
            file_extension="${file_basename#*.}"
            file_basename_no_ext="${file_basename%%.*}"
            if ! $GSUTIL_CMD ls "${DESTINATION_BUCKET_PREFIX}/${RUN_BASENAME}/${RUN_BASENAME}_${file_basename}" &> /dev/null; then
                $GSUTIL_CMD cp "${PATH_TO_UPLOAD}/${filename}" "${DESTINATION_BUCKET_PREFIX}/${RUN_BASENAME}/${RUN_BASENAME}_${file_basename}"
            else
                echo "Already exists in bucket; skipping upload: ${DESTINATION_BUCKET_PREFIX}/${RUN_BASENAME}/${RUN_BASENAME}_${file_basename}"
            fi
        fi
    done
    
    while true; do
        sync # flush write buffers to disk
        sleep 15 # pause for disk writes
        
        current_time=$(date +%s)

        if [ "$(uname)" != "Darwin" ]; then
            # on Linux
            current_size="$(du -s -B 1 "${PATH_TO_UPLOAD}" | cut -f1)"
        else
            # on macOS
            # BSD du lacks "-B", but we can use 1024-byte blocks and multiply
            current_size="$( expr 1024 \* $(du -s -k "${PATH_TO_UPLOAD}" | cut -f1) )"
        fi

        if [[ $(($current_time - $START_TIME)) -ge $RUN_COMPLETION_TIMEOUT_SEC ]]; then
            echo "Timed out waiting for run to finish: ${PATH_TO_UPLOAD}"
            exit 1
        fi

        run_is_finished=$([ -e "${PATH_TO_UPLOAD}/RTAComplete.txt" ] || [ -e "${PATH_TO_UPLOAD}/RTAComplete.xml" ] && echo "true" || echo "false")

        # if enough additional data has been added, or the run is complete, initiate incremental upload
        if [[ $current_size -ge $(($size_at_last_check + $chunk_size_bytes)) ]] || [ "$run_is_finished" = 'true' ]; then
            echo "commencing sync on latest data"
            size_at_last_check=$current_size
            timestamp=$(date +%s) # intentionally called before tar so time is a little older    

            # increate incremental tarballs
            # see: https://www.gnu.org/software/tar/manual/html_node/Incremental-Dumps.html
            #      https://www.gnu.org/software/tar/manual/html_node/Snapshot-Files.html
            # '--no-check-device' is for NFS
            # '-C "${PATH_TO_UPLOAD}" ." so we don't store the full path (-C is cd)
            # '--blocking-factor=1' prevents extra zero-padding blocks for efficient concatenation
            # '--sparse' consolidates runs of zeros in input files
            # '--label' adds human-readable note with run ID
            # 'head --bytes -1024' trims EOF blocks for incremental tarballs; final tarball preserves EOF blocks
            if [[ "$SOURCE_PATH_IS_ON_NFS" == "true" ]]; then SHOULD_CHECK_DEVICE_STR="--no-check-device"; else SHOULD_CHECK_DEVICE_STR=""; fi
            if [[ "$run_is_finished" == 'true' ]]; then EOF_PROCESSOR="cat"; else EOF_PROCESSOR="head --bytes -1024"; fi
                $TAR_BIN --exclude='Thumbnail_Images' --exclude="Images" --exclude "FocusModelGeneration" --exclude='Autocenter' --exclude='InstrumentAnalyticsLogs' --exclude "Logs" \
                --create \
                --blocking-factor=1 \
                --sparse \
                --label="${RUN_BASENAME}" \
                $SHOULD_CHECK_DEVICE_STR \
                --listed-incremental="${STAGING_AREA_PATH}/${RUN_BASENAME}/index" \
                -C "${PATH_TO_UPLOAD}" . | $EOF_PROCESSOR | gzip > "${STAGING_AREA_PATH}/${RUN_BASENAME}/${timestamp}_part-1.tar.gz"

            # -------------------------------------------------------------------------
            # # (WIP alternative to the above tar call)
            # # NOT WORKING attempt at chunked (multi-volume) uploads. 
            # # Seems to fail  during extraction when volumes are concatenated. Do not use this.
            # $TAR_BIN --exclude='Thumbnail_Images' \
            #     --create \
            #     --multi-volume \
            #     -L50M \
            #     -F '$(echo "$(expr $TAR_ARCHIVE : "\(.*\)-.*\.tar")-$TAR_VOLUME.tar" >&$TAR_FD)' \
            #     $SHOULD_CHECK_DEVICE_STR \
            #     --file="${STAGING_AREA_PATH}/${RUN_BASENAME}/${timestamp}_part-1.tar" \
            #     --listed-incremental="${STAGING_AREA_PATH}/${RUN_BASENAME}/index" \
            #     -C "${PATH_TO_UPLOAD}" . 
            # if [ $? -eq 0 ]; then
            #     gzip "${STAGING_AREA_PATH}/${RUN_BASENAME}/"*.tar
            # fi
            # -------------------------------------------------------------------------

            # try (and retry) to rsync incremental tarballs to bucket
            retry_count=0
            until [ "$retry_count" -ge $RSYNC_RETRY_MAX_ATTEMPTS ]; do
                # -m parallel uploads
                # -c Causes the rsync command to compute and compare checksums
                #    (instead of comparing mtime) for files if the size of source
                #    and destination match.
                # -C If an error occurs, continue to attempt to copy the remaining
                #    files. If errors occurred, gsutil's exit status will be
                #    non-zero even if this flag is set. This option is implicitly
                #    set when running "gsutil -m rsync..." (included below in case '-m' is removed).
                #
                # see: https://cloud.google.com/storage/docs/gsutil/commands/rsync
                $GSUTIL_CMD rsync -cC -x '.*index$' "${STAGING_AREA_PATH}/${RUN_BASENAME}/" "${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/parts" && break
                retry_count=$((retry_count+1)) 
                #sleep $RSYNC_RETRY_DELAY_SEC
                sleep $(expr $RSYNC_RETRY_DELAY_SEC \* $retry_count) # each retry scale delay by a multiple of the count
            done

            # remove uploaded incremental tarballs
            # from the local STAGING_AREA_PATH once uploaded
            for incremental_tarball in $(find "${STAGING_AREA_PATH}/${RUN_BASENAME}" -type f -name "*.tar.gz"); do
                # if the local incremental tarball has indeed been synced
                # remove the local copy of it...
                if ! $GSUTIL_CMD ls "${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/parts/$(basename ${incremental_tarball})"; then
                    $GSUTIL_CMD cp "${incremental_tarball}" "${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/parts/$(basename ${incremental_tarball})" && rm "${incremental_tarball}"
                else
                    rm "${incremental_tarball}"
                fi
            done

            # if the run is finished, stop checking after this tar call
            if [ "$run_is_finished" = "true" ]; then 
                break; 
            fi
        fi
        # sleep before checking for new files to pack
        sleep $DELAY_BETWEEN_INCREMENTS_SEC
    done

    # make sure the composed tarball does not exist on GS; if it does not...
    if ! $GSUTIL_CMD ls "${DESTINATION_BUCKET_PREFIX}/${RUN_BASENAME}/${RUN_BASENAME}.tar.gz"; then
        # get the archive started with a blank file
        dummyfile="${STAGING_AREA_PATH}/${RUN_BASENAME}/dummyfile.tar.gz"
        touch $dummyfile
        $GSUTIL_CMD cp "${dummyfile}" "${DESTINATION_BUCKET_PREFIX}/${RUN_BASENAME}/$RUN_BASENAME.tar.gz"
        rm "${dummyfile}"
    fi

    # compose tar.gz increments into single tar.gz on gcp in chunks of <=32 incremental tarballs

    # append the first 31 incremental tarballs
    # to the main tarball, then remove the incremental tarballs
    # keep doing this until there are no more incremental tarballs
    # see: https://cloud.google.com/storage/docs/gsutil/commands/compose
    until [[ "$($GSUTIL_CMD du ${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/parts/'*.tar.gz' 2> /dev/null | wc -l | awk '{print $1}' || echo '0')" == "0" ]]; do
        first_files=$($GSUTIL_CMD ls "${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/parts/"'*.tar.gz' | sort -V | head -n 31 | tr '\n' ' ')
        if [ ${#first_files} -ge 0 ]; then
            $GSUTIL_CMD compose "${DESTINATION_BUCKET_PREFIX}/${RUN_BASENAME}/$RUN_BASENAME.tar.gz" \
                ${first_files} \
                "${DESTINATION_BUCKET_PREFIX}/${RUN_BASENAME}/$RUN_BASENAME.tar.gz" && sleep 10 && $GSUTIL_CMD rm ${first_files}
        fi
    done

    # create a note about the tarball
    echo "$RUN_BASENAME.tar.gz created using optimized tar settings for efficient concatenation. Can be extracted with standard tar commands." | gsutil cp - "${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/$RUN_BASENAME.tar.gz.README.txt"

    # if only the index file is present, remove it
    if [[ $(ls -1 "${STAGING_AREA_PATH}/${RUN_BASENAME}" | wc -l) -eq 1 ]]; then
        rm "${STAGING_AREA_PATH}/${RUN_BASENAME}/index"
    fi

    # if staging dir is empty, remove it (rmdir only does this if empty).
    rmdir "${STAGING_AREA_PATH}/${RUN_BASENAME}" &> /dev/null
else
    echo "Exiting; already exists: ${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/${RUN_BASENAME}.tar.gz"
    exit 0
fi
