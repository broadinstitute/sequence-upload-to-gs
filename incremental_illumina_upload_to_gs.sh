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
    echo "Each tar chunk includes metadata labels with run info, timestamps,"
    echo "and machine details. Use 'tar --test-label -f chunk.tar.gz' to view"
    echo "individual chunk labels, or 'tar -tvf combined.tar.gz | grep Volume'"
    echo "to see all labels in the final composed archive."
    echo ""
    echo "Dependencies: GNU tar, google-cloud-sdk (gcloud CLI with storage commands)"
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
DELAY_BETWEEN_INCREMENTS_SEC=${DELAY_BETWEEN_INCREMENTS_SEC:-'600'}
RUN_COMPLETION_TIMEOUT_DAYS=${RUN_COMPLETION_TIMEOUT_DAYS:-'16'}
RUN_BASENAME="$(basename ${PATH_TO_UPLOAD})"
STAGING_AREA_PATH="${STAGING_AREA_PATH:-$DEFAULT_STAGING_AREA}"
RSYNC_RETRY_MAX_ATTEMPTS=${RSYNC_RETRY_MAX_ATTEMPTS:-"12"}
RSYNC_RETRY_DELAY_SEC=${RSYNC_RETRY_DELAY_SEC:-"600"}
TERRA_RUN_TABLE_NAME=${TERRA_RUN_TABLE_NAME:-"flowcell"}

# Default directories to exclude from tar archives (large non-essential directories)
DEFAULT_TAR_EXCLUSIONS=("Thumbnail_Images" "Images" "FocusModelGeneration" "Autocenter" "InstrumentAnalyticsLogs" "Logs")
# Allow override via environment variable (space-separated list)
if [[ -n "$TAR_EXCLUSIONS" ]]; then
    IFS=' ' read -ra TAR_EXCLUSIONS_ARRAY <<< "$TAR_EXCLUSIONS"
else
    TAR_EXCLUSIONS_ARRAY=("${DEFAULT_TAR_EXCLUSIONS[@]}")
fi

# -------------------------------
# Dependency checking
# -------------------------------

# Hard dependencies - script will exit if any are missing
HARD_DEPENDENCIES=(gcloud tar date basename mkdir rm find head wc cut awk sort tr expr du uname realpath stat touch cat gzip base64 sed ls whoami hostname ps)

# Optional dependencies - script will work without them but with reduced functionality
OPTIONAL_DEPENDENCIES=(dig curl ip route grep pstree)

# Check hard dependencies
echo "Checking required dependencies..."
missing_deps=()
for dependency in "${HARD_DEPENDENCIES[@]}"; do
    if ! command -v "$dependency" &> /dev/null; then
        missing_deps+=("$dependency")
    fi
done

if [[ ${#missing_deps[@]} -gt 0 ]]; then
    echo "ERROR! Missing required dependencies. Aborting..."
    echo "The following commands need to be installed and available on PATH:"
    for dep in "${missing_deps[@]}"; do
        echo "  - $dep"
    done
    echo ""
    echo "Please install the missing dependencies and try again."
    exit 1
fi

# Check and track optional dependencies
echo "Checking optional dependencies..."
available_optional_deps=()
for dependency in "${OPTIONAL_DEPENDENCIES[@]}"; do
    if command -v "$dependency" &> /dev/null; then
        available_optional_deps+=("$dependency")
    fi
done

if [[ ${#available_optional_deps[@]} -eq 0 ]]; then
    echo "Warning: No optional tools available. Some features may have reduced functionality."
else
    echo "Available optional tools: ${available_optional_deps[*]}"
    
    # Check specific functionality
    ip_tools=(dig curl ip route)
    available_ip_tools=()
    for tool in "${ip_tools[@]}"; do
        if [[ " ${available_optional_deps[*]} " =~ " ${tool} " ]]; then
            available_ip_tools+=("$tool")
        fi
    done
    
    if [[ ${#available_ip_tools[@]} -eq 0 ]]; then
        echo "Warning: No external IP detection tools available. External IP will be set to 0.0.0.0"
    fi
    
    if [[ " ${available_optional_deps[*]} " =~ " pstree " ]]; then
        echo "Note: pstree available for enhanced cron detection"
    else
        echo "Note: Using ps fallback for cron detection (pstree not available)"
    fi
fi

# Check for GNU tar specifically on macOS
if [ "$(uname)" == "Darwin" ]; then
    if ! command -v gtar &> /dev/null; then
        echo "ERROR! macOS detected but GNU tar (gtar) is not available."
        echo "Please install GNU tar: brew install gnu-tar"
        exit 1
    fi
fi

echo "All required dependencies satisfied."
echo ""

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
tar_increment_counter=0

TAR_BIN="tar"
GCLOUD_STORAGE_CMD='gcloud storage'
if [ "$(uname)" == "Darwin" ]; then
    TAR_BIN="gtar" # GNU tar must be installed and available on the path as gtar

    #export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=true # workaround for https://bugs.python.org/issue33725
    # Note: gcloud storage handles parallelization automatically, no manual tuning needed
    GCLOUD_STORAGE_CMD='gcloud storage'
fi

gcloud version

# Function to detect external IP address using multiple methods
get_external_ip() {
    local ip=""
    
    # Check if required tools are available
    local has_dig=$(command -v dig &> /dev/null && echo "true" || echo "false")
    local has_curl=$(command -v curl &> /dev/null && echo "true" || echo "false")
    local has_awk=$(command -v awk &> /dev/null && echo "true" || echo "false")
    
    if [[ "$has_dig" == "true" && "$has_awk" == "true" ]]; then
        # Method 1: Google DNS TXT record
        ip=$(dig +short txt o-o.myaddr.l.google.com @ns1.google.com 2>/dev/null | awk -F'"' '{print $2}' | tr -d '\n\r"' | head -1)
        if [[ -n "$ip" && "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo "$ip"
            return
        fi
        
        # Method 2: OpenDNS
        ip=$(dig +short myip.opendns.com @resolver1.opendns.com 2>/dev/null | tr -d '\n\r"' | head -1)
        if [[ -n "$ip" && "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo "$ip"
            return
        fi
        
        # Method 3: Cloudflare
        ip=$(dig +short txt ch whoami.cloudflare @1.0.0.1 2>/dev/null | tr -d '\n\r"' | head -1)
        if [[ -n "$ip" && "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo "$ip"
            return
        fi
        
        # Method 4: Google DNS (alternative)
        ip=$(dig +short txt o-o.myaddr.l.google.com @ns1.google.com 2>/dev/null | tr -d '\n\r"' | head -1)
        if [[ -n "$ip" && "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo "$ip"
            return
        fi
    fi
    
    if [[ "$has_curl" == "true" ]]; then
        # Method 5: AWS checkip
        ip=$(curl -s --max-time 5 checkip.amazonaws.com 2>/dev/null | tr -d '\n\r"' | head -1)
        if [[ -n "$ip" && "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo "$ip"
            return
        fi
    fi
    
    # Fallback: try to get local interface IP
    if command -v ip &> /dev/null; then
        local local_ip=$(ip route get 8.8.8.8 2>/dev/null | grep -oP 'src \K\S+' 2>/dev/null | head -1)
        if [[ -n "$local_ip" && "$local_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo "$local_ip"
            return
        fi
    elif command -v route &> /dev/null; then
        # macOS/BSD fallback using route command
        local local_ip=$(route get 8.8.8.8 2>/dev/null | awk '/interface:/ {getline; if(/inet/) print $2}' | head -1)
        if [[ -n "$local_ip" && "$local_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo "$local_ip"
            return
        fi
    fi
    
    # Final fallback
    echo "0.0.0.0"
}

# Function to detect if running via cron
is_cron_execution() {
    # Check if CRON_INVOKED is set by monitor_runs.sh
    if [[ -n "$CRON_INVOKED" ]]; then
        echo "$CRON_INVOKED"
        return
    fi
    
    # Use same cron detection logic as monitor_runs.sh
    local cron_detected=0
    if command -v pstree &> /dev/null; then
        # Use pstree if available
        if pstree -s $$ 2>/dev/null | grep -q cron 2>/dev/null; then
            cron_detected=1
        else
            cron_detected=0
        fi
    else
        # Fallback using ps - works on both GNU/Linux and macOS/BSD
        if [ "$(uname)" == "Darwin" ]; then
            # macOS/BSD ps format - check current and parent processes
            local current_pid=$$
            while [[ $current_pid -ne 1 ]]; do
                if ps -o comm= -p $current_pid 2>/dev/null | grep -q cron; then
                    cron_detected=1
                    break
                fi
                local parent_pid=$(ps -o ppid= -p $current_pid 2>/dev/null | tr -d ' ')
                [[ -z "$parent_pid" || "$parent_pid" == "0" || "$parent_pid" == "1" ]] && break
                current_pid=$parent_pid
            done
        else
            # GNU/Linux ps format - trace up the process tree
            local current_pid=$$
            while [[ $current_pid -ne 1 ]]; do
                if ps -o comm= -p $current_pid 2>/dev/null | grep -q cron; then
                    cron_detected=1
                    break
                fi
                current_pid=$(ps -o ppid= -p $current_pid 2>/dev/null | tr -d ' ' || echo 1)
                [[ -z "$current_pid" || "$current_pid" == "0" ]] && break
            done
        fi
    fi
    
    if [[ $cron_detected -gt 0 ]]; then
        echo "true"
    else
        echo "false"
    fi
}

# Function to generate enhanced tar label metadata
generate_tar_label() {
    local run_basename="$1"
    local increment_num="$2"
    local timestamp_formatted=$(date +%y-%m-%dT%H:%M)
    local hostname=$(hostname)
    local username=$(whoami)
    local external_ip=$(get_external_ip)
    local is_cron=$(is_cron_execution)
    
    # Intelligently shorten run basename: extract last component after underscores, then truncate if needed
    local short_run_basename="$run_basename"
    if [[ "$run_basename" =~ _ ]]; then
        # Extract last component after final underscore
        short_run_basename="${run_basename##*_}"
        # If still too long, truncate to 15 chars
        if [[ ${#short_run_basename} -gt 15 ]]; then
            short_run_basename="${short_run_basename:0:15}"
        fi
    else
        # No underscores, just truncate
        short_run_basename="${run_basename:0:15}"
    fi
    
    # GNU tar volume labels have a strict 99-byte limit, but support control characters
    # Try compact JSON first, then fall back to pipe-separated format if too long
    local json_metadata="{\"r\":\"$short_run_basename\",\"t\":\"$timestamp_formatted\",\"i\":$increment_num,\"h\":\"${hostname:0:8}\",\"u\":\"${username:0:8}\",\"ip\":\"$external_ip\",\"c\":$([ "$is_cron" = "true" ] && echo 1 || echo 0)}"
    
    if [[ ${#json_metadata} -le 99 ]]; then
        # Use JSON format (human and machine readable)
        echo "$json_metadata"
    else
        # Fallback to pipe-separated format (very compact)
        local pipe_format="$short_run_basename|$timestamp_formatted|$increment_num|${hostname:0:8}|${username:0:8}|$external_ip|$([ "$is_cron" = "true" ] && echo 1 || echo 0)"
        if [[ ${#pipe_format} -le 99 ]]; then
            echo "$pipe_format"
        else
            # Last resort: compress with gzip and base64 encode
            local compressed=$(echo "$json_metadata" | gzip | base64 | tr -d '\n')
            local max_len=$((99 - 3)) # Reserve 3 chars for "gz:" prefix
            echo "gz:${compressed:0:$max_len}"
        fi
    fi
}

# Function to generate verbose metadata JSON file for the entire upload process
generate_verbose_metadata() {
    local run_basename="$1"
    local run_path="$2"
    local destination_bucket="$3"
    local start_time="$4"
    local current_time=$(date +%s)
    local timestamp_formatted=$(date -d "@$current_time" +%Y-%m-%dT%H:%M:%S%z 2>/dev/null || date -r "$current_time" +%Y-%m-%dT%H:%M:%S%z)
    local start_timestamp=$(date -d "@$start_time" +%Y-%m-%dT%H:%M:%S%z 2>/dev/null || date -r "$start_time" +%Y-%m-%dT%H:%M:%S%z)
    local hostname=$(hostname)
    local username=$(whoami)
    local external_ip=$(get_external_ip)
    local is_cron=$(is_cron_execution)
    local script_version=$(grep "^# version: " "$0" | head -1 | sed 's/^# version: //' || echo "unknown")
    local final_increment=$tar_increment_counter
    local upload_duration=$((current_time - start_time))
    
    # Get executable versions
    local gcloud_version=$(gcloud version --format='value("Google Cloud SDK")' 2>/dev/null || echo "unknown")
    local gcloud_storage_version=$(gcloud components list --filter="id:gcloud-storage" --format="value(version.string)" 2>/dev/null)
    # If gcloud storage version is empty (bundled), fall back to main gcloud version
    if [[ -z "$gcloud_storage_version" ]]; then
        gcloud_storage_version="$gcloud_version"
    fi
    local tar_version=$($TAR_BIN --version 2>/dev/null | head -1 || echo "unknown")
    local bash_version=$($BASH --version 2>/dev/null | head -1 || echo "$BASH_VERSION")
    
    # Get run size information
    local run_size_bytes=0
    if [ -d "$run_path" ]; then
        if [ "$(uname)" != "Darwin" ]; then
            run_size_bytes=$(du -sb "$run_path" 2>/dev/null | cut -f1 || echo 0)
        else
            run_size_bytes=$(du -sk "$run_path" 2>/dev/null | awk '{print $1 * 1024}' || echo 0)
        fi
    fi
    
    # Create comprehensive metadata JSON
    cat << EOF
{
  "upload_metadata": {
    "run_basename": "$run_basename",
    "run_path": "$run_path",
    "destination_bucket": "$destination_bucket",
    "upload_start_time": "$start_timestamp",
    "upload_completion_time": "$timestamp_formatted",
    "upload_duration_seconds": $upload_duration,
    "total_increments": $final_increment,
    "run_size_bytes": $run_size_bytes,
    "cron_invoked": $is_cron
  },
  "uploading_machine_info": {
    "hostname": "$hostname",
    "username": "$username",
    "external_ip": "$external_ip",
    "operating_system": "$(uname -s)",
    "architecture": "$(uname -m)",
    "script_version": "$script_version"
  },
  "tool_versions": {
    "gcloud_version": "$gcloud_version",
    "gcloud_storage_version": "$gcloud_storage_version", 
    "tar_version": "$tar_version",
    "bash_version": "$bash_version"
  },
  "environment_variables": {
    "chunk_size_mb": $CHUNK_SIZE_MB,
    "delay_between_increments_sec": $DELAY_BETWEEN_INCREMENTS_SEC,
    "run_completion_timeout_days": $RUN_COMPLETION_TIMEOUT_DAYS,
    "staging_area_path": "$STAGING_AREA_PATH",
    "source_path_is_on_nfs": "$SOURCE_PATH_IS_ON_NFS"
  },
  "tar_settings": {
    "tar_binary": "$TAR_BIN",
    "blocking_factor": 1,
    "sparse_enabled": true,
    "eof_trimming": "incremental_only",
    "excluded_directories": [$(printf '"%s",' "${TAR_EXCLUSIONS_ARRAY[@]}" | sed 's/,$//')]
  },
  "generation_timestamp": "$timestamp_formatted"
}
EOF
}

# Function to generate Terra-compatible TSV file for data table import
generate_terra_tsv() {
    local run_basename="$1"
    local tarball_path="$2"
    
    # Create TSV with POSIX line endings (LF only)
    cat << EOF
entity:${TERRA_RUN_TABLE_NAME}_id	biosample_attributes	flowcell_tar	samplesheets	sample_rename_map_tsv
$run_basename		$tarball_path		
EOF
}

# Function to generate dynamic exclusions for incomplete cycles and recently modified files
# Only generates exclusions when run is not finished to prevent tarball bloat from partial *.cbcl files
generate_dynamic_exclusions() {
    local run_path="$1"
    local exclusions_file="$2"
    local run_is_finished="$3"
    
    # Only generate exclusions if run is not finished
    if [[ "$run_is_finished" != 'true' ]]; then
        # Clear the exclusions file
        > "$exclusions_file"
        
        # Find and exclude the most recent cycle directory to prevent capturing partial *.cbcl files
        # Look for pattern: Data/Intensities/BaseCalls/L00*/C###.# (cycle directories)
        if [[ -d "${run_path}/Data/Intensities/BaseCalls" ]]; then
            local most_recent_cycle=$(find "${run_path}/Data/Intensities/BaseCalls/"L* \
                -type d \
                -regextype posix-extended \
                -regex '^.+/C[0-9]+\.[0-9]+$' 2>/dev/null | \
                sort -r -k1,1 -V | \
                head -n1 | \
                sed -E 's|(BaseCalls/)L([0-9]+)|\\1L*|g' 2>/dev/null || true)
            
            if [[ -n "$most_recent_cycle" ]]; then
                # Convert absolute path to relative path for tar exclusion
                local relative_cycle_path="${most_recent_cycle#${run_path}/}"
                echo "$relative_cycle_path" >> "$exclusions_file"
                echo "Dynamic exclusion: $relative_cycle_path (most recent cycle)"
            fi
        fi
        
        # Add files that have been modified in the past 3 minutes (optional, but recommended)
        # This helps avoid capturing files that are actively being written
        find "$run_path" -mmin -3 -type f 2>/dev/null | while IFS= read -r recent_file; do
            # Convert to relative path for tar exclusion
            local relative_file="${recent_file#${run_path}/}"
            echo "$relative_file" >> "$exclusions_file"
        done
        
        # Sort and deduplicate exclusions (files may match both recent cycle and recent time criteria)
        if [[ -f "$exclusions_file" && -s "$exclusions_file" ]]; then
            local temp_file="${exclusions_file}.tmp"
            sort -u "$exclusions_file" > "$temp_file" && mv "$temp_file" "$exclusions_file"
        fi
        
        # Show exclusions count for logging
        local exclusion_count=$(wc -l < "$exclusions_file" 2>/dev/null || echo 0)
        if [[ $exclusion_count -gt 0 ]]; then
            echo "Generated $exclusion_count dynamic exclusions to prevent capturing incomplete files"
        fi
        
        return 0
    else
        echo "Run is finished - no dynamic exclusions applied"
        # Ensure exclusions file doesn't exist for finished runs
        [[ -f "$exclusions_file" ]] && rm "$exclusions_file"
        return 1
    fi
}

# Define the final tarball path once to avoid repetition
FINAL_TARBALL_PATH="${DESTINATION_BUCKET_PREFIX}/${RUN_BASENAME}/${RUN_BASENAME}.tar.gz"

# if the run does not already exist on the destination, commence upload process...
if ! $GCLOUD_STORAGE_CMD ls "$FINAL_TARBALL_PATH" &> /dev/null; then
    START_TIME=$(date +%s)

    echo "Does not already exist in bucket: $FINAL_TARBALL_PATH"

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
            if ! $GCLOUD_STORAGE_CMD ls "${DESTINATION_BUCKET_PREFIX}/${RUN_BASENAME}/${RUN_BASENAME}_${file_basename}" &> /dev/null; then
                $GCLOUD_STORAGE_CMD cp "${PATH_TO_UPLOAD}/${filename}" "${DESTINATION_BUCKET_PREFIX}/${RUN_BASENAME}/${RUN_BASENAME}_${file_basename}"
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
        if [[ $current_size -ge $(($size_at_last_check + $chunk_size_bytes)) || "$run_is_finished" == "true" ]]; then
            echo "commencing sync on latest data"
            size_at_last_check=$current_size
            timestamp=$(date +%s) # intentionally called before tar so time is a little older    
            
            # increment counter for this tarball
            tar_increment_counter=$((tar_increment_counter + 1))
            
            # generate enhanced tar label with metadata
            tar_label=$(generate_tar_label "$RUN_BASENAME" "$tar_increment_counter")

            # Generate dynamic exclusions to prevent tarball bloat from partial *.cbcl files
            EXCLUSIONS_FILE="${STAGING_AREA_PATH}/${RUN_BASENAME}/dynamic_exclusions.txt"
            generate_dynamic_exclusions "$PATH_TO_UPLOAD" "$EXCLUSIONS_FILE" "$run_is_finished"
            
            # If this is the final tarball for a completed run, sync and wait to ensure all writes are flushed
            if [[ "$run_is_finished" == 'true' ]]; then
                echo "Run completed - performing final sync and wait before capturing final tarball"
                sync
                sleep 10  # Wait 10 seconds to ensure all file writes are fully committed
            fi

            # increate incremental tarballs
            # see: https://www.gnu.org/software/tar/manual/html_node/Incremental-Dumps.html
            #      https://www.gnu.org/software/tar/manual/html_node/Snapshot-Files.html
            # '--no-check-device' is for NFS
            # '-C "${PATH_TO_UPLOAD}" ." so we don't store the full path (-C is cd)
            # '--blocking-factor=1' prevents extra zero-padding blocks for efficient concatenation
            # '--sparse' consolidates runs of zeros in input files
            # '--label' adds enhanced metadata (JSON or pipe-separated format within 99-byte tar limit)
            # '--exclude-from' excludes patterns from file to prevent capturing incomplete *.cbcl files
            # 'head --bytes -1024' trims EOF blocks for incremental tarballs; final tarball preserves EOF blocks
            if [[ "$SOURCE_PATH_IS_ON_NFS" == "true" ]]; then SHOULD_CHECK_DEVICE_STR="--no-check-device"; else SHOULD_CHECK_DEVICE_STR=""; fi
            if [[ "$run_is_finished" == 'true' ]]; then EOF_PROCESSOR="cat"; else EOF_PROCESSOR="head --bytes -1024"; fi
            if [[ -f "$EXCLUSIONS_FILE" && -s "$EXCLUSIONS_FILE" ]]; then EXCLUSION_STR="--exclude-from=$EXCLUSIONS_FILE"; else EXCLUSION_STR=""; fi
            
            # Build static exclusion arguments from array
            STATIC_EXCLUSIONS=()
            for exclusion in "${TAR_EXCLUSIONS_ARRAY[@]}"; do
                STATIC_EXCLUSIONS+=("--exclude=$exclusion")
            done
            
                $TAR_BIN "${STATIC_EXCLUSIONS[@]}" \
                $EXCLUSION_STR \
                --create \
                --blocking-factor=1 \
                --sparse \
                --label="$tar_label" \
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
                # --checksums-only: Causes the rsync command to compute and compare checksums
                #    (instead of comparing mtime) for files if the size of source
                #    and destination match. (gcloud storage handles parallelization automatically)
                # --continue-on-error: If an error occurs, continue to attempt to copy the remaining
                #    files. If errors occurred, gcloud storage's exit status will be
                #    non-zero even if this flag is set. gcloud storage handles parallelization automatically.
                #
                # see: https://cloud.google.com/sdk/gcloud/reference/storage/rsync
                $GCLOUD_STORAGE_CMD rsync --checksums-only --continue-on-error --exclude='.*index$' "${STAGING_AREA_PATH}/${RUN_BASENAME}/" "${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/parts" && break
                retry_count=$((retry_count+1)) 
                #sleep $RSYNC_RETRY_DELAY_SEC
                sleep $(expr $RSYNC_RETRY_DELAY_SEC \* $retry_count) # each retry scale delay by a multiple of the count
            done

            # remove uploaded incremental tarballs
            # from the local STAGING_AREA_PATH once uploaded
            for incremental_tarball in $(find "${STAGING_AREA_PATH}/${RUN_BASENAME}" -type f -name "*.tar.gz"); do
                # if the local incremental tarball has indeed been synced
                # remove the local copy of it...
                if ! $GCLOUD_STORAGE_CMD ls "${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/parts/$(basename ${incremental_tarball})"; then
                    $GCLOUD_STORAGE_CMD cp "${incremental_tarball}" "${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/parts/$(basename ${incremental_tarball})" && rm "${incremental_tarball}"
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
    if ! $GCLOUD_STORAGE_CMD ls "$FINAL_TARBALL_PATH"; then
        # get the archive started with a blank file
        dummyfile="${STAGING_AREA_PATH}/${RUN_BASENAME}/dummyfile.tar.gz"
        touch $dummyfile
        $GCLOUD_STORAGE_CMD cp "${dummyfile}" "$FINAL_TARBALL_PATH"
        rm "${dummyfile}"
    fi

    # compose tar.gz increments into single tar.gz on gcp in chunks of <=32 incremental tarballs

    # append the first 31 incremental tarballs
    # to the main tarball, then remove the incremental tarballs
    # keep doing this until there are no more incremental tarballs
    # see: https://cloud.google.com/storage/docs/composing-objects#create-composite-cli
    until [[ "$($GCLOUD_STORAGE_CMD du ${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/parts/'*.tar.gz' 2> /dev/null | wc -l | awk '{print $1}' || echo '0')" == "0" ]]; do
        first_files=$($GCLOUD_STORAGE_CMD ls "${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/parts/"'*.tar.gz' | sort -V | head -n 31 | tr '\n' ' ')
        if [ ${#first_files} -ge 0 ]; then
            $GCLOUD_STORAGE_CMD objects compose "$FINAL_TARBALL_PATH" \
                ${first_files} \
                "$FINAL_TARBALL_PATH" && sleep 10 && $GCLOUD_STORAGE_CMD rm ${first_files}
        fi
    done

    # create a note about the tarball
    echo "$RUN_BASENAME.tar.gz created using optimized tar settings for efficient concatenation. Can be extracted with standard tar commands. The $RUN_BASENAME.terra.tsv file can be used to add a row for this tarball to a table on Terra." | $GCLOUD_STORAGE_CMD cp - "${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/$RUN_BASENAME.tar.gz.README.txt"
    
    # create and upload verbose metadata JSON file
    generate_verbose_metadata "$RUN_BASENAME" "$PATH_TO_UPLOAD" "$DESTINATION_BUCKET_PREFIX" "$START_TIME" | $GCLOUD_STORAGE_CMD cp - "${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/$RUN_BASENAME.upload_metadata.json"
    
    # create and upload Terra-compatible TSV file
    generate_terra_tsv "$RUN_BASENAME" "$FINAL_TARBALL_PATH" | $GCLOUD_STORAGE_CMD cp - "${DESTINATION_BUCKET_PREFIX}/$RUN_BASENAME/$RUN_BASENAME.terra.tsv"

    # if only the index file is present, remove it
    if [[ $(ls -1 "${STAGING_AREA_PATH}/${RUN_BASENAME}" | wc -l) -eq 1 ]]; then
        rm "${STAGING_AREA_PATH}/${RUN_BASENAME}/index"
    fi

    # if staging dir is empty, remove it (rmdir only does this if empty).
    rmdir "${STAGING_AREA_PATH}/${RUN_BASENAME}" &> /dev/null
else
    echo "Exiting; already exists: $FINAL_TARBALL_PATH"
    exit 0
fi
