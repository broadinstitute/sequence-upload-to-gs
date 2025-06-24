#!/usr/bin/env bash

DEST_DIR=${DEST_DIR:-'/media/seqdata'}
DEFAULT_DELAY_BEFORE_DIR_MOVE_SEC=7200

# set DELAY_BEFORE_DIR_MOVE_SEC to default value of DEFAULT_DELAY_BEFORE_DIR_MOVE_SEC
# if and only if DELAY_BEFORE_DIR_MOVE_SEC is not set
# if DELAY_BEFORE_DIR_MOVE_SEC is set to an empty value, the default value will NOT
# be used and no sleeping will occur 
if [ -z ${DELAY_BEFORE_DIR_MOVE_SEC+x} ]; then
  DELAY_BEFORE_DIR_MOVE_SEC=$DEFAULT_DELAY_BEFORE_DIR_MOVE_SEC
fi

SOURCE_DIR="$1"
if [ -d "${SOURCE_DIR}" ]; then
  SOURCE_DIR="$(realpath ${SOURCE_DIR})"
fi

if [ "$#" -ne 1 ] || ! [ -d "${SOURCE_DIR}" ]; then
  echo "Usage: "
  echo "  [DEST_DIR=/override/destination/path] [DELAY_BEFORE_DIR_MOVE_SEC=7200] [OTHER_SUBDIRS_TO_EXCLUDE="Thumbnail_Images"] $0 SOURCE_DIR/" >&2
  echo ""
  echo "  Where:"
  echo "        SOURCE_DIR is the directory to move to the env var DEST_DIR specified (default: /media/seqdata)"
  echo ""
  echo "  Note 1: Some sequencers (i.e. MiSeq, perhaps others) expect a run directory to be present at its output"
  echo "          location during the post-run wash stage, even though sequencing has finished."
  echo "          The wash can take a while, so this script will wait DELAY_BEFORE_DIR_MOVE_SEC (env var; default is $DEFAULT_DELAY_BEFORE_DIR_MOVE_SEC seconds, or $(bc --expression "scale=2; ${DEFAULT_DELAY_BEFORE_DIR_MOVE_SEC}/(60*60)" | sed 's/\.00$//') hours)"
  echo "          after invocation before relocating the directory."
  echo ""
  echo "  Note 2: The subdirectory SOURCE_DIR/Images will be removed from SOURCE_DIR prior to moving other files, if it is present."
  echo "          Additional subdirs can be specified via env var OTHER_SUBDIRS_TO_EXCLUDE."
  echo "          The value for OTHER_SUBDIRS_TO_EXCLUDE should be a colon-separated list of subdirectories to remove prior to moving the directory."
  echo "          It's assumed that each subdir path specified via OTHER_SUBDIRS_TO_EXCLUDE has SOURCE_DIR as its parent."
  echo "            ex.:"
  echo "              OTHER_SUBDIRS_TO_EXCLUDE=Thumbnail_Images $0 SOURCE_DIR/"
  echo "          If no directories should be removed, unset SUBDIRS_TO_EXCLUDE_FROM_BACKUP and leave OTHER_SUBDIRS_TO_EXCLUDE unset."
  echo "            ex. (NB: no value is given below on RHS of assignment for SUBDIRS_TO_EXCLUDE_FROM_BACKUP):"
  echo "                'SUBDIRS_TO_EXCLUDE_FROM_BACKUP=  $0 SOURCE_DIR/'"
  echo ""
  exit 1
fi

if [ -z "${SUBDIRS_TO_EXCLUDE_FROM_BACKUP+x}" ]; then
  #echo "SUBDIRS_TO_EXCLUDE_FROM_BACKUP is not set; excluding default dir(s)"
    SUBDIRS_TO_EXCLUDE_FROM_BACKUP=(
    "${SOURCE_DIR}/Images"
    "${SOURCE_DIR}/Thumbnail_Images"
  )
else
  #echo "SUBDIRS_TO_EXCLUDE_FROM_BACKUP is set but empty; 
  # excluding no dirs by default but considering any passed via OTHER_SUBDIRS_TO_EXCLUDE"
  SUBDIRS_TO_EXCLUDE_FROM_BACKUP=()
fi

# if OTHER_SUBDIRS_TO_EXCLUDE is set (comma-delimited list paths relative to SOURCE_DIR), 
# add those directories to the exclusion list.
#
# Note:
#   that the assumed parent of each subdir is ${SOURCE_DIR}, so
#   ${SOURCE_DIR} will be prepended to each item added.
if [ -n "$OTHER_SUBDIRS_TO_EXCLUDE" ]; then
  echo "Other subdirs to exclude: $OTHER_SUBDIRS_TO_EXCLUDE"
  IFS=':' read -ra OTHER_SUBDIRS_TO_EXCLUDE <<< "$OTHER_SUBDIRS_TO_EXCLUDE"
  for subdir in "${OTHER_SUBDIRS_TO_EXCLUDE[@]}"; do
    SUBDIRS_TO_EXCLUDE_FROM_BACKUP+=("${SOURCE_DIR}/${subdir}")
  done
fi

# --------------------
# Function: check_backup_dest
# Usage: check_backup_dest SRC_DIR DEST_DIR SUBDIRS_TO_EXCLUDE_FROM_BACKUP
# Returns success (0) if all backup pre-check criteria are met; otherwise returns failure (non-zero).
check_backup_dest() {
    local SRC_DIR="$1"
    local DEST_DIR="$2"
    local exclusions_array_nameref="$3"

    # Ensure the third argument is treated as an array name (Bash 4.3+ nameref/pointer)
    local -n EXCLUSIONS_ARRAY="$exclusions_array_nameref"

    # 1. Check DEST_DIR exists, is a directory, and is accessible
    if [[ ! -d "$DEST_DIR" ]]; then
        echo "ERROR: Destination directory '$DEST_DIR' does not exist or is not a directory." >&2
        return 1
    fi
    # Check we have write permission on DEST_DIR
    if [[ ! -w "$DEST_DIR" ]]; then
        echo "ERROR: Destination directory '$DEST_DIR' is not writable." >&2
        return 1
    fi

    # 2. If DEST_DIR is (or is within) a mount point, ensure it is mounted and writable
    if command -v mountpoint >/dev/null 2>&1; then
        if mountpoint -q "$DEST_DIR"; then
            # DEST_DIR is a mount point and is currently mounted
            : #noop
        else
            # DEST_DIR itself is not a mount point. Check if an ancestor is a mount point.
            # Find the mount point containing DEST_DIR
            local mount_target=""
            if command -v findmnt >/dev/null 2>&1; then
                mount_target="$(findmnt -n -T "$DEST_DIR" -o TARGET)"
            fi
            if [[ -n "$mount_target" && "$mount_target" != "/" ]]; then
                # An ancestor directory is a mount point (mount_target), check if that is mounted
                if ! mountpoint -q "$mount_target"; then
                    echo "ERROR: Mount point '$mount_target' for destination is not mounted." >&2
                    return 1
                fi
            fi
        fi
    fi

    # Also ensure the destination (or its mount) is not read-only
    if command -v findmnt >/dev/null 2>&1; then
        local mount_opts
        mount_opts="$(findmnt -n -T "$DEST_DIR" -o OPTIONS)"
        # Look for 'ro' (read-only) option that is not part of a longer word
        if grep -qE '(^|,)ro($|,)' <<< "$mount_opts"; then
            echo "ERROR: Destination filesystem is mounted read-only." >&2
            return 1
        fi
    fi

    # 3. Check free space and inodes on DEST can accomodate space/inodes required for SRC
    # Calculate total bytes in SRC_DIR (excluding specified subdirs)
    local total_bytes=0
    local total_inodes=0
    local exclude

    # get total size in bytes (block-size=1) on one filesystem (-x)
    total_bytes=$(du -s -B1 -x "$SRC_DIR" 2>/dev/null | awk '{print $1}')
    if [[ -z "$total_bytes" ]]; then total_bytes=0; fi

    # Calculate total number of files (and directories) in SRC (for inode count)
    # but exclude the (parent) source directory itself from the count
    total_inodes=$(find "$SRC_DIR" -mindepth 1 -xdev 2>/dev/null | wc -l)
    if [[ -z "$total_inodes" ]]; then total_inodes=0; fi

    # Subtract sizes and inodes of excluded subdirectories
    for exclude in "${EXCLUSIONS_ARRAY[@]}"; do
        # Only consider top-level excludes (relative to SRC_DIR)
        if [[ -e "$SRC_DIR/$exclude" ]]; then
            # Get size of this subdirectory
            local ex_bytes
            ex_bytes=$(du -s -B1 -x "$SRC_DIR/$exclude" 2>/dev/null | awk '{print $1}')
            if [[ -n "$ex_bytes" ]]; then
                total_bytes=$(( total_bytes - ex_bytes ))
            fi
            # Get inode count of this subdirectory (include all entries under it)
            local ex_inodes
            ex_inodes=$(find "$SRC_DIR/$exclude" -xdev 2>/dev/null | wc -l)
            if [[ -n "$ex_inodes" ]]; then
                total_inodes=$(( total_inodes - ex_inodes ))
            fi
        fi
    done
    if (( total_bytes < 0 )); then total_bytes=0; fi
    if (( total_inodes < 0 )); then total_inodes=0; fi

    # Get available space and free inodes on DEST_DIR filesystem using stat -f
    local free_blocks block_size free_bytes free_inodes total_fs_inodes
    free_blocks=$(stat -f -c "%a" "$DEST_DIR" 2>/dev/null)
    block_size=$(stat -f -c "%S" "$DEST_DIR" 2>/dev/null)
    free_inodes=$(stat -f -c "%d" "$DEST_DIR" 2>/dev/null)
    total_fs_inodes=$(stat -f -c "%c" "$DEST_DIR" 2>/dev/null)
    if [[ -z "$free_blocks" || -z "$block_size" ]]; then
        echo "ERROR: Unable to determine free space on filesystem containing '$DEST_DIR'." >&2
        return 1
    fi
    # Calculate free bytes
    free_bytes=$(( free_blocks * block_size ))

    # Compare required vs available
    if (( free_bytes < total_bytes )); then
        echo "ERROR: Insufficient disk space in '$DEST_DIR'. Needed $total_bytes bytes, but only $free_bytes bytes available." >&2
        return 1
    fi

    # If filesystem reports inodes (total_fs_inodes != 0), then enforce inode availability
    if [[ -n "$free_inodes" && -n "$total_fs_inodes" && "$total_fs_inodes" -ne 0 ]]; then
        if (( free_inodes < total_inodes )); then
            echo "ERROR: Insufficient free inodes in '$DEST_DIR'. Need $total_inodes, but only $free_inodes available." >&2
            return 1
        fi
    fi

    # 4. Ensure DEST does not already contain SRC data
    # Check for identical paths or nesting
    local real_src real_dest
    real_src="$(readlink -f "$SRC_DIR")"   # canonical absolute path
    real_dest="$(readlink -f "$DEST_DIR")"
    if [[ -z "$real_src" || -z "$real_dest" ]]; then
        echo "ERROR: Unable to resolve real paths of source or destination." >&2
        return 1
    fi
    if [[ "$real_src" == "$real_dest" ]]; then
        echo "ERROR: Source and destination are the same directory ('$SRC_DIR')." >&2
        return 1
    fi
    # Check if destination is inside source path
    if [[ "$real_dest" == "$real_src/"* ]]; then
        echo "ERROR: Destination '$DEST_DIR' is located inside source '$SRC_DIR' (invalid backup path)." >&2
        return 1
    fi
    # Or if source is inside destination
    if [[ "$real_src" == "$real_dest/"* ]]; then
        echo "ERROR: Source '$SRC_DIR' is located inside destination '$DEST_DIR' (already contains source data)." >&2
        return 1
    fi
    # Check if a folder with the same name as SRC exists in DEST (possible prior backup)
    local src_name
    src_name="$(basename "$SRC_DIR")"
    if [[ -d "$DEST_DIR/$src_name" ]]; then
        echo "ERROR: Destination already contains a directory named '$src_name' (potential existing backup of source)." >&2
        return 1
    fi

    # 5. Check filesystem and device health for DEST_DIR
    # Determine the device backing DEST_DIR
    local dev_name dev_base
    if command -v findmnt >/dev/null 2>&1; then
        dev_name="$(findmnt -n -T "$DEST_DIR" -o SOURCE)"
    fi
    if [[ -z "$dev_name" ]]; then
        # Fallback: use df to get device name
        dev_name="$(df -P "$DEST_DIR" 2>/dev/null | tail -1 | awk '{print $1}')"
    fi
    # If dev_name is a partition, get the base disk (e.g., /dev/sda1 -> /dev/sda)
    dev_base="$dev_name"
    if [[ "$dev_name" =~ ^/dev/(sd[a-z]+|hd[a-z]+|vd[a-z]+)([0-9]+)$ ]]; then
        dev_base="/dev/${BASH_REMATCH[1]}"   # strip partition number
    elif [[ "$dev_name" =~ ^/dev/(mmcblk[0-9]+|nvme[0-9]+n[0-9]+)p[0-9]+$ ]]; then
        dev_base="/dev/${BASH_REMATCH[1]}"   # strip partition part for mmcblk or nvme
    fi

    # Use smartctl to check drive health, if it's available (not generally installed by default)
    if command -v smartctl >/dev/null 2>&1; then
        if [[ -n "$dev_base" && -e "$dev_base" ]]; then
            # Only run smartctl on real disks (avoid LVM or virtual devices)
            if [[ "$dev_base" == /dev/sd* || "$dev_base" == /dev/hd* || "$dev_base" == /dev/vd* || "$dev_base" == /dev/nvme* || "$dev_base" == /dev/mmcblk* ]]; then
                # Perform SMART health check
                if ! smartctl -H "$dev_base" >/dev/null 2>&1; then
                    local smart_status=$?
                    if [[ $smart_status -eq 2 ]]; then
                        echo "ERROR: SMART indicates drive $dev_base is failing or predictive failure detected." >&2
                        return 1
                    fi
                    # Exit status 1 or others means either smartctl couldn't run or SMART not available; do not treat as fatal (unknown health)
                else
                    # smartctl -H succeeded, now parse output for explicit failure (in case exit code didn't cover it)
                    # (Typically, smartctl -H returns exit 2 on fail, exit 0 on pass, but we'll double-check the text)
                    local smart_output
                    smart_output="$(smartctl -H "$dev_base")"
                    if grep -q "SMART overall-health self-assessment test result: FAILED" <<< "$smart_output"; then
                        echo "ERROR: SMART health check FAILED for device $dev_base." >&2
                        return 1
                    fi
                fi
            fi
        fi
    fi

    # Check system logs (dmesg) for disk or filesystem errors related to the destination device
    # by searching for the base device name (e.g., sda, nvme0n1, etc.) in dmesg for any "error" occurrences.
    # Commented out for now pending further testing and validation.
    # local dev_pattern log_errors
    # if [[ -n "$dev_base" ]]; then
    #     dev_pattern=$(basename "$dev_base")  # e.g., "sda" for "/dev/sda"
    #     log_errors=$(dmesg 2>/dev/null | grep -Ei "($dev_pattern).*error")
    #     if [[ -n "$log_errors" ]]; then
    #         echo "ERROR: Detected errors in system logs for device $dev_base:" >&2
    #         # Print a representative error line (or all, if needed)
    #         echo "$log_errors" | tail -1 >&2
    #         echo "(Destination device or filesystem has recent errors, failing health check.)" >&2
    #         return 1
    #     fi
    # fi
    # # Also check /var/log messages if available (for older systems or more persistent log of errors)
    # if [[ -r /var/log/syslog ]]; then
    #     if grep -qEi "($dev_pattern).*error" /var/log/syslog; then
    #         echo "ERROR: Detected errors involving $dev_pattern in syslog (filesystem/device health issue)." >&2
    #         return 1
    #     fi
    # elif [[ -r /var/log/messages ]]; then
    #     if grep -qEi "($dev_pattern).*error" /var/log/messages; then
    #         echo "ERROR: Detected errors involving $dev_pattern in system log (filesystem/device health issue)." >&2
    #         return 1
    #     fi
    # fi

    # If all checks passed:
    return 0
}
# --------------------

# check that the destination directory exists, is writable, and has enough space to accommodate the files in SOURCE_DIR
# if ! check_backup_dest "/srv/samba/home/miseq/run_dir" "/media/seqdata/run_dir" SUBDIRS_TO_EXCLUDE_FROM_BACKUP; then
#     # One or more conditions failed
#     exit 1
# fi


if [ -d "$DEST_DIR" ]; then
  echo "INFO:    $DEST_DIR exists; proceeding with move operation..."


  # check if $DEST_DIR is a mount point, if so, check if it is currently mounted.
  # If currently mounted, check whether the available free space on the mounted drive
  # is greater than the size of SOURCE_DIR (after excluding the directories specified in SUBDIRS_TO_EXCLUDE_FROM_BACKUP)


  #if ! $(mount | grep "$DEST_DIR" &> /dev/null); then
    # The MiSeq expects the run directory to be present during the post-run wash stage, even if sequencing has finished.
    # The wash takes ~20 minutes, so we'll wait for 2 hours to give it a chance to finish before copying files.
    
    # if $DELAY_BEFORE_DIR_MOVE_SEC is set and greater in value than zero, sleep for $DELAY_BEFORE_DIR_MOVE_SEC seconds
    if [ ! -z "$DELAY_BEFORE_DIR_MOVE_SEC" ] && [ "$DELAY_BEFORE_DIR_MOVE_SEC" -gt 0 ]; then
      echo "sleeping for ${DELAY_BEFORE_DIR_MOVE_SEC} sec (~$(bc --expression "scale=2; ${DELAY_BEFORE_DIR_MOVE_SEC}/(60*60)" | sed 's/\.00$//') hr) until operations expecting the present location of SOURCE_DIR have finished"
      sleep $DELAY_BEFORE_DIR_MOVE_SEC
    fi

    for subdir in "${SUBDIRS_TO_EXCLUDE_FROM_BACKUP[@]}"; do
      # if the basename of $subdir is "Data", warn the user that this directory will not be removed unless REMOVE_DATA_DIR is set to "true"
      if [ "$(basename "$subdir")" == "Data" ]; then
        if [ "${REMOVE_DATA_DIR:-false}" != "true" ]; then
          echo "ERROR:   The directory $subdir was specified for exclusion but will not be removed unless REMOVE_DATA_DIR is set to 'true'."
          echo "  Exiting..."
          #continue
          exit 1
        else
          echo "INFO:    REMOVE_DATA_DIR is set to 'true'; the directory $subdir WILL be removed."
        fi
      fi

      if [ -d "$subdir" ]; then
        echo "Removing directory: $subdir"
        rm -rf $subdir
      else
        echo "WARNING: Directory to remove does not exist: $subdir"
      fi
    done
    #exit 0

    # ToDo: add retry logic around rsync call, with exponential backoff delay between attempts if it fails, 
    #       as well as a limit on the number of retries to attempt before giving up.
    #       

    # rsync key:
    # -r = recursive
    # -l = copy links
    # -t = maintain times
    # -D = copy special files
    # -c = copy based on checksum not times
    # --remove-source-files = cause rsync to behave more like mv (but with verification)
    # since empty source directories are NOT removed by rsync, we need to find and remove them
    rsync \
      -rltDc \
      --remove-source-files \
      $(dirname $SOURCE_DIR)/$(basename $SOURCE_DIR) $(dirname $DEST_DIR)/$(basename $DEST_DIR) \
    && find $SOURCE_DIR \
      -depth \
      -type d \
      -empty \
      -exec rm -rf {} \;

    # simple fallback:
    # mv $SOURCE_DIR $DEST_DIR
  #fi
fi