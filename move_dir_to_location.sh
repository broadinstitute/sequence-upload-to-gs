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