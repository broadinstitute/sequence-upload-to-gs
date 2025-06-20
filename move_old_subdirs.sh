#!/usr/bin/env bash

# Args are as follows:

# $1 source directory (directory to look within)
# $2 destination directory (directory to move old directories to)
# $3 older-than time (15m, 17d, etc.)

if [ "$#" -ne 3 ] || ! [ -d "$1" ] || ! [ -d "$2" ]; then
  echo "Usage: $(basename $0) dir_to_search/ dir_to_move_to/ age" >&2
  echo ""
  echo "Where:"
  echo "        dir_to_search is the source directory (directory to look within)"
  echo "        dir_to_move_to is the destination mountpoint (directory to move old directories to)"
  echo "        age is the older-than time (in days); directories older than this will be moved"
  exit 1
fi

SOURCE_DIR="$1"
DEST_DIR="$2"
MOVE_OLDER_THAN_DAYS="$3"

# ---
#   determine the absolute path of this script for finding adjacent scripts
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
# ---

if [ -d "$DEST_DIR" ]; then
    # if the destination is a mounted drive, copy to it
    if $(mount | grep "$DEST_DIR" &> /dev/null); then
        #  - On OSX we can use -Btime to use the inode birth time, but on Linux mtime is the best we can check
        #  - Note the use of the wildcard in "$SOURCE_DIR/*" and "-maxdepth 0", and "-d": this limits the search
        #    to only directories within the source dir (but not the source dir itself)
        find $SOURCE_DIR/* \
            -maxdepth 0 \
            -type d \
            -mtime +$MOVE_OLDER_THAN_DAYS \
            -exec cp \
            -R \
            --no-preserve=mode,ownership \
            "{}" $DEST_DIR \; \
            -exec rm -r "{}" \;
    fi

    # ToDo: replace copy and remove (i.e. move) execution functionality of the above 
    #       `find` call above with a find call that passes the directories identified to 
    #       invocations of `move_dir_to_location.sh`, assuming `move_dir_to_location.sh`
    #       exists in the same directory as this script (i.e. relative to $SCRIPTPATH as determined above).
fi