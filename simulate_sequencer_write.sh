#!/bin/bash

# Chris Tomkins-Tinch
# tomkinsc@broadinstitute.org

# comment to quiet
# audio_alerts=yes # only applies where the "say" command is available (macOS)

# -----------------------------------

if [[ "$#" -ne 2 ]]; then
    echo "Usage: $(basename $0) /path/to/actual_run /path/to/synced_copy_of_run"
    exit 1
fi

SOURCE_RUN="$1"
SIMULATED_RUN="$2"

mkdir -p "${SIMULATED_RUN}"

alert_user() {
    msg="$1"
    echo "$msg"
    if ! [ -z $audio_alerts ]; then
        if [ "$(uname)" == "Darwin" ]; then
            say -r 290 "$msg"
        fi
    fi
}

alert_user "Beginning simulated sequencing run copy job. Here we go!"

sleep 1
alert_user "Copying lightweight top-level files quickly"
# copy the top-level lightweight files relatively quickly
for f in $(cd "${SOURCE_RUN}" && find . -type f -maxdepth 1 -exec bash -c 'printf "%q\n" "$@"' sh {} \; 2>/dev/null);do     
    rsync -aR "${SOURCE_RUN}/${f}" "${SIMULATED_RUN}/"
    sleep 5
done

sleep 10
alert_user "Syncing top-level directories except for the Data directory. More slowly."
# sync the all top-level directories except Data, one every so often
for f in $(cd "${SOURCE_RUN}" && find . -path './Data' -prune -o -type d -print -maxdepth 1 -mindepth 1 -exec bash -c 'printf "%q\n" "$@"' sh {} \; 2>/dev/null | sed 's/^\.\///');do 
    rsync -a "${SOURCE_RUN}/${f}/" "${SIMULATED_RUN}/${f}"
    sleep 60
done

sleep 5
alert_user "Copying basecall data quickly, apart from cycle data"
# copy the other files relatively quickly, excluding the data for all of the cycles
# handle the invalid unix filenames (w/ spaces) Illumina uses for RTA...
for f in $(cd "${SOURCE_RUN}" && find ./Data -path './Data/Intensities/BaseCalls/L*' -prune -o -type f -print0 2>/dev/null | xargs -I {} -0 echo '{}' | sed 's/\ /_______/g'); do 
    rsync -aR "${SOURCE_RUN}/$(echo "$f" | sed 's/_______/ /g')" "${SIMULATED_RUN}/"
    sleep 2
done

sleep 8
alert_user "Copying basecall cycle data. This will take a while..."
# only copy basecalls, sleeping after every few
counter=0
for f in $(cd "${SOURCE_RUN}" && find ./Data -path './Data/Intensities/BaseCalls/L*' -type f -print 2>/dev/null | sort -V);do 
    rsync -aR "${SOURCE_RUN}/${f}" "${SIMULATED_RUN}/"
    ((counter++))
    # sleep for 3 seconds every 20 files
    if ! ((counter % 20)); then
        sleep 3
    fi
done

sleep 5
alert_user "Copying anything we missed before."
# sweep up anything missed
rsync -av "${SOURCE_RUN}/" "${SIMULATED_RUN}"

sleep 10
alert_user "Creating an R-T-A complete file to signal run completion."
# actual contents of RTAComplete.txt don't matter
echo "1/21/2021,01:54:07.037,Illumina RTA 1.18.54" > "${SIMULATED_RUN}/RTAComplete.txt" 

alert_user "Done copying the simulated sequencing run."