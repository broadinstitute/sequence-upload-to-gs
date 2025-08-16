# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains bash scripts for incremental upload of Illumina sequencing runs to Google Cloud Storage. The system creates incremental gzipped tarballs and syncs them to a single tarball in a GS bucket, designed to work on Linux-based Illumina sequencers (like NextSeq 2000) or companion computers.

## Architecture

**Core Scripts:**
- `incremental_illumina_upload_to_gs.sh` - Main upload script that creates incremental tar archives and uploads them to GS
- `monitor_runs.sh` - Monitoring script that watches for new run directories and launches upload processes
- `simulate_sequencer_write.sh` - Testing utility that simulates a sequencer writing data incrementally

**Key Components:**
- **Incremental Archiving**: Uses GNU tar with `--listed-incremental` to create incremental backups
- **Chunked Uploads**: Splits large runs into manageable chunks (default 100MB) with retry logic
- **GS Composition**: Uses `gcloud storage objects compose` to merge incremental tarballs into single archives
- **Cross-platform Support**: Handles differences between Linux (Illumina sequencers) and macOS

## Dependencies

Required tools that must be available:
- `gcloud storage` (Google Cloud SDK)
- `tar` (GNU tar, installed as `gtar` on macOS)
- `pstree` (for monitoring script, installed via `brew install pstree` on macOS)

## Environment Variables

Key configuration variables (with defaults):
- `CHUNK_SIZE_MB=100` - Size of incremental tar chunks
- `DELAY_BETWEEN_INCREMENTS_SEC=30` - Wait time between upload attempts
- `RUN_COMPLETION_TIMEOUT_DAYS=16` - Max time to wait for run completion
- `STAGING_AREA_PATH` - Location for temporary files (defaults to `/usr/local/illumina/seq-run-uploads` on Illumina machines, `/tmp/seq-run-uploads` elsewhere)
- `RSYNC_RETRY_MAX_ATTEMPTS=12` - Maximum retry attempts for uploads
- `INCLUSION_TIME_INTERVAL_DAYS=7` - Age limit for runs to be considered for upload
- `TERRA_RUN_TABLE_NAME=flowcell` - Table name for Terra TSV file generation (creates `entity:{table_name}_id` column)

## Usage Patterns

**Main upload script:**
```bash
./incremental_illumina_upload_to_gs.sh /path/to/run gs://bucket-prefix
```

**Monitoring script:**
```bash
./monitor_runs.sh /path/to/monitored-directory gs://bucket-prefix
```

**Simulation script (for testing):**
```bash
./simulate_sequencer_write.sh /path/to/actual_run /path/to/simulated_run
```

## Important Implementation Details

- **Excluded Directories**: The upload excludes large non-essential directories: `Thumbnail_Images`, `Images`, `FocusModelGeneration`, `Autocenter`, `InstrumentAnalyticsLogs`, `Logs`
- **Individual Files**: `SampleSheet.csv` and `RunInfo.xml` are uploaded separately before tarball creation
- **Run Completion Detection**: Looks for `RTAComplete.txt` or `RTAComplete.xml` files
- **Tarball Extraction**: Resulting tarballs must be extracted with GNU tar using `--ignore-zeros`
- **NFS Support**: Uses `--no-check-device` flag for NFS mounted storage
- **Platform Detection**: Automatically detects Illumina machines vs other environments
- **Cleanup**: Removes local incremental tarballs after successful upload

## Cron Integration

The monitoring script is designed to work with cron scheduling. Example crontab entry:
```
@hourly ~/monitor_runs.sh /usr/local/illumina/runs gs://bucket/flowcells >> ~/upload_monitor.log
```

## File Paths

Staging areas:
- Illumina machines: `/usr/local/illumina/seq-run-uploads`
- Other systems: `/tmp/seq-run-uploads`

Run detection based on presence of `RunInfo.xml` files in monitored directories.