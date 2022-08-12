# sequence-upload-to-gs
Incremental upload of Illumina sequencing runs to GS from a companion computer, or directly from Linux-based Illumina sequencers

## Setup for execution on a network server or companion computer

> _Forthcoming_

## Setup on a Linux-based Illumina sequencer

The following Illumina sequencer models are known to be Linux-based and compatible with the upload scripts in this repository:
 * NextSeq 2000 (CentOS)

Download `monitor_runs.sh` and `incremental_illumina_upload_to_gs.sh` from this repository to the `~ilmnadmin/` home folder (or elsewhere, though adjust accordingly below).

`chmod u+x monitor_runs.sh incremental_illumina_upload_to_gs.sh`

```
# install google-cloud-sdk
sudo tee -a /etc/yum.repos.d/google-cloud-sdk.repo << EOM
[google-cloud-sdk]
name=Google Cloud SDK
baseurl=https://packages.cloud.google.com/yum/repos/cloud-sdk-el7-x86_64
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg
       https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
EOM
```

`sudo yum install google-cloud-cli`

`sudo mkdir /usr/local/illumina/seq-run-uploads && sudo chown ilmnadmin:ilmnusers /usr/local/illumina/seq-run-uploads`

`crontab -e `

Add this line:

`@hourly ~/monitor_runs.sh /usr/local/illumina/runs gs://bucket/flowcells >> ~/upload_monitor.log`
