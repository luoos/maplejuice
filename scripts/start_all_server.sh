#!/bin/bash
~/distributed_log_querier/build.sh
for h in `cat ~/distributed_log_querier/scripts/hosts`;
do scp ~/distributed_log_querier/server $h:~; done
#pssh -h hosts -i "nohup ~/server &"
