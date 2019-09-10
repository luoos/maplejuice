#!/bin/bash
~/distributed_log_querier/scripts/build.sh
for h in `cat ~/distributed_log_querier/scripts/servers`;
do scp ~/distributed_log_querier/server $h:~ ;done
pssh -h ~/distributed_log_querier/scripts/servers -i "nohup ~/server </dev/null &>/dev/null &"
