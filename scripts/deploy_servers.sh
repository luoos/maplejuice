#!/bin/bash
./scripts/build.sh
for h in `cat ./scripts/servers`;
do
    scp ./server $h:/usr/app/log_querier;
    scp ./client $h:/usr/app/log_querier;
    scp ./scripts/servers $h:/usr/app/log_querier;
done
pssh -h ./scripts/servers -i "systemctl restart log_querier"
