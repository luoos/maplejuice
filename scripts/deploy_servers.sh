#!/bin/bash
./scripts/build.sh
for h in `cat ./scripts/servers_luo`;
do
    scp ./server $h:/usr/app/log_querier;
    scp ./client $h:/usr/app/log_querier;
    scp ./grep_servers $h:/usr/app/log_querier;
done
