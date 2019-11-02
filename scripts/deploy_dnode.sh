#!/bin/bash
./scripts/build.sh
./scripts/kill_all_nodes.sh
for h in `cat ./scripts/servers`;
do
    scp ./node_starter $h:/usr/app/dnode;
done
./scripts/start_all_nodes.sh
