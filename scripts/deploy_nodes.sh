#!/bin/bash
./scripts/build.sh
for h in `cat ./scripts/servers`;
do
    scp ./node_starter $h:/usr/app/dnode;
done
