#!/bin/bash
./scripts/build.sh mp2
for h in `cat ./scripts/servers`;
do
    scp ./node_starter $h:/usr/app/dnode;
done
