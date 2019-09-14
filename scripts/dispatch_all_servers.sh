#!/bin/bash
./scripts/build.sh
for h in `cat scripts/servers`;
do scp server client $h:/usr/logs/ ;done
pssh -h scripts/servers -i "systemctl restart dlogger"
