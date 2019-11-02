#!/bin/bash
for h in `cat scripts/servers`; do sleep 1 && echo $h && ssh $h systemctl start dnode; done
