#!/bin/bash
for h in `cat scripts/servers`; do sleep 1 && ssh $h systemctl start dnode; done
