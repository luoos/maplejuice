#!/bin/bash
pssh -h ~/distributed_log_querier/scripts/servers_luo -i "kill \$(ps -aux|grep server|grep -v grep|awk '{print \$2}')"
