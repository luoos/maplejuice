#!/bin/bash
pssh -h ~/distributed_log_querier/scripts/servers -i "kill \$(ps -aux|grep server|grep -v grep|awk '{print \$2}'); rm ~/server"
