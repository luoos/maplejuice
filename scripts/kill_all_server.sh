#!/bin/bash
pssh -h ./scripts/servers -i "kill \$(ps -aux|grep server|grep -v grep|awk '{print \$2}'); rm ~/server"
