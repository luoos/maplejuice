PORT=${1:-8000} # default port 8000

pssh -h ./scripts/servers_luo -i "nohup /usr/app/log_querier/server --port=$PORT </dev/null &>/dev/null &"
