# Distributed Log Querier

Group members:
1. Ruochen Shen
2. Jun Luo

### Demo

#### localhost demo
1. cd to project root folder
2. run `./scripts/build.sh`. You will get two bin `server` and `client` under project root folder
3. run `./server`
4. run `./client "<command>"` such as `./client "grep -rnI "Safari" ./sample_logs"`

#### distributed demo
1. cd to project root folder
2. run `./scripts/kill_all_servers.sh` error output means servers are already dead, success means killed successfully
3. run `./scripts/start_all_servers.sh` sends server binary file to 10 servers and running them in background
4. run `./client "find . -regex './vm[1-10].log' -exec grep hello {} \;"` to grep hello in all these log files.

### Scripts

Scripts under `scripts` folder

#### Download Sample Log

`./scripts/download_sample_logs.sh`: Download a sample http server log into `sample_logs` folder

#### Build

`./scripts/build.sh`: Build server and client and download sample log file if necessary
