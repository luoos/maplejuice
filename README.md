# Distributed Log Querier

Group members:
1. Ruochen Shen
2. Jun Luo

### Demo

1. cd to project root folder
2. run `./scripts/build.sh`. You will get two bin `server` and `client` under project root folder
3. run `./server`
4. run `./client <regexp>` such as `./client Safari`

### Scripts

Scripts under `scripts` folder

#### Download Sample Log

`./scripts/download_sample_logs.sh`: Download a sample http server log into `sample_logs` folder

#### Build

`./scripts/build.sh`: Build server and client and download sample log file if necessary