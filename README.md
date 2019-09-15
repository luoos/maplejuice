# Distributed Log Querier

Group members:
1. Ruochen Shen
2. Jun Luo

### Demo

#### localhost demo
1. cd to project project root folder
2. run `./scripts/build.sh`. You will get two bin `server` and `client` under project root folder
3. run `./server`
4. run `./client "<command>"` such as `./client "grep "Safari" ./sample_logs"`

#### distributed demo
if servers need to be updated: we will use the following method to deploy new servers
1. cd to project root folder
2. set user to be root for all host in ~/.ssh/config
3. run `./scripts/deploy_servers.sh` if server.go is modified and need to be rebuilt, it will restart log_querier as a service

else: we just run grep command
```bash
$ log_client "grep <pattern> -c /usr/logs/<your log pattern>"
```

#### Example 
```bash
$ log_client "grep -HcE '^[0-9]*[a-z]{5}' /usr/logs/vm*" |sort | awk -F '/' '{print $4}'
vm1.log:4102
vm2.log:4012
vm3.log:4154
vm4.log:4246
vm5.log:4130
vm6.log:4165
vm7.log:4083
vm8.log:4211
vm9.log:4069
vm10.log:4075
```

#### kill a server
we can will a server by login a remote vm and type `sudo systemctl stop log_querier`
alternatively, we can find out the PID by `sudo systemctl status log_querier` or `ps -aux |grep log_querier/server`
then `sudo kill <PID>`

### Scripts

Scripts under `scripts` folder

#### Build

```
[distributed_log_querier]$ ./scripts/build.sh
```
Build server and client and download sample log file if necessary

### Test

1. To send logs to 10 vms, run 
```bash
[distributed_log_querier]$ sh scripts/deploy_test_log.sh
```
2. To run test, 
```bash
[distributed_log_querier]$ go test -v ./.../test
```

> the test function included 6 tests:  grep Rare/Frequent/SomewhatFrequent word is efficient, grep pattern that appears in only one file, some files, or all files, determine if result is expeted
