# Distributed File System - MP3

Group members:
1. Ruochen Shen   rs20
2. Jun Luo        junluo2

## Setup
modify any code file and then run:
1. `cd` to project root folder
2. `./scripts/deploy_nodes.sh`
> we used systemctl so you will need root privalege to do it.

## Command
In any of our vms, Use `dcli` to see available commands:
1. `exec "<command>"` - execute command on all servers
2. `dump` - dump local host membership list
3. `ls <sdfsfilename>` - list all machine addresses where this file is currently being stored
4. `store` - list all files currently being stored at this machine
5. `put <localfilename> <sdfsfilename>` - Insert or update a local file to the distributed file system
6. `put <localdirname>` - Insert or update all local files in a directory
7. `get <sdfsfilename> <localfilename>` - Get the file from the distributed file system, and store it to <localfilename>
8. `delete <sdfsfilename>` - Delete a file from the distributed file system`



# Distributed Node System - MP2

Group members:
1. Ruochen Shen   rs20
2. Jun Luo        junluo2

## Setup
modify any code file and then run:
1. `cd` to project root folder
2. `./scripts/deploy_nodes.sh`

## Demo
1. start a node as introducer: at any machine: `sudo systemctl start dnode`
2. start all other machine: `./scripts/start_all_nodes.sh`

## Monitor
1. use `dcli --dump` to see memberlist of current node
2. use `dcli "tail /apps/logs/node.log"` to see all logs from other machine
3. use grep for above command for specific target

## Leave
 1. to tell a node to leave. We login into that machine and type command `kill -2 <PID>` which sends a SIGINT
 2. the <PID> can be found by checking `systemctl status dnode`

## Docker

```shell
# build
docker build -f Dockerfile-dnode -t luojl/dnode_starter .
# run
docker run -d -v /apps/logs:/apps/logs -v /tmp:/tmp luojl/dnode_starter
```

# Distributed Log Querier - MP1

Group members:
1. Ruochen Shen   rs20
2. Jun Luo        junluo2

## Setup
**This section is already done. Safely skip it.**

If servers need to be updated: we will use the following method to deploy new servers
1. `cd` to project root folder
2. set user to be root for all host in ~/.ssh/config
3. run `./scripts/deploy_servers.sh` if server.go is modified and need to be rebuilt, it will restart log_querier as a service
> to start a service, we must have root privilege, so it is requires you to type root password every time,
> We put our own pubkey in the root directory to make it possible, so the deploy_servers script can not be used by others.

## Demo

Log files are under `/usr/logs` and we should use the **absolute** path when `grep` log files.

Usage:
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

#### Start a server

Login a remote vm and type `sudo systemctl start log_querier`

#### Kill a server

We can kill a server by login a remote vm and type `sudo systemctl stop log_querier`
alternatively, we can find out the PID by `sudo systemctl status log_querier` or `ps -aux |grep log_querier/server`
then `sudo kill <PID>`


## Scripts

Scripts under `scripts` folder

#### Build

```
[distributed_log_querier]$ ./scripts/build.sh
```

Build server and client, you will get two bin, `server` and `client`, under project root folder


## Test

1. To send logs to 10 vms, `/usr/logs/`, run
```bash
[distributed_log_querier]$ sh scripts/deploy_test_log.sh
```
2. To run test,
```bash
[distributed_log_querier]$ go test -v ./.../test
```

the test function included 6 tests:
- grep Rare/Frequent/SomewhatFrequent word is efficient
- grep pattern that appears in only one file, some files, or all files, determine if result is expeted
