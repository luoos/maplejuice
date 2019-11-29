package main

import (
	"fmt"
	"net"
	"node"
	"os"
	"os/signal"
	"path/filepath"
	. "slogger"
	"syscall"
)

const (
	PORT string = "8180"
)

var SERVER_LIST = []string{
	"fa19-cs425-g17-01.cs.illinois.edu:" + PORT,
	"fa19-cs425-g17-02.cs.illinois.edu:" + PORT,
	"fa19-cs425-g17-03.cs.illinois.edu:" + PORT,
	"fa19-cs425-g17-04.cs.illinois.edu:" + PORT,
	"fa19-cs425-g17-05.cs.illinois.edu:" + PORT,
	"fa19-cs425-g17-06.cs.illinois.edu:" + PORT,
	"fa19-cs425-g17-07.cs.illinois.edu:" + PORT,
	"fa19-cs425-g17-08.cs.illinois.edu:" + PORT,
	"fa19-cs425-g17-09.cs.illinois.edu:" + PORT,
	"fa19-cs425-g17-10.cs.illinois.edu:" + PORT,
}

func clearDir(dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil {
		SLOG.Fatal(err)
	}
	for _, file := range files {
		err = os.RemoveAll(file)
		if err != nil {
			SLOG.Fatal(err)
		}
	}
	return nil
}

func main() {
	sigCh := make(chan os.Signal, 1)
	done := make(chan bool, 2)
	hostname, _ := os.Hostname()
	addr_raw, err := net.LookupIP(hostname)
	if err != nil {
		fmt.Println("Unknown host")
	}
	addr := fmt.Sprintf("%s", addr_raw[0])
	SLOG.Printf("Hostname: %s", addr)
	selfNode := node.CreateNode(addr, PORT, node.RPC_DEFAULT_PORT)
	mj := &node.MapleJuiceService{TaskQueue: make(chan *node.MapleJuiceTaskArgs, 10), SelfNode: selfNode}
	clearDir(selfNode.Root_dir)
	selfNode.UpdateHostname(hostname)
	go selfNode.MonitorInputPacket()
	go selfNode.StartRPCFileService()
	go selfNode.StartRPCMapleJuiceService(mj)
	add, success := selfNode.ScanIntroducer(SERVER_LIST)
	if success {
		selfNode.Join(add)
	} else {
		selfNode.InitMemberList()
	}
	go selfNode.SendHeartbeatRoutine()

	signal.Notify(sigCh, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		selfNode.Leave()
		SLOG.Print(sig)
		done <- true
	}()

	<-done
}
