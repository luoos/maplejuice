package main

import (
	"fmt"
	"net"
	"node"
	"os"
	"os/signal"
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

func main() {
	sigCh := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	hostname, _ := os.Hostname()
	addr_raw, err := net.LookupIP(hostname)
	if err != nil {
		fmt.Println("Unknown host")
	}
	addr := fmt.Sprintf("%s", addr_raw[0])
	SLOG.Printf("Hostname: %s", addr)
	node := node.CreateNode(addr, PORT)
	go node.MonitorInputPacket()
	add, success := node.ScanIntroducer(SERVER_LIST)
	if success {
		node.Join(add)
	} else {
		node.InitMemberList()
	}
	go node.SendHeartbeatRoutine()

	signal.Notify(sigCh, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		node.Leave()
		SLOG.Print(sig)
		done <- true
	}()

	<-done
}
