package main

import (
	"node"
	"os"
	"os/signal"
	. "slogger"
	"syscall"
	"time"
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
	node := node.CreateNode(hostname, PORT)
	go node.MonitorInputPacket()
	add, success := node.ScanIntroducer(SERVER_LIST)
	if success {
		node.Join(add)
	} else {
		node.InitMemberList()
	}
	for node.MbList == nil {
		time.Sleep(1 * time.Second)
		SLOG.Print("waiting for setup...")
	}
	go node.SendHeartbeatRoutine()
	go node.CheckFailureRoutine()

	signal.Notify(sigCh, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		node.Leave()
		SLOG.Print(sig)
		done <- true
	}()

	<-done
}
