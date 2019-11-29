package test

import (
	"fmt"
	"io/ioutil"
	"log"
	"node"
	"os"
	"path/filepath"
	"plugin"
	"testing"
	"time"
)

func TestAssignFiles(t *testing.T) {
	const NODES = 10
	var nodes [NODES]*node.Node
	for i := 0; i < NODES; i++ {
		nodes[i] = node.CreateNode("0.0.0.0", fmt.Sprintf("1100%d", i), "")
		nodes[i].DisableMonitorHB = true
	}
	nodes[0].InitMemberList()
	for _, nod := range nodes {
		go nod.MonitorInputPacket()
	}
	time.Sleep(1 * time.Second)
	for i := 1; i < NODES; i++ {
		nodes[i].Join(nodes[0].IP + ":" + nodes[0].Port)
	}
	for i, nod := range nodes {
		if nod.MbList.Size != NODES {
			t.Fatalf("wrong size for nod: %d size: %d", i, nod.MbList.Size)
		}
	}
	// files := []string{"file1", "file2", "file3", "file4", "file5", "file6", "file7", "file8", "file9", "file10", "file11"}
	// for _, f := range files {
	// 	fmt.Print(getHashID(f), " ")
	// }
	// log.Printf("%+v", nodes[0].PartitionFiles(files, 5, "hash"))
	// log.Printf("%+v", nodes[0].PartitionFiles(files, 5, "range"))
}

// func TestAddAndProcessMapleTask(t *testing.T) {
// 	master := node.CreateNode("0.0.0.0", "11100", "21100")
// 	master.InitMemberList()
// 	go master.MonitorInputPacket()
// 	mj := &node.MapleJuiceService{TaskQueue: make(chan *node.MapleJuiceTaskArgs, 10), SelfNode: master}
// 	go master.StartRPCMapleJuiceService(mj)
// 	time.Sleep((100 * time.Millisecond))
// 	worker := node.CreateNode("0.0.0.0", "11101", "21101")
// 	worker.Join(master.IP + ":" + master.Port)
// 	time.Sleep((100 * time.Millisecond))
// 	assert(master.Id < worker.Id, "assert wrong")
// 	// log.Print(master.Id, worker.Id)
// 	var reply node.RPCResultType
// 	args := &node.MapleJuiceTaskArgs{node.MapleTask, "", 1, "", "", ""}
// 	_ = mj.ForwardMapleJuiceRequest(args, &reply)
// 	time.Sleep((100 * time.Millisecond))
// 	assert(len(mj.TaskQueue) == 0, "task not processed ")
// }

func TestMapleTask(t *testing.T) {
	worker := node.CreateNode("0.0.0.0", "11102", "21202")
	exe_path := "/tmp/maple.so"
	p, _ := plugin.Open(exe_path)
	// 3. load func from exec
	f, err := p.Lookup("Maple")
	if err != nil {
		t.Fatal(err)
	}
	input_dir := "/tmp/input___1___wordcount"
	output_dir := "/tmp/output___1___wordcount"
	os.RemoveAll(input_dir)
	os.RemoveAll(output_dir)
	os.MkdirAll(input_dir, 0777)
	os.MkdirAll(output_dir, 0777)
	line := "hello world! Maple Juice Juice"
	err = ioutil.WriteFile(input_dir+"/testfile", []byte(line), 0777)
	if err != nil {
		t.Fatal(err)
	}
	worker.HandleMapleTask(input_dir, output_dir, f)
	if _, err := os.Stat(output_dir + "/Maple"); os.IsNotExist(err) {
		t.Fatal("wrong1")
	}
	if _, err := os.Stat(output_dir + "/Juice"); os.IsNotExist(err) {
		t.Fatal("wrong2")
	}
	if _, err := os.Stat(output_dir + "/hello"); os.IsNotExist(err) {
		t.Fatal("wrong3")
	}
	if _, err := os.Stat(output_dir + "/world"); os.IsNotExist(err) {
		t.Fatal("wrong4")
	}
	err = filepath.Walk(output_dir, func(path string, info os.FileInfo, err error) error {
		log.Println(path)
		return nil
	})
	os.RemoveAll(input_dir)
	os.RemoveAll(output_dir)
	worker_and_files := make(map[int][]string)
	worker_and_files[1] = []string{"1", "2"}
}
