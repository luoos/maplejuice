package test

import (
	"fmt"
	"io/ioutil"
	"node"
	"os"
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

func TestMapleJuiceTask(t *testing.T) {
	t.Skip("add files in /tmp for this test")
	worker := node.CreateNode("0.0.0.0", "11102", "21102")
	exe_path := "/tmp/wordcount.so"
	p, _ := plugin.Open(exe_path)

	// Maple
	f, err := p.Lookup("Maple")
	if err != nil {
		t.Fatal(err)
	}
	input_dir := "/tmp/input___1___wordcount"
	output_path := "/tmp/output___1___wordcount"
	os.RemoveAll(input_dir)
	os.RemoveAll(output_path)
	os.MkdirAll(input_dir, 0777)
	os.MkdirAll(output_path, 0777)
	line := "hello world! Maple Juice Juice Maple"
	err = ioutil.WriteFile(input_dir+"/testfile", []byte(line), 0777)
	if err != nil {
		t.Fatal(err)
	}
	worker.HandleMapleTask(input_dir, output_path, f)
	if _, err := os.Stat(output_path + "/Maple"); os.IsNotExist(err) {
		t.Fatal("wrong1")
	}
	if _, err := os.Stat(output_path + "/Juice"); os.IsNotExist(err) {
		t.Fatal("wrong2")
	}
	if _, err := os.Stat(output_path + "/hello"); os.IsNotExist(err) {
		t.Fatal("wrong3")
	}
	if _, err := os.Stat(output_path + "/world"); os.IsNotExist(err) {
		t.Fatal("wrong4")
	}
	// err = filepath.Walk(output_path, func(path string, info os.FileInfo, err error) error {
	// 	log.Println(path)
	// 	return nil
	// })
	os.RemoveAll(input_dir)

	// Juice
	f, err = p.Lookup("Juice")
	if err != nil {
		t.Fatal(err)
	}
	input_dir = output_path
	output_path = "/tmp/juiceResult"
	os.MkdirAll(output_path, 0777)
	worker.HandleJuiceTask(input_dir, output_path, f)
	if _, err := os.Stat(output_path); os.IsNotExist(err) {
		t.Fatal("wrong1")
	}
	data, err := ioutil.ReadFile(output_path + "/output")
	if err != nil {
		t.Fatal(err)
	}
	assert(string(data) == "Juice 2\nMaple 2\nhello 1\nworld 1\n", "wrong res")
	os.RemoveAll(input_dir)
	os.RemoveAll(output_path)
}

func TestMapleJuiceURLPercentTask(t *testing.T) {
	t.Skip("add files in /tmp for this test")
	worker := node.CreateNode("0.0.0.0", "11103", "21103")
	exe_path := "/tmp/urlcount.so"
	p, _ := plugin.Open(exe_path)

	// Maple
	f, err := p.Lookup("Maple")
	if err != nil {
		t.Fatal(err)
	}
	input_dir := "/tmp/sample_logs"
	output_path := "/tmp/phase1mapleRes"
	os.RemoveAll(output_path)
	os.MkdirAll(output_path, 0777)
	worker.HandleMapleTask(input_dir, output_path, f)

	// Juice
	f, err = p.Lookup("Juice")
	if err != nil {
		t.Fatal(err)
	}
	input_dir = output_path
	output_path = "/tmp/phase1juiceRes"
	os.RemoveAll(output_path)
	os.MkdirAll(output_path, 0777)
	worker.HandleJuiceTask(input_dir, output_path, f)
	if _, err := os.Stat(output_path + "/output"); os.IsNotExist(err) {
		t.Fatal("wrong1")
	}
	os.RemoveAll(input_dir)

	exe_path = "/tmp/urlpercent.so"
	p, _ = plugin.Open(exe_path)

	// Maple 2
	f, err = p.Lookup("Maple")
	if err != nil {
		t.Fatal(err)
	}
	input_dir = "/tmp/phase1juiceRes"
	output_path = "/tmp/pahse2mapleRes"
	os.RemoveAll(output_path)
	os.MkdirAll(output_path, 0777)
	worker.HandleMapleTask(input_dir, output_path, f)

	os.RemoveAll(input_dir)
	// Juice 2
	f, err = p.Lookup("Juice")
	if err != nil {
		t.Fatal(err)
	}
	input_dir = output_path
	output_path = "/tmp/phase2juiceRes"
	os.RemoveAll(output_path)
	os.MkdirAll(output_path, 0777)
	worker.HandleJuiceTask(input_dir, output_path, f)
	if _, err := os.Stat(output_path); os.IsNotExist(err) {
		t.Fatal("wrong1")
	}
	os.RemoveAll(input_dir)
}
