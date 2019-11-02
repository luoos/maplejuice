package test

import (
	"node"
	"os"
	"testing"
)

func TestGetIPsWithIds(t *testing.T) {
	node0 := node.CreateNode("0.0.0.0", "9500", "")
	node0.InitMemberList()
	node0.MbList.InsertNode(3, "0.0.0.3", "3", "1333", 3)
	node0.MbList.InsertNode(5, "0.0.0.5", "5", "1444", 3)
	node0.MbList.InsertNode(7, "0.0.0.7", "7", "1555", 3)
	ip_list := node0.GetAddressesWithIds([]int{3, 5, 7})
	assert(ip_list[0] == "0.0.0.3:1333", "wrong ip")
	assert(ip_list[1] == "0.0.0.5:1444", "wrong ip")
	assert(ip_list[2] == "0.0.0.7:1555", "wrong ip")
}

func TestSetFileDir(t *testing.T) {
	node0 := node.CreateNode("0.0.0.0", "9510", "9511")
	test_dir := "/tmp/nodetestfiledir"
	os.Remove(test_dir)
	_, err := os.Stat(test_dir)
	assert(os.IsNotExist(err), "should not exist")
	node0.SetFileDir(test_dir)
	_, err = os.Stat(test_dir)
	assert(err == nil, "should exist")
	os.Remove(test_dir)
	_, err = os.Stat(test_dir)
	assert(os.IsNotExist(err), "should not exist")
}
