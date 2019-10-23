package test

import (
	"log"
	"math/rand"
	"node"
	"testing"
)

func randomInt(min, max int) int {
	return min + rand.Intn(max-min)
}

// Generate a random string of A-Z chars with len = l
func randomString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		bytes[i] = byte(randomInt(65, 90))
	}
	return string(bytes)
}

func createDummyFile() (string, *node.FileInfo) {
	hashID := rand.Intn(1024)
	sdfsfilename := randomString(10)
	localpath := "/app/files/" + sdfsfilename
	timestamp := rand.Intn(1024)
	masterNodeID := rand.Intn(1024)
	return sdfsfilename, &node.FileInfo{
		HashID:       hashID,
		Sdfsfilename: sdfsfilename,
		Localpath:    localpath,
		Timestamp:    timestamp,
		MasterNodeID: masterNodeID}
}

func TestAddFileInfo(t *testing.T) {
	fl := node.CreateFileList(1)
	hashID := 123
	sdfsfilename := "testFilename"
	localpath := "/app/fs/testFilename"
	timestamp := 100
	masterNodeID := 128
	fl.PutFileInfo(hashID, sdfsfilename, localpath, timestamp, masterNodeID)
	fi := fl.GetFileInfo(sdfsfilename)
	if fi == nil {
		t.Fatalf("not added")
	}
	if fi.HashID != hashID {
		t.Fatalf("unmatched")
	}
	if fi.Localpath != localpath {
		t.Fatalf("unmatched")
	}
	if fi.Timestamp != timestamp {
		t.Fatalf("unmatched")
	}
	if fi.MasterNodeID != masterNodeID {
		t.Fatalf("unmatched")
	}
}

func TestDeleteFile(t *testing.T) {
	fl := node.CreateFileList(1)
	sdfsfilename, fi := createDummyFile()
	fl.PutFileInfoObject(sdfsfilename, fi)
	log.Printf("%+v\n", fi)
	if fl.GetFileInfo(sdfsfilename) == nil {
		t.Fatalf("not added")
	}
	fl.DeleteFileInfo(sdfsfilename)
	if fl.GetFileInfo(sdfsfilename) != nil {
		t.Fatalf("not delete")
	}
}

func TestGetResponsibleFileWithID(t *testing.T) {
	fl := node.CreateFileList(1)
	for i := 0; i < 10; i++ {
		sdfsfilename, fi := createDummyFile()
		fl.PutFileInfoObject(sdfsfilename, fi)
		log.Printf("%+v\n", fi)
	}
	files := fl.GetResponsibleFileWithID(-1, 1024)
	if len(files) != 10 {
		t.Fatal("didn't get right files")
	}
	log.Println(files)
}
