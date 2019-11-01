package test

import (
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
	sdfsfilename := "testFilename"
	localpath := "/app/fs"
	timestamp := 100
	masterNodeID := 128
	fl.PutFileInfo(sdfsfilename, localpath, timestamp, masterNodeID)
	fi := fl.GetFileInfo(sdfsfilename)
	if fi == nil {
		t.Fatalf("not added")
	}
	if fi.Localpath != localpath+"/"+sdfsfilename {
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
	// log.Printf("%+v\n", fi)
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
		//log.Printf("%+v\n", fi)
	}
	files := fl.GetFilesInRange(-1, 1024)
	if len(files) != 10 {
		t.Fatal("didn't get right files")
	}
	// log.Println(files)
}

func TestGetTimeStamp(t *testing.T) {
	fl := node.CreateFileList(1)
	sdfsfilename := "testFilename"
	localpath := "/app/fs"
	timestamp := 100
	masterNodeID := 128
	fl.PutFileInfo(sdfsfilename, localpath, timestamp, masterNodeID)
	ts := fl.GetTimeStamp(sdfsfilename)
	if ts != 100 {
		t.Fail()
	}
	ts = fl.GetTimeStamp("foo")
	if ts != -1 {
		t.Fail()
	}
}

// func TestGetAllFileInfo(t *testing.T) {
// 	fl := node.CreateFileList(1)
// 	fl.PutFileInfo("testFilename", "/app/fs", 10, 128)
// 	fl.PutFileInfo("testFilename1", "/app/fs", 10, 128)
// 	fl.PutFileInfo("testFilename2", "/app/fs", 10, 128)
// 	fl.PutFileInfo("testFilename3", "/app/fs", 10, 128)
// 	fl.PutFileInfo("testFilename4", "/app/fs", 10, 128)
// 	flList := fl.GetAllFileInfo()
// 	flList[0].MasterNodeID = 0
// 	filename := flList[0].Sdfsfilename
// 	fileInfo := fl.GetFileInfo(filename)
// 	assert(fileInfo.MasterNodeID == 0, "wrong id")
// }

func TestUpdateMasterId(t *testing.T) {
	fl := node.CreateFileList(1)
	fl.PutFileInfo("testFilename", "/app/fs", 1, 2)
	fl.PutFileInfo("testFilename1", "/app/fs", 4, 3)
	fl.PutFileInfo("testFilename2", "/app/fs", 6, 40)
	fl.PutFileInfo("testFilename3", "/app/fs", 10, 40)
	fl.PutFileInfo("testFilename4", "/app/fs", 20, 128)
	fl.UpdateMasterID(10, func(fileInfo *node.FileInfo) bool {
		return node.IsInCircleRange(fileInfo.MasterNodeID, 0, 3)
	})
	assert(fl.GetFileInfo("testFilename").MasterNodeID == 10, "wrong1")
	assert(fl.GetFileInfo("testFilename1").MasterNodeID == 10, "wrong2")
	fl.UpdateMasterID(50, func(fileInfo *node.FileInfo) bool {
		return fileInfo.Timestamp == 20
	})
	assert(fl.GetFileInfo("testFilename4").MasterNodeID == 50, "wrong4")
}

func TestDeleteFileInfosOutOfRange(t *testing.T) {
	fl := node.CreateFileList(1)
	fl.PutFileInfoTest(1, "testFilename1", "/app/fs", 1, 2)
	fl.PutFileInfoTest(2, "testFilename2", "/app/fs", 4, 3)
	fl.PutFileInfoTest(3, "testFilename3", "/app/fs", 6, 40)
	fl.PutFileInfoTest(4, "testFilename4", "/app/fs", 10, 40)
	fl.PutFileInfoTest(5, "testFilename5", "/app/fs", 20, 128)
	toDelete := fl.DeleteFileInfosOutOfRange(2, 4)
	assert(len(toDelete) == 3, "wrong len")
	assert(len(fl.FileMap) == 2, "wrong size")
	assert(fl.GetFileInfo("testFilename1") == nil, "should not exist")
	assert(fl.GetFileInfo("testFilename3") != nil, "should exist")
}
