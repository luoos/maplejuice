package node

import (
	"log"
	. "slogger"
)

type FileInfo struct {
	HashID       int
	Sdfsfilename string
	Localpath    string
	Timestamp    int
	MasterNodeID int
}

type FileList struct {
	ID      int
	FileMap map[int]map[string]*FileInfo
}

func CreateFileInfo(hashID int,
	sdfsfilename string,
	localpath string,
	timestamp int,
	masterNodeID int) *FileInfo {
	new_fileinfo := &FileInfo{
		HashID:       hashID,
		Sdfsfilename: sdfsfilename,
		Localpath:    localpath,
		Timestamp:    timestamp,
		MasterNodeID: masterNodeID,
	}
	return new_fileinfo
}

func CreateFileList(selfID int) *FileList {
	return &FileList{ID: selfID, FileMap: make(map[int]map[string]*FileInfo)}
}

// PutFileInfoObject is used For testing
func (fl *FileList) PutFileInfoObject(sdfsfilename string, fi *FileInfo) {
	if fl.GetFileInfo(sdfsfilename) != nil {
		SLOG.Printf("%s already exist, updating all metainfo", sdfsfilename)
	}
	hashid := getHashID(sdfsfilename)
	if fl.FileMap[hashid] == nil {
		fl.FileMap[hashid] = make(map[string]*FileInfo)
	}
	fl.FileMap[hashid][sdfsfilename] = fi
}

func (fl *FileList) PutFileInfo(hashID int,
	sdfsfilename string,
	localpath string,
	timestamp int,
	masterNodeID int) {
	if fl.GetFileInfo(sdfsfilename) != nil {
		SLOG.Printf("%s already exist, updating all metainfo", sdfsfilename)
	}
	hashid := getHashID(sdfsfilename)
	if fl.FileMap[hashid] == nil {
		fl.FileMap[hashid] = make(map[string]*FileInfo)
	}
	fl.FileMap[hashid][sdfsfilename] = &FileInfo{
		HashID:       hashID,
		Sdfsfilename: sdfsfilename,
		Localpath:    localpath,
		Timestamp:    timestamp,
		MasterNodeID: masterNodeID,
	}
}

func (fl *FileList) DeleteFileInfo(sdfsfilename string) bool {
	if fl.GetFileInfo(sdfsfilename) == nil {
		log.Fatal("File not found")
	}
	hashid := getHashID(sdfsfilename)
	delete(fl.FileMap[hashid], sdfsfilename)
	return true
}

func (fl *FileList) GetFileInfo(sdfsfilename string) *FileInfo {
	hashid := getHashID(sdfsfilename)
	if fl.FileMap[hashid] == nil {
		return nil
	}
	return fl.FileMap[hashid][sdfsfilename]
}

func (fl *FileList) GetTimeStamp(sdfsfilename string) int {
	hashid := getHashID(sdfsfilename)
	return fl.FileMap[hashid][sdfsfilename].Timestamp
}

func (fl *FileList) GetResponsibleFileWithID(prevID, curID int) []string {
	res := []string{}
	for id := prevID + 1; id <= curID; id++ {
		if fl.FileMap[id] != nil {
			for filename := range fl.FileMap[id] {
				res = append(res, filename)
			}
		}
	}
	return res
}
