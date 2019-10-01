package slogger

// This is a singleton logger.
// Just call the func start with "Log_".
// The log file is defined by const LOG_FILE

import (
	"log"
	"os"
)

var (
	Slogger *log.Logger // singleton
)

const LOG_FILE = "/apps/logs/node.log"

func Init() {
	if Slogger == nil {
		outfile, err := os.OpenFile(LOG_FILE, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatal(err)
		}
		Slogger = log.New(outfile, "", log.LstdFlags)
	}
}

func Log_Info(s string) {
	Init()
	Slogger.Println(s)
}
