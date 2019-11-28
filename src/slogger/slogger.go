package slogger

// This is a singleton logger.
// The log file is defined by const LOG_FILE

import (
	"log"
	"os"
	"os/user"
	"strings"
)

var (
	SLOG *log.Logger // singleton
)

var LOG_FILE = "/apps/logs/node.log"

func init() {
	if strings.HasSuffix(os.Args[0], ".test") {
		usr, _ := user.Current()
		LOG_FILE = usr.HomeDir + "/apps/logs/node.logtest"
	}

	if SLOG == nil {
		outfile, err := os.OpenFile(LOG_FILE, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
		os.Chmod(LOG_FILE, 0777)
		if err != nil {
			log.Fatal(err)
		}
		SLOG = log.New(outfile, "", log.LstdFlags)
	}
}
