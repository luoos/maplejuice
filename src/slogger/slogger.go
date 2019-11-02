package slogger

// This is a singleton logger.
// The log file is defined by const LOG_FILE

import (
	"log"
	"os"
	"strings"
)

var (
	SLOG *log.Logger // singleton
)

var LOG_FILE = "/apps/logs/node.log"

func init() {
	if strings.HasSuffix(os.Args[0], ".test") {
		LOG_FILE = LOG_FILE + "test"
	}

	if SLOG == nil {
		outfile, err := os.OpenFile(LOG_FILE, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
		if err != nil {
			log.Fatal(err)
		}
		SLOG = log.New(outfile, "", log.LstdFlags)
	}
}
