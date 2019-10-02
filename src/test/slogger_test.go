package test

import (
	"os"
	"slogger"
	"testing"
)

func TestLogToFile(t *testing.T) {
	slogger.Log_Info("123")

	// check existence
	_, err := os.Stat(slogger.LOG_FILE)
	if os.IsNotExist(err) {
		t.Fatal(err)
	}

	// remove
	err = os.Remove(slogger.LOG_FILE)
	if err != nil {
		t.Fatal(err)
	}
}
