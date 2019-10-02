package test

import (
	"os"
	"testing"

	. "slogger"
)

func TestLogToFile(t *testing.T) {
	SLOG.Print("123")

	// check existence
	_, err := os.Stat(LOG_FILE)
	if os.IsNotExist(err) {
		t.Fatal(err)
	}

	// remove
	err = os.Remove(LOG_FILE)
	if err != nil {
		t.Fatal(err)
	}
}
