package test

import (
	"apps/wordcount"
	"testing"
)

func TestWordCntMaple(t *testing.T) {
	lines := []string{"hello world! Maple Juice Juice"}
	output := wordcount.Maple(lines)
	assert(len(output) == 4, "wrong length")
	assert(output["hello"] == "1", "wrong1")
	assert(output["world"] == "1", "wrong2")
	assert(output["Juice"] == "2", "wrong3")
	assert(output["Maple"] == "1", "wrong3")
}
