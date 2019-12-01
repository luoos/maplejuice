package test

import (
	"plugin"
	"testing"
)

// func TestWordCntMaple(t *testing.T) {
// 	lines := []string{"hello world! Maple Juice Juice"}
// 	output := wordcount.Maple(lines)
// 	assert(len(output) == 4, "wrong length")
// 	assert(output["hello"] == "1", "wrong1")
// 	assert(output["world"] == "1", "wrong2")
// 	assert(output["Juice"] == "2", "wrong3")
// 	assert(output["Maple"] == "1", "wrong3")
// }

func TestWordCount(t *testing.T) {
	// Maple
	lines := []string{"hello world! Maple Juice Juice"}
	exe_path := "/tmp/wordcount.so"
	p, err := plugin.Open(exe_path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := p.Lookup("Maple")
	if err != nil {
		t.Fatal(err)
	}
	mapleFunc := f.(func([]string) map[string]string)
	output := mapleFunc(lines)
	assert(len(output) == 4, "wrong length")
	assert(output["hello"] == "1", "wrong1")
	assert(output["world"] == "1", "wrong2")
	assert(output["Juice"] == "2", "wrong3")
	assert(output["Maple"] == "1", "wrong3")

	// Juice
	juiceF, err := p.Lookup("Juice")
	if err != nil {
		t.Fatal(err)
	}
	juiceFunc := juiceF.(func(string, []string) []string)
	lines = []string{"1", "2", "3"}
	joutput := juiceFunc("coco", lines)
	assert(joutput[0] == "coco", "wrong key")
	assert(joutput[1] == "6", "wrong value")

	lines = []string{"1", "2", "."}
	joutput = juiceFunc("coco", lines)
	assert(joutput[0] == "coco", "wrong key")
	assert(joutput[1] == "-1", "wrong value")
}
