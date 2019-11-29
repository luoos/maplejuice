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

func TestLoadMaple(t *testing.T) {
	lines := []string{"hello world! Maple Juice Juice"}
	exe_path := "/tmp/maple.so"
	p, _ := plugin.Open(exe_path)
	// 3. load func from exec
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
}
