package main

import (
	"fmt"
	"strconv"
	"strings"
)

func Maple(lines []string) map[string]string {
	res := make(map[string]string)
	res["allurl"] = strings.Join(lines, "___") // key is dummy
	return res
}

func Juice(key string, lines []string) map[string]string {
	// key is dummy
	total_count := 0
	res := make(map[string]string)
	kvMap := make(map[string]float64)
	for _, line := range lines {
		url_and_count := strings.Split(line, "___")
		for _, pairString := range url_and_count {
			pair := strings.Split(pairString, " ")
			n, _ := strconv.Atoi(pair[1])
			total_count += n
			kvMap[pair[0]] = float64(n)
		}
	}
	for k, v := range kvMap {
		kvMap[k] = v / float64(total_count) * 100
	}
	for k, v := range kvMap {
		res[k] = fmt.Sprintf("%.2f", v) + `%`
	}
	return res
}
