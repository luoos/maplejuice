package main

import (
	"strconv"
	"strings"
)

func Maple(lines []string) map[string]string {
	kvMap := make(map[string]int)
	for _, line := range lines {
		url := strings.Split(line, " ")[6]
		kvMap[url] = kvMap[url] + 1
	}
	res := make(map[string]string)
	for k, v := range kvMap {
		res[k] = strconv.Itoa(v)
	}
	return res
}

func Juice(key string, lines []string) map[string]string {
	s := 0
	for _, l := range lines {
		n, err := strconv.Atoi(l)
		if err != nil {
			s = -1
			break
		}
		s = s + n
	}
	res := make(map[string]string)
	res[key] = strconv.Itoa(s)
	return res
}
