package main

import (
	"strconv"
	"strings"
)

func Maple(lines []string) map[string]string {
	kvMap := make(map[string]int)
	// reg, _ := regexp.Compile("[^a-zA-Z0-9]+")
	for _, line := range lines {
		// line = reg.ReplaceAllString(line, " ")
		words := strings.Fields(line)
		for _, w := range words {
			if val, exist := kvMap[w]; exist {
				kvMap[w] = val + 1
			} else {
				kvMap[w] = 1
			}
		}
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
