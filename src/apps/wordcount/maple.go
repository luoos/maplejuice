package wordcount

import (
	"regexp"
	"strconv"
	"strings"
)

func Maple(lines []string) map[string]string {
	kvMap := make(map[string]int)
	reg, _ := regexp.Compile("[^a-zA-Z0-9]+")
	for _, line := range lines {
		line = reg.ReplaceAllString(line, " ")
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
