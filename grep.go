package main

import (
    "fmt"
    "os"
    "bufio"
    "strings"
    "strconv"
)

func main() {
    if len(os.Args) == 1 {
        fmt.Println("usage: ./grep <keyword> <file>")
        os.Exit(1)
    }
    keyword, filename := os.Args[1], os.Args[2]

    file, err := os.Open(filename)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    scanner.Split(bufio.ScanLines)

    var res []string
    line_number := 1
    for scanner.Scan() {
        line := scanner.Text()
        if strings.Contains(line, keyword) {
            res = append(res, strconv.Itoa(line_number) + ": " + line)
        }
        line_number++
    }

    for _, line := range res {
        fmt.Println(line)
    }
}
