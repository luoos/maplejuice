package test1

import (
	"testing"
	"os/exec"
	"time"
	"bytes"
    "strings"
)

func runtime_local(cmd string) time.Duration {
    start := time.Now()
    exec.Command("bash", "-c", cmd).Run()
    return time.Since(start)
}

func TestRareWordsEfficient(t *testing.T) {
	cmd := exec.Command("bash", "-c", "log_client 'grep Imrare /usr/logs/random*'")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	start := time.Now()
	err := cmd.Run()
	if err != nil {
		t.Errorf(err.Error())
        return
	}
    elapsed := time.Since(start)
    start2 := time.Now()
    runtime_local("log_client 'grep Imrare /usr/logs/random*'")
    elapsed2 := time.Since(start2)
    if 2 * elapsed2 < elapsed {
        t.Errorf("Too slow")
    }
    t.Logf("remote time:%v local time:%v", elapsed, elapsed2)
}
func TestFrequentWordsEfficient(t *testing.T) {
	cmd := exec.Command("bash", "-c", "log_client 'grep ImFreqent /usr/logs/random*'")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	start := time.Now()
	err := cmd.Run()
	if err != nil {
		t.Errorf(err.Error())
        return
	}
    elapsed := time.Since(start)
    start2 := time.Now()
    runtime_local("log_client 'grep Imrare /usr/logs/random*'")
    elapsed2 := time.Since(start2)
    if 2 * elapsed2 < elapsed {
        t.Errorf("Too slow")
    }
    t.Logf("remote time:%v local time:%v", elapsed, elapsed2)
}
func TestSomewhatFrequentWordsEfficient(t *testing.T) {
	cmd := exec.Command("bash", "-c", "log_client 'grep ImsomewhatFrequent /usr/logs/random*'")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	start := time.Now()
	err := cmd.Run()
	if err != nil {
		t.Errorf(err.Error())
        return
	}
    elapsed := time.Since(start)
    start2 := time.Now()
    runtime_local("log_client 'grep Imrare /usr/logs/random*'")
    elapsed2 := time.Since(start2)
    if 2 * elapsed2 < elapsed {
        t.Errorf("Too slow")
    }
    t.Logf("remote time:%v local time:%v", elapsed, elapsed2)
}
func TestOneFileExist(t *testing.T) {
	cmd := exec.Command("bash", "-c", "log_client 'grep -c ImtheOnlyOne /usr/logs/random*'")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
    err := cmd.Run()
    if err != nil {
        t.Errorf(err.Error())
    }
    res := stdout.String()
    count := 0
    for _, line := range strings.Split(res, "\n") {
        if line == "" {
            continue
        }
        if strings.Contains(line, ":1") {
            count++
        }
    }
    if count != 1 {
        t.Errorf("the appearence of this word is not 1")
    }
}
func TestSomeFileExist(t *testing.T) {
	cmd := exec.Command("bash", "-c", "log_client 'grep IminSomeFiles /usr/logs/random*'")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
    err := cmd.Run()
    if err != nil {
        t.Errorf(err.Error())
    }
    res := stdout.String()
    for _, line := range strings.Split(res, "\n") {
        if line == "" {
            continue
        }
        if strings.Contains(line, ":1") {
            t.Logf("does contains in some file")
        }
    }
}
func TestAllFileExist(t *testing.T) {
	cmd := exec.Command("bash", "-c", "log_client 'grep -c IminAllFiles /usr/logs/random*'")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
    err := cmd.Run()
    if err != nil {
        t.Errorf(err.Error())
    }
    res := stdout.String()
    for _, line := range strings.Split(res, "\n") {
        if line == "" {
            continue
        }
        if !strings.Contains(line, ":1") {
            t.Errorf("did not appear in all files: " + line)
        }
    }
}

