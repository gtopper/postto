package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
	"io"
	"os"
	"strconv"
)

type cmdData struct {
	targetUrl       string
	numWorkers      int
	workerQueueSize int
}

func main() {
	if len(os.Args) < 2 {
		_, _ = fmt.Fprintln(os.Stderr, "Usage: postto <url>")
		return
	}
	cmd := cmdData{
		targetUrl:       os.Args[1],
		numWorkers:      64,
		workerQueueSize: 64,
	}
	var err error
	numWorkersStr := os.Getenv("POSTTO_NUM_WORKERS")
	if numWorkersStr != "" {
		cmd.numWorkers, err = strconv.Atoi(numWorkersStr)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	workerQueueSizeStr := os.Getenv("POSTTO_WORKER_QUEUE_SIZE")
	if workerQueueSizeStr != "" {
		cmd.workerQueueSize, err = strconv.Atoi(workerQueueSizeStr)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	err = do(cmd)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}
}

//const targetUrl = "http://192.168.224.90:8081/bigdata/samsung/poc2_table/"

var password = func() string {
	p := os.Getenv("V3IO_PASSWORD")
	if p == "" {
		return "datal@ke!"
	}
	return p
}()

var authorization = "Basic " + base64.StdEncoding.EncodeToString([]byte("iguazio:"+password))

func do(cmd cmdData) error {

	workerChannels := make([]chan []byte, cmd.numWorkers)
	terminationChannels := make([]chan error, cmd.numWorkers)
	for i := 0; i < cmd.numWorkers; i++ {
		workerChannels[i] = make(chan []byte, cmd.workerQueueSize)
		terminationChannels[i] = make(chan error, 1)
		go worker(cmd, workerChannels[i], terminationChannels[i])
	}

	//in, _ := os.Open("test.txt")
	in := os.Stdin
	reader := bufio.NewReader(in)
	eof := false
	i := 0
readLoop:
	for !eof {
		for i := 0; i < cmd.numWorkers; i++ {
			if len(terminationChannels[i]) > 0 {
				break readLoop
			}
		}
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			eof = true
		} else if err != nil {
			return err
		}
		bytes := []byte(line)
		if !eof {
			bytes = bytes[:len(bytes)-1]
		}
		if len(bytes) > 0 {
			workerChannels[i] <- bytes
		}
		i++
		if i%cmd.numWorkers == 0 {
			i = 0
		}
	}
	for i := 0; i < cmd.numWorkers; i++ {
		close(workerChannels[i])
	}
	for i := 0; i < cmd.numWorkers; i++ {
		<-terminationChannels[i]
	}
	return nil
}

func worker(cmd cmdData, ch <-chan []byte, termination chan<- error) {
	var err error
	for entry := range ch {
		err = post(cmd, entry)
		if err != nil {
			termination <- err
			return
		}
	}
	termination <- nil
}

func post(cmd cmdData, entry []byte) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(cmd.targetUrl)
	req.Header.SetMethod("POST")
	req.Header.Set("Authorization", authorization)
	req.Header.Set("X-v3io-function", "PutItem")
	req.SetBody(entry)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	err := fasthttp.Do(req, resp)
	if err != nil {
		return errors.Wrap(err, "http error")
	} else if resp.StatusCode() >= 300 {
		return errors.Errorf("status code not OK: Request:\n%v\nResponse:\n%v\n", req, resp)
	}
	return nil
}
