package main

import (
	"bufio"
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
	lineChannelSize int
	lineBatchSize   int
}

func main() {
	if len(os.Args) < 2 {
		_, _ = fmt.Fprintln(os.Stderr, "Usage: postto <url>")
		return
	}
	cmd := cmdData{
		targetUrl:       os.Args[1],
		numWorkers:      64,
		lineChannelSize: 4096,
		lineBatchSize:   1,
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
	lineChannelSizeStr := os.Getenv("POSTTO_LINE_CHANNEL_SIZE")
	if lineChannelSizeStr != "" {
		cmd.lineChannelSize, err = strconv.Atoi(lineChannelSizeStr)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	lineBatchSizeStr := os.Getenv("POSTTO_LINE_BATCH_SIZE")
	if lineBatchSizeStr != "" {
		cmd.lineBatchSize, err = strconv.Atoi(lineBatchSizeStr)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
		if cmd.lineBatchSize < 1 {
			_, _ = fmt.Fprintln(os.Stderr, "POSTTO_LINE_BATCH_SIZE must be larger than zero")
			return
		}
	}
	err = do(cmd)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}
}

//const targetUrl = "http://192.168.224.90:8081/bigdata/samsung/poc2_table/"

//var password = func() string {
//	p := os.Getenv("V3IO_PASSWORD")
//	if p == "" {
//		return "datal@ke!"
//	}
//	return p
//}()

//var authorization = "Basic " + base64.StdEncoding.EncodeToString([]byte("iguazio:"+password))

func do(cmd cmdData) error {

	workersChannel := make(chan []byte, cmd.numWorkers*cmd.lineChannelSize)
	terminationChannel := make(chan error, cmd.numWorkers)
	for i := 0; i < cmd.numWorkers; i++ {
		go worker(cmd, workersChannel, terminationChannel)
	}

	//in, _ := os.Open("/Users/galt/Downloads/haproxy_json_logs_small.txt")
	in := os.Stdin
	reader := bufio.NewReader(in)
	eof := false
	i := 0
	terminationCount := 0
readLoop:
	for !eof {
	checkTerminationLoop:
		for {
			select {
			case <-terminationChannel:
				terminationCount++
				if terminationCount == cmd.numWorkers {
					break readLoop
				}
			default:
				break checkTerminationLoop
			}
		}

		bytes, err := reader.ReadBytes('\n')
		if err == io.EOF {
			eof = true
		} else if err != nil {
			return err
		}
		if !eof {
			bytes = bytes[:len(bytes)-1]
		}
		if len(bytes) > 0 {
			workersChannel <- bytes
		}
		i++
		if i%cmd.numWorkers == 0 {
			i = 0
		}
	}
	close(workersChannel)
	var err error
	for i := 0; i < cmd.numWorkers; i++ {
		errTmp := <-terminationChannel
		if errTmp != nil {
			err = errTmp
		}
	}
	return err
}

func worker(cmd cmdData, ch <-chan []byte, termination chan<- error) {
	var err error
	var lineBatch [][]byte
	var i int

	req := fasthttp.AcquireRequest()
	req.SetRequestURI(cmd.targetUrl)
	req.Header.SetMethod("POST")

	for entry := range ch {
		lineBatch = append(lineBatch, entry)
		i++
		if i == cmd.lineBatchSize {
			err = post(lineBatch, req)
			if err != nil {
				termination <- err
				return
			}
			req.SetBodyString("")
			i = 0
			lineBatch = nil
		}
	}
	termination <- nil
}

func post(entry [][]byte, req *fasthttp.Request) error {
	for i, e := range entry {
		if i != len(entry)-1 {
			e = append(e, '\n')
		}
		req.AppendBody(e)
	}
	resp := fasthttp.AcquireResponse()
	err := fasthttp.Do(req, resp)
	if err != nil {
		return errors.Wrap(err, "http error")
	} else if resp.StatusCode() >= 300 {
		return errors.Errorf("status code not OK: Request:\n%v\nResponse:\n%v\n", req, resp)
	}
	return nil
}
