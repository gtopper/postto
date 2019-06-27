package main

import (
	"bufio"
	"fmt"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
)

import _ "net/http/pprof"

type cmdData struct {
	targetUrl             string
	numRequestBuilders    int
	numConcurrentRequests int
	lineChannelSize       int
	requestChannelSize    int
	requestPoolSize       int
	lineBatchSize         int
}

func main() {
	if len(os.Args) < 2 {
		_, _ = fmt.Fprintln(os.Stderr, "Usage: postto <url>")
		return
	}
	cmd := cmdData{
		targetUrl:             os.Args[1],
		numRequestBuilders:    1,
		numConcurrentRequests: 8,
		lineChannelSize:       1024,
		requestChannelSize:    1024,
		requestPoolSize:       1024,
		lineBatchSize:         1,
	}
	var err error
	numRequestBuildersStr := os.Getenv("POSTTO_NUM_REQUEST_BUILDERS")
	if numRequestBuildersStr != "" {
		cmd.numRequestBuilders, err = strconv.Atoi(numRequestBuildersStr)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	numConcurrentRequestsStr := os.Getenv("POSTTO_NUM_CONCURRENT_REQUESTS")
	if numConcurrentRequestsStr != "" {
		cmd.numConcurrentRequests, err = strconv.Atoi(numConcurrentRequestsStr)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	requestChannelSizeStr := os.Getenv("POSTTO_REQUEST_CHANNEL_SIZE")
	if requestChannelSizeStr != "" {
		cmd.requestChannelSize, err = strconv.Atoi(requestChannelSizeStr)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	requestPoolSizeStr := os.Getenv("POSTTO_REQUEST_POOL_SIZE")
	if requestPoolSizeStr != "" {
		cmd.requestPoolSize, err = strconv.Atoi(requestPoolSizeStr)
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

	go func() { // for pprof
		_, _ = fmt.Fprintln(os.Stderr, http.ListenAndServe("localhost:6060", nil))
	}()

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
	lineChannel := make(chan []byte, cmd.lineChannelSize)
	reqChannel := make(chan *fasthttp.Request, cmd.requestChannelSize)
	availableReqChannel := make(chan *fasthttp.Request, cmd.requestPoolSize)
	for i := 0; i < cmd.numRequestBuilders; i++ {
		go buildRequests(cmd, lineChannel, reqChannel, availableReqChannel)
	}

	terminationChannel := make(chan error, cmd.numConcurrentRequests)
	for i := 0; i < cmd.numConcurrentRequests; i++ {
		go makeRequests(reqChannel, availableReqChannel, terminationChannel)
	}

	for i := 0; i < cmd.requestPoolSize; i++ {
		req := fasthttp.AcquireRequest()
		req.SetRequestURI(cmd.targetUrl)
		req.Header.SetMethod("POST")
		availableReqChannel <- req
	}

	//in, _ := os.Open("/Users/galt/Downloads/haproxy_json_logs_small.txt")
	in := os.Stdin
	reader := bufio.NewReader(in)
	eof := false
	terminationCount := 0
readLoop:
	for !eof {
	checkTerminationLoop:
		for {
			select {
			case err := <-terminationChannel:
				if err != nil {
					return err
				}
				terminationCount++
				if terminationCount == cmd.numConcurrentRequests {
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
			lineChannel <- bytes
		}
	}
	close(lineChannel)
	var err error
	for i := 0; i < cmd.numConcurrentRequests; i++ {
		errTmp := <-terminationChannel
		if errTmp != nil {
			err = errTmp
		}
	}
	return err
}

var requestBuilderCount int64

func buildRequests(cmd cmdData, lineChan <-chan []byte, reqChan chan<- *fasthttp.Request, availableReqChan <-chan *fasthttp.Request) {
	var i int
	var req *fasthttp.Request
	for line := range lineChan {
		i++
		if i != cmd.lineBatchSize {
			line = append(line, '\n')
		}
		if i == 1 {
			req = <-availableReqChan
			req.SetBody(line)
		} else {
			req.AppendBody(line)
		}
		if i == cmd.lineBatchSize {
			i = 0
			reqChan <- req
		}
	}
	atomic.AddInt64(&requestBuilderCount, 1)
	if requestBuilderCount == int64(cmd.numRequestBuilders) {
		close(reqChan)
	}
}

func makeRequests(reqChan <-chan *fasthttp.Request, availableReqChan chan<- *fasthttp.Request, terminationChan chan<- error) {
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	for req := range reqChan {
		err := post(req, resp)
		if err != nil {
			terminationChan <- err
		}
		availableReqChan <- req
	}
	terminationChan <- nil
}

func post(req *fasthttp.Request, resp *fasthttp.Response) error {
	err := fasthttp.Do(req, resp)
	if err != nil {
		return errors.Wrap(err, "http error")
	} else if resp.StatusCode() >= 300 {
		return errors.Errorf("status code not OK: Request:\n%v\nResponse:\n%v\n", req, resp)
	}
	return nil
}
