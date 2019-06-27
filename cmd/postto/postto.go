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
)

import _ "net/http/pprof"

type cmdData struct {
	targetUrl             string
	numConcurrentRequests int
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
		numConcurrentRequests: 8,
		requestChannelSize:    1024,
		requestPoolSize:       1024,
		lineBatchSize:         1,
	}
	var err error
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

	fmt.Printf("Configuration: %+v\n", cmd)

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
	reqChannel := make(chan *fasthttp.Request, cmd.requestChannelSize)
	availableReqChannel := make(chan *fasthttp.Request, cmd.requestPoolSize)

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
	var err error
	var req *fasthttp.Request
	var lineCount int
	for !eof {
	checkTerminationLoop:
		for {
			select {
			case err := <-terminationChannel:
				return err // Can never be nil
			default:
				break checkTerminationLoop
			}
		}

		var isNewReq bool
		if req == nil {
			req = <-availableReqChannel
			isNewReq = true
		}
		for {
			var bytes []byte
			bytes, err = reader.ReadSlice('\n')
			if err == nil && lineCount == cmd.lineBatchSize-1 {
				bytes = bytes[:len(bytes)-1] // drop last \n
			}
			if len(bytes) > 0 && (err == nil || err == bufio.ErrBufferFull) {
				if isNewReq {
					req.SetBody(bytes)
				} else {
					req.AppendBody(bytes)
				}
			}
			if err != bufio.ErrBufferFull {
				break
			}
		}
		if err == io.EOF {
			eof = true
			err = nil
		} else if err != nil {
			return err
		}
		lineCount++
		if lineCount == cmd.lineBatchSize || eof {
			reqChannel <- req
			req = nil
			lineCount = 0
		}
	}
	close(reqChannel)
	for i := 0; i < cmd.numConcurrentRequests; i++ {
		errTmp := <-terminationChannel
		if errTmp != nil {
			err = errTmp
		}
	}
	return err
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
