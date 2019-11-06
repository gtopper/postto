package main

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type cmdData struct {
	targetUrl             string
	headers               map[string]string
	numConcurrentRequests int
	requestPoolSize       int
	lineBatchSize         int
}

var count uint64
var client fasthttp.HostClient

func main() {
	if len(os.Args) < 2 {
		_, _ = fmt.Fprintln(os.Stderr, "Usage: postto <url>")
		return
	}

	cmd := cmdData{
		targetUrl:             os.Args[1],
		numConcurrentRequests: 8,
		lineBatchSize:         1,
	}

	targetUrl, err := url.Parse(cmd.targetUrl)
	if err == nil && targetUrl.Scheme != "http" && targetUrl.Scheme != "https" {
		err = errors.Errorf("unsupported scheme '%s'", targetUrl.Scheme)
	}
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Bad URL '%s': %v\n", cmd.targetUrl, err)
		return
	}

	numConcurrentRequestsStr := os.Getenv("POSTTO_NUM_CONCURRENT_REQUESTS")
	if numConcurrentRequestsStr != "" {
		cmd.numConcurrentRequests, err = strconv.Atoi(numConcurrentRequestsStr)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	cmd.requestPoolSize = 2 * cmd.numConcurrentRequests
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
	headersStr := os.Getenv("POSTTO_HEADERS")
	if headersStr != "" {
		cmd.headers = make(map[string]string)
		for _, headerStr := range strings.Split(headersStr, ",") {
			parts := strings.SplitN(headerStr, ":", 2)
			if len(parts) < 2 {
				_, _ = fmt.Fprintf(os.Stderr, "bad header '%s'\n", headerStr)
				return
			}
			cmd.headers[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}

	fmt.Printf("Configuration: %+v\n", cmd)

	client = fasthttp.HostClient{
		Addr:      targetUrl.Host,
		IsTLS:     targetUrl.Scheme == "https",
		TLSConfig: &tls.Config{InsecureSkipVerify: true},
	}

	go printAndResetLoop()

	err = do(cmd)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	}
}

func printAndResetLoop() {
	var lastCount uint64
	for i := 1; true; i++ {
		time.Sleep(5 * time.Second)
		progress := count - lastCount
		timePassed := i * 5
		ratePerSec := float64(progress) / 5.0
		fmt.Printf("Received %d OK responses in %d seconds... [%.2f/s]\n", count, timePassed, ratePerSec)
		lastCount = count
	}
}

func do(cmd cmdData) error {
	reqChannel := make(chan *fasthttp.Request, cmd.requestPoolSize)
	availableReqChannel := make(chan *fasthttp.Request, cmd.requestPoolSize)

	terminationChannel := make(chan error, cmd.numConcurrentRequests)
	for i := 0; i < cmd.numConcurrentRequests; i++ {
		go makeRequests(reqChannel, availableReqChannel, terminationChannel)
	}

	for i := 0; i < cmd.requestPoolSize; i++ {
		req := fasthttp.AcquireRequest()
		req.SetRequestURI(cmd.targetUrl)
		req.Header.SetMethod("POST")
		for key, value := range cmd.headers {
			req.Header.Set(key, value)
		}
		availableReqChannel <- req
	}

	//in, _ := os.Open("test.txt")
	in := os.Stdin
	reader := bufio.NewReader(in)
	eof := false
	var err error
	var req *fasthttp.Request
	var lineCount int
	var isReqInitialized bool
	for !eof {
		if req == nil {
			select {
			case req = <-availableReqChannel:
				isReqInitialized = false
			case err := <-terminationChannel:
				return err
			}
		}
		for {
			var bytes []byte
			bytes, err = reader.ReadSlice('\n')
			if err == nil && lineCount == cmd.lineBatchSize-1 {
				bytes = bytes[:len(bytes)-1] // drop last \n
			}
			if len(bytes) > 0 && (err == nil || err == bufio.ErrBufferFull) {
				if !isReqInitialized {
					req.SetBody(bytes)
					isReqInitialized = true
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
			return errors.Wrap(err, "error reading from file")
		}
		lineCount++
		if (lineCount == cmd.lineBatchSize || eof) && isReqInitialized {
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
	err := client.Do(req, resp)
	if err != nil {
		return errors.Wrap(err, "http error")
	} else if resp.StatusCode() >= 300 {
		return errors.Errorf("status code not OK: Request:\n%v\nResponse:\n%v\n", req, resp)
	}
	atomic.AddUint64(&count, 1)
	return nil
}
