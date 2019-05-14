package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
	"net/http"
	"os"
	"strconv"
)

import _ "net/http/pprof"

type cmdData struct {
	targetUrl       string
	numWorkers      int
	workerQueueSize int
	template        map[string]interface{}
}

func main() {
	if len(os.Args) < 2 {
		_, _ = fmt.Fprintln(os.Stderr, "Usage: scan <url>")
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
	templateStr := os.Getenv("POSTTO_GET_ITEMS_TEMPLATE")
	cmd.template = make(map[string]interface{})
	if templateStr != "" {
		err = json.Unmarshal([]byte(templateStr), &cmd.template)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	cmd.template["TotalSegment"] = cmd.numWorkers
	if workerQueueSizeStr != "" {
		cmd.workerQueueSize, err = strconv.Atoi(workerQueueSizeStr)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	go func() {
		_, _ = fmt.Fprintln(os.Stderr, http.ListenAndServe("localhost:6060", nil))
	}()
	err = do(cmd)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}
}

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
	terminationChannel := make(chan error, cmd.numWorkers)
	for i := 0; i < cmd.numWorkers; i++ {
		workerChannels[i] = make(chan []byte, cmd.workerQueueSize)
		template := make(map[string]interface{}, len(cmd.template)+1)
		for k, v := range cmd.template {
			template[k] = v
		}
		template["Segment"] = i
		go worker(cmd, template, workerChannels[i], terminationChannel)
	}

	err := <-terminationChannel
	return err
}

type Response struct {
	LastItemIncluded string
	NumItems         int
	NextMarker       string
	Items            []*json.RawMessage
}

func worker(cmd cmdData, template map[string]interface{}, ch chan<- []byte, termination chan<- error) {
	var response Response
	for {
		templateStr, err := json.Marshal(template)
		if err != nil {
			termination <- err
			return
		}
		err = getitems(cmd, templateStr, &response)
		if err != nil {
			termination <- err
			return
		}

		for _, item := range response.Items {
			line, err := json.Marshal(item)
			if err != nil {
				termination <- err
				return
			}
			//ch <- line
			fmt.Println(string(line))
		}
		if response.LastItemIncluded == "TRUE" {
			break
		}
		template["Marker"] = response.NextMarker
	}
	//close(ch)
}

func getitems(cmd cmdData, body []byte, response *Response) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(cmd.targetUrl)
	req.Header.SetMethod("PUT")
	req.Header.Set("Authorization", authorization)
	req.Header.Set("X-v3io-function", "GetItems")
	req.SetBody(body)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	err := fasthttp.Do(req, resp)
	if err != nil {
		return errors.Wrap(err, "http error")
	} else if resp.StatusCode() >= 300 {
		return errors.Errorf("status code not OK: Request:\n%v\nResponse:\n%v\n", req, resp)
	}
	//var response Response
	respBody := resp.Body()
	err = json.Unmarshal(respBody, response)
	if err != nil {
		return errors.Wrapf(err, "Failed to unmarshal json: %s", string(respBody))
	}
	return nil
}
