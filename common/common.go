package common

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

const (
	ServerPort   = 7777
	ProxyPort    = 7776
	ClientPort   = 7775
	ProducerPort = 7774
	QueuePort    = 7773

	MetricsEndpoint = "/metrics"
	RequestEndpoint = "/request"
	KillEndpoint    = "/kill"

	MetricProducerProdRatePerSecond  = "producer_prod_rate_second"
	MetricClientDeadletterQueueCount = "client_deadletter_queue_count"

	ProducerEndpointOk       = "/feedback/ok"
	ProducerEndpointSlowDown = "/feedback/slowdown"

	QueueEndpointEnqueue = "/enqueue"
	QueueEndpointDequeue = "/dequeue"

	ProxyEndpointRequest = "/request"
)

// SimpleResponseBody is a common http response payload
type SimpleResponseBody struct {
	Status string `json:"status"`
}

func SetJsonContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}

// NewUrl builds a url for a given port and endpoint
// make sure that endpoint starts with /
func NewURL(port int, endpoint string) string {
	return fmt.Sprintf("http://localhost:%d%s", port, endpoint)
}

// Checkerr checks an error
func Checkerr(m string, err error) {
	if err != nil {
		log.Fatal(m, err)
	}
}

// EndpointHandler provides a generic simple endpoint handler
func EndpointHandler(handle func()) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		handle()
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("ACK"))
	}
}

// KillEndpointHandler kills the process
func KillEndpointHandler(name string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Println("kill signal received for " + name)
		os.Exit(1)
	}
}

// WriteJsonResponse writes response
func WriteJsonResponse(payload interface{}, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	payloadBytes, err := json.Marshal(payload)
	Checkerr("failed to serialize payload", err)
	w.Write(payloadBytes)
}

//HttpGet makes a simple http get request
func HttpGet(url string) (int, []byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

//ReadMetric reads a specific metric from an endpoint for host with specifiied port
func ReadMetric(port int, metric string) (string, error) {
	msg := fmt.Sprintf("port=%d metric=%s", port, metric)
	statusCode, bytes, err := HttpGet(fmt.Sprintf("http://localhost:%d/metrics", port))
	if err != nil {
		return "", err
	}
	if statusCode != 200 {
		return "", errors.New(fmt.Sprintf("unexpected status code %d for %s", statusCode, msg))
	}
	metrics := string(bytes)
	scanner := bufio.NewScanner(strings.NewReader(metrics))
	for scanner.Scan() {
		line := scanner.Text() //for each metrics line
		if strings.HasPrefix(line, metric) {
			return line[len(metric)+1:], nil
		}
	}
	return "", errors.New(fmt.Sprintf("metric not found for %s", msg))
}
