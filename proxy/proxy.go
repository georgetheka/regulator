package proxy

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"io/ioutil"
	"log"
	"net/http"
	"regulator/common"
	"time"
)

const (
	//endpoints
	mockRequestEndpoint       = common.RequestEndpoint
	prometheusMetricsEndpoint = common.MetricsEndpoint
	//ports
	proxyPort  = common.ProxyPort
	serverPort = common.ServerPort
)

var (
	// params
	previousTotalRequests float64
	//metrics
	metricTotalRequests       prometheus.Counter
	metricTotalFailedRequests prometheus.Counter
	metricRequestsPerSecond   prometheus.Gauge
	requestURL                = common.NewURL(serverPort, common.RequestEndpoint)
)

func readPromCounter(m prometheus.Counter) float64 {
	pb := &dto.Metric{}
	m.Write(pb)
	return pb.GetCounter().GetValue()
}

func reportRateMetrics() {
	go func() {
		for {
			time.Sleep(5 * time.Second)
			previousTotalRequests = readPromCounter(metricTotalRequests)
			rate := readPromCounter(metricTotalRequests) - previousTotalRequests
			metricRequestsPerSecond.Set(rate)
		}
	}()
}

func init() {
	metricTotalRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "proxy",
		Name:      "total_requests",
		Help:      "Total Requests"})

	metricTotalFailedRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "proxy",
		Name:      "total_failed_requests",
		Help:      "Total failed requests"})

	metricRequestsPerSecond = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "proxy",
		Name:      "requests_per_second",
		Help:      "Requests per second"})

	prometheus.MustRegister(
		metricTotalRequests,
		metricTotalFailedRequests,
		metricRequestsPerSecond)
}

//make a generic request to the server
//in order to emulate proxy behavior
func proxyRequest() ([]byte, int, error) {
	resp, err := http.Get(requestURL)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

func mockRequestEndpointHandler(w http.ResponseWriter, r *http.Request) {
	bytes, statusCode, err := proxyRequest()
	metricTotalRequests.Inc()
	w.WriteHeader(statusCode)
	if err != nil {
		//measure failed requests
		metricTotalFailedRequests.Inc()
		w.Write([]byte(err.Error()))
	} else if statusCode == 500 {
		//measure failed requests
		metricTotalFailedRequests.Inc()
		w.Write(bytes)
	} else {
		common.SetJsonContentType(w)
		w.Write(bytes)
	}
}

// Run runs the proxy server
func Run() {
	mux := http.NewServeMux()
	mux.HandleFunc(common.KillEndpoint, common.KillEndpointHandler("proxy"))
	mux.HandleFunc(mockRequestEndpoint, mockRequestEndpointHandler)
	mux.Handle(prometheusMetricsEndpoint, promhttp.Handler())

	log.Println("PROXY::INFO: starting server")

	reportRateMetrics()

	err := http.ListenAndServe(fmt.Sprintf(":%d", proxyPort), mux)
	common.Checkerr("failed to start server", err)
}
