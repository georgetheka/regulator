package queue

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"regulator/common"
	"strconv"
	"sync"
	"time"
)

const (
	//endpoints
	enqueueEndpoint           = common.QueueEndpointEnqueue
	dequeueEndpoint           = common.QueueEndpointDequeue
	prometheusMetricsEndpoint = common.MetricsEndpoint
	//ports
	queuePort = common.QueuePort
)

var (
	//default response
	response *common.SimpleResponseBody
	//queue implemented as a simple list
	queue *list.List
	//metrics
	metricTotalEnqueued prometheus.Counter
	metricTotalDequeued prometheus.Counter
	metricQueueSize     prometheus.Gauge
	//mutex used to synchronize enqueueing items
	//while producer is the primary enqueuer
	//the client will ocassionally enqueue items for retry
	mutex *sync.Mutex
)

func init() {
	response = &common.SimpleResponseBody{Status: "ok"}

	mutex = &sync.Mutex{}
	queue = list.New()

	metricTotalEnqueued = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "queue",
		Name:      "total_enqueued",
		Help:      "Total enqueued"})

	metricTotalDequeued = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "queue",
		Name:      "total_dequeued",
		Help:      "Total dequeued"})

	metricQueueSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "queue",
		Name:      "queue_size",
		Help:      "Queue size"})

	prometheus.MustRegister(
		metricTotalEnqueued,
		metricTotalDequeued,
		metricQueueSize)
}

func reportQueueMetrics() {
	go func() {
		for {
			time.Sleep(2 * time.Second)
			metricQueueSize.Set(float64(queue.Len()))
		}
	}()
}

func enqueueEndpointHandler(w http.ResponseWriter, r *http.Request) {
	if v, ok := r.URL.Query()["id"]; ok {
		id := v[0]

		//synchronize
		mutex.Lock()
		queue.PushFront(id)
		mutex.Unlock()

		metricTotalEnqueued.Inc()
	}

	common.SetJsonContentType(w)
	bytes, err := json.Marshal(response)
	common.Checkerr("failed to serialize payload", err)
	w.Write(bytes)
}

func dequeueEndpointHandler(w http.ResponseWriter, r *http.Request) {
	limit := 1

	if v, ok := r.URL.Query()["limit"]; ok && len(v) > 0 {
		value, err := strconv.Atoi(v[0])
		common.Checkerr("failed to convert to int", err)
		limit = value
	}

	if limit > queue.Len() {
		limit = queue.Len()
	}

	events := make([]string, limit)
	for i := 0; i < limit; i++ {
		e := queue.Back()
		events[i] = string(e.Value.(string))
		queue.Remove(e)
		metricTotalDequeued.Inc()
	}

	common.SetJsonContentType(w)
	bytes, err := json.Marshal(events)
	common.Checkerr("failed to serialize payload", err)
	w.Write(bytes)
}

// Run runs the queue server
func Run() {
	mux := http.NewServeMux()

	mux.HandleFunc(common.KillEndpoint, common.KillEndpointHandler("queue"))

	mux.HandleFunc(enqueueEndpoint, enqueueEndpointHandler)
	mux.HandleFunc(dequeueEndpoint, dequeueEndpointHandler)
	mux.Handle(prometheusMetricsEndpoint, promhttp.Handler())

	log.Println("QUEUE::INFO: starting server")

	reportQueueMetrics()

	err := http.ListenAndServe(fmt.Sprintf(":%d", queuePort), mux)
	common.Checkerr("failed to start server", err)
}
