package client

import (
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"log"
	"net/http"
	"regulator/common"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//batch of producer events
type events []string

//represents a single unit of work for processing one event
type job func() (string, interface{}, error)

//job output container
type result struct {
	data interface{}
	err  error
}

const (
	//adding some delay between polling request
	//to reduce CPU polling load
	delayBetweenQueueRequestsMillis = 1
	delayBetweenRateChecksSeconds   = 10
	//endpoints
	prometheusMetricsEndpoint = common.MetricsEndpoint
	//some initial assumptions about current performance
	//and throughput needed until server telemetry is available
	defaultInitialRequestTimeSeconds = 0.1
	initialPoolSize                  = 1
	//threshold trigger for when server starts
	//responding with 429-too-many-requests status code
	tooManyRequestsThreshold = 100
	//ports
	proxyPort  = common.ProxyPort
	queuePort  = common.QueuePort
	clientPort = common.ClientPort
)

var (
	//metrics that this component broadcasts
	metricDeadletterQueueCount prometheus.Counter
	metricEventsPulledCount    prometheus.Counter
	metricEventsProcessedCount prometheus.Counter
	metricEventProcessTime     prometheus.Gauge
	metricPoolSize             prometheus.Gauge
	//measured producer rate
	productionRatePerSecond float64
	//urls
	dequeueURL = common.NewURL(queuePort, common.QueueEndpointDequeue)
	proxyURL   = common.NewURL(proxyPort, common.ProxyEndpointRequest)

	isRunning = false
	poolSize  = initialPoolSize

	rateLimit      = make(chan bool, 1000)
	rateLimitCount uint64

	done    = make(chan bool)
	wait    sync.WaitGroup
	jobs    chan job
	results chan result
)

func init() {
	metricDeadletterQueueCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "client",
		Name:      "deadletter_queue_count",
		Help:      "Dead-letter queue count"})

	metricEventsPulledCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "client",
		Name:      "events_pulled_count",
		Help:      "Events pulled count"})

	metricEventsProcessedCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "client",
		Name:      "events_processed_count",
		Help:      "Events processed count"})

	metricEventProcessTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "client",
		Name:      "event_process_time",
		Help:      "Event process time"})

	metricPoolSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "client",
		Name:      "pool_size",
		Help:      "Pool Size"})

	prometheus.MustRegister(
		metricDeadletterQueueCount,
		metricEventsPulledCount,
		metricEventsProcessedCount,
		metricEventProcessTime,
		metricPoolSize)
}

func getQueueEvents(limit int) events {
	_, bytes, err := common.HttpGet(fmt.Sprintf("%s?limit=%d", dequeueURL, limit))
	common.Checkerr("failed to dequeue", err)

	var events events
	err = json.Unmarshal(bytes, &events)
	common.Checkerr("failed to serialize events", err)

	return events
}

func newJob(id string) job {
	url := proxyURL + "?id=" + id
	return func() (string, interface{}, error) {
		start := time.Now()
		statusCode, bytes, err := common.HttpGet(url)
		elapsed := float64(time.Now().Sub(start)) / float64(time.Second)
		metricEventProcessTime.Set(elapsed)

		if statusCode != 200 {
			return id, nil, fmt.Errorf("%d - %s", statusCode, string(bytes))
		}

		if err != nil {
			return id, nil, err
		}
		return id, fmt.Sprintf("%s=%s", id, string(bytes)), nil
	}
}

func initQueueConsumer() {
	for isRunning {
		events := getQueueEvents(1)
		if len(events) > 0 {
			firstEvent := events[0]
			jobs <- newJob(firstEvent)
			metricEventsPulledCount.Inc()
		}
		time.Sleep(delayBetweenQueueRequestsMillis * time.Millisecond)
	}
	close(jobs)
}

func initResultsConsumer() {
	for r := range results {
		if r.err != nil {
			metricDeadletterQueueCount.Inc()
		}
		metricEventsProcessedCount.Inc()
	}
	done <- true
}

func process() {
	for j := range jobs {
		id, data, err := j()
		if err != nil && strings.HasPrefix(string(err.Error()), "429") {
			rateLimit <- true
			//re-enqueue this job for retrying
			go func() {
				url := common.NewURL(queuePort, common.QueueEndpointEnqueue+"?id="+id)
				_, _, err := common.HttpGet(url)
				common.Checkerr("failed to re-enqueue item", err)
			}()
			//we are not reporting 429 as a failure since
			//it's being requeued for a retry
			results <- result{data, nil}
		} else {
			results <- result{data, err}
		}
	}
	wait.Done()
}

func initPool(poolSize int) {
	for i := 0; i < poolSize; i++ {
		wait.Add(1)
		go process()
	}
	wait.Wait()
	close(results)
}

func reportPoolSize() {
	go func() {
		for {
			time.Sleep(5 * time.Second)
			metricPoolSize.Set(float64(poolSize))
		}
	}()
}

func runClient(poolSize int) {
	go func() {
		buffSize := 10 * poolSize

		isRunning = true
		jobs = make(chan job, buffSize)
		results = make(chan result, buffSize)

		go initQueueConsumer()
		go initResultsConsumer()

		log.Println("CLIENT::INFO: starting pool with size =", poolSize)
		initPool(poolSize)
		reportPoolSize()
	}()
}

func throughputController() {
	go func() {
		for {
			//adjust client throughput every T seconds
			time.Sleep(delayBetweenRateChecksSeconds * time.Second)
			//read meric
			metric, err := common.ReadMetric(common.ProducerPort, common.MetricProducerProdRatePerSecond)
			if err != nil {
				log.Println("CLIENT::ERROR: failed to read metric", err)
				continue
			}
			v, err := strconv.ParseFloat(metric, 64)
			common.Checkerr("failed to convert metric to int", err)
			newRate := v

			//read request time from metric
			pb := &dto.Metric{}
			metricEventProcessTime.Write(pb)
			requestTimeSeconds := *pb.GetGauge().Value

			//calculate new pool size
			//little's law implemented
			newPoolSize := int(newRate * requestTimeSeconds)
			if newPoolSize == 0 {
				newPoolSize = 1 // set pool size to at least one
			}

			log.Printf("CLIENT::INFO: new_prod_rate = %f new_req_time = %f, new_pool_size = %d", newRate, requestTimeSeconds, newPoolSize)
			if newPoolSize != poolSize {
				poolSize = newPoolSize
				log.Println("CLIENT::INFO: adjusting pool size...")
				if isRunning {
					isRunning = false // signal to complete and close current pool
					<-done            // wait for completion
				}
				go runClient(poolSize) //run with a new pool
			}
		}
	}()
}

func server() {
	mux := http.NewServeMux()
	mux.Handle(prometheusMetricsEndpoint, promhttp.Handler())
	mux.HandleFunc(common.KillEndpoint, common.KillEndpointHandler("client"))

	log.Println("CLIENT::INFO: starting server")
	err := http.ListenAndServe(fmt.Sprintf(":%d", clientPort), mux)
	common.Checkerr("failed to start server", err)
}

func manageRateLimits() {
	go func() {
		for _ = range rateLimit {
			atomic.AddUint64(&rateLimitCount, 1)
		}
	}()

	go func() {
		for {
			time.Sleep(delayBetweenRateChecksSeconds * time.Second)
			c := atomic.LoadUint64(&rateLimitCount)
			if c > tooManyRequestsThreshold {
				log.Println("CLIENT:WARN: TooManyRequests Threshold reached, asking producer to slow down...")
				//reset the count
				atomic.StoreUint64(&rateLimitCount, 0)
				_, _, _ = common.HttpGet(common.NewURL(common.ProducerPort, common.ProducerEndpointSlowDown))
			} else {
				_, _, _ = common.HttpGet(common.NewURL(common.ProducerPort, common.ProducerEndpointOk))
			}
		}
	}()
}

// Run runs the client server
func Run() {
	runClient(poolSize)
	throughputController()
	manageRateLimits()
	server()
}
