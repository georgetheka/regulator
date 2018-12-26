package producer

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"math/rand"
	"net/http"
	"regulator/common"
	"strconv"
	"time"
)

const (
	//desired throughput
	maxProductionRatePerSecond = 100.0
	//minimum allowed throughput
	minProductionRatePerSecond = 2.0
	//maximum number of failures allowed
	maxNumberOfFailures = 10000.0

	//endpoints
	enqueueEndpoint           = common.QueueEndpointEnqueue
	okEndpoint                = common.ProducerEndpointOk
	slowdownEndpoint          = common.ProducerEndpointSlowDown
	prometheusMetricsEndpoint = common.MetricsEndpoint

	//ports
	producerPort         = common.ProducerPort
	queuePort            = common.QueuePort
	clientPort           = common.ClientPort
	deadletterQueueCount = common.MetricClientDeadletterQueueCount
)

var (
	//enqueue url
	enqueueEndpointURL = common.NewURL(queuePort, enqueueEndpoint) + "?id="

	//production parameters
	productionRatePerSecond float64
	currentProdRateCeiling  float64

	//metrics
	metricDesiredProductionRate prometheus.Gauge
	metricProductionRate        prometheus.Gauge
	metricTotalProduced         prometheus.Counter
)

func init() {
	currentProdRateCeiling = maxProductionRatePerSecond
	productionRatePerSecond = maxProductionRatePerSecond

	metricDesiredProductionRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "producer",
		Name:      "desired_prod_rate_second",
		Help:      "Desired production rate per second"})

	metricProductionRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "producer",
		Name:      "prod_rate_second",
		Help:      "Production rate per second"})

	metricTotalProduced = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "producer",
		Name:      "total_produced",
		Help:      "Total produced"})

	prometheus.MustRegister(
		metricTotalProduced,
		metricProductionRate,
		metricDesiredProductionRate)
}

func reportRateMetrics() {
	go func() {
		for {
			time.Sleep(2 * time.Second)
			metricProductionRate.Set(productionRatePerSecond)
			metricDesiredProductionRate.Set(maxProductionRatePerSecond)
		}
	}()
}

func listenForTooManyFailures() {
	go func() {
		for {
			time.Sleep(10 * time.Second)
			val, err := common.ReadMetric(clientPort, deadletterQueueCount)
			common.Checkerr("failed to read dl queue metric", err)

			metric, err := strconv.ParseFloat(val, 64)
			common.Checkerr("failed to parse metric to float64", err)

			if metric >= maxNumberOfFailures {
				log.Println("PRODUCER::ALERT: service health has reached minimum threshold, number of failures =", metric)
			} else {
				log.Println("PRODUCER::INFO: number of failures =", metric)
			}
		}
	}()
}

func raiseCeiling() {
	go func() {
		for {
			//producer will attempt to raise ceiling back to the desired value
			time.Sleep(time.Minute)
			if currentProdRateCeiling < maxProductionRatePerSecond {
				log.Println("PRODUCER::INFO: attempting to raise ceiling to approach max rate, current rate ceiling =", currentProdRateCeiling)
				//raise by 50%
				currentProdRateCeiling += (maxProductionRatePerSecond - currentProdRateCeiling) / 2
				log.Println("PRODUCER::INFO: new rate ceiling =", currentProdRateCeiling)
			}
		}
	}()
}

func produce() {
	go func() {
		for {
			time.Sleep(time.Duration(1000/productionRatePerSecond) * time.Millisecond)
			url := fmt.Sprintf("%s%d", enqueueEndpointURL, rand.Int63())
			resp, err := http.Get(url)
			common.Checkerr("failed to make request to queue", err)
			resp.Body.Close()
			metricTotalProduced.Inc()
		}
	}()
}

func setRate(rate float64) {
	if rate < productionRatePerSecond {
		ceiling := currentProdRateCeiling
		currentProdRateCeiling = rate
		log.Println("PRODUCER::INFO: lowered rate ceiling from ", ceiling, " to ", currentProdRateCeiling)
	}

	if rate > currentProdRateCeiling {
		log.Println("PRODUCER::INFO: production rate is optimal for ceiling = ", currentProdRateCeiling)
		productionRatePerSecond = currentProdRateCeiling
	} else if rate < minProductionRatePerSecond {
		log.Println("PRODUCER::ALERT: production rate is too slow")
		productionRatePerSecond = rate
	} else {
		productionRatePerSecond = rate
	}
	log.Println("PRODUCER::INFO: rate =", productionRatePerSecond)
}

// Run runs the producer server
func Run() {
	mux := http.NewServeMux()
	mux.HandleFunc(common.KillEndpoint, common.KillEndpointHandler("producer"))

	mux.HandleFunc(okEndpoint, common.EndpointHandler(func() {
		//raise rate by 25%
		rate := productionRatePerSecond + (currentProdRateCeiling-productionRatePerSecond)*0.25
		setRate(rate)
	}))

	mux.HandleFunc(slowdownEndpoint, common.EndpointHandler(func() {
		//lower rate by 25%
		rate := currentProdRateCeiling - (currentProdRateCeiling-minProductionRatePerSecond)*0.25
		setRate(rate)
	}))

	mux.Handle(prometheusMetricsEndpoint, promhttp.Handler())

	log.Println("PRODUCER::INFO: starting server")

	produce()
	listenForTooManyFailures()
	raiseCeiling()
	reportRateMetrics()

	err := http.ListenAndServe(fmt.Sprintf(":%d", producerPort), mux)
	common.Checkerr("failed to start server", err)
}
