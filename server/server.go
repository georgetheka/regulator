package server

import (
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/time/rate"
	"log"
	"math/rand"
	"net/http"
	"regulator/common"
	"strconv"
	"time"
)

// configuration is a serializable data record for server parameter configuration
type configuration struct {
	//mean response time in millis
	//based on a normal distribution
	MeanRespTimeMillis int `json:"mean_resp_time_millis" onempty:"ignore"`
	//standard deviation for the response time distribution
	RespTimeStdDev int `json:"resp_time_std_dev" onempty:"ignore"`

	//maximum allowed number of requests per second
	RateLimitRequestPerSecond int `json:"rate_limit_req_second" onempty:"ignore"`
	//maximum allowed number of concurrent requests at any time
	RateLimitBurst int `json:"rate_limit_burst" onempty:"ignore"`

	//percentage of requests to fail in a controlled manner
	//used to test error thresholds
	PercentageRequestsToFail int `json:"percentage_reqs_fail" onempty:"ignore"`
}

const (
	//endpoints
	controlEndpoint           = "/control"
	mockRequestEndpoint       = common.RequestEndpoint
	prometheusMetricsEndpoint = common.MetricsEndpoint
	//default variables
	defaultRespTimeMillis = 100
	defaultRateLimit      = 100
	defaultRateLimitBurst = 200
	//ports
	port = common.ServerPort
)

var (
	//server configuration
	conf configuration
	//rate limiter instance
	limiter *rate.Limiter
	//just a standard ok response
	responseBody common.SimpleResponseBody
	//metrics
	metricMeanRespTimeMillis        prometheus.Gauge
	metricRespTimeStdDev            prometheus.Gauge
	metricRateLimitRequestPerSecond prometheus.Gauge
	metricRateLimitBurst            prometheus.Gauge
	metricPercentageRequestsToFail  prometheus.Gauge
)

func reportMetrics() {
	go func() {
		for {
			time.Sleep(5 * time.Second)
			metricMeanRespTimeMillis.Set(float64(conf.MeanRespTimeMillis))
			metricRespTimeStdDev.Set(float64(conf.RespTimeStdDev))
			metricRateLimitRequestPerSecond.Set(float64(conf.RateLimitRequestPerSecond))
			metricRateLimitBurst.Set(float64(conf.RateLimitBurst))
			metricPercentageRequestsToFail.Set(float64(conf.PercentageRequestsToFail))
		}
	}()
}

func init() {
	responseBody = common.SimpleResponseBody{Status: "ok"}

	conf = configuration{
		MeanRespTimeMillis:        defaultRespTimeMillis,
		RespTimeStdDev:            0,
		RateLimitRequestPerSecond: defaultRateLimit,
		RateLimitBurst:            defaultRateLimitBurst,
		PercentageRequestsToFail:  0}

	limiter = rate.NewLimiter(rate.Limit(conf.RateLimitRequestPerSecond), conf.RateLimitBurst)

	metricMeanRespTimeMillis = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "server",
		Name:      "mean_resp_time_millis",
		Help:      "Mean response time in millis"})

	metricRespTimeStdDev = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "server",
		Name:      "resp_time_stddev",
		Help:      "Response time standard deviation"})

	metricRateLimitRequestPerSecond = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "server",
		Name:      "rate_limit_req_second",
		Help:      "Rate limit request per second"})

	metricRateLimitRequestPerSecond = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "server",
		Name:      "rate_limit_req_second",
		Help:      "Rate limit request per second"})

	metricRateLimitBurst = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "server",
		Name:      "rate_limit_burst",
		Help:      "Rate limit burst"})

	metricPercentageRequestsToFail = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "server",
		Name:      "percentage_reqs_fail",
		Help:      "Percentage of requests to fail"})

	prometheus.MustRegister(
		metricMeanRespTimeMillis,
		metricRespTimeStdDev,
		metricRateLimitRequestPerSecond,
		metricRateLimitBurst,
		metricPercentageRequestsToFail)
}

func rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !limiter.Allow() {
			//respond with 429 when limit has been reached
			http.Error(w, http.StatusText(429), http.StatusTooManyRequests)
		} else {
			next.ServeHTTP(w, r)
		}
	})
}

func setRateLimit() {
	//update the limiter
	var rl rate.Limit
	if conf.RateLimitRequestPerSecond == 0 {
		rl = defaultRateLimit
	} else {
		rl = rate.Limit(conf.RateLimitRequestPerSecond)
	}

	var rb int
	if conf.RateLimitBurst == 0 {
		rb = defaultRateLimitBurst
	} else {
		rb = conf.RateLimitBurst
	}
	limiter = rate.NewLimiter(rl, rb)

}

//a hacky and non-idempotent endpoint solution that allows
//simple tweaking server parameters via query string parameters
func controlEndpointHandler(w http.ResponseWriter, r *http.Request) {
	//convert query string parameters to a simple map<string,int>
	params := make(map[string]int)
	for k, v := range r.URL.Query() {
		if len(v) > 0 {
			i, err := strconv.Atoi(v[0])
			common.Checkerr("failed to convert value to int", err)
			params[k] = i
		}
	}

	//query string map is serialized to JSON
	//which is then deserialized to a specific type, configuration
	if len(params) > 0 {
		bytes, err := json.Marshal(params)
		common.Checkerr("failed to serialize query params to json", err)

		err = json.Unmarshal(bytes, &conf)
		common.Checkerr("failed to serialize json to configuration", err)

		setRateLimit()
		reportMetrics()
	}

	common.WriteJsonResponse(&conf, w, r)
}

func mockRequestEndpointHandler(w http.ResponseWriter, r *http.Request) {
	if conf.MeanRespTimeMillis > 0 {
		//normally distributed duration based on configured standard deviation and mean
		duration := int(rand.NormFloat64()*float64(conf.RespTimeStdDev) + float64(conf.MeanRespTimeMillis))
		time.Sleep(time.Duration(duration) * time.Millisecond)
	}

	//fail
	if conf.PercentageRequestsToFail > 0 && rand.Intn(100) <= conf.PercentageRequestsToFail {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Controlled Failure Response"))
		//success
	} else {
		common.WriteJsonResponse(&responseBody, w, r)
	}
}

// Run runs web server
func Run() {
	mux := http.NewServeMux()
	mux.HandleFunc(controlEndpoint, controlEndpointHandler)
	mux.HandleFunc(mockRequestEndpoint, mockRequestEndpointHandler)
	mux.HandleFunc(common.KillEndpoint, common.KillEndpointHandler("server"))

	mux.HandleFunc("/slowdown", common.EndpointHandler(func() {
		conf.MeanRespTimeMillis *= 2.0
		log.Println("SERVER::INFO: response_time_millis =", conf.MeanRespTimeMillis)
	}))

	mux.HandleFunc("/speedup", common.EndpointHandler(func() {
		conf.MeanRespTimeMillis /= 2.0
		log.Println("SERVER::INFO: response_time_millis =", conf.MeanRespTimeMillis)
	}))

	mux.HandleFunc("/reduceratelimit", common.EndpointHandler(func() {
		conf.RateLimitRequestPerSecond /= 2.0
		conf.RateLimitBurst = 2.0 * conf.RateLimitRequestPerSecond
		setRateLimit()
		reportMetrics()
		log.Println("SERVER::INFO: rate_limit_reqs_second =", conf.RateLimitRequestPerSecond)
		log.Println("SERVER::INFO: burst_rate =", conf.RateLimitBurst)
	}))

	mux.HandleFunc("/increaseratelimit", common.EndpointHandler(func() {
		conf.RateLimitRequestPerSecond *= 2.0
		conf.RateLimitBurst = 2.0 * conf.RateLimitRequestPerSecond
		setRateLimit()
		reportMetrics()
		log.Println("SERVER::INFO: rate_limit_reqs_second =", conf.RateLimitRequestPerSecond)
		log.Println("SERVER::INFO: burst_rate =", conf.RateLimitBurst)
	}))

	mux.Handle(prometheusMetricsEndpoint, promhttp.Handler())
	handler := rateLimitMiddleware(mux)

	log.Println("SERVER::INFO: starting server")

	reportMetrics()

	err := http.ListenAndServe(fmt.Sprintf(":%d", port), handler)
	common.Checkerr("failed to start server", err)
}
