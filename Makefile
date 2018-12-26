all: build run

install:
	go get golang.org/x/time/rate
	go get github.com/prometheus/client_golang/prometheus
	go get github.com/prometheus/client_golang/prometheus/promauto
	go get github.com/prometheus/client_golang/prometheus/promhttp

build:
	go build

run:
	./regulator -c server &\
		./regulator -c proxy &\
		./regulator -c queue &\
		./regulator -c client &\
		./regulator -c producer &
		echo

stop:
	./regulator -c killall

